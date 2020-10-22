using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;
using Nemo.Attributes;
using Nemo.Data;
using Nemo.Extensions;
using Nemo.Reflection;
using Nemo.Serialization;
using System.Collections.Concurrent;

namespace Nemo.UnitOfWork
{
    public static class ObjectScopeExtensions
    {
        private static readonly ConcurrentDictionary<Type, RuntimeMethodHandle> CommitMethods = new ConcurrentDictionary<Type, RuntimeMethodHandle>();
        private static readonly ConcurrentDictionary<Type, RuntimeMethodHandle> RollbackMethods = new ConcurrentDictionary<Type, RuntimeMethodHandle>();

        public static bool Commit<T>(this T dataEntity)
            where T : class
        {
            var success = true;
            var context = ObjectScope.Current;

            if (context != null)
            {
                DbTransaction transaction = null;
                if (context.ChangeTracking == ChangeTrackingMode.Automatic)
                {
                    var connection = context.Connection ?? DbFactory.CreateConnection(null, typeof(T));
                    var externalConnection = context.Connection != null;
                    var openConnectionRequired = !externalConnection || context.Connection.State != ConnectionState.Open;
                    try
                    {
                        if (openConnectionRequired) connection.Open();
                        if (context.Transaction == null)
                        {
                            transaction = connection.BeginTransaction();
                        }

                        var changes = CompareObjects(dataEntity, dataEntity.Old());
                        var statement = GetCommitStatement(changes, connection);
                        if (!string.IsNullOrEmpty(statement.Item1))
                        {
                            var response =
                                ObjectFactory.Execute<T>(new OperationRequest
                                {
                                    Operation = statement.Item1,
                                    OperationType = OperationType.Sql,
                                    Parameters = statement.Item2,
                                    Connection = connection,
                                    ReturnType = OperationReturnType.SingleResult,
                                    Transaction = transaction
                                });
                            success = response.Value != null;
                            if (success)
                            {
                                SetGeneratedPropertyValues(statement.Item3, (IDataReader)response.Value);
                            }
                        }

                        transaction?.Commit();
                    }
                    catch (Exception ex)
                    {
                        transaction?.Rollback();
                        throw;
                    }
                    finally
                    {
                        if (!externalConnection)
                        {
                            connection.Dispose();
                        }
                    }
                }
                else if (context.ChangeTracking == ChangeTrackingMode.Debug)
                {
                    var connection = context.Connection ?? DbFactory.CreateConnection(null, typeof(T));
                    var externalConnection = context.Connection != null;
                    var openConnectionRequired = !externalConnection || context.Connection.State != ConnectionState.Open;
                    try
                    {
                        if (openConnectionRequired) connection.Open();
                        if (context.Transaction == null)
                        {
                            transaction = connection.BeginTransaction();
                        }
                        var changes = CompareObjects(dataEntity, dataEntity.Old());
                        var statement = GetCommitStatement(changes, connection);
                        Console.WriteLine(statement.Item1);

                        transaction?.Commit();
                    }
                    catch (Exception ex)
                    {
                        transaction?.Rollback();
                        throw;
                    }
                    finally
                    {
                        if (!externalConnection)
                        {
                            connection.Dispose();
                        }
                    }
                }

                if (context.IsNested)
                {
                    success = context.UpdateOuterSnapshot(dataEntity);
                }

                if (success)
                {
                    context.Cleanup();

                    context.Transaction?.Complete();
                }
            }

            return success;
        }

        internal static bool Commit(this object dataEntity, Type objectType)
        {
            var methodHandle = CommitMethods.GetOrAdd(objectType, type =>
            {
                var commitMethod = typeof(ObjectScopeExtensions).GetMethods(BindingFlags.Static | BindingFlags.Public).FirstOrDefault(m => m.Name == "Commit");
                var genericCommitMethod = commitMethod.MakeGenericMethod(type);
                return genericCommitMethod.MethodHandle;
            });
            var invoker = Reflector.Method.CreateDelegate(methodHandle);
            return (bool)invoker(null, new[] { dataEntity });
        }

        public static bool Rollback<T>(this T dataEntity)
            where T : class
        {
            dataEntity.ThrowIfNull("dataEntity");

            var context = ObjectScope.Current;
            if (context == null) return false;

            var oldObject = dataEntity.Old();
            ObjectFactory.Map(oldObject, dataEntity, true);
            context.Cleanup();
            return true;
        }

        internal static bool Rollback(this object dataEntity, Type objectType)
        {
            var methodHandle = RollbackMethods.GetOrAdd(objectType, type =>
            {
                var rollbackMethod = typeof(ObjectScopeExtensions).GetMethods(BindingFlags.Static | BindingFlags.Public).FirstOrDefault(m => m.Name == "Rollback");
                var genericRollbackMethod = rollbackMethod.MakeGenericMethod(type);
                return genericRollbackMethod.MethodHandle;
            });
            var invoker = Reflector.Method.CreateDelegate(methodHandle);
            return (bool)invoker(null, new[] { dataEntity });
        }

        public static T Old<T>(this T dataEntity)
            where T : class
        {
            dataEntity.ThrowIfNull("dataEntity");

            var context = ObjectScope.Current;
            if (context == null) return default(T);

            if (context.OriginalItem != null)
            {
                return context.OriginalItem as T;
            }

            var result = context.ItemSnapshot.Deserialize<T>();
            context.OriginalItem = result;
            return result;
        }

        #region Cascade Methods

        public static void Cascade<T>(this T dataEntity, string propertyName, object propertyValue)
            where T : class
        {
            Cascade((object)dataEntity, propertyName, propertyValue);
        }

        private static void Cascade(object dataEntity, string propertyName, object propertyValue)
        {
            if (dataEntity == null) return;

            var objectType = dataEntity.GetType();
            var propertyMap = Reflector.GetPropertyMap(objectType);

            foreach (var property in propertyMap.Values)
            {
                if (property.IsDataEntityList)
                {
                    var childCollection = (IEnumerable)dataEntity.Property(property.PropertyName);
                    if (childCollection != null)
                    {
                        foreach (var item in childCollection)
                        {
                            CascadeToChild(dataEntity, item, propertyName, propertyValue);
                        }
                    }
                }
                else if (property.IsDataEntity)
                {
                    var childObject = dataEntity.Property(property.PropertyName);
                    CascadeToChild(dataEntity, childObject, propertyName, propertyValue);
                }
            }
        }

        private static void CascadeToChild(object parentObject, object childObject, string propertyName, object propertyValue)
        {
            if (childObject != null)
            {
                var childProperty = Reflector.GetProperty(childObject.GetType(), propertyName);
                if (childProperty != null)
                {
                    childObject.Property(propertyName, propertyValue);
                }
                Cascade(childObject, propertyName, propertyValue);
            }
        }

        private static void SetGeneratedPropertyValues(IReadOnlyList<object> dataEntities, IDataReader generatedValues)
        {
            var map = generatedValues.AsEnumerable().Select(r => new
            {
                StatementId = r.GetInt32(r.GetOrdinal("StatementId")),
                GeneratedId = r.GetValue(r.GetOrdinal("GeneratedId")),
                ParameterName = r.GetString(r.GetOrdinal("ParameterName")),
                PropertyName = r.GetString(r.GetOrdinal("PropertyName"))
            }).ToDictionary(r => r.StatementId, r => Tuple.Create(r.GeneratedId, r.ParameterName, r.PropertyName));

            for (var i = 0; i < dataEntities.Count; i++)
            {
                if (!map.TryGetValue(i, out var value)) continue;
                dataEntities[i].Property(value.Item3, value.Item1);
                dataEntities[i].Cascade(value.Item3, value.Item1);
            }
        }

        #endregion

        #region Change Detection Methods

        private static ChangeNode CompareObjects(object currentObject, object oldObject, ChangeNode parentNode = null)
        {
            if (currentObject == null && oldObject == null)
            {
                throw new ArgumentException("currentObject and oldObject cannot be null at the same time");
            }

            var rootNode = new ChangeNode { Value = currentObject };
            if (parentNode != null)
            {
                rootNode.Parent = parentNode;
            }

            var context = ObjectScope.Current;
            if (context == null) return null;

            // Get properties and build a property map
            var type = (currentObject ?? oldObject).GetType();

            var primaryKey = (currentObject ?? oldObject).GetPrimaryKey();

            var propertyMap = Reflector.GetPropertyMap(type);

            foreach (var property in propertyMap.Values)
            {
                object currentValue = null;
                if (currentObject != null && currentObject.PropertyExists(property.PropertyName))
                {
                    currentValue = currentObject.Property(property.PropertyName);
                }

                object oldValue = null;
                if (oldObject != null && oldObject.PropertyExists(property.PropertyName))
                {
                    oldValue = oldObject.Property(property.PropertyName);
                }

                if (currentValue == null && oldValue == null) continue;

                var changeNode = new ChangeNode { ObjectState = ObjectState.Clean };

                var objectType = property.PropertyType;
                changeNode.Type = objectType;
                changeNode.Property = property;

                if (!context.IsNew && rootNode.ObjectState != ObjectState.DirtyPrimaryKey && (property.IsSimpleType || property.IsSimpleList || property.IsBinary))
                {
                    if (property.IsSimpleList)
                    {
                        currentValue = ((IEnumerable)currentValue)?.Cast<object>().ToArray();

                        oldValue = ((IEnumerable)oldValue)?.Cast<object>().ToArray();
                    }

                    if (currentObject != null && oldObject != null)
                    {
                        var same = property.IsSimpleList
                            ? ((IEnumerable<object>)currentValue).SequenceEqual((IEnumerable<object>)oldValue)
                            : property.IsBinary
                                ? ((byte[])currentValue).SequenceEqual((byte[])oldValue)
                                : Equals(currentValue, oldValue);
                        if (!same)
                        {
                            changeNode.Value = currentValue;
                            changeNode.ObjectState = ObjectState.Dirty;
                        }
                    }
                    else if (currentObject != null && oldObject == null)
                    {
                        changeNode.Value = currentValue;
                        changeNode.ObjectState = ObjectState.New;
                    }
                }
                /*else if (!context.IsNew && Reflector.IsXmlDocument(objectType))
                {
                    if (currentObject != null && oldObject != null && 
                        ((currentValue != null && oldValue != null && ((XmlDocument)currentValue).OuterXml.GetHashCode() != ((XmlDocument)oldValue).OuterXml.GetHashCode())
                        || (currentValue != null && oldValue == null) || (currentValue == null && oldValue != null))) 
                    {
                        changeNode.Value = currentValue;
                        changeNode.ObjectState = ObjectState.Dirty;
                    }
                }*/
                else if (property.IsDataEntityList)
                {
                    var changes = CompareLists((IList)currentValue, (IList)oldValue, changeNode, property.PropertyName);
                    if (changes.Count > 0)
                    {
                        changeNode.Value = currentValue ?? oldValue;
                    }
                    rootNode.ListProperties.Add(property.PropertyName);
                }
                else if (property.IsDataEntity)
                {
                    var changes = CompareObjects(currentValue, oldValue, parentNode);
                    if (changes.Count > 0)
                    {
                        changeNode.Value = currentValue ?? oldValue;
                        changes.Nodes.ForEach(n => changeNode.Nodes.Add(n));
                    }
                    rootNode.ObjectProperties.Add(property.PropertyName);
                }

                if (!changeNode.IsEmpty)
                {
                    changeNode.PropertyName = property.PropertyName;
                    if (changeNode.ObjectState == ObjectState.Clean)
                    {
                        if (context.IsNew)
                        {
                            changeNode.ObjectState = ObjectState.New;
                        }
                        else if (currentValue != null && oldValue != null)
                        {
                            changeNode.ObjectState = ObjectState.Dirty;
                        }
                        else if (currentValue != null)
                        {
                            changeNode.ObjectState = ObjectState.New;
                        }
                        else if (oldValue != null)
                        {
                            changeNode.ObjectState = ObjectState.Deleted;
                        }
                    }

                    // Primary key modifications should translate into insert and delete
                    if (changeNode.ObjectState == ObjectState.Dirty && primaryKey.ContainsKey(property.PropertyName))
                    {
                        rootNode.Nodes.RemoveAll(n => n.ObjectState == ObjectState.Dirty);

                        var insertNode = new ChangeNode { Parent = rootNode, Value = currentObject, ObjectState = ObjectState.New };

                        var deleteNode = new ChangeNode { Parent = rootNode, Value = oldObject, ObjectState = ObjectState.Deleted };

                        rootNode.ObjectState = ObjectState.DirtyPrimaryKey;

                        rootNode.Nodes.Add(insertNode);
                        rootNode.Nodes.Add(deleteNode);
                    }
                    else
                    {
                        changeNode.Parent = rootNode;
                        rootNode.Nodes.Add(changeNode);
                    }
                }
            }

            // Root node can never be deleted!
            if (context.IsNew)
            {
                rootNode.ObjectState = ObjectState.New;
            }
            else if (!rootNode.IsRoot)
            {
                if (rootNode.ObjectState != ObjectState.Dirty)
                {
                    rootNode.ObjectState = ObjectState.Clean;
                }
            }
            else if (currentObject != null && oldObject == null)
            {
                rootNode.ObjectState = ObjectState.New;
            }

            return rootNode;
        }

        private static List<ChangeNode> CompareLists(IList currentList, IList oldList, ChangeNode parentNode, string propertyName)
        {
            var changeList = new List<ChangeNode>();

            if (currentList == null && oldList == null)
            {
                return changeList;
            }

            var objectMapCurrent = new Dictionary<string, object>();
            var objectMapOld = new Dictionary<string, object>();

            if (currentList != null)
            {
                for (var i = 0; i < currentList.Count; i++)
                {
                    objectMapCurrent.Add(currentList[i].ComputeHash(), currentList[i]);
                }
            }

            if (oldList != null)
            {
                for (var i = 0; i < oldList.Count; i++)
                {
                    objectMapOld.Add(oldList[i].ComputeHash(), oldList[i]);
                }
            }

            var modifications = objectMapCurrent.Where(k => objectMapOld.ContainsKey(k.Key));
            var additions = objectMapCurrent.Where(k => !objectMapOld.ContainsKey(k.Key));
            var deletions = objectMapOld.Where(k => !objectMapCurrent.ContainsKey(k.Key));

            foreach (var pair in modifications)
            {
                var changes = CompareObjects(pair.Value, objectMapOld[pair.Key]);

                var changeNode = new ChangeNode { PropertyName = propertyName, Value = pair.Value, Parent = parentNode, Index = changeList.Count };

                if (changes.Count > 0)
                {
                    changeNode.ObjectState = ObjectState.Dirty;
                    changes.Parent = changeNode;
                    changeNode.Nodes.AddRange(changes.Nodes);
                }

                changeList.Add(changeNode);
            }

            foreach (var pair in additions)
            {
                var changes = CompareObjects(pair.Value, null);

                var changeNode = new ChangeNode { ObjectState = ObjectState.New, PropertyName = propertyName, Value = pair.Value, Parent = parentNode, Index = changeList.Count };

                if (changes.Count > 0)
                {
                    changes.Parent = changeNode;
                    changeNode.Nodes.AddRange(changes.Nodes);
                }

                changeList.Add(changeNode);
            }

            foreach (var pair in deletions)
            {
                var changes = CompareObjects(null, pair.Value);

                var changeNode = new ChangeNode { ObjectState = ObjectState.Deleted, PropertyName = propertyName, Value = pair.Value, Parent = parentNode, Index = changeList.Count };

                if (changes.Count > 0)
                {
                    changes.Parent = changeNode;
                    changeNode.Nodes.AddRange(changes.Nodes);
                }

                changeList.Add(changeNode);
            }

            if (changeList.Count > 0)
            {
                parentNode.Nodes.AddRange(changeList);
            }

            return changeList;
        }

        private static List<ChangeNode> GetChanges(ChangeNode rootNode, ObjectState objectState)
        {
            var result = new List<ChangeNode>();
            switch (objectState)
            {
                case ObjectState.New:
                    TraverseDepthFirst(rootNode, result, n => n.ObjectState == ObjectState.New);
                    break;
                case ObjectState.Deleted:
                    TraverseBreadthFirst(rootNode, result, n => n.ObjectState == ObjectState.Deleted);
                    break;
                case ObjectState.Dirty:
                    TraverseDepthFirst(rootNode, result, n => n.ObjectState == ObjectState.Dirty);
                    break;
            }
            return result;
        }

        private static void TraverseDepthFirst(ChangeNode rootNode, List<ChangeNode> accumulator, Predicate<ChangeNode> predicate)
        {
            if (accumulator == null)
            {
                accumulator = new List<ChangeNode>();
            }
            if ((rootNode.IsLeaf || rootNode.IsObject) && predicate != null && predicate(rootNode))
            {
                accumulator.Add(rootNode);
            }
            foreach (var childNode in rootNode.Nodes)
            {
                TraverseDepthFirst(childNode, accumulator, predicate);
            }
        }

        private static void TraverseBreadthFirst(ChangeNode rootNode, List<ChangeNode> accumulator, Predicate<ChangeNode> predicate)
        {
            if (accumulator == null)
            {
                accumulator = new List<ChangeNode>();
            }
            foreach (var childNode in rootNode.Nodes)
            {
                TraverseBreadthFirst(childNode, accumulator, predicate);
            }
            if ((rootNode.IsLeaf || rootNode.IsObject) && predicate != null && predicate(rootNode))
            {
                accumulator.Add(rootNode);
            }
        }

        #endregion

        #region Batch Update Methods

        private static Tuple<string, Param[], List<object>> GetCommitStatement(ChangeNode rootNode, DbConnection connection)
        {
            var dialect = DialectFactory.GetProvider(connection);
            var dataEntities = new List<object>();
            var sql = new StringBuilder();
            var statementId = 0;

            // Inserts
            var newNodes = GetChanges(rootNode, ObjectState.New);
            var tableCreated = false;

            var tempTableName = dialect.GetTemporaryTableName("ID");
            var allParameters = new List<Param>();

            foreach (var newNode in newNodes.Where(n => n.IsObject))
            {
                var item = newNode.Value;
                if (item != null)
                {
                    dataEntities.Add(item);
                }
                else
                {
                    continue;
                }

                var objectType = newNode.Value.GetType();
                var propertyMap = Reflector.GetPropertyMap(objectType);
                var autoGenProperty = propertyMap.Where(p => p.Key.CanWrite && p.Value != null && p.Value.IsAutoGenerated).Select(p => p.Key).FirstOrDefault();
                var autoGenType = DbType.String;

                if (!tableCreated)
                {
                    if (autoGenProperty != null)
                    {
                        autoGenType = Reflector.ClrToDbType(autoGenProperty.PropertyType);
                    }
                    sql.AppendFormat(dialect.CreateTemporaryTable("ID", new Dictionary<string, DbType> { { "StatementId", DbType.Int32 }, { "GeneratedId", autoGenType }, { "ParameterName", DbType.AnsiString }, { "PropertyName", DbType.AnsiString } })).AppendLine();
                    tableCreated = true;
                }

                var parameters = ObjectExtensions.GetInsertParameters(newNode.Value, propertyMap, statementId);
                allParameters.AddRange(parameters);

                if (parameters.Length > 0)
                {
                    string commandName = SqlBuilder.GetInsertStatement(objectType, parameters, dialect);
                    var autoGenParameter = parameters.FirstOrDefault(p => p.IsAutoGenerated);

                    if (autoGenParameter != null)
                    {
                        sql.AppendFormat(dialect.DeclareVariable(autoGenParameter.Name, autoGenType)).AppendLine();
                        sql.AppendFormat(dialect.AssignVariable(autoGenParameter.Name, autoGenParameter.Type.GetDefault())).AppendLine();
                    }
                    sql.AppendLine(commandName);

                    if (autoGenParameter != null)
                    {
                        sql.AppendFormat(dialect.ComputeAutoIncrement(autoGenParameter.Name, () => SqlBuilder.GetTableNameForSql(objectType, dialect))).AppendLine();
                        sql.AppendFormat("INSERT INTO " + tempTableName + " ({4}StatementId{5}, {4}GeneratedId{5}, {4}ParameterName{5}, {4}PropertyName{5}) VALUES ({0}, {1}, '{2}', '{3}')", statementId, dialect.EvaluateVariable(dialect.ParameterPrefix + autoGenParameter.Name), autoGenParameter.Name, autoGenProperty.Name, dialect.IdentifierEscapeStartCharacter, dialect.IdentifierEscapeEndCharacter).AppendLine();
                    }

                    statementId++;
                }
            }

            if (newNodes.Count > 0)
            {
                sql.AppendLine("SELECT * FROM " + tempTableName);
            }

            if (tableCreated && !dialect.SupportsTemporaryTables)
            {
                sql.AppendLine("DROP TABLE " + tempTableName);
            }

            // Updates
            var dirtyNodes = GetChanges(rootNode, ObjectState.Dirty);
            var dirtyNodeParents = dirtyNodes.Where(n => n.IsSimpleLeaf).Select(n => n.Parent).Distinct();

            foreach (var dirtyNode in dirtyNodeParents)
            {
                var objectType = dirtyNode.Value.GetType();
                var propertyMap = Reflector.GetPropertyMap(objectType).ToDictionary(p => p.Key.Name, p => p);

                var parameters = new List<Param>();
                foreach (var change in dirtyNode.Nodes.Where(n => n.IsSimpleLeaf))
                {
                    KeyValuePair<PropertyInfo, ReflectedProperty> map;
                    if (propertyMap.TryGetValue(change.PropertyName, out map))
                    {
                        var property = map.Key;
                        var parameterName = change.PropertyName;
                        if (map.Value != null && !string.IsNullOrEmpty(map.Value.ParameterName))
                        {
                            parameterName = map.Value.ParameterName;
                        }

                        parameters.Add(new Param { Name = parameterName + "_" + statementId, Value = change.Value, Source = MapColumnAttribute.GetMappedColumnName(property) });
                    }
                }

                var primaryKey = new List<Param>();
                foreach (var primaryKeyMap in propertyMap.Values.Where(p => p.Value.IsPrimaryKey))
                {
                    var value = dirtyNode.Value.Property(primaryKeyMap.Key.Name);

                    var parameterName = primaryKeyMap.Key.Name;
                    if (primaryKeyMap.Value != null && !string.IsNullOrEmpty(primaryKeyMap.Value.ParameterName))
                    {
                        parameterName = primaryKeyMap.Value.ParameterName;
                    }
                    primaryKey.Add(new Param { Name = parameterName + "_" + statementId, Value = value, Source = MapColumnAttribute.GetMappedColumnName(primaryKeyMap.Key) });
                }

                var commandName = SqlBuilder.GetUpdateStatement(objectType, parameters, primaryKey, dialect);
                allParameters.AddRange(parameters);
                allParameters.AddRange(primaryKey);
                sql.Append(commandName).AppendLine();

                statementId++;
            }

            // Deletes
            var deletedNodes = GetChanges(rootNode, ObjectState.Deleted);
            foreach (var deletedNode in deletedNodes)
            {
                var objectType = deletedNode.Value.GetType();
                var propertyMap = Reflector.GetPropertyMap(objectType);

                var parameters = ObjectExtensions.GetDeleteParameters(deletedNode.Value, propertyMap, statementId);
                var commandName = SqlBuilder.GetDeleteStatement(objectType, parameters, dialect);
                allParameters.AddRange(parameters);
                sql.Append(commandName).AppendLine();

                statementId++;
            }

            return Tuple.Create(sql.ToString(), allParameters.ToArray(), dataEntities);
        }

        #endregion
    }
}
