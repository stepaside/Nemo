using Nemo.Attributes;
using Nemo.Collections;
using Nemo.Collections.Extensions;
using Nemo.Configuration;
using Nemo.Configuration.Mapping;
using Nemo.Data;
using Nemo.Extensions;
using Nemo.Fn;
using Nemo.Fn.Extensions;
using Nemo.Reflection;
using Nemo.Security.Cryptography;
using Nemo.Utilities;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Transactions;
using ObjectActivator = Nemo.Reflection.Activator.ObjectActivator;

namespace Nemo
{
    public static class ObjectFactory
    {
        #region Declarations

        public const string OperationRetrieve = "Retrieve";
        public const string OperationInsert = "Insert";
        public const string OperationUpdate = "Update";
        public const string OperationDelete = "Delete";
        public const string OperationDestroy = "Destroy";

        #endregion

        #region Instantiation Methods

        private static readonly ConcurrentDictionary<Type, RuntimeMethodHandle?> _createMethods = new ConcurrentDictionary<Type, RuntimeMethodHandle?>();

        public static T Create<T>()
            where T : class
        {
            return Create<T>(typeof(T).IsInterface);
        }
        
        public static T Create<T>(bool isInterface)
            where T : class
        {
            var value = isInterface ? Adapter.Implement<T>() : FastActivator<T>.New();

            var entity = value as ITrackableDataEntity;
            if (entity != null)
            {
                entity.ObjectState = ObjectState.New;
            }

            return value;
        }

        public static object Create(Type targetType)
        {
            if (targetType == null) return null;

            var genericCreateMethod = _createMethods.GetOrAdd(targetType, type =>
            {
                var createMethod = typeof(ObjectFactory).GetMethods().FirstOrDefault(m => m.Name == "Create" && m.GetGenericArguments().Length == 1 && m.GetParameters().Length == 1);
                if (createMethod != null)
                {
                    return createMethod.MakeGenericMethod(targetType).MethodHandle;
                }
                return null;
            });

            if (genericCreateMethod == null) return null;

            var mapDelegate = Reflector.Method.CreateDelegate(genericCreateMethod.Value);
            return mapDelegate(null, new object[] { targetType.IsInterface });
        }

        #endregion

        #region Configuration Methods

        public static string DefaultConnectionName
        {
            get
            {
                return ConfigurationFactory.Configuration.DefaultConnectionName;
            }
        }

        #endregion

        #region Map Methods

        private static readonly ConcurrentDictionary<Tuple<Type, Type, bool>, RuntimeMethodHandle?> _mapMethods = new ConcurrentDictionary<Tuple<Type, Type, bool>, RuntimeMethodHandle?>();

        public static object Map(object source, Type targetType, bool ignoreMappings = false)
        {
            return Map(source, targetType, targetType.IsInterface, ignoreMappings);
        }

        internal static object Map(object source, Type targetType, bool isInterface, bool ignoreMappings = false)
        {
            if (source == null) return null;

            var instanceType = source.GetType();
            var key = Tuple.Create(instanceType, targetType, ignoreMappings);
            var genericMapMethodHandle = _mapMethods.GetOrAdd(key, t =>
            {
                if (!Reflector.IsAnonymousType(instanceType))
                {
                    var mapMethod = typeof(ObjectFactory).GetMethods(BindingFlags.NonPublic | BindingFlags.Static).FirstOrDefault(m => m.Name == "Map" && m.GetGenericArguments().Length == 2 && m.GetParameters().Length == 3);
                    if (mapMethod != null)
                    {
                        return mapMethod.MakeGenericMethod(t.Item1, t.Item2).MethodHandle;
                    }
                }
                return null;
            });

            if (genericMapMethodHandle == null) return null;

            var mapDelegate = Reflector.Method.CreateDelegate(genericMapMethodHandle.Value);
            return mapDelegate(null, new[] { source, isInterface, ignoreMappings });
        }

        internal static TResult Map<TSource, TResult>(TSource source, bool isInterface, bool ignoreMappings = false)
            where TResult : class
            where TSource : class
        {
            var target = Create<TResult>(isInterface);
            return Map(source, target, ignoreMappings);
        }

        public static TResult Map<TSource, TResult>(TSource source, bool ignoreMappings = false)
            where TResult : class
            where TSource : class
        {
            return Map<TSource, TResult>(source, typeof(TResult).IsInterface, ignoreMappings);
        }

        public static TResult Map<TSource, TResult>(TSource source, TResult target, bool ignoreMappings = false)
            where TResult : class
            where TSource : class
        {
            var indexer = source is IDictionary<string, object> || (source is IDataRecord) || source is DataRow;

            if (indexer)
            {
                if (ignoreMappings)
                {
                    FastExactIndexerMapper<TSource, TResult>.Map(source, target);
                }
                else
                {
                    var reader = source as IDataReader;
                    if (reader != null)
                    {
                       FastIndexerMapper<IDataRecord, TResult>.Map(reader, target);
                    }
                    else
                    {
                        FastIndexerMapper<TSource, TResult>.Map(source, target);
                    }
                }
            }
            else
            {
                if (ignoreMappings)
                {
                    FastExactMapper<TSource, TResult>.Map(source, target);
                }
                else
                {
                    FastMapper<TSource, TResult>.Map(source, target);
                }
            }
            return target;
        }

        /// <summary>
        /// Maps values from source to target by copying object's properties
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="ignoreMappings"></param>
        /// <returns></returns>
        public static T Map<T>(object source, bool ignoreMappings = false)
            where T : class
        {
            return (T)Map(source, typeof(T), ignoreMappings);
        }

        #endregion

        #region Bind Methods

        private static readonly ConcurrentDictionary<Type, RuntimeMethodHandle?> _bindMethods = new ConcurrentDictionary<Type, RuntimeMethodHandle?>();

        /// <summary>
        /// Binds interface implementation to the existing object type.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <returns></returns>
        public static TResult Bind<TSource, TResult>(TSource source)
            where TResult : class
            where TSource : class
        {
            if (source == null) throw new ArgumentNullException("source");
            var target = Adapter.Bind<TResult>(source);
            return target;
        }

        /// <summary>
        /// Binds interface implementation to the existing object type. 
        /// This method uses reflection to invoke generic implementation.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <returns></returns>
        public static T Bind<T>(object source)
            where T : class
        {
            if (source == null) return null;

            var instanceType = source.GetType();
            if (Reflector.IsAnonymousType(instanceType))
            {
                return Adapter.Bind<T>(source);
            }
            var genericBindMethod = _bindMethods.GetOrAdd(instanceType, type =>
            {
                var bindMethod = typeof(ObjectFactory).GetMethods().FirstOrDefault(m => m.Name == "Bind" && m.GetGenericArguments().Length == 2);
                if (bindMethod != null)
                {
                    return bindMethod.MakeGenericMethod(type, typeof(T)).MethodHandle;
                }
                return null;
            });

            if (genericBindMethod == null) return null;

            var bindDelegate = Reflector.Method.CreateDelegate(genericBindMethod.Value);
            return (T)bindDelegate(null, new[] { source });
        }

        #endregion

        #region Wrap Methods

        /// <summary>
        /// Converts a dictionary to an instance of the object specified by the interface.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="map"></param>
        /// <param name="ignoreMappings"></param>
        /// <param name="includeAllProperties"></param>
        /// <returns></returns>
        public static T Wrap<T>(IDictionary<string, object> map, bool ignoreMappings = false, bool includeAllProperties = false)
             where T : class
        {
            ObjectActivator activator;
            if (ignoreMappings)
            {
                activator = includeAllProperties ? FastExactComplexWrapper<T>.Instance : FastExactWrapper<T>.Instance;
            }
            else
            {
                activator = includeAllProperties ? FastComplexWrapper<T>.Instance : FastWrapper<T>.Instance;
            }
            return (T)activator(map);
        }

        public static object Wrap(IDictionary<string, object> value, Type targetType, bool ignoreMappings = false, bool includeAllProperties = false)
        {
            return Adapter.Wrap(value, targetType, ignoreMappings, includeAllProperties);
        }

        #endregion

        #region Count Methods

        public static int Count<T>(Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null)
            where T : class
        {
            string providerName = null;
            if (connection == null)
            {
                providerName = DbFactory.GetProviderInvariantName(connectionName, typeof(T));
                connection = DbFactory.CreateConnection(connectionName, typeof(T));
            }
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }
            var sql = SqlBuilder.GetSelectCountStatement<T>(predicate, DialectFactory.GetProvider(connection, providerName));
            return RetrieveScalar<int>(sql, connection: connection);
        }

        #endregion

        #region Select Methods

        public static IEnumerable<T> Select<T>(Expression<Func<T, bool>> predicate = null, string connectionName = null, DbConnection connection = null, int page = 0, int pageSize = 0, bool? cached = null)
            where T : class
        {
            string providerName = null;
            if (connection == null)
            {
                providerName = DbFactory.GetProviderInvariantName(connectionName, typeof(T));
                connection = DbFactory.CreateConnection(connectionName, typeof(T));
            }
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }
            var sql = SqlBuilder.GetSelectStatement(predicate, page, pageSize, DialectFactory.GetProvider(connection, providerName));
            return RetrieveImplemenation<T>(sql, OperationType.Sql, null, OperationReturnType.SingleResult, connectionName, connection, cached: cached);
        }
        
        #endregion

        #region Retrieve Methods

        public static T RetrieveScalar<T>(string sql, Param[] parameters = null, string connectionName = null, DbConnection connection = null, string schema = null)
            where T : struct
        {
            var response = connection != null
                ? Execute(sql, parameters, OperationReturnType.Scalar, OperationType.Sql, connection: connection, schema: schema)
                : Execute(sql, parameters, OperationReturnType.Scalar, OperationType.Sql, connectionName: connectionName, schema: schema);

            var value = response.Value;
            if (value == null)
            {
                return default(T);
            }
            return ((T)value).ToMaybe();
        }

        private static IEnumerable<TResult> RetrieveImplemenation<TResult>(string operation, OperationType operationType, IList<Param> parameters, OperationReturnType returnType, string connectionName, DbConnection connection, Func<object[], TResult> map = null, IList<Type> types = null, MaterializationMode mode = MaterializationMode.Default, string schema = null, bool? cached = null)
            where TResult : class
        {
            Log.CaptureBegin(() => string.Format("RetrieveImplemenation: {0}::{1}", typeof(TResult).FullName, operation));
            IEnumerable<TResult> result;
            
            string queryKey = null;
            IdentityMap<TResult> identityMap = null;

            if (!cached.HasValue)
            {
                cached = ConfigurationFactory.Configuration.DefaultL1CacheRepresentation != L1CacheRepresentation.None;
            }

            if (cached.Value)
            {
                queryKey = GetQueryKey<TResult>(operation, parameters ?? new Param[] { }, returnType);

                Log.CaptureBegin(() => string.Format("Retrieving from L1 cache: {0}", queryKey));

                if (returnType == OperationReturnType.MultiResult)
                {
                    result = ConfigurationFactory.Configuration.ExecutionContext.Get(queryKey) as IEnumerable<TResult>;
                }
                else
                {
                    identityMap = Identity.Get<TResult>();
                    result = identityMap.GetIndex(queryKey);
                }
                
                Log.CaptureEnd();

                if (result != null)
                {
                    Log.Capture(() => string.Format("Found in L1 cache: {0}", queryKey));
                    
                    if (returnType == OperationReturnType.MultiResult)
                    {
                        ((IMultiResult)result).Reset();
                    }
                    
                    Log.CaptureEnd();
                    return result;
                }
                else
                {
                    Log.Capture(() => string.Format("Not found in L1 cache: {0}", queryKey));
                }
            }

            result = RetrieveItems(operation, parameters, operationType, returnType, connectionName, connection, types, map, cached.Value, mode, schema);
            
            // Cache in the local context if the 2nd level cache is distributed
            if (queryKey != null)
            {
                Log.CaptureBegin(() => string.Format("Saving to L1 cache: {0}", queryKey));

                if (!(result is IList<TResult>) && !(result is IMultiResult))
                {
                    if (ConfigurationFactory.Configuration.DefaultL1CacheRepresentation == L1CacheRepresentation.List)
                    {
                        result = result.ToList();
                    }
                    else if (result.Any())
                    {
                        result = result.AsStream();
                    }
                    else
                    {
                        result = new List<TResult>();
                    }
                }

                if (identityMap != null)
                {
                    identityMap.AddIndex(queryKey, result);
                }
                else if (result is IMultiResult)
                {
                    ConfigurationFactory.Configuration.ExecutionContext.Set(queryKey, result);
                }

                Log.CaptureEnd();
            }

            Log.CaptureEnd();
            return result;
        }

        private static IEnumerable<T> RetrieveItems<T>(string operation, IList<Param> parameters, OperationType operationType, OperationReturnType returnType, string connectionName, DbConnection connection, IList<Type> types, Func<object[], T> map, bool cached, MaterializationMode mode, string schema)
            where T : class
        {
            if (operationType == OperationType.Guess)
            {
                operationType = operation.Any(Char.IsWhiteSpace) ? OperationType.Sql : OperationType.StoredProcedure;
            }

            var operationText = GetOperationText(typeof(T), operation, operationType, schema);

            var response = connection != null 
                ? Execute(operationText, parameters, returnType, connection: connection, operationType: operationType, types: types, schema: schema) 
                : Execute(operationText, parameters, returnType, connectionName: connectionName, operationType: operationType, types: types, schema: schema);

            var result = Transform(response, map, types, cached, mode);
            return result;
        }

        private static string GetQueryKey<T>(string operation, IEnumerable<Param> parameters, OperationReturnType returnType)
        {
            var hash = Jenkins96Hash.Compute(Encoding.UTF8.GetBytes(returnType + "/" + operation + "/" + string.Join(",", parameters.OrderBy(p => p.Name).Select(p => p.Name + "=" + p.Value))));
            return typeof(T).FullName + "/" + hash;
        }
        
        /// <summary>
        /// Retrieves an enumerable of type T using provided rule parameters.
        /// </summary>
        /// <returns></returns>
        public static IEnumerable<TResult> Retrieve<TResult, T1, T2, T3, T4>(string operation = OperationRetrieve, string sql = null, object parameters = null, Func<TResult, T1, T2, T3, T4, TResult> map = null, string connectionName = null, DbConnection connection = null, FetchMode mode = FetchMode.Default, MaterializationMode materialization = MaterializationMode.Default, string schema = null, bool? cached = null)
            where T1 : class
            where T2 : class
            where T3 : class
            where T4 : class
            where TResult : class
        {
            var fakeType = typeof(Fake);
            var realTypes = new List<Type> { typeof(TResult) };
            if (fakeType != typeof(T1))
            {
                realTypes.Add(typeof(T1));
            }
            if (fakeType != typeof(T2))
            {
                realTypes.Add(typeof(T2));
            }
            if (fakeType != typeof(T3))
            {
                realTypes.Add(typeof(T3));
            }
            if (fakeType != typeof(T4))
            {
                realTypes.Add(typeof(T4));
            }

            var returnType = OperationReturnType.SingleResult;

            if (mode == FetchMode.Default) mode = ConfigurationFactory.Configuration.DefaultFetchMode;
            if (materialization == MaterializationMode.Default) materialization = ConfigurationFactory.Configuration.DefaultMaterializationMode;

            Func<object[], TResult> func = null;
            if (map == null && realTypes.Count > 1)
            {
                returnType = mode == FetchMode.Lazy ? OperationReturnType.MultiResult : OperationReturnType.DataSet;
            }
            else if (map != null && realTypes.Count > 1)
            {
                switch (realTypes.Count)
                {
                    case 5:
                        func = args => map((TResult)args[0], (T1)args[1], (T2)args[2], (T3)args[3], (T4)args[4]);
                        break;
                    case 4:
                        func = args => map.Curry((TResult)args[0], (T1)args[1], (T2)args[2], (T3)args[3])(null);
                        break;
                    case 3:
                        func = args => map.Curry((TResult)args[0], (T1)args[1], (T2)args[2])(null, null);
                        break;
                    case 2:
                        func = args => map.Curry((TResult)args[0], (T1)args[1])(null, null, null);
                        break;
                }
            }
            else if (mode == FetchMode.Eager && realTypes.Count == 1)
            {
                returnType = OperationReturnType.DataTable;
            }

            var command = sql ?? operation;
            var commandType = sql == null ? OperationType.StoredProcedure : OperationType.Sql;
            IList<Param> parameterList = null;
            if (parameters != null)
            {
                var list = parameters as ParamList;
                if (list != null)
                {
                    parameterList = list.ExtractParameters(typeof(TResult), operation);
                }
                else
                {
                    var array = parameters as Param[];
                    if (array != null)
                    {
                        parameterList = array;
                    }
                }
            }
            return RetrieveImplemenation(command, commandType, parameterList, returnType, connectionName, connection, func, realTypes, materialization, schema, cached);
        }

        public static IEnumerable<TResult> Retrieve<TResult, T1, T2, T3>(string operation = OperationRetrieve, string sql = null, object parameters = null, Func<TResult, T1, T2, T3, TResult> map = null, string connectionName = null, DbConnection connection = null, FetchMode mode = FetchMode.Default, MaterializationMode materialization = MaterializationMode.Default, string schema = null, bool? cached = null)
            where T1 : class
            where T2 : class
            where T3 : class
            where TResult : class
        {
            var newMap = map != null ? (t, t1, t2, t3, f4) => map(t, t1, t2, t3) : (Func<TResult, T1, T2, T3, Fake, TResult>)null;
            return Retrieve(operation, sql, parameters, newMap, connectionName, connection, mode, materialization, schema, cached);
        }

        public static IEnumerable<TResult> Retrieve<TResult, T1, T2>(string operation = OperationRetrieve, string sql = null, object parameters = null, Func<TResult, T1, T2, TResult> map = null, string connectionName = null, DbConnection connection = null, FetchMode mode = FetchMode.Default, MaterializationMode materialization = MaterializationMode.Default, string schema = null, bool? cached = null)
            where T1 : class
            where T2 : class
            where TResult : class
        {
            var newMap = map != null ? (t, t1, t2, f3, f4) => map(t, t1, t2) : (Func<TResult, T1, T2, Fake, Fake, TResult>)null;
            return Retrieve(operation, sql, parameters, newMap, connectionName, connection, mode, materialization, schema, cached);
        }

        public static IEnumerable<TResult> Retrieve<TResult, T1>(string operation = OperationRetrieve, string sql = null, object parameters = null, Func<TResult, T1, TResult> map = null, string connectionName = null, DbConnection connection = null, FetchMode mode = FetchMode.Default, MaterializationMode materialization = MaterializationMode.Default, string schema = null, bool? cached = null)
            where T1 : class
            where TResult : class
        {
            var newMap = map != null ? (t, t1, f1, f2, f3) => map(t, t1) : (Func<TResult, T1, Fake, Fake, Fake, TResult>)null;
            return Retrieve(operation, sql, parameters, newMap, connectionName, connection, mode, materialization, schema, cached);
        }

        public static IEnumerable<T> Retrieve<T>(string operation = OperationRetrieve, string sql = null, object parameters = null, string connectionName = null, DbConnection connection = null, FetchMode mode = FetchMode.Default, MaterializationMode materialization = MaterializationMode.Default, string schema = null, bool? cached = null)
            where T : class
        {
            var returnType = OperationReturnType.SingleResult;

            if (mode == FetchMode.Default) mode = ConfigurationFactory.Configuration.DefaultFetchMode;
            if (mode == FetchMode.Eager) returnType = OperationReturnType.DataTable;

            if (materialization == MaterializationMode.Default) materialization = ConfigurationFactory.Configuration.DefaultMaterializationMode;

            var command = sql ?? operation;
            var commandType = sql == null ? OperationType.StoredProcedure : OperationType.Sql;
            IList<Param> parameterList = null;
            if (parameters != null)
            {
                var list = parameters as ParamList;
                if (list != null)
                {
                    parameterList = list.ExtractParameters(typeof(T), operation);
                }
                else
                {
                    var array = parameters as Param[];
                    if (array != null)
                    {
                        parameterList = array;
                    }
                }
            }
            return RetrieveImplemenation<T>(command, commandType, parameterList, returnType, connectionName, connection, null, new[] { typeof(T) }, materialization, schema, cached);
        }

        internal class Fake { }

        #endregion

        #region Insert/Update/Delete/Execute Methods

        public static OperationResponse Insert<T>(ParamList parameters, string connectionName = null, bool captureException = false, string schema = null)
            where T : class
        {
            return Insert<T>(parameters.ExtractParameters(typeof(T), OperationInsert), connectionName, captureException, schema);
        }

        public static OperationResponse Insert<T>(Param[] parameters, string connectionName = null, bool captureException = false, string schema = null)
            where T : class
        {
            var request = new OperationRequest { Parameters = parameters, ReturnType = OperationReturnType.NonQuery, ConnectionName = connectionName, CaptureException = captureException };
            if (ConfigurationFactory.Configuration.GenerateInsertSql)
            {
                request.Operation = SqlBuilder.GetInsertStatement(typeof(T), parameters, DialectFactory.GetProvider(request.ConnectionName ?? ConfigurationFactory.Configuration.DefaultConnectionName));
                request.OperationType = OperationType.Sql;
            }
            else
            {
                request.Operation = OperationInsert;
                request.OperationType = OperationType.StoredProcedure;
                request.SchemaName = schema;
            }
            var response = Execute<T>(request);
            return response;
        }

        public static OperationResponse Update<T>(ParamList parameters, string connectionName = null, bool captureException = false, string schema = null)
            where T : class
        {
            return Update<T>(parameters.ExtractParameters(typeof(T), OperationUpdate), connectionName, captureException, schema);
        }

        public static OperationResponse Update<T>(Param[] parameters, string connectionName = null, bool captureException = false, string schema = null)
            where T : class
        {
            var request = new OperationRequest { Parameters = parameters, ReturnType = OperationReturnType.NonQuery, ConnectionName = connectionName, CaptureException = captureException };
            if (ConfigurationFactory.Configuration.GenerateUpdateSql)
            {
                var partition = parameters.Partition(p => p.IsPrimaryKey);
                // if p.IsPrimaryKey is not set then
                // we need to infer it from reflected property 
                if (partition.Item1.Count == 0)
                {
                    var propertyMap = Reflector.GetPropertyMap<T>();
                    var pimaryKeySet = propertyMap.Values.Where(p => p.IsPrimaryKey).Select(p => p.ParameterName ?? p.PropertyName).ToHashSet();
                    partition = parameters.Partition(p => pimaryKeySet.Contains(p.Name));
                }
                request.Operation = SqlBuilder.GetUpdateStatement(typeof(T), partition.Item2, partition.Item1, DialectFactory.GetProvider(request.ConnectionName ?? ConfigurationFactory.Configuration.DefaultConnectionName));
                request.OperationType = OperationType.Sql;
            }
            else
            {
                request.Operation = OperationUpdate;
                request.OperationType = OperationType.StoredProcedure;
                request.SchemaName = schema;
            }
            var response = Execute<T>(request);
            return response;
        }

        public static OperationResponse Delete<T>(ParamList parameters, string connectionName = null, bool captureException = false, string schema = null)
            where T : class
        {
            return Delete<T>(parameters.ExtractParameters(typeof(T), OperationDelete), connectionName, captureException);
        }

        public static OperationResponse Delete<T>(Param[] parameters, string connectionName = null, bool captureException = false, string schema = null)
            where T : class
        {
            var request = new OperationRequest { Parameters = parameters, ReturnType = OperationReturnType.NonQuery, ConnectionName = connectionName, CaptureException = captureException };
            if (ConfigurationFactory.Configuration.GenerateDeleteSql)
            {
                string softDeleteColumn = null; 
                var map = MappingFactory.GetEntityMap<T>();
                if (map != null)
                {
                    softDeleteColumn = map.SoftDeleteColumnName;
                }

                if (softDeleteColumn == null)
                {
                    var attr = Reflector.GetAttribute<T, TableAttribute>();
                    if (attr != null)
                    {
                        softDeleteColumn = attr.SoftDeleteColumn;
                    }
                }
                
                request.Operation = SqlBuilder.GetDeleteStatement(typeof(T), parameters, DialectFactory.GetProvider(request.ConnectionName ?? ConfigurationFactory.Configuration.DefaultConnectionName), softDeleteColumn);
                request.OperationType = OperationType.Sql;
            }
            else
            {
                request.Operation = OperationDelete;
                request.OperationType = OperationType.StoredProcedure;
                request.SchemaName = schema;
            }
            var response = Execute<T>(request);
            return response;
        }

        public static OperationResponse Destroy<T>(ParamList parameters, string connectionName = null, bool captureException = false, string schema = null)
            where T : class
        {
            return Destroy<T>(parameters.ExtractParameters(typeof(T), OperationDestroy), connectionName, captureException, schema);
        }

        public static OperationResponse Destroy<T>(Param[] parameters, string connectionName = null, bool captureException = false, string schema = null)
            where T : class
        {
            var request = new OperationRequest { Parameters = parameters, ReturnType = OperationReturnType.NonQuery, ConnectionName = connectionName, CaptureException = captureException, SchemaName = schema };
            if (ConfigurationFactory.Configuration.GenerateDeleteSql)
            {
                request.Operation = SqlBuilder.GetDeleteStatement(typeof(T), parameters, DialectFactory.GetProvider(request.ConnectionName ?? ConfigurationFactory.Configuration.DefaultConnectionName));
                request.OperationType = OperationType.Sql;
            }
            else
            {
                request.Operation = OperationDestroy;
                request.OperationType = OperationType.StoredProcedure;
                request.SchemaName = schema;
            }
            var response = Execute<T>(request);
            return response;
        }

        internal static OperationResponse Execute(string operationText, IList<Param> parameters, OperationReturnType returnType, OperationType operationType, IList<Type> types = null, string connectionName = null, DbConnection connection = null, DbTransaction transaction = null, bool captureException = false, string schema = null)
        {
            var rootType = types != null ? types[0] : null;

            DbConnection dbConnection = null;
            var closeConnection = false;

            if (transaction != null)
            {
                dbConnection = transaction.Connection;
            }
            else if (connection != null)
            {
                dbConnection = connection;
            }
            else
            {
                dbConnection = DbFactory.CreateConnection(connectionName, rootType);
                closeConnection = true;
            }
            
            if (returnType == OperationReturnType.Guess)
            {
                if (operationText.IndexOf("insert", StringComparison.OrdinalIgnoreCase) > -1
                         || operationText.IndexOf("update", StringComparison.OrdinalIgnoreCase) > -1
                         || operationText.IndexOf("delete", StringComparison.OrdinalIgnoreCase) > -1)
                {
                    returnType = OperationReturnType.NonQuery;
                }
                else
                {
                    returnType = OperationReturnType.SingleResult;
                }
            }

            Dictionary<DbParameter, Param> outputParameters = null;

            var command = dbConnection.CreateCommand();
            command.CommandText = operationText;
            command.CommandType = operationType == OperationType.StoredProcedure ? CommandType.StoredProcedure : CommandType.Text;
            command.CommandTimeout = 0;
            if (parameters != null)
            {
                for (int i = 0; i < parameters.Count; ++i)
                {
                    var parameter = parameters[i];
                    var dbParam = command.CreateParameter();
                    dbParam.ParameterName = parameter.Name.TrimStart('@', '?', ':');
                    dbParam.Direction = parameter.Direction;
                    dbParam.Value = parameter.Value ?? DBNull.Value;
                    
                    if (parameter.Value != null)
                    {
                        if (parameter.Size > -1)
                        {
                            dbParam.Size = parameter.Size;
                        }

                        if (!parameter.DbType.HasValue)
                        {
                            dbParam.DbType = Reflector.ClrToDbType(parameter.Type);
                        }
                        else
                        {
                            dbParam.DbType = parameter.DbType.Value;
                        }
                    }

                    if (dbParam.Direction == ParameterDirection.Output)
                    {
                        if (outputParameters == null)
                        {
                            outputParameters = new Dictionary<DbParameter, Param>();
                        }
                        outputParameters.Add(dbParam, parameter);
                    }

                    command.Parameters.Add(dbParam);
                }
            }

            if (dbConnection.State != ConnectionState.Open)
            {
                dbConnection.Open();
            }

            var response = new OperationResponse();
            response.ReturnType = returnType;
            try
            {
                switch (returnType)
                {
                    case OperationReturnType.NonQuery:
                        response.RecordsAffected = command.ExecuteNonQuery();
                        break;
                    case OperationReturnType.MultiResult:
                    case OperationReturnType.SingleResult:
                    case OperationReturnType.SingleRow:
                        var behavior = CommandBehavior.Default;
                        if (returnType == OperationReturnType.SingleResult)
                        {
                            behavior = CommandBehavior.SingleResult;
                        }
                        else if (returnType == OperationReturnType.SingleRow)
                        {
                            behavior = CommandBehavior.SingleRow;
                        }
                        // else MultiResult
                        closeConnection = false;
                        //if (closeConnection)
                        //{
                        //    behavior |= CommandBehavior.CloseConnection;
                        //}
                        response.Value = command.ExecuteReader(behavior);
                        break;
                    case OperationReturnType.Scalar:
                        response.Value = command.ExecuteScalar();
                        break;
                    case OperationReturnType.DataSet:
                    case OperationReturnType.DataTable:
                        var adapter = DbFactory.CreateDataAdapter(dbConnection);
                        adapter.SelectCommand = command;
                        if (returnType == OperationReturnType.DataTable)
                        {
                            var table = rootType != null ? new DataTable(GetTableName(rootType)) : new DataTable();
                            adapter.Fill(table);
                            response.Value = table;
                        }
                        else
                        {
                            var set = new DataSet();
                            adapter.Fill(set);
                            if (types != null)
                            {
                                for (var i = 0; i < set.Tables.Count; i++)
                                {
                                    Type tableType = types.ElementAtOrDefault(i);
                                    if (tableType != null)
                                    {
                                        set.Tables[i].TableName = GetTableName(tableType);
                                    }
                                }
                            }
                            else
                            {
                                var tableName = rootType != null ? GetTableName(rootType) : null;
                                if (tableName.NullIfEmpty() != null)
                                {
                                    set.Tables[0].TableName = tableName;
                                }
                            }
                            response.Value = set;
                            if (rootType != null)
                            {
                                InferRelations(set, rootType);
                            }
                        }
                        break;
                }

                // Handle output parameters
                if (outputParameters != null)
                {
                    foreach (var entry in outputParameters)
                    {
                        entry.Value.Value = Convert.IsDBNull(entry.Key.Value) ? null : entry.Key.Value;
                    }
                }
            }
            catch (Exception ex)
            {
                if (captureException)
                {
                    response.Exception = ex;
                }
                else
                {
                    throw;
                }
            }
            finally
            {
                command.Dispose();
                if (dbConnection != null && (closeConnection || response.HasErrors))
                {
                    dbConnection.Close();
                }
            }

            return response;
        }

        public static OperationResponse Execute<T>(OperationRequest request)
            where T : class
        {
            if (request.Types == null)
            {
                request.Types = new[] { typeof(T) };
            }

            var operationType = request.OperationType;
            if (operationType == OperationType.Guess)
            {
                operationType = request.Operation.Any(Char.IsWhiteSpace) ? OperationType.Sql : OperationType.StoredProcedure;
            }

            var operationText = GetOperationText(typeof(T), request.Operation, request.OperationType, request.SchemaName);

            var response = request.Connection != null 
                ? Execute(operationText, request.Parameters, request.ReturnType, operationType, request.Types, connection: request.Connection, transaction: request.Transaction, captureException: request.CaptureException, schema: request.SchemaName) 
                : Execute(operationText, request.Parameters, request.ReturnType, operationType, request.Types, request.ConnectionName, transaction: request.Transaction, captureException: request.CaptureException, schema: request.SchemaName);
            return response;
        }

        #endregion

        #region Transform/Convert Methods

        private static IEnumerable<T> Transform<T>(OperationResponse response, Func<object[], T> map, IList<Type> types, bool cached, MaterializationMode mode)
            where T : class
        {
            object value = response.Value;
            if (value == null)
            {
                return Enumerable.Empty<T>();
            }

            var isInterface = Reflector.GetReflectedType<T>().IsInterface;

            var reader = value as IDataReader;
            if (reader != null)
            {
                if (map == null && types != null && types.Count > 1)
                {
                    var multiResultItems = ConvertDataReaderMultiResult(reader, types, mode, isInterface);
                    return (IEnumerable<T>)MultiResult.Create(types, multiResultItems, cached);
                }
                return ConvertDataReader<T>(reader, map, types, mode, isInterface);
            }

            var dataSet = value as DataSet;
            if (dataSet != null)
            {
                return ConvertDataSet<T>(dataSet, mode, isInterface);
            }

            var dataTable = value as DataTable;
            if (dataTable != null)
            {
                return ConvertDataTable<T>(dataTable, mode, isInterface);
            }

            var dataRow = value as DataRow;
            if (dataRow != null)
            {
                return ConvertDataRow<T>(dataRow, mode, isInterface).Return();
            }
            
            if (value is T)
            {
                return ((T)value).Return();
            }
            
            if (value is IList<T>)
            {
                return (IList<T>)value;
            }

            var list = value as IList;
            if (list != null)
            {
                return list.Cast<object>().Select(i => mode == MaterializationMode.Exact ? Map<T>(i) : Bind<T>(i));
            }
            
            return Bind<T>(value).Return();
        }

        private static IEnumerable<T> ConvertDataSet<T>(DataSet dataSet, MaterializationMode mode, bool isInterface)
             where T : class
        {
            string tableName = GetTableName(typeof(T));
            return dataSet.Tables.Count != 0 ? ConvertDataTable<T>(dataSet.Tables.Contains(tableName) ? dataSet.Tables[tableName] : dataSet.Tables[0], mode, isInterface) : Enumerable.Empty<T>();
        }

        private static IEnumerable<T> ConvertDataTable<T>(DataTable table, MaterializationMode mode, bool isInterface)
             where T : class
        {
            return ConvertDataTable<T>(table.AsEnumerable(), mode, isInterface);
        }

        private static IEnumerable<T> ConvertDataTable<T>(IEnumerable<DataRow> table, MaterializationMode mode, bool isInterface)
             where T : class
        {
            return table.Select(row => ConvertDataRow<T>(row, mode, isInterface));
        }

        private static IEnumerable<object> ConvertDataTable(IEnumerable<DataRow> table, Type targetType, MaterializationMode mode, bool isInterface)
        {
            return table.Select(row => ConvertDataRow(row, targetType, mode, isInterface));
        }

        private static T ConvertDataRow<T>(DataRow row, MaterializationMode mode, bool isInterface)
             where T : class
        {
            var value = mode == MaterializationMode.Exact || !isInterface ? Map<DataRow, T>(row, isInterface) : Wrap<T>(GetSerializableDataRow(row));
            var entity = value as ITrackableDataEntity;
            if (entity != null)
            {
                entity.ObjectState = ObjectState.Clean;
            }

            LoadRelatedData(row, value, typeof(T), mode);
            
            return value;
        }

        private static object ConvertDataRow(DataRow row, Type targetType, MaterializationMode mode, bool isInterface)
        {
            var value = mode == MaterializationMode.Exact || !isInterface ? Map((object)row, targetType, isInterface) : Wrap(GetSerializableDataRow(row), targetType);
            var entity = value as ITrackableDataEntity;
            if (entity != null)
            {
                entity.ObjectState = ObjectState.Clean;
            }

            LoadRelatedData(row, value, targetType, mode);

            return value;
        }

        private static void LoadRelatedData(DataRow row, object value, Type targetType, MaterializationMode mode)
        {
            DataTable table = row.Table;
            if (table != null && table.ChildRelations.Count > 0)
            {
                var propertyMap = Reflector.GetPropertyMap(targetType);
                var relations = table.ChildRelations.Cast<DataRelation>().ToArray();

                foreach (var p in propertyMap)
                {
                    // By convention each relation should end with the name of the property prefixed with underscore
                    var relation = relations.FirstOrDefault(r => r.RelationName.EndsWith("_" + p.Key.Name));

                    if (relation == null) continue;
                    
                    var childRows = row.GetChildRows(relation);
                    
                    if (childRows.Length <= 0) continue;

                    object propertyValue = null;
                    if (p.Value.IsDataEntity)
                    {
                        propertyValue = ConvertDataRow(childRows[0], p.Key.PropertyType, mode, p.Key.PropertyType.IsInterface);
                    }
                    else if (p.Value.IsDataEntityList)
                    {
                        var elementType = p.Value.ElementType;
                        if (elementType != null)
                        {
                            var items = ConvertDataTable(childRows, elementType, mode, elementType.IsInterface);
                            IList list;
                            if (!p.Value.IsListInterface)
                            {
                                list = (IList)p.Key.PropertyType.New();
                            }
                            else
                            {
                                list = List.Create(elementType, p.Value.Distinct, p.Value.Sorted);
                            }

                            foreach (var item in items)
                            {
                                list.Add(item);
                            }

                            propertyValue = list;
                        }
                    }
                    Reflector.Property.Set(value.GetType(), value, p.Key.Name, propertyValue);
                }
            }
        }
        
        private static IEnumerable<T> ConvertDataReader<T>(IDataReader reader, Func<object[], T> map, IList<Type> types, MaterializationMode mode, bool isInterface)
            where T : class
        {
            try
            {
                var isAccumulator = false;
                var count = reader.FieldCount;
                var references = new Dictionary<Tuple<Type, string>, object>();
                while (reader.Read())
                {
                    if (!isInterface || mode == MaterializationMode.Exact)
                    {
                        var item = Map<IDataRecord, T>(reader, isInterface: isInterface);

                        if (map != null)
                        {
                            var args = new object[types.Count];
                            args[0] = item;
                            for (var i = 1; i < types.Count; i++)
                            {
                                var identity = CreateIdentity(types[i], reader);
                                object reference;
                                if (!references.TryGetValue(identity, out reference))
                                {
                                    reference = Map((object)reader, types[i]);
                                    references.Add(identity, reference);
                                }
                                args[i] = reference;
                            }
                            var mappedItem = map(args);
                            if (mappedItem != null)
                            {
                                yield return mappedItem;
                            }
                            else
                            {
                                isAccumulator = true;
                            }
                        }
                        else
                        {
                            yield return item;
                        }
                    }
                    else
                    {
                        var bag = new Dictionary<string, object>();
                        for (var index = 0; index < count; index++)
                        {
                            bag.Add(reader.GetName(index), reader[index]);
                        }
                        var item = Wrap<T>(bag);

                        if (map != null)
                        {
                            var args = new object[types.Count];
                            args[0] = item;
                            for (var i = 1; i < types.Count; i++)
                            {
                                var identity = CreateIdentity(types[i], reader);
                                object reference;
                                if (!references.TryGetValue(identity, out reference))
                                {
                                    reference = Wrap(bag, types[i]);
                                    references.Add(identity, reference);
                                }
                                args[i] = reference;
                            }
                            var mappedItem = map(args);
                            if (mappedItem != null)
                            {
                                yield return mappedItem;
                            }
                            else
                            {
                                isAccumulator = true;
                            }
                        }
                        else
                        {
                            yield return item;
                        }
                    }
                }

                // Flush accumulating item
                if (isAccumulator)
                {
                    var mappedItem = map(new object[types.Count]);
                    if (mappedItem != null)
                    {
                        yield return mappedItem;
                    }
                }
            }
            finally
            {
                if (reader != null)
                {
                    reader.Dispose();
                }
            }
        }

        private static Tuple<Type, string> CreateIdentity(Type objectType, IDataRecord record)
        {
            var nameMap = Reflector.GetPropertyNameMap(objectType);
            var identity = Tuple.Create(objectType, string.Join(",", nameMap.Values.Where(p => p.IsPrimaryKey)
                                                                                .Select(p => p.MappedColumnName ?? p.PropertyName)
                                                                                .OrderBy(_ => _)
                                                                                .Select(n => Convert.ToString(record.GetValue(record.GetOrdinal(n))))));
            return identity;
        }

        private static IEnumerable<ITypeUnion> ConvertDataReaderMultiResult(IDataReader reader, IList<Type> types, MaterializationMode mode, bool isInterface)
        {
            try
            {
                int resultIndex = 0;
                do
                {
                    int count = reader.FieldCount;
                    while (reader.Read())
                    {
                        if (!isInterface || mode == MaterializationMode.Exact)
                        {
                            var item = Map(reader, types[resultIndex], isInterface: isInterface);
                            yield return TypeUnion.Create(types, item);
                        }
                        else
                        {
                            var bag = new Dictionary<string, object>();
                            for (int index = 0; index < count; index++)
                            {
                                bag.Add(reader.GetName(index), reader.GetValue(index));
                            }
                            var item = Wrap(bag, types[resultIndex]);
                            yield return TypeUnion.Create(types, item);
                        }
                    }
                    resultIndex++;
                    if (resultIndex < types.Count)
                    {
                        isInterface = types[resultIndex].IsInterface;
                    }
                } while (reader.NextResult());
            }
            finally
            {
                if (reader != null)
                {
                    reader.Dispose();
                }
            }
        }

        private static IDictionary<string, object> GetSerializableDataRow(DataRow row)
        {
            IDictionary<string, object> result = new Dictionary<string, object>(StringComparer.Ordinal);
            if (row != null)
            {
                foreach (DataColumn column in row.Table.Columns)
                {
                    result.Add(column.ColumnName, row[column]);
                }
            }
            return result;
        }

        #endregion

        #region Helper Methods

        private static void InferRelations(DataSet set, Type objectType, string tableName = null)
        {
            var propertyMap = Reflector.GetPropertyMap(objectType);
            if (tableName == null)
            {
                tableName = GetTableName(objectType);
            }

            var primaryKey = propertyMap.Where(p => p.Value.IsPrimaryKey).OrderBy(p => p.Value.KeyPosition).Select(p => p.Value).ToList();
            var references = propertyMap.Where(p => p.Value.IsDataEntity || p.Value.IsDataEntityList).Select(p => p.Value);
            foreach (var reference in references)
            {
                var elementType = reference.IsDataEntityList ? Reflector.ExtractGenericCollectionElementType(reference.PropertyType) : reference.PropertyType;

                var referencedPropertyMap = Reflector.GetPropertyMap(elementType);
                var referencedProperties = referencedPropertyMap.Where(p => p.Value != null && p.Value.Parent == objectType).OrderBy(p => p.Value.RefPosition).Select(p => p.Value).ToList();
                if (referencedProperties.Count > 0)
                {
                    var referencedTableName = GetTableName(elementType);

                    if (set.Tables.Contains(tableName) && set.Tables.Contains(referencedTableName))
                    {
                        var sourceColumns = primaryKey.Select(p => set.Tables[tableName].Columns[p.MappedColumnName]).ToArray();
                        var targetColumns = referencedProperties.Select(p => set.Tables[referencedTableName].Columns[p.MappedColumnName]).ToArray();
                        var relation = new DataRelation("_" + reference.PropertyName, sourceColumns, targetColumns, false);
                        set.Relations.Add(relation);
                        InferRelations(set, elementType, referencedTableName);
                    }
                }
            }
        }
        
        public static TransactionScope CreateTransactionScope(System.Transactions.IsolationLevel isolationLevel = System.Transactions.IsolationLevel.ReadCommitted)
        {
            var options = new TransactionOptions { IsolationLevel = isolationLevel, Timeout = TimeSpan.MaxValue };
            return new TransactionScope(TransactionScopeOption.Required, options);
        }

        internal static string GetOperationText(Type objectType, string operation, OperationType operationType, string schema)
        {
            if (operationType == OperationType.StoredProcedure)
            {
                var namingConvention = ConfigurationFactory.Configuration.OperationNamingConvention;
                var typeName = objectType.Name;
                if (objectType.IsInterface && typeName[0] == 'I')
                {
                    typeName = typeName.Substring(1);
                }

                var procName = ConfigurationFactory.Configuration.OperationPrefix + typeName + "_" + operation;
                if (namingConvention == OperationNamingConvention.PrefixTypeNameOperation)
                {
                    procName = ConfigurationFactory.Configuration.OperationPrefix + typeName + operation;
                }
                else if (namingConvention == OperationNamingConvention.TypeName_Operation)
                {
                    procName = typeName + "_" + operation;
                }
                else if (namingConvention == OperationNamingConvention.TypeNameOperation)
                {
                    procName = typeName + operation;
                }
                else if (namingConvention == OperationNamingConvention.PrefixOperation_TypeName)
                {
                    procName = ConfigurationFactory.Configuration.OperationPrefix + operation + "_" + typeName;
                }
                else if (namingConvention == OperationNamingConvention.PrefixOperationTypeName)
                {
                    procName = ConfigurationFactory.Configuration.OperationPrefix + operation + typeName;
                }
                else if (namingConvention == OperationNamingConvention.Operation_TypeName)
                {
                    procName = operation + "_" + typeName;
                }
                else if (namingConvention == OperationNamingConvention.OperationTypeName)
                {
                    procName = operation + typeName;
                }
                else if (namingConvention == OperationNamingConvention.PrefixOperation)
                {
                    procName = ConfigurationFactory.Configuration.OperationPrefix + operation;
                }
                else if (namingConvention == OperationNamingConvention.Operation)
                {
                    procName = operation;
                }

                if (!string.IsNullOrEmpty(schema))
                {
                    procName = schema + "." + procName;
                }

                operation = procName;
            }
            return operation;
        }

        internal static string GetTableName<T>()
            where T : class
        {
            string tableName = null;
            
            var map = MappingFactory.GetEntityMap<T>();
            if (map != null)
            {
                tableName = map.TableName;
            }

            if (tableName == null)
            {
                var attr = Reflector.GetAttribute<T, TableAttribute>();
                if (attr != null)
                {
                    tableName = attr.Name;
                }
            }
            
            if (tableName == null)
            {
                var objectType = typeof(T);
                tableName = objectType.Name;
                if (objectType.IsInterface && tableName[0] == 'I')
                {
                    tableName = tableName.Substring(1);
                }
            }
            return tableName;
        }

        internal static string GetTableName(Type objectType)
        {
            string tableName = null;
            if (Reflector.IsEmitted(objectType))
            {
                objectType = Reflector.ExtractInterface(objectType);
            }

            var map = MappingFactory.GetEntityMap(objectType);
            if (map != null)
            {
                tableName = map.TableName;
            }

            if (tableName == null)
            {
                var attr = Reflector.GetAttribute<TableAttribute>(objectType);
                if (attr != null)
                {
                    tableName = attr.Name;
                }
            }

            if (tableName == null)
            {
                tableName = objectType.Name;
                if (objectType.IsInterface && tableName[0] == 'I')
                {
                    tableName = tableName.Substring(1);
                }
            }
            return tableName;
        }

        public static string[] GetPrimaryKeyProperties(Type interfaceType)
        {
            var propertyMap = Reflector.GetPropertyMap(interfaceType);
            return propertyMap.Values.Where(p => p.CanRead && p.IsPrimaryKey).Select(p => p.PropertyName).ToArray();
        }

        #endregion
    }
}
