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
using Nemo.Utilities;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Dynamic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Transactions;
using ObjectActivator = Nemo.Reflection.Activator.ObjectActivator;

namespace Nemo
{
    public static partial class ObjectFactory
    {
        #region Declarations

        public const string OperationRetrieve = "Retrieve";
        public const string OperationInsert = "Insert";
        public const string OperationUpdate = "Update";
        public const string OperationDelete = "Delete";
        public const string OperationDestroy = "Destroy";

        #endregion

        #region Instantiation Methods

        public static T Create<T>()
        {
            return Create<T>(typeof(T).IsInterface);
        }
        
        public static T Create<T>(bool isInterface)
        {
            var value = isInterface ? Adapter.Implement<T>() : FastActivator<T>.New();

            TrySetObjectState(value);

            return value;
        }

        public static object Create(Type targetType)
        {
            if (targetType == null) return null;

            var value = targetType.IsInterface ? Adapter.InternalImplement(targetType)() : Reflection.Activator.CreateDelegate(targetType)();

            TrySetObjectState(value);

            return value;
        }

        #endregion

        #region Map Methods

        public static IDictionary<string, object> ToDictionary(this object source)
        {
            var mapper = Mapper.CreateDelegate(source.GetType());
            var map = new Dictionary<string, object>();
            mapper(source, map);
            return map;
        }

        public static object Map(object source, Type targetType)
        {
            if (source == null) return null;
            var target = Create(targetType);
            var indexer = MappingFactory.IsIndexer(source);
            if (indexer)
            {
                var autoTypeCoercion = (ConfigurationFactory.Get(targetType)?.AutoTypeCoercion).GetValueOrDefault();
                Mapper.CreateDelegate(MappingFactory.GetIndexerType(source), targetType, indexer, autoTypeCoercion)(source, target);
            }
            else
            {
                Mapper.CreateDelegate(source.GetType(), targetType, indexer, false)(source, target);
            }
            return target;
        }

        internal static object Map(object source, Type targetType, bool autoTypeCoercion)
        {
            if (source == null) return null;
            var target = Create(targetType);
            var indexer = MappingFactory.IsIndexer(source);
            Mapper.CreateDelegate(indexer ? MappingFactory.GetIndexerType(source) : source.GetType(), targetType, indexer, autoTypeCoercion)(source, target);
            return target;
        }

        public static T Map<T>(object source)
        {
            return (T)Map(source, typeof(T));
        }

        internal static T Map<T>(object source, bool autoTypeCoercion)
        {
            return (T)Map(source, typeof(T), autoTypeCoercion);
        }

        public static TResult Map<TSource, TResult>(TSource source)
        {
            var target = Create<TResult>(typeof(TResult).IsInterface);
            return Map(source, target);
        }

        internal static TResult Map<TSource, TResult>(TSource source, bool autoTypeCoercion)
        {
            var target = Create<TResult>(typeof(TResult).IsInterface);
            return Map(source, target, autoTypeCoercion);
        }

        public static TResult Map<TSource, TResult>(TSource source, TResult target)
        {
            var indexer = MappingFactory.IsIndexer(source);

            if (indexer)
            {
                var autoTypeCoercion = (ConfigurationFactory.Get<TResult>()?.AutoTypeCoercion).GetValueOrDefault();
                if (autoTypeCoercion)
                {
                    if (source is IDataRecord record)
                    {
                        FastIndexerMapperWithTypeCoercion<IDataRecord, TResult>.Map(record, target);
                    }
                    else
                    {
                        FastIndexerMapperWithTypeCoercion<TSource, TResult>.Map(source, target);
                    }
                }
                else
                {
                    if (source is IDataRecord record)
                    {
                        FastIndexerMapper<IDataRecord, TResult>.Map(record, target);
                    }
                    else
                    {
                        FastIndexerMapper<TSource, TResult>.Map(source, target);
                    }
                }
            }
            else
            {
                FastMapper<TSource, TResult>.Map(source, target);
            }
            return target;
        }

        internal static TResult Map<TSource, TResult>(TSource source, TResult target, bool autoTypeCoercion)
        {
            var indexer = MappingFactory.IsIndexer(source);

            if (indexer)
            {
                if (autoTypeCoercion)
                {
                    if (source is IDataRecord record)
                    {
                        FastIndexerMapperWithTypeCoercion<IDataRecord, TResult>.Map(record, target);
                    }
                    else
                    {
                        FastIndexerMapperWithTypeCoercion<TSource, TResult>.Map(source, target);
                    }
                }
                else
                {
                    if (source is IDataRecord record)
                    {
                        FastIndexerMapper<IDataRecord, TResult>.Map(record, target);
                    }
                    else
                    {
                        FastIndexerMapper<TSource, TResult>.Map(source, target);
                    }
                }
            }
            else
            {
                FastMapper<TSource, TResult>.Map(source, target);
            }
            return target;
        }

        public static T Map<T>(IDictionary<string, object> source)
        {
            var target = Create<T>();
            Map(source, target);
            return target;
        }

        internal static T Map<T>(IDictionary<string, object> source, bool autoTypeCoercion)
        {
            var target = Create<T>();
            Map(source, target, autoTypeCoercion);
            return target;
        }

        public static void Map<T>(IDictionary<string, object> source, T target)
        {
            var autoTypeCoercion = (ConfigurationFactory.Get<T>()?.AutoTypeCoercion).GetValueOrDefault();
            Map(source, target, autoTypeCoercion);
        }

        internal static void Map<T>(IDictionary<string, object> source, T target, bool autoTypeCoercion)
        {
            if (autoTypeCoercion)
            {
                FastIndexerMapperWithTypeCoercion<IDictionary<string, object>, T>.Map(source, target ?? throw new ArgumentNullException(nameof(target)));
            }
            else
            {
               FastIndexerMapper<IDictionary<string, object>, T>.Map(source, target ?? throw new ArgumentNullException(nameof(target)));
            }
        }

        public static T Map<T>(DataRow source)
        {
            var target = Create<T>();
            Map(source, target);
            return target;
        }

        internal static T Map<T>(DataRow source, bool autoTypeCoercion)
        {
            var target = Create<T>();
            Map(source, target, autoTypeCoercion);
            return target;
        }

        public static void Map<T>(DataRow source, T target)
        {
            var autoTypeCoercion = (ConfigurationFactory.Get<T>()?.AutoTypeCoercion).GetValueOrDefault();
            Map(source, target, autoTypeCoercion);
        }

        internal static void Map<T>(DataRow source, T target, bool autoTypeCoercion)
        {
            if (autoTypeCoercion)
            {
                FastIndexerMapperWithTypeCoercion<DataRow, T>.Map(source, target ?? throw new ArgumentNullException(nameof(target)));
            }
            else
            {
                FastIndexerMapper<DataRow, T>.Map(source, target ?? throw new ArgumentNullException(nameof(target)));
            }
        }

        public static T Map<T>(IDataReader source)
        {
            return Map<T>((IDataRecord)source);
        }

        internal static T Map<T>(IDataReader source, bool autoTypeCoercion)
        {
            return Map<T>((IDataRecord)source, autoTypeCoercion);
        }

        public static void Map<T>(IDataReader source, T target)
        {
            Map((IDataRecord)source, target);
        }

        internal static void Map<T>(IDataReader source, T target, bool autoTypeCoercion)
        {
            Map((IDataRecord)source, target, autoTypeCoercion);
        }

        public static T Map<T>(IDataRecord source)
        {
            var target = Create<T>();
            Map(source, target);
            return target;
        }

        internal static T Map<T>(IDataRecord source, bool autoTypeCoercion)
        {
            var target = Create<T>();
            Map(source, target, autoTypeCoercion);
            return target;
        }

        public static void Map<T>(IDataRecord source, T target)
        {
            var autoTypeCoercion = (ConfigurationFactory.Get<T>()?.AutoTypeCoercion).GetValueOrDefault();
            Map(source, target, autoTypeCoercion);
        }

        internal static void Map<T>(IDataRecord source, T target, bool autoTypeCoercion)
        {
            if (autoTypeCoercion)
            {
                FastIndexerMapperWithTypeCoercion<IDataRecord, T>.Map(source, target ?? throw new ArgumentNullException(nameof(target)));
            }
            else
            {
                FastIndexerMapper<IDataRecord, T>.Map(source, target ?? throw new ArgumentNullException(nameof(target)));
            }
        }

        #endregion

        #region Bind Methods

        /// <summary>
        /// Binds interface implementation to the existing object type.
        /// </summary>
        public static T Bind<T>(object source)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            var target = Adapter.Bind<T>(source);
            return target;
        }

        public static object Bind(object source, Type targetType)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            var target = Adapter.Bind(source, targetType);
            return target;
        }

        #endregion

        #region Wrap Methods

        /// <summary>
        /// Converts a dictionary to an instance of the object specified by the interface.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="map"></param>
        /// <param name="simpleAndComplexProperties"></param>
        /// <returns></returns>
        public static T Wrap<T>(IDictionary<string, object> map, bool simpleAndComplexProperties = false)
        {
            var wrapper = simpleAndComplexProperties ? FastComplexWrapper<T>.Instance : FastWrapper<T>.Instance;
            return (T)wrapper(map);
        }

        public static object Wrap(IDictionary<string, object> value, Type targetType, bool simpleAndComplexProperties = false)
        {
            return Adapter.Wrap(value, targetType, simpleAndComplexProperties);
        }

        #endregion

        #region Insert/Update/Delete/Execute Methods
        
        private static IEnumerable<OperationRequest> BuildBatchInsert<T>(IEnumerable<T> items, DbTransaction transaction, bool captureException, IDictionary<PropertyInfo, ReflectedProperty> propertyMap, DialectProvider provider, IConfiguration config, int batchSize = 500)
             where T : class
        {
            var statementId = 0;
            var insertSql = new StringBuilder();
            var insertParameters = new List<Param>();

            var batches = items.Split(batchSize <= 0 ? 500 : batchSize);
            foreach (var batch in batches)
            {
                var entities = batch as T[] ?? batch.ToArray();
                entities.GenerateKeys(config);

                foreach (var item in entities)
                {
                    var parameters = ObjectExtensions.GetInsertParameters(item, propertyMap, statementId++);
                    var sql = SqlBuilder.GetInsertStatement(typeof(T), parameters, provider);
                    insertSql.Append(sql).AppendLine(";");
                    insertParameters.AddRange(parameters);
                }

                var request = new OperationRequest
                {
                    Parameters = insertParameters,
                    ReturnType = OperationReturnType.NonQuery,
                    Transaction = transaction,
                    CaptureException = captureException,
                    OperationType = OperationType.Sql,
                    Operation = insertSql.ToString(),
                    Configuration = config
                };

                yield return request;
            }
        }

        public static long Insert<T>(IEnumerable<T> items, string connectionName = null, DbConnection connection = null, DbTransaction transaction = null, bool captureException = false, IConfiguration config = null)
            where T : class
        {
            var count = 0L;
            var connectionOpenedHere = false;
            var externalTransaction = transaction != null;
            var externalConnection = externalTransaction || connection != null;

            if (config == null)
            {
                config = ConfigurationFactory.Get<T>();
            }

            if (externalTransaction)
            {
                connection = transaction.Connection;
            }
            if (!externalConnection)
            {
                connection = DbFactory.CreateConnection(connectionName ?? config.DefaultConnectionName, config);
            }

            try
            {
                if (connection.State != ConnectionState.Open)
                {
                    connection.Open();
                    connectionOpenedHere = true;
                }
                if (transaction == null)
                {
                    transaction = connection.BeginTransaction();
                }

                var propertyMap = Reflector.GetPropertyMap<T>();
                var provider = DialectFactory.GetProvider(transaction.Connection);

                var requests = BuildBatchInsert(items, transaction, captureException, propertyMap, provider, config);
                count = requests.Select(Execute<T>).Where(response => !response.HasErrors).Aggregate(count, (current, response) => current + response.RecordsAffected);
                transaction.Commit();

                return count;
            }
            catch (Exception ex)
            {
                transaction?.Rollback();
                throw;
            }
            finally
            {
                if (connectionOpenedHere)
                {
                    connection.Clone();
                }

                if (!externalConnection)
                {
                    connection.Dispose();
                }
            }
        }

        public static OperationResponse Insert<T>(ParamList parameters, string connectionName = null, bool captureException = false, string schema = null, DbConnection connection = null, IConfiguration config = null)
            where T : class
        {
            return Insert<T>(parameters.GetParameters(), connectionName, captureException, schema, connection, config);
        }

        public static OperationResponse Insert<T>(Param[] parameters, string connectionName = null, bool captureException = false, string schema = null, DbConnection connection = null, IConfiguration config = null)
            where T : class
        {
            config ??= ConfigurationFactory.Get<T>();

            var request = new OperationRequest { Parameters = parameters, ReturnType = OperationReturnType.NonQuery, ConnectionName = connectionName, Connection = connection, CaptureException = captureException, Configuration = config };
            
            if (config.GenerateInsertSql)
            {
                request.Operation = SqlBuilder.GetInsertStatement(typeof(T), parameters, request.Connection != null ? DialectFactory.GetProvider(request.Connection) : DialectFactory.GetProvider(request.ConnectionName ?? config.DefaultConnectionName, config));
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

        public static OperationResponse Update<T>(ParamList parameters, string connectionName = null, bool captureException = false, string schema = null, DbConnection connection = null, IConfiguration config = null)
            where T : class
        {
            return Update<T>(parameters.GetParameters(), connectionName, captureException, schema, connection, config);
        }

        public static OperationResponse Update<T>(Param[] parameters, string connectionName = null, bool captureException = false, string schema = null, DbConnection connection = null, IConfiguration config = null)
            where T : class
        {
            config ??= ConfigurationFactory.Get<T>();

            var request = new OperationRequest { Parameters = parameters, ReturnType = OperationReturnType.NonQuery, ConnectionName = connectionName, Connection = connection, CaptureException = captureException, Configuration = config };
            
            if (config.GenerateUpdateSql)
            {
                var partition = parameters.Partition(p => p.IsPrimaryKey);
                // if p.IsPrimaryKey is not set then
                // we need to infer it from reflected property 
                if (partition.Item1.Count == 0)
                {
                    var propertyMap = Reflector.GetPropertyMap<T>();
                    var pimaryKeySet = propertyMap.Values.Where(p => p.IsPrimaryKey).ToDictionary(p => p.ParameterName ?? p.PropertyName, p => p.MappedColumnName);
                    partition = parameters.Partition(p =>
                    {
                        if (!pimaryKeySet.TryGetValue(p.Name, out var column)) return false;
                        p.Source = column;
                        p.IsPrimaryKey = true;
                        return true;
                    });
                }

                request.Operation = SqlBuilder.GetUpdateStatement(typeof(T), partition.Item2, partition.Item1, request.Connection != null ? DialectFactory.GetProvider(request.Connection) : DialectFactory.GetProvider(request.ConnectionName ?? config.DefaultConnectionName, config));
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

        public static OperationResponse Delete<T>(ParamList parameters, string connectionName = null, bool captureException = false, string schema = null, DbConnection connection = null, IConfiguration config = null)
            where T : class
        {
            return Delete<T>(parameters.GetParameters(), connectionName, captureException, schema, connection, config);
        }

        public static OperationResponse Delete<T>(Param[] parameters, string connectionName = null, bool captureException = false, string schema = null, DbConnection connection = null, IConfiguration config = null)
            where T : class
        {
            config ??= ConfigurationFactory.Get<T>();

            var request = new OperationRequest { Parameters = parameters, ReturnType = OperationReturnType.NonQuery, ConnectionName = connectionName, Connection = connection, CaptureException = captureException, Configuration = config };

            if (config.GenerateDeleteSql)
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

                var partition = parameters.Partition(p => p.IsPrimaryKey);
                // if p.IsPrimaryKey is not set then
                // we need to infer it from reflected property 
                if (partition.Item1.Count == 0)
                {
                    var propertyMap = Reflector.GetPropertyMap<T>();
                    var pimaryKeySet = propertyMap.Values.Where(p => p.IsPrimaryKey).ToDictionary(p => p.ParameterName ?? p.PropertyName, p => p.MappedColumnName);
                    partition = parameters.Partition(p =>
                    {
                        if (!pimaryKeySet.TryGetValue(p.Name, out var column)) return false;
                        p.Source = column;
                        p.IsPrimaryKey = true;
                        return true;
                    });
                }
                
                request.Operation = SqlBuilder.GetDeleteStatement(typeof(T), partition.Item1, request.Connection != null ? DialectFactory.GetProvider(request.Connection) : DialectFactory.GetProvider(request.ConnectionName ?? config.DefaultConnectionName, config), softDeleteColumn);
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

        public static OperationResponse Destroy<T>(ParamList parameters, string connectionName = null, bool captureException = false, string schema = null, DbConnection connection = null, IConfiguration config = null)
            where T : class
        {
            return Destroy<T>(parameters.GetParameters(), connectionName, captureException, schema, connection, config);
        }

        public static OperationResponse Destroy<T>(Param[] parameters, string connectionName = null, bool captureException = false, string schema = null, DbConnection connection = null, IConfiguration config = null)
            where T : class
        {
            config ??= ConfigurationFactory.Get<T>();

            var request = new OperationRequest { Parameters = parameters, ReturnType = OperationReturnType.NonQuery, ConnectionName = connectionName, Connection = connection, CaptureException = captureException, Configuration = config };

            if (config.GenerateDeleteSql)
            {
                request.Operation = SqlBuilder.GetDeleteStatement(typeof(T), parameters, request.Connection != null ? DialectFactory.GetProvider(request.Connection) : DialectFactory.GetProvider(request.ConnectionName ?? config.DefaultConnectionName, config));
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

        internal static OperationResponse Execute(string operationText, IEnumerable<Param> parameters, OperationReturnType returnType, OperationType operationType, IList<Type> types = null, string connectionName = null, DbConnection connection = null, DbTransaction transaction = null, bool captureException = false, string schema = null, IConfiguration config = null)
        {
            var rootType = types?[0];

            DbConnection dbConnection;
            var closeConnection = false;

            if (transaction != null)
            {
                dbConnection = transaction.Connection;
                closeConnection = dbConnection.State != ConnectionState.Open;
            }
            else if (connection != null)
            {
                dbConnection = connection;
                closeConnection = dbConnection.State != ConnectionState.Open;
            }
            else
            {
                dbConnection = DbFactory.CreateConnection(connectionName, rootType, config);
                closeConnection = true;
            }

            var dialect = new Lazy<DialectProvider>(() => DialectFactory.GetProvider(dbConnection));

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

            var command = dbConnection.CreateCommand();
            command.CommandText = operationText;
            command.CommandType = operationType == OperationType.StoredProcedure ? CommandType.StoredProcedure : CommandType.Text;
            command.CommandTimeout = 0;
            var outputParameters = DbFactory.SetupParameters(command, parameters, dialect, config);

            if (dbConnection.State != ConnectionState.Open)
            {
                dbConnection.Open();
            }

            var response = new OperationResponse { ReturnType = returnType };
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
                        switch (returnType)
                        {
                            case OperationReturnType.SingleResult:
                                behavior = CommandBehavior.SingleResult;
                                break;
                            case OperationReturnType.SingleRow:
                                behavior = CommandBehavior.SingleRow;
                                break;
                        }

                        if (closeConnection)
                        {
                            behavior |= CommandBehavior.CloseConnection;
                        }

                        closeConnection = false;
                        response.Value = command.ExecuteReader(behavior);
                        break;
                    case OperationReturnType.Scalar:
                        response.Value = command.ExecuteScalar();
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
                operationType = GuessOperationType(request.Operation);
            }

            var config = request.Configuration ?? ConfigurationFactory.Get<T>();

            var operationText = GetOperationText(typeof(T), request.Operation, request.OperationType, request.SchemaName, config);

            var response = request.Connection != null 
                ? Execute(operationText, request.Parameters, request.ReturnType, operationType, request.Types, connection: request.Connection, transaction: request.Transaction, captureException: request.CaptureException, schema: request.SchemaName, config: config) 
                : Execute(operationText, request.Parameters, request.ReturnType, operationType, request.Types, request.ConnectionName, transaction: request.Transaction, captureException: request.CaptureException, schema: request.SchemaName, config: config);
            return response;
        }

        public static OperationResponse Execute(OperationRequest request)
        {
            var operationType = request.OperationType;
            if (operationType == OperationType.Guess)
            {
                operationType = GuessOperationType(request.Operation);
            }

            var config = request.Configuration ?? ConfigurationFactory.DefaultConfiguration;

            var operationText = GetOperationText(null, request.Operation, request.OperationType, request.SchemaName, config);

            var response = request.Connection != null
                ? Execute(operationText, request.Parameters, request.ReturnType, operationType, request.Types, connection: request.Connection, transaction: request.Transaction, captureException: request.CaptureException, schema: request.SchemaName, config: config)
                : Execute(operationText, request.Parameters, request.ReturnType, operationType, request.Types, request.ConnectionName, transaction: request.Transaction, captureException: request.CaptureException, schema: request.SchemaName, config: config);
            return response;
        }

        public static OperationResponse ExecuteSql(string sql, bool nonQuery, object parameters = null, string connectionName = null, DbConnection connection = null, bool captureException = false, IConfiguration config = null)
        {
            var request = new OperationRequest
            {
                Operation = sql,
                Parameters = ExtractParameters(parameters),
                ConnectionName = connectionName,
                Connection = connection,
                Configuration = config,
                OperationType = OperationType.Sql,
                ReturnType = nonQuery ? OperationReturnType.NonQuery : OperationReturnType.MultiResult,
                CaptureException = captureException
            };
            return Execute(request);
        }

        public static OperationResponse ExecuteProcedure(string procedure, bool nonQuery, object parameters = null, string connectionName = null, DbConnection connection = null, bool captureException = false, IConfiguration config = null)
        {
            var request = new OperationRequest
            {
                Operation = procedure,
                Parameters = ExtractParameters(parameters),
                ConnectionName = connectionName,
                Connection = connection,
                Configuration = config,
                OperationType = OperationType.StoredProcedure,
                ReturnType = nonQuery ? OperationReturnType.NonQuery : OperationReturnType.MultiResult,
                CaptureException = captureException
            };
            return Execute(request);
        }

        private static OperationType GuessOperationType(string operation)
        {
            return operation.Any(char.IsWhiteSpace) ? OperationType.Sql : OperationType.StoredProcedure;
        }

        #endregion

        #region Translate Methods

        public static IEnumerable<T> Translate<T>(OperationResponse response, IConfiguration config)
        {
            config ??= ConfigurationFactory.Get<T>();
            var reflectedType = Reflector.GetReflectedType<T>();
            return Translate<T>(response, null, null, reflectedType.IsInterface, reflectedType.IsSimpleType, config, Identity.Get<T>(config));
        }

        private static IEnumerable<T> Translate<T>(OperationResponse response, Func<object[], T> map, IList<Type> types, bool isInterface, bool isSimpleType, IConfiguration config, IIdentityMap identityMap)
        {
            var cached = config.DefaultCacheRepresentation != CacheRepresentation.None;
            var mode = config.DefaultMaterializationMode;

            var value = response?.Value;
            if (value == null)
            {
                return Enumerable.Empty<T>();
            }

            switch (value)
            {
                case IDataReader reader:
                    if (map != null || types == null || types.Count == 1) return ConvertDataReader(reader, map, types, isInterface, isSimpleType, config);
                    var multiResultItems = ConvertDataReaderMultiResult(reader, types, config);
                    return (IEnumerable<T>)MultiResult.Create(types, multiResultItems, cached, config);
                case DataSet dataSet:
                    return ConvertDataSet<T>(dataSet, isInterface, isSimpleType, config, identityMap);
                case DataTable dataTable:
                    return ConvertDataTable<T>(dataTable, isInterface, isSimpleType, config, identityMap);
                case DataRow dataRow:
                    return ConvertDataRow<T>(dataRow, isInterface, isSimpleType, config, identityMap, null).Return();
                case T item:
                    return item.Return();
                case IList<T> genericList:
                    return genericList;
                case IList list:
                    return list.Cast<object>().Select(i => mode == MaterializationMode.Exact ? Map<T>(i, config.AutoTypeCoercion) : Bind<T>(i));
            }

            return Bind<T>(value).Return();
        }

        private static IEnumerable<T> ConvertDataSet<T>(DataSet dataSet, bool isInterface, bool isSimpleType, IConfiguration config, IIdentityMap identityMap)
        {
            var tableName = GetTableName(typeof(T));
            return dataSet.Tables.Count != 0 ? ConvertDataTable<T>(dataSet.Tables.Contains(tableName) ? dataSet.Tables[tableName] : dataSet.Tables[0], isInterface, isSimpleType, config, identityMap) : Enumerable.Empty<T>();
        }

        private static IEnumerable<T> ConvertDataTable<T>(DataTable table, bool isInterface, bool isSimpleType, IConfiguration config, IIdentityMap identityMap)
        {
            return ConvertDataTable<T>(table.Rows.Cast<DataRow>(), isInterface, isSimpleType, config, identityMap);
        }

        private static IEnumerable<T> ConvertDataTable<T>(IEnumerable<DataRow> table, bool isInterface, bool isSimpleType, IConfiguration config, IIdentityMap identityMap)
        {
            var primaryKey = GetPrimaryKeyColumns(typeof(T));
            return table.Select(row => ConvertDataRow<T>(row, isInterface, isSimpleType, config, identityMap, primaryKey));
        }

        private static IEnumerable<object> ConvertDataTable(IEnumerable<DataRow> table, Type targetType, bool isInterface, bool isSimpleType, IConfiguration config, IIdentityMap identityMap)
        {
            var primaryKey = GetPrimaryKeyColumns(targetType);
            return table.Select(row => ConvertDataRow(row, targetType, isInterface, isSimpleType, config, identityMap, primaryKey));
        }
        
        private static T ConvertDataRow<T>(DataRow row, bool isInterface, bool isSimpleType, IConfiguration config, IIdentityMap identityMap, string[] primaryKey)
        {
            var result = identityMap.GetEntityByKey<DataRow, T>(row.GetKeySelector(primaryKey), out var hash);

            if (result != null) return result;

            T value;
            if (isSimpleType)
            {
                value = row[0].SafeCast<T>();
            }
            else if (typeof(T) == typeof(object))
            {
                value = (T)GetSerializableDataRow(row, true);
            }
            else if (config.DefaultMaterializationMode == MaterializationMode.Exact || !isInterface)
            {
                value = Create<T>(isInterface);
                Map(row, value, config.AutoTypeCoercion);
            }
            else
            {
                value = Wrap<T>(GetSerializableDataRow(row, false));
            }

            TrySetObjectState(value);

            // Write-through for identity map
            identityMap.WriteThrough(value, hash);

            LoadRelatedData(row, value, typeof(T), config, identityMap, primaryKey);
            
            return value;
        }

        private static object ConvertDataRow(DataRow row, Type targetType, bool isInterface, bool isSimpleType, IConfiguration config, IIdentityMap identityMap, string[] primaryKey)
        {
            string hash = null;

            if (identityMap != null)
            {
                var primaryKeyValue = new SortedDictionary<string, object>(primaryKey.ToDictionary(k => k, k => row[k]), StringComparer.Ordinal);
                hash = primaryKeyValue.ComputeHash(targetType);

                if (identityMap.TryGetValue(hash, out var result))
                {
                    return result;
                }
            }

            object value;
            if (isSimpleType)
            {
                value = Reflector.ChangeType(row[0], targetType);
            }
            else if (targetType == typeof(object))
            {
                value = GetSerializableDataRow(row, true);
            }
            else if (config.DefaultMaterializationMode == MaterializationMode.Exact || !isInterface)
            {
                value = Map((object)row, targetType, config.AutoTypeCoercion);
            }
            else
            {
                value = Wrap(GetSerializableDataRow(row, false), targetType);
            }

            TrySetObjectState(value);

            // Write-through for identity map
            identityMap.WriteThrough(value, hash);

            LoadRelatedData(row, value, targetType, config, identityMap, primaryKey);

            return value;
        }

        private static void LoadRelatedData(DataRow row, object value, Type targetType, IConfiguration config, IIdentityMap identityMap, string[] primaryKey)
        {
            var table = row.Table;
            if (table.ChildRelations.Count <= 0) return;

            var propertyMap = Reflector.GetPropertyMap(targetType);
            var relations = table.ChildRelations.Cast<DataRelation>().ToArray();

            primaryKey = primaryKey ?? GetPrimaryKeyColumns(targetType);
            
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
                    var propertyReflectedType = Reflector.GetReflectedType(p.Key.PropertyType);
                    var propertyKey = GetPrimaryKeyColumns(p.Key.PropertyType);
                    IIdentityMap relatedIdentityMap = null;
                    if (identityMap != null)
                    {
                        relatedIdentityMap = Identity.Get(p.Key.PropertyType, config);
                    }
                    propertyValue = ConvertDataRow(childRows[0], p.Key.PropertyType, propertyReflectedType.IsInterface, propertyReflectedType.IsSimpleType, config, relatedIdentityMap, propertyKey);
                }
                else if (p.Value.IsDataEntityList)
                {
                    var elementType = p.Value.ElementType;
                    if (elementType != null)
                    {
                        var propertyReflectedType = Reflector.GetReflectedType(elementType);
                        IIdentityMap relatedIdentityMap = null;
                        if (identityMap != null)
                        {
                            relatedIdentityMap = Identity.Get(elementType, config);
                        }

                        var items = ConvertDataTable(childRows, elementType, propertyReflectedType.IsInterface, propertyReflectedType.IsSimpleType, config, relatedIdentityMap);
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

        private static IEnumerable<T> ConvertDataReader<T>(IDataReader reader, Func<object[], T> map, IList<Type> types, bool isInterface, bool isSimpleType, IConfiguration config)
        {
            try
            {
                var isAccumulator = false;
                var count = reader.FieldCount;
                var references = new Dictionary<Tuple<Type, string>, object>();
                var useMapper = !isInterface || config.DefaultMaterializationMode == MaterializationMode.Exact;
                var columns = !isSimpleType ? reader.GetColumns() : null;
                var isAnonymous = typeof(T) == typeof(object);
                while (reader.Read())
                {
                    if (isSimpleType)
                    {
                        yield return reader.GetValue(0).SafeCast<T>();
                    }
                    else if (isAnonymous)
                    {
                        var bag = CreateDynamicItem(reader, columns, true);
                        yield return (T)bag;
                    }
                    else if (useMapper)
                    {
                        var item = Create<T>(isInterface);
                        var record = (IDataRecord)new WrappedRecord(reader, columns);
                        Map(record, item, config.AutoTypeCoercion);
                        
                        TrySetObjectState(item);

                        if (map != null)
                        {
                            var args = new object[types.Count];
                            args[0] = item;
                            for (var i = 1; i < types.Count; i++)
                            {
                                var identity = CreateIdentity(types[i], reader);
                                if (!references.TryGetValue(identity, out var reference))
                                {
                                    reference = Map((object)record, types[i], config.AutoTypeCoercion);
                                    TrySetObjectState(reference);
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
                        var bag = CreateDynamicItem(reader, columns, false);
                        var item = (T)Wrap(bag, typeof(T));

                        TrySetObjectState(item);

                        if (map != null)
                        {
                            var args = new object[types.Count];
                            args[0] = item;
                            for (var i = 1; i < types.Count; i++)
                            {
                                var identity = CreateIdentity(types[i], reader);
                                if (!references.TryGetValue(identity, out var reference))
                                {
                                    reference = Wrap(bag, types[i]);
                                    TrySetObjectState(reference);
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

        private static IDictionary<string, object> CreateDynamicItem(IDataReader reader, ISet<string> columns, bool isDynamic)
        {
            IDictionary<string, object> bag = !isDynamic ? new Dictionary<string, object>() : new ExpandoObject();
            foreach (var column in columns)
            {
                bag[isDynamic ? NormalizeDynamicPropertyName(column) : column] = reader[column];
            }
            return bag;
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

        private static IEnumerable<MultiResultItem> ConvertDataReaderMultiResult(IDataReader reader, IList<Type> types, IConfiguration config)
        {
            var skipNext = false;
            void changeSkipNext()
            {
                skipNext = true;
            }

            var reflectedTypes = types.Select(t => Reflector.GetReflectedType(t)).ToList();

            try
            {
                int resultIndex = 0;
                do
                {
                    var isInterface = reflectedTypes[resultIndex].IsInterface;
                    var isAnonymous = types[resultIndex] == typeof(object);
                    var isSimpleType = reflectedTypes[resultIndex].IsSimpleType;
                    var useMapper = !isInterface || config.DefaultMaterializationMode == MaterializationMode.Exact;
                    var columns = !isSimpleType ? reader.GetColumns() : null;
                    while (reader.Read())
                    {
                        if (isSimpleType)
                        {
                            var item = reader.GetValue(0);
                            yield return new MultiResultItem { Item = Reflector.ChangeType(item, types[resultIndex]), ItemType = types[resultIndex], ItemTypeIndex = resultIndex, SkipNextCallback = changeSkipNext };
                        }
                        else if (isAnonymous)
                        {
                            var item = CreateDynamicItem(reader, columns, true);
                            yield return new MultiResultItem { Item = item, ItemType = types[resultIndex], ItemTypeIndex = resultIndex, SkipNextCallback = changeSkipNext };
                        }
                        else if (useMapper)
                        {
                            var item = Map((object)new WrappedReader(reader, columns), types[resultIndex], config.AutoTypeCoercion);
                            TrySetObjectState(item);
                            yield return new MultiResultItem { Item = item, ItemType = types[resultIndex], ItemTypeIndex = resultIndex, SkipNextCallback = changeSkipNext };
                        }
                        else
                        {
                            var bag = CreateDynamicItem(reader, columns, false);
                            var item = Wrap(bag, types[resultIndex]);
                            TrySetObjectState(item);
                            yield return new MultiResultItem { Item = item, ItemType = types[resultIndex], ItemTypeIndex = resultIndex, SkipNextCallback = changeSkipNext };
                        }

                        if (skipNext)
                        {
                            skipNext = false;
                            break;
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

        internal static void TrySetObjectState(object item, ObjectState state = ObjectState.Clean)
        {
            if (item is ITrackableDataEntity entity)
            {
                entity.ObjectState = state;
            }
        }
        
        private static IDictionary<string, object> GetSerializableDataRow(DataRow row, bool isDynamic)
        {
            IDictionary<string, object> result = !isDynamic ? new Dictionary<string, object>(StringComparer.Ordinal) : new ExpandoObject();
            if (row != null)
            {
                foreach (DataColumn column in row.Table.Columns)
                {
                    result.Add(isDynamic ? NormalizeDynamicPropertyName(column.ColumnName) : column.ColumnName, row[column]);
                }
            }
            return result;
        }

        private static string NormalizeDynamicPropertyName(string name)
        {
            var output = new StringBuilder(name.Trim());
            var index = 0;
            foreach (var ch in name)
            {
                if (index == 0 && !char.IsLetter(ch) && ch != '_')
                {
                    output.Append('_');
                }
                if (char.IsLetterOrDigit(ch) || ch == '_')
                {
                    output.Append(ch);
                }
                else
                {
                    output.Append('_');
                }
                index++;
            }

            return output.ToString();
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
            var references = propertyMap.Where(p => p.Value.IsDataEntity || p.Value.IsDataEntityList || p.Value.IsObject || p.Value.IsObjectList).Select(p => p.Value);
            foreach (var reference in references)
            {
                var elementType = (reference.IsDataEntityList || reference.IsObjectList) ? reference.ElementType : reference.PropertyType;

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

        internal static string GetOperationText(Type objectType, string operation, OperationType operationType, string schema, IConfiguration config)
        {
            if (operationType != OperationType.StoredProcedure) return operation;

            var namingConvention = config.OperationNamingConvention;

            string typeName = null;
            if (objectType != null)
            {
                typeName = objectType.Name;
                if (objectType.IsInterface && typeName[0] == 'I')
                {
                    typeName = typeName.Substring(1);
                }
            }
            
            var procName = operation;

            if (string.IsNullOrEmpty(typeName))
            {
                switch (namingConvention)
                {
                    case OperationNamingConvention.PrefixOperation:
                        procName = config.OperationPrefix + operation;
                        break;
                    case OperationNamingConvention.Operation:
                        procName = operation;
                        break;
                }
            }
            else
            {
                switch (namingConvention)
                {
                    case OperationNamingConvention.PrefixTypeName_Operation:
                        procName = config.OperationPrefix + typeName + "_" + operation;
                        break;
                    case OperationNamingConvention.PrefixTypeNameOperation:
                        procName = config.OperationPrefix + typeName + operation;
                        break;
                    case OperationNamingConvention.TypeName_Operation:
                        procName = typeName + "_" + operation;
                        break;
                    case OperationNamingConvention.TypeNameOperation:
                        procName = typeName + operation;
                        break;
                    case OperationNamingConvention.PrefixOperation_TypeName:
                        procName = config.OperationPrefix + operation + "_" + typeName;
                        break;
                    case OperationNamingConvention.PrefixOperationTypeName:
                        procName = config.OperationPrefix + operation + typeName;
                        break;
                    case OperationNamingConvention.Operation_TypeName:
                        procName = operation + "_" + typeName;
                        break;
                    case OperationNamingConvention.OperationTypeName:
                        procName = operation + typeName;
                        break;
                    case OperationNamingConvention.PrefixOperation:
                        procName = config.OperationPrefix + operation;
                        break;
                    case OperationNamingConvention.Operation:
                        procName = operation;
                        break;
                }
            }

            if (!string.IsNullOrEmpty(schema))
            {
                procName = schema + "." + procName;
            }

            operation = procName;
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
                objectType = Reflector.GetInterface(objectType);
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

        internal static string GetUnmappedTableName(Type objectType)
        {
            string tableName = null;
            if (Reflector.IsEmitted(objectType))
            {
                objectType = Reflector.GetInterface(objectType);
            }

            var attr = Reflector.GetAttribute<TableAttribute>(objectType);
            if (attr != null)
            {
                tableName = attr.Name;
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

        public static string[] GetPrimaryKeyProperties(Type objectType)
        {
            var propertyMap = Reflector.GetPropertyMap(objectType);
            return propertyMap.Values.Where(p => p.CanRead && p.IsPrimaryKey).Select(p => p.PropertyName).ToArray();
        }

        private static string[] GetPrimaryKeyColumns(Type objectType)
        {
            var propertyMap = Reflector.GetPropertyMap(objectType);
            return propertyMap.Values.Where(p => p.CanRead && p.IsPrimaryKey).Select(p => p.MappedColumnName ?? p.PropertyName).ToArray();
        }

        #endregion
    }
}
