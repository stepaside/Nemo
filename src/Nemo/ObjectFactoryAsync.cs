using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
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

namespace Nemo
{
    public static partial class ObjectFactory
    {
        #region Insert/Update/Delete/Execute Methods

        public static async Task<long> InsertAsync<T>(IEnumerable<T> items, string connectionName = null, DbConnection connection = null, DbTransaction transaction = null, bool captureException = false, IConfiguration config = null)
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
                foreach (var request in requests)
                {
                    var response = await ExecuteAsync<T>(request).ConfigureAwait(false);
                    if (!response.HasErrors)
                    {
                        count += response.RecordsAffected;
                    }
                }
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

        public static async Task<OperationResponse> InsertAsync<T>(ParamList parameters, string connectionName = null, bool captureException = false, string schema = null, DbConnection connection = null, IConfiguration config = null)
            where T : class
        {
            return await InsertAsync<T>(parameters.GetParameters(), connectionName, captureException, schema, connection, config).ConfigureAwait(false);
        }

        public static async Task<OperationResponse> InsertAsync<T>(Param[] parameters, string connectionName = null, bool captureException = false, string schema = null, DbConnection connection = null, IConfiguration config = null)
            where T : class
        {
            config ??= ConfigurationFactory.Get<T>();

            var request = new OperationRequest { Parameters = parameters, ReturnType = OperationReturnType.NonQuery, ConnectionName = connectionName, Connection = null, CaptureException = captureException, Configuration = config };

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
            var response = await ExecuteAsync<T>(request).ConfigureAwait(false);
            return response;
        }

        public static async Task<OperationResponse> UpdateAsync<T>(ParamList parameters, string connectionName = null, bool captureException = false, string schema = null, DbConnection connection = null, IConfiguration config = null)
            where T : class
        {
            return await UpdateAsync<T>(parameters.GetParameters(), connectionName, captureException, schema, connection, config).ConfigureAwait(false);
        }

        public static async Task<OperationResponse> UpdateAsync<T>(Param[] parameters, string connectionName = null, bool captureException = false, string schema = null, DbConnection connection = null, IConfiguration config = null)
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
                        if (pimaryKeySet.TryGetValue(p.Name, out var column))
                        {
                            p.Source = column;
                            p.IsPrimaryKey = true;
                            return true;
                        }
                        return false;
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
            var response = await ExecuteAsync<T>(request).ConfigureAwait(false);
            return response;
        }

        public static async Task<OperationResponse> DeleteAsync<T>(ParamList parameters, string connectionName = null, bool captureException = false, string schema = null, DbConnection connection = null, IConfiguration config = null)
            where T : class
        {
            return await DeleteAsync<T>(parameters.GetParameters(), connectionName, captureException, schema, connection, config).ConfigureAwait(false);
        }

        public static async Task<OperationResponse> DeleteAsync<T>(Param[] parameters, string connectionName = null, bool captureException = false, string schema = null, DbConnection connection = null, IConfiguration config = null)
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
                        if (pimaryKeySet.TryGetValue(p.Name, out var column))
                        {
                            p.Source = column;
                            p.IsPrimaryKey = true;
                            return true;
                        }
                        return false;
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
            var response = await ExecuteAsync<T>(request).ConfigureAwait(false);
            return response;
        }

        public static async Task<OperationResponse> DestroyAsync<T>(ParamList parameters, string connectionName = null, bool captureException = false, string schema = null, DbConnection connection = null, IConfiguration config = null)
            where T : class
        {
            return await DestroyAsync<T>(parameters.GetParameters(), connectionName, captureException, schema, connection, config).ConfigureAwait(false);
        }

        public static async Task<OperationResponse> DestroyAsync<T>(Param[] parameters, string connectionName = null, bool captureException = false, string schema = null, DbConnection connection = null, IConfiguration config = null)
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
            var response = await ExecuteAsync<T>(request).ConfigureAwait(false);
            return response;
        }

        internal static async Task<OperationResponse> ExecuteAsync(string operationText, IEnumerable<Param> parameters, OperationReturnType returnType, OperationType operationType, IList<Type> types = null, string connectionName = null, DbConnection connection = null, DbTransaction transaction = null, bool captureException = false, string schema = null, string connectionStringSection = "ConnectionStrings", IConfiguration config = null)
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
                await dbConnection.OpenAsync().ConfigureAwait(false);
            }

            var response = new OperationResponse { ReturnType = returnType };
            try
            {
                switch (returnType)
                {
                    case OperationReturnType.NonQuery:
                        response.RecordsAffected = await command.ExecuteNonQueryAsync().ConfigureAwait(false);
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
                            default:
                                closeConnection = false;
                                break;
                        }

                        if (closeConnection)
                        {
                            behavior |= CommandBehavior.CloseConnection;
                        }

                        closeConnection = false;
                        response.Value = await command.ExecuteReaderAsync(behavior).ConfigureAwait(false);
                        break;
                    case OperationReturnType.Scalar:
                        response.Value = await command.ExecuteScalarAsync().ConfigureAwait(false);
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

        public static async Task<OperationResponse> ExecuteAsync<T>(OperationRequest request)
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
                ? await ExecuteAsync(operationText, request.Parameters, request.ReturnType, operationType, request.Types, connection: request.Connection, transaction: request.Transaction, captureException: request.CaptureException, schema: request.SchemaName, config: config).ConfigureAwait(false)
                : await ExecuteAsync(operationText, request.Parameters, request.ReturnType, operationType, request.Types, request.ConnectionName, transaction: request.Transaction, captureException: request.CaptureException, schema: request.SchemaName, config: config).ConfigureAwait(false);
            return response;
        }

        public static async Task<OperationResponse> ExecuteAsync(OperationRequest request)
        {
            var operationType = request.OperationType;
            if (operationType == OperationType.Guess)
            {
                operationType = GuessOperationType(request.Operation);
            }

            var config = request.Configuration ?? ConfigurationFactory.DefaultConfiguration;

            var operationText = GetOperationText(null, request.Operation, request.OperationType, request.SchemaName, config);

            var response = request.Connection != null
                ? await ExecuteAsync(operationText, request.Parameters, request.ReturnType, operationType, request.Types, connection: request.Connection, transaction: request.Transaction, captureException: request.CaptureException, schema: request.SchemaName, config: config).ConfigureAwait(false)
                : await ExecuteAsync(operationText, request.Parameters, request.ReturnType, operationType, request.Types, request.ConnectionName, transaction: request.Transaction, captureException: request.CaptureException, schema: request.SchemaName, config: config).ConfigureAwait(false);
            return response;
        }

        public static async Task<OperationResponse> ExecuteSqlAsync(string sql, bool nonQuery, object parameters = null, string connectionName = null, DbConnection connection = null, bool captureException = false, IConfiguration config = null)
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
            return await ExecuteAsync(request).ConfigureAwait(false);
        }

        public static async Task<OperationResponse> ExecuteProcedureAsync(string procedure, bool nonQuery, object parameters = null, string connectionName = null, DbConnection connection = null, bool captureException = false, IConfiguration config = null)
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
            return await ExecuteAsync(request).ConfigureAwait(false);
        }

        #endregion
    }
}
