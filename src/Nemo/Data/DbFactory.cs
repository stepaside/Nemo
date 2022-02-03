using Nemo.Attributes;
using Nemo.Configuration;
using Nemo.Configuration.Mapping;
using Nemo.Extensions;
using Nemo.Reflection;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace Nemo.Data
{
    public static class DbFactory
    {
        public const string ProviderInvariantSqlClient = "System.Data.SqlClient";
        public const string ProviderInvariantMysqlClient = "MySql.Data.MySqlClient";
        public const string ProviderInvariantMysql = "MySql.Data";
        public const string ProviderInvariantSqlite = "System.Data.SQLite";
        public const string ProviderInvariantMicrosoftSqlite = "Microsoft.Data.SQLite";
        public const string ProviderInvariantOracle = "Oracle.DataAccess.Client";
        public const string ProviderInvariantPostgres = "Npgsql";
        public const string ProviderInvariantMicrosoftSqlClient = "Microsoft.Data.SqlClient";

        public static readonly ISet<string> ProviderInvariantNames = new HashSet<string>(new string[] { ProviderInvariantSqlClient, ProviderInvariantMysql, ProviderInvariantMysqlClient, ProviderInvariantSqlite, ProviderInvariantOracle, ProviderInvariantPostgres, ProviderInvariantMicrosoftSqlClient, ProviderInvariantMicrosoftSqlite }, StringComparer.OrdinalIgnoreCase);

        public static readonly char[] ParameterPrexifes = new char[] { '@', '?', ':' };

        private static string CleanConnectionString(string connectionString)
        {
            var builder = new DbConnectionStringBuilder { ConnectionString = connectionString };

            if (builder.TryGetValue("provider", out object providerValue))
            {
                builder.Remove("provider");

                return builder.ConnectionString;
            }
            return connectionString;
        }

        private static string GetDefaultConnectionName(Type objectType)
        {
            string connectionName = null;
            if (Reflector.IsEmitted(objectType))
            {
                objectType = Reflector.GetInterface(objectType);
            }

            var map = MappingFactory.GetEntityMap(objectType);
            if (map != null)
            {
                connectionName = map.ConnectionStringName;
            }

            if (connectionName == null)
            {
                var attr = Reflector.GetAttribute<ConnectionAttribute>(objectType, false, true);
                if (attr != null)
                {
                    connectionName = attr.Name;
                }
            }

            if (connectionName == null)
            {
                var attr = Reflector.GetAttribute<ConnectionAttribute>(objectType, true);
                if (attr != null)
                {
                    connectionName = attr.Name;
                }
            }

            return connectionName ?? ConfigurationFactory.Get(objectType).DefaultConnectionName;
        }

        internal static string GetProviderInvariantName(string connectionName, Type objectType, IConfiguration config)
        {
            if (connectionName.NullIfEmpty() == null && objectType != null)
            {
                connectionName = GetDefaultConnectionName(objectType);
            }

            if (connectionName.NullIfEmpty() == null)
            {
                connectionName = ConfigurationFactory.DefaultConnectionName;
            }

            string cleanConnectionString;

#if NETSTANDARD
            var connectionStringSetting = (config ?? ConfigurationFactory.DefaultConfiguration).SystemConfiguration?.ConnectionString(connectionName);
            if (connectionStringSetting != null)
            {
                return connectionStringSetting.ProviderName ?? GetProviderInvariantNameByConnectionString(connectionStringSetting.ConnectionString, config, out cleanConnectionString);
            }
#endif
            return ConfigurationManager.ConnectionStrings[connectionName]?.ProviderName ?? GetProviderInvariantNameByConnectionString(ConfigurationManager.ConnectionStrings[connectionName]?.ConnectionString, config, out cleanConnectionString);

        }

        internal static string GetProviderInvariantNameByConnectionString(string connectionString, IConfiguration config, out string cleanConnectionString)
        {
            cleanConnectionString = connectionString;

            if (connectionString == null) return null;
            
            var builder = new DbConnectionStringBuilder { ConnectionString = connectionString };

            if (builder.TryGetValue("provider", out object providerValue))
            {
                builder.Remove("provider");

                cleanConnectionString = builder.ConnectionString;

                return providerValue.ToString();
            }

            var persistSecurityInfo = false;
            if (builder.TryGetValue("persist security info", out object persistSecurityInfoValue))
            {
                persistSecurityInfo = Convert.ToBoolean(persistSecurityInfoValue);
            }

            var lostPassword = !persistSecurityInfo && !builder.ContainsKey("pwd") && !builder.ContainsKey("password");

            if (!lostPassword)
            {
#if NETSTANDARD
                dynamic connectionStrings = (config ?? ConfigurationFactory.DefaultConfiguration).SystemConfiguration?.ConnectionStrings();

                if (connectionStrings == null)
                {
                    connectionStrings = ConfigurationManager.ConnectionStrings;
                }
#else
                var connectionStrings = ConfigurationManager.ConnectionStrings;
#endif
                for (var i = 0; i < connectionStrings.Count; i++)
                {
                    var connectionStringsSettings = connectionStrings[i];
                    if (string.Equals(connectionStringsSettings.ConnectionString, connectionString, StringComparison.OrdinalIgnoreCase))
                    {
                        return connectionStringsSettings.ProviderName;
                    }
                }
            }
            else
            {
                if (builder.TryGetValue("uid", out object uid))
                {
                    builder.Remove("uid");
                    builder["user id"] = uid;
                }

#if NETSTANDARD
                dynamic connectionStrings = (config ?? ConfigurationFactory.DefaultConfiguration).SystemConfiguration?.ConnectionStrings();

                if (connectionStrings == null)
                {
                    connectionStrings = ConfigurationManager.ConnectionStrings;
                }
#else
                var connectionStrings = ConfigurationManager.ConnectionStrings;
#endif
                for (var i = 0; i < connectionStrings.Count; i++)
                {
                    var connectionStringsSettings = connectionStrings[i];

                    var otherBuilder = new DbConnectionStringBuilder { ConnectionString = connectionStringsSettings.ConnectionString };
                    otherBuilder.Remove("pwd");
                    otherBuilder.Remove("password");

                    if (otherBuilder.TryGetValue("uid", out object otherUid))
                    {
                        otherBuilder.Remove("uid");
                        otherBuilder["user id"] = otherUid;
                    }

                    if (otherBuilder.Count != builder.Count) continue;

                    var equivalenCount = builder.Cast<KeyValuePair<string, object>>()
                                            .Select(p => otherBuilder.TryGetValue(p.Key, out var value) && string.Equals(Convert.ToString(value), Convert.ToString(p.Value), StringComparison.OrdinalIgnoreCase) ? 1 : 0)
                                            .Sum();

                    if (equivalenCount == builder.Count)
                    {
                        return connectionStringsSettings.ProviderName;
                    }
                }
            }

            return null;
        }

        internal static DbConnection CreateConnection(string connectionString, string providerName, IConfiguration config)
        {
            var cleanConnectionString = connectionString;
            providerName ??= GetProviderInvariantNameByConnectionString(connectionString, config, out cleanConnectionString);

            var factory = GetDbProviderFactory(providerName);

            var connection = factory.CreateConnection();
            if (connection != null)
            {
                connection.ConnectionString = cleanConnectionString;
            }
            return connection;
        }

        public static DbConnection CreateConnection(string connectionString, DataAccessProviderTypes providerType)
        {
            return CreateConnection(connectionString, GetProviderInvariantName(providerType));
        }

        public static DbConnection CreateConnection(string connectionString, string providerName)
        {
            connectionString.ThrowIfNull(nameof(connectionString));
            providerName.ThrowIfNull(nameof(providerName));

            return CreateConnection(connectionString, providerName, null);
        }

        internal static DbConnection CreateConnection(string connectionName, Type objectType, IConfiguration config)
        {
            if (connectionName.NullIfEmpty() == null && objectType != null)
            {
                connectionName = GetDefaultConnectionName(objectType);
            }

            if (connectionName.NullIfEmpty() == null)
            {
                connectionName = ConfigurationFactory.DefaultConnectionName;
            }

            string cleanConnectionString = null;

#if NETSTANDARD

            string connectionString = null;
            string providerName = null;

            var connectionStringSetting = (config ?? ConfigurationFactory.DefaultConfiguration).SystemConfiguration?.ConnectionString(connectionName);
            if (connectionStringSetting != null)
            {
                connectionString = connectionStringSetting.ConnectionString;
                providerName = connectionStringSetting.ProviderName ?? GetProviderInvariantNameByConnectionString(connectionString, config, out cleanConnectionString);
            }
            else
            {
                connectionString = ConfigurationManager.ConnectionStrings[connectionName]?.ConnectionString;
                providerName = ConfigurationManager.ConnectionStrings[connectionName]?.ProviderName ?? GetProviderInvariantNameByConnectionString(connectionString, config, out cleanConnectionString);
            }

            var factory = GetDbProviderFactory(providerName);
#else
            var connectionStringsSettings = ConfigurationManager.ConnectionStrings[connectionName];
            var connectionString = connectionStringsSettings?.ConnectionString;
            var factory = DbProviderFactories.GetFactory(connectionStringsSettings?.ProviderName ?? GetProviderInvariantNameByConnectionString(connectionString, config, out cleanConnectionString));
#endif
            var connection = factory.CreateConnection();
            if (connection != null)
            {
                connection.ConnectionString = cleanConnectionString ?? connectionString;
            }
            return connection;
        }

        public static DbConnection CreateConnection(string connectionStringOrName)
        {
            return CreateConnection(connectionStringOrName, ConfigurationFactory.DefaultConfiguration);
        }

        public static DbConnection CreateConnection(string connectionStringOrName, IConfiguration config)
        {
            string connectionString = null;
            string providerName = null;
            string cleanConnectionString = null;

#if NETSTANDARD
            var connectionStringSetting = (config ?? ConfigurationFactory.DefaultConfiguration).SystemConfiguration?.ConnectionString(connectionStringOrName);

            if (connectionStringSetting != null)
            {
                connectionString = connectionStringSetting.ConnectionString;
                providerName = connectionStringSetting.ProviderName ?? GetProviderInvariantNameByConnectionString(connectionString, config, out cleanConnectionString);
                return CreateConnection(cleanConnectionString ?? connectionString, providerName, config);
            }
            else
            {
                var connectionStringSettings = ConfigurationManager.ConnectionStrings[connectionStringOrName];
                if (connectionStringSettings != null)
                {
                    connectionString = connectionStringSettings.ConnectionString;
                    providerName = connectionStringSettings.ProviderName ?? GetProviderInvariantNameByConnectionString(connectionString, config, out cleanConnectionString);
                    return CreateConnection(cleanConnectionString ?? connectionString, providerName, config);
                }
                else
                {
                    providerName = GetProviderInvariantNameByConnectionString(connectionStringOrName, config, out cleanConnectionString);
                    return CreateConnection(cleanConnectionString ?? connectionStringOrName, providerName, config);
                }
            }
#else
            var connectionStringSetting = ConfigurationManager.ConnectionStrings[connectionStringOrName];
            if (connectionStringSetting != null)
            {
                connectionString = connectionStringSetting.ConnectionString;
                providerName = connectionStringSetting.ProviderName ?? GetProviderInvariantNameByConnectionString(connectionStringSetting.ConnectionString, config, out cleanConnectionString);
                return CreateConnection(cleanConnectionString ?? connectionString, providerName, config);
            }
            else
            {
                providerName = GetProviderInvariantNameByConnectionString(connectionStringOrName, config, out cleanConnectionString);
                return CreateConnection(cleanConnectionString ?? connectionStringOrName, providerName, config);
            }
#endif
        }

        internal static DbDataAdapter CreateDataAdapter(DbConnection connection, IConfiguration config)
        {
            var providerName = GetProviderInvariantNameByConnectionString(connection.ConnectionString, config, out var _);
#if NETSTANDARD
            return providerName != null ? GetDbProviderFactory(providerName).CreateDataAdapter() : null;
#else
            return providerName != null ? DbProviderFactories.GetFactory(providerName).CreateDataAdapter() : null;
#endif
        }

        internal static string GetProviderInvariantName(DbConnection connection)
        {
            var connectionType = connection.GetType().FullName?.ToLower();

            if (connectionType == "system.data.sqlclient.sqlconnection")
            {
                return ProviderInvariantSqlClient;
            }

            if (connectionType == "microsoft.data.sqlclient.sqlconnection")
            {
                return ProviderInvariantMicrosoftSqlClient;
            }

            if (connectionType == "system.data.sqlite.sqliteconnection")
            {
                return ProviderInvariantSqlite;
            }

            if (connectionType == "microsoft.data.sqlite.sqliteconnection")
            {
                return ProviderInvariantMicrosoftSqlite;
            }

            if (connectionType == "mysql.data.mysqlclient.mysqlconnection")
            {
                return ProviderInvariantMysqlClient;
            }

            if (connectionType == "oracle.dataaccess.client.oracleconnection")
            {
                return ProviderInvariantOracle;
            }

            if (connectionType == "npgsql.npgsqlconnection")
            {
                return ProviderInvariantPostgres;
            }

            return null;
        }

        internal static string GetProviderInvariantName(DataAccessProviderTypes type)
        {
            if (type == DataAccessProviderTypes.SqlServer)
            {
                return ProviderInvariantSqlClient;
            }

            if (type == DataAccessProviderTypes.SqlServerCore)
            {
                return ProviderInvariantMicrosoftSqlClient;
            }

            if (type == DataAccessProviderTypes.SqLite)
            {
                return ProviderInvariantSqlite;
            }

            if (type == DataAccessProviderTypes.MySql)
            {
                return ProviderInvariantMysqlClient;
            }

            if (type == DataAccessProviderTypes.PostgreSql)
            {
                return ProviderInvariantPostgres;
            }

            if (type == DataAccessProviderTypes.Oracle)
            {
                return ProviderInvariantOracle;
            }

            throw new NotSupportedException($"Unsupported Provider Factory specified: {type}");
        }

        public static DbProviderFactory GetDbProviderFactory(string providerName)
        {
            if (providerName == null)
            {
                throw new ArgumentNullException(nameof(providerName));
            }
            
#if NETSTANDARD2_1 || NET472_OR_GREATER
            if (!ProviderInvariantNames.Contains(providerName)) throw new NotSupportedException($"Unsupported Provider Factory specified: {providerName}");

            try
            {
                return DbProviderFactories.GetFactory(providerName);
            }
            catch { }
#endif

            if (string.Equals(providerName, ProviderInvariantSqlClient, StringComparison.OrdinalIgnoreCase))
            {
                return GetDbProviderFactory(DataAccessProviderTypes.SqlServer);
            }

            if (string.Equals(providerName, ProviderInvariantMicrosoftSqlClient, StringComparison.OrdinalIgnoreCase))
            {
                return GetDbProviderFactory(DataAccessProviderTypes.SqlServerCore);
            }

            if (string.Equals(providerName, ProviderInvariantSqlite, StringComparison.OrdinalIgnoreCase) || string.Equals(providerName, ProviderInvariantMicrosoftSqlite, StringComparison.OrdinalIgnoreCase))
            {
                return GetDbProviderFactory(DataAccessProviderTypes.SqLite);
            }

            if (string.Equals(providerName, ProviderInvariantMysql, StringComparison.OrdinalIgnoreCase) || string.Equals(providerName, ProviderInvariantMysqlClient, StringComparison.OrdinalIgnoreCase))
            {
                return GetDbProviderFactory(DataAccessProviderTypes.MySql);
            }

            if (string.Equals(providerName, ProviderInvariantOracle, StringComparison.OrdinalIgnoreCase))
            {
                return GetDbProviderFactory(DataAccessProviderTypes.Oracle);
            }

            if (string.Equals(providerName, ProviderInvariantPostgres, StringComparison.OrdinalIgnoreCase))
            {
                return GetDbProviderFactory(DataAccessProviderTypes.PostgreSql);
            }

            throw new NotSupportedException($"Unsupported Provider Factory specified: {providerName}");
        }

        public static DbProviderFactory GetDbProviderFactory(DataAccessProviderTypes type)
        {
            if (type == DataAccessProviderTypes.SqlServer)
            {
                return SqlClientFactory.Instance; // this library has a ref to SqlClient so this works
            }

            if (type == DataAccessProviderTypes.SqlServerCore)
            {
                return GetDbProviderFactory("Microsoft.Data.SqlClient.SqlClientFactory", "Microsoft.Data.SqlClient");
            }

            if (type == DataAccessProviderTypes.SqLite)
            {
                return GetDbProviderFactory("Microsoft.Data.Sqlite.SqliteFactory", "Microsoft.Data.Sqlite");
            }

            if (type == DataAccessProviderTypes.MySql)
            {
                return GetDbProviderFactory("MySql.Data.MySqlClient.MySqlClientFactory", "MySql.Data");
            }

            if (type == DataAccessProviderTypes.PostgreSql)
            {
                return GetDbProviderFactory("Npgsql.NpgsqlFactory", "Npgsql");
            }

            if (type == DataAccessProviderTypes.Oracle)
            {
                return GetDbProviderFactory("Oracle.DataAccess.Client.OracleClientFactory", "Oracle.DataAccess");
            }

            throw new NotSupportedException($"Unsupported Provider Factory specified: {type}");
        }

        public static DbProviderFactory GetDbProviderFactory(string dbProviderFactoryTypename, string assemblyName)
        {
            var instance = Reflector.GetStaticProperty(dbProviderFactoryTypename, "Instance");
            if (instance == null)
            {
                var a = Reflector.LoadAssembly(assemblyName);
                if (a != null)
                {
                    instance = Reflector.GetStaticProperty(dbProviderFactoryTypename, "Instance");
                }
            }

            if (instance == null)
            {
                throw new InvalidOperationException($"Unable to retrieve DbProviderFactory for: {dbProviderFactoryTypename}");
            }

            return instance as DbProviderFactory;
        }

        internal static ISet<string> GetProcedureParameters(DbConnection connection, string procedureName, bool keepOpen, IConfiguration config, DialectProvider dialect)
        {
            dialect ??= DialectFactory.GetProvider(connection);
            if (string.IsNullOrEmpty(dialect.StoredProcedureParameterListQuery)) return null;

            var key = $"{nameof(GetProcedureParameters)}:{procedureName}";
            if (config?.ExecutionContext?.Get(key) is ISet<string> set) return set;

            set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var shouldClose = false;
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
                shouldClose = true;
            }
            try
            {
                using var command = connection.CreateCommand();
                var parameter = command.CreateParameter();
                parameter.ParameterName = dialect.ParameterPrefix + "name";
                parameter.Value = procedureName;
                command.Parameters.Add(parameter);
                command.CommandText = dialect.StoredProcedureParameterListQuery;
                using var reader = command.ExecuteReader();
                while (reader.Read())
                {
                    set.Add(reader.GetString(reader.GetOrdinal("parameter_name")).TrimStart(ParameterPrexifes));
                }
            }
            finally
            {
                if (shouldClose && !keepOpen)
                {
                    connection.Close();
                }
            }

            config?.ExecutionContext?.Set(key, set);
            return set;
        }

        internal static ISet<string> GetQueryParameters(DbConnection connection, string sql, bool keepOpen, IConfiguration config, DialectProvider dialect)
        {
            dialect ??= DialectFactory.GetProvider(connection);
            if (dialect.UseOrderedParameters || string.IsNullOrEmpty(dialect.ParameterPrefix)) return null;

            if (dialect.ParameterNameMatcher == null) return null;

            return new HashSet<string>(dialect.ParameterNameMatcher.Matches(sql).Cast<Match>().Select(m => m.Value.TrimStart(ParameterPrexifes)), StringComparer.OrdinalIgnoreCase);
        }

        internal static Dictionary<DbParameter, Param> SetupParameters(DbCommand command, IEnumerable<Param> parameters, DialectProvider dialect, IConfiguration config)
        {
            Dictionary<DbParameter, Param> outputParameters = null;
            if (parameters != null)
            {
                var isPositional = command.CommandType != CommandType.StoredProcedure && command.CommandText.IndexOf('?') >= 0;

                ISet<string> parsedParameters = null;
                if ((config?.IgnoreInvalidParameters).GetValueOrDefault())
                {
                    if (command.CommandType == CommandType.StoredProcedure)
                    {
                        parsedParameters = DbFactory.GetProcedureParameters(command.Connection, command.CommandText, true, config, dialect);
                    }
                    else
                    {
                        parsedParameters = DbFactory.GetQueryParameters(command.Connection, command.CommandText, true, config, dialect);
                    }
                }

                var count = 0;
                foreach (var parameter in parameters)
                {
                    var originalName = parameter.Name;
                    var name = originalName.TrimStart(DbFactory.ParameterPrexifes);

                    if (parsedParameters != null && !parsedParameters.Contains(name))
                    {
                        continue;
                    }

                    var dbParam = command.CreateParameter();
                    dbParam.ParameterName = name;
                    dbParam.Direction = parameter.Direction;

                    var addParameter = true;

                    if (dbParam.Direction == ParameterDirection.Output && command.CommandType == CommandType.StoredProcedure)
                    {
                        if (outputParameters == null)
                        {
                            outputParameters = new Dictionary<DbParameter, Param>();
                        }
                        outputParameters.Add(dbParam, parameter);
                    }
                    else
                    {
                        dbParam.Value = parameter.Value ?? DBNull.Value;

                        if (parameter.IsArray && !dialect.SupportsArrays)
                        {
                            var items = dbParam.Value as IEnumerable;
                            if (command.CommandType != CommandType.StoredProcedure)
                            {
                                var itemType = Reflector.GetElementType(parameter.Type);
                                var dbType = Reflector.ClrToDbType(itemType);
                                var splitString = !isPositional ? dialect.SplitString(originalName, dialect.GetColumnType(dbType), null) : null;

                                if (!string.IsNullOrEmpty(splitString))
                                {
                                    command.CommandText = dialect.ParameterNameMatcherWithGroups.Replace(command.CommandText, match => $"({splitString})");
                                    parameter.Value = string.Join(",", items.Cast<object>().ToArray());
                                    dbParam.Value = parameter.Value;
                                }
                                else
                                {
                                    var expansionParameters = new List<string>();
                                    foreach (var item in items)
                                    {
                                        var listEpxansionParam = command.CreateParameter();
                                        listEpxansionParam.ParameterName = $"{name}{expansionParameters.Count}";
                                        listEpxansionParam.Direction = parameter.Direction;
                                        listEpxansionParam.Value = item;
                                        listEpxansionParam.DbType = dbType;
                                        expansionParameters.Add(listEpxansionParam.ParameterName);
                                        command.Parameters.Add(listEpxansionParam);
                                    }

                                    var expanedParameters = isPositional ? Enumerable.Repeat('?', expansionParameters.Count).ToDelimitedString(",") : expansionParameters.Select(n => $"{dialect.ParameterPrefix}{n}").ToDelimitedString(",");
                                    if (isPositional)
                                    {

                                        var current = 0;
                                        command.CommandText = dialect.PositionalParameterMatcher.Replace(command.CommandText, match =>
                                        {
                                            current++;
                                            if (current > count)
                                            {
                                                return $"({expanedParameters})";
                                            }
                                            return match.Value;
                                        }, count + 1);
                                    }
                                    else
                                    {
                                        command.CommandText = dialect.ParameterNameMatcherWithGroups.Replace(command.CommandText, match => match.Groups[1].Success && match.Groups[3].Success ? expanedParameters : $"({expanedParameters})");
                                    }

                                    count += expansionParameters.Count;

                                    addParameter = false;
                                }
                            }
                            else
                            {
                                parameter.Value = string.Join(",", items.Cast<object>().ToArray());
                                dbParam.Value = parameter.Value;
                                count++;
                            }
                        }
                        else
                        {
                            count++;
                        }

                        if (addParameter)
                        {
                            if (parameter.Value != null)
                            {
                                if (parameter.Size > -1)
                                {
                                    dbParam.Size = parameter.Size;
                                }

                                dbParam.DbType = parameter.DbType ?? Reflector.ClrToDbType(parameter.Type);
                            }
                            else if (parameter.DbType != null)
                            {
                                dbParam.DbType = parameter.DbType.Value;
                            }
                        }
                    }

                    if (addParameter)
                    {
                        command.Parameters.Add(dbParam);
                    }
                }
            }

            return outputParameters;
        }
    }
}
