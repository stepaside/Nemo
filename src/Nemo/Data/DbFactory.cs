using Nemo.Attributes;
using Nemo.Configuration;
using Nemo.Configuration.Mapping;
using Nemo.Extensions;
using Nemo.Reflection;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace Nemo.Data
{
    public static class DbFactory
    {
        public const string ProviderInvariantSql = "System.Data.SqlClient";
        public const string ProviderInvariantMysql = "MySql.Data.MySqlClient";
        public const string ProviderInvariantSqlite = "System.Data.SQLite";
        public const string ProviderInvariantOracle = "Oracle.DataAccess.Client";
        public const string ProviderInvariantPostgres = "Npgsql";
        public const string ProviderInvariantSqlCore = "Microsoft.Data.SqlClient";

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

        internal static string GetProviderInvariantName(string connectionName, Type objectType)
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
            var connectionStringSetting = ConfigurationFactory.DefaultConfiguration.SystemConfiguration?.ConnectionString(connectionName);
            if (connectionStringSetting != null)
            {
                return connectionStringSetting.ProviderName ?? GetProviderInvariantNameByConnectionString(connectionStringSetting.ConnectionString, out cleanConnectionString);
            }
#endif
            return ConfigurationManager.ConnectionStrings[connectionName]?.ProviderName ?? GetProviderInvariantNameByConnectionString(ConfigurationManager.ConnectionStrings[connectionName]?.ConnectionString, out cleanConnectionString);

        }

        internal static string GetProviderInvariantNameByConnectionString(string connectionString, out string cleanConnectionString)
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
                dynamic connectionStrings = ConfigurationFactory.DefaultConfiguration.SystemConfiguration?.ConnectionStrings();

                if (connectionStrings == null)
                {
                    connectionStrings = ConfigurationManager.ConnectionStrings;
                }
#else
                var connectionStrings = ConfigurationManager.ConnectionStrings;
#endif
                for (var i = 0; i < connectionStrings.Count; i++)
                {
                    var config = connectionStrings[i];
                    if (string.Equals(config.ConnectionString, connectionString, StringComparison.OrdinalIgnoreCase))
                    {
                        return config.ProviderName;
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
                dynamic connectionStrings = ConfigurationFactory.DefaultConfiguration.SystemConfiguration?.ConnectionStrings();

                if (connectionStrings == null)
                {
                    connectionStrings = ConfigurationManager.ConnectionStrings;
                }
#else
                var connectionStrings = ConfigurationManager.ConnectionStrings;
#endif
                for (var i = 0; i < connectionStrings.Count; i++)
                {
                    var config = connectionStrings[i];

                    var otherBuilder = new DbConnectionStringBuilder { ConnectionString = config.ConnectionString };
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
                        return config.ProviderName;
                    }
                }
            }

            return null;
        }

        internal static DbConnection CreateConnection(string connectionString, string providerName)
        {
            var cleanConnectionString = connectionString;
            providerName ??= GetProviderInvariantNameByConnectionString(connectionString, out cleanConnectionString);

#if NETSTANDARD
            var factory = GetDbProviderFactory(providerName);
#else
            var factory = DbProviderFactories.GetFactory(providerName);
#endif
            var connection = factory.CreateConnection();
            if (connection != null)
            {
                connection.ConnectionString = cleanConnectionString;
            }
            return connection;
        }

        internal static DbConnection CreateConnection(string connectionName, Type objectType)
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

            var connectionStringSetting = ConfigurationFactory.DefaultConfiguration.SystemConfiguration?.ConnectionString(connectionName);
            if (connectionStringSetting != null)
            {
                connectionString = connectionStringSetting.ConnectionString;
                providerName = connectionStringSetting.ProviderName ?? GetProviderInvariantNameByConnectionString(connectionString, out cleanConnectionString);
            }
            else
            {
                connectionString = ConfigurationManager.ConnectionStrings[connectionName]?.ConnectionString;
                providerName = ConfigurationManager.ConnectionStrings[connectionName]?.ProviderName ?? GetProviderInvariantNameByConnectionString(connectionString, out cleanConnectionString);
            }

            var factory = GetDbProviderFactory(providerName);
#else
            var config = ConfigurationManager.ConnectionStrings[connectionName];
            var connectionString = config?.ConnectionString;
            var factory = DbProviderFactories.GetFactory(config?.ProviderName ?? GetProviderInvariantNameByConnectionString(connectionString, out cleanConnectionString));
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
            string connectionString = null;
            string providerName = null;
            string cleanConnectionString = null;

#if NETSTANDARD
            var connectionStringSetting = ConfigurationFactory.DefaultConfiguration.SystemConfiguration?.ConnectionString(connectionStringOrName);

            if (connectionStringSetting != null)
            {
                connectionString = connectionStringSetting.ConnectionString;
                providerName = connectionStringSetting.ProviderName ?? GetProviderInvariantNameByConnectionString(connectionString, out cleanConnectionString);
                return CreateConnection(cleanConnectionString ?? connectionString, providerName);
            }
            else
            {
                var config = ConfigurationManager.ConnectionStrings[connectionStringOrName];
                if (config != null)
                {
                    connectionString = ConfigurationManager.ConnectionStrings[connectionStringOrName]?.ConnectionString;
                    providerName = ConfigurationManager.ConnectionStrings[connectionStringOrName]?.ProviderName ?? GetProviderInvariantNameByConnectionString(connectionString, out cleanConnectionString);
                    return CreateConnection(cleanConnectionString ?? connectionString, providerName);
                }
                else
                {
                    providerName = GetProviderInvariantNameByConnectionString(connectionStringOrName, out cleanConnectionString);
                    return CreateConnection(cleanConnectionString ?? connectionStringOrName, providerName);
                }
            }
#else
            var config = ConfigurationManager.ConnectionStrings[connectionStringOrName];
            if (config != null)
            {
                connectionString = config.ConnectionString;
                providerName = config.ProviderName ?? GetProviderInvariantNameByConnectionString(config.ConnectionString, out cleanConnectionString);
                return CreateConnection(cleanConnectionString ?? connectionString, providerName);
            }
            else
            {
                providerName = GetProviderInvariantNameByConnectionString(connectionStringOrName, out cleanConnectionString);
                return CreateConnection(cleanConnectionString ?? connectionStringOrName, providerName);
            }
#endif
        }

        internal static DbDataAdapter CreateDataAdapter(DbConnection connection)
        {
            var providerName = GetProviderInvariantNameByConnectionString(connection.ConnectionString, out var _);
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
                return ProviderInvariantSql;
            }

            if (connectionType == "microsoft.data.sqlclient.sqlconnection")
            {
                return ProviderInvariantSqlCore;
            }

            if (connectionType == "system.data.sqlite.sqliteconnection" || connectionType == "microsoft.data.sqlite.sqliteconnection")
            {
                return ProviderInvariantSqlite;
            }

            if (connectionType == "mysql.data.mysqlclient.mysqlconnection")
            {
                return ProviderInvariantMysql;
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

        public static DbProviderFactory GetDbProviderFactory(string providerName)
        {
            if (providerName == null)
            {
                throw new ArgumentNullException(nameof(providerName));
            }

            providerName = providerName.ToLower();

            if (providerName == "system.data.sqlclient")
            {
                return GetDbProviderFactory(DataAccessProviderTypes.SqlServer);
            }

            if (providerName == "microsoft.data.sqlclient")
            {
                return GetDbProviderFactory(DataAccessProviderTypes.SqlServerCore);
            }

            if (providerName == "system.data.sqlite" || providerName == "microsoft.data.sqlite")
            {
                return GetDbProviderFactory(DataAccessProviderTypes.SqLite);
            }

            if (providerName == "mysql.data.mysqlclient" || providerName == "mysql.data")
            {
                return GetDbProviderFactory(DataAccessProviderTypes.MySql);
            }

            if (providerName == "oracle.dataaccess.client")
            {
                return GetDbProviderFactory(DataAccessProviderTypes.Oracle);
            }

            if (providerName == "npgsql")
            {
                return GetDbProviderFactory(DataAccessProviderTypes.PostgreSql);
            }

            throw new NotSupportedException(string.Format("Unsupported Provider Factory specified: {0}", providerName));
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

            throw new NotSupportedException(string.Format("Unsupported Provider Factory specified: {0}", type.ToString()));
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
                throw new InvalidOperationException(string.Format("Unable to retrieve DbProviderFactory for: {0}", dbProviderFactoryTypename));
            }

            return instance as DbProviderFactory;
        }
    }
}
