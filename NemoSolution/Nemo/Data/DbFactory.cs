﻿using Nemo.Attributes;
using Nemo.Configuration;
using Nemo.Configuration.Mapping;
using Nemo.Extensions;
using Nemo.Reflection;
using Nemo.Utilities;
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
#if NETCOREAPP2_0
            return ConfigurationFactory.Default.SystemConfiguration?.ConnectionString(connectionName)?.ProviderName;
#else
            return ConfigurationManager.ConnectionStrings[ConfigurationFactory.DefaultConnectionName]?.ProviderName;
#endif
        }

        internal static string GetProviderInvariantNameByConnectionString(string connectionString)
        {
            if (connectionString == null) return null;

            var builder = new DbConnectionStringBuilder { ConnectionString = connectionString };

            object providerValue;
            if (builder.TryGetValue("provider", out providerValue))
            {
                return providerValue.ToString();
            }

            var persistSecurityInfo = false;
            object persistSecurityInfoValue;
            if (builder.TryGetValue("persist security info", out persistSecurityInfoValue))
            {
                persistSecurityInfo = Convert.ToBoolean(persistSecurityInfoValue);
            }

            var lostPassword = !persistSecurityInfo && !builder.ContainsKey("pwd") && !builder.ContainsKey("password");

            if (!lostPassword)
            {
#if NETCOREAPP2_0
                var connectionStrings = ConfigurationFactory.Default.SystemConfiguration?.ConnectionStrings();
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

#if NETCOREAPP2_0
                var connectionStrings = ConfigurationFactory.Default.SystemConfiguration?.ConnectionStrings();
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

                    var equivalenCount = builder.Cast<KeyValuePair<string, object>>().Select(p =>
                    {
                        object value;
                        return otherBuilder.TryGetValue(p.Key, out value) && string.Equals(Convert.ToString(value), Convert.ToString(p.Value), StringComparison.OrdinalIgnoreCase) ? 1 : 0;
                    }).Sum();

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
#if NETCOREAPP2_0
            var factory = GetDbProviderFactory(providerName);
#else
            var factory = DbProviderFactories.GetFactory(providerName);
#endif
            var connection = factory.CreateConnection();
            if (connection != null)
            {
                connection.ConnectionString = connectionString;
            }
            return connection;
        }

        internal static DbConnection CreateConnection(string connectionName, Type objectType)
        {
            if (connectionName.NullIfEmpty() == null && objectType != null)
            {
                connectionName = GetDefaultConnectionName(objectType);
            }
#if NETCOREAPP2_0
            var config = ConfigurationFactory.Default.SystemConfiguration?.ConnectionString(connectionName);
            var factory = GetDbProviderFactory(config.ProviderName);
#else
            var config = ConfigurationManager.ConnectionStrings[connectionName];
            var factory = DbProviderFactories.GetFactory(config.ProviderName);
#endif
            var connection = factory.CreateConnection();
            if (connection != null)
            {
                connection.ConnectionString = config.ConnectionString;
            }
            return connection;
        }

        public static DbConnection CreateConnection(string connectionStringOrName)
        {
            var providerName = GetProviderInvariantNameByConnectionString(connectionStringOrName);
            return providerName != null ? CreateConnection(connectionStringOrName, providerName) : CreateConnection(connectionStringOrName, (Type)null);
        }

        internal static DbDataAdapter CreateDataAdapter(DbConnection connection)
        {
            var providerName = GetProviderInvariantNameByConnectionString(connection.ConnectionString);
#if NETCOREAPP2_0
            return providerName != null ? GetDbProviderFactory(providerName).CreateDataAdapter() : null;
#else
            return providerName != null ? DbProviderFactories.GetFactory(providerName).CreateDataAdapter() : null;
#endif
        }

        public static DbProviderFactory GetDbProviderFactory(string providerName)
        {
            var providername = providerName.ToLower();

            if (providerName == "system.data.sqlclient")
            {
                return GetDbProviderFactory(DataAccessProviderTypes.SqlServer);
            }

            if (providerName == "system.data.sqlite" || providerName == "microsoft.data.sqlite")
            {
                return GetDbProviderFactory(DataAccessProviderTypes.SqLite);
            }

            if (providerName == "mysql.data.mysqlclient" || providername == "mysql.data")
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
