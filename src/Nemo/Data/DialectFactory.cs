using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Common;
using System.Configuration;
using System.Data;
using Nemo.Configuration;

namespace Nemo.Data
{
    public static class DialectFactory
    {
        public static DialectProvider GetProvider(string connectionName)
        {
#if NETSTANDARD
            dynamic config = ConfigurationFactory.DefaultConfiguration.SystemConfiguration?.ConnectionString(connectionName);

            if (config == null)
            {
                config = ConfigurationManager.ConnectionStrings[connectionName];
            }
#else
            var config = ConfigurationManager.ConnectionStrings[connectionName];
#endif
            var connection = DbFactory.CreateConnection(config.ConnectionString, config.ProviderName);
            return GetProvider(connection, config.ProviderName);
        }

        public static DialectProvider GetProvider(DbConnection connection, string providerName = null)
        {
            if (providerName == null)
            {
                providerName = DbFactory.GetProviderInvariantName(connection);
            }

            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }

            switch (providerName)
            {
                case DbFactory.ProviderInvariantSql:
                {
                    var isLegacy = new Version(connection.ServerVersion).Major <= 8;
                    return isLegacy ? SqlServerLegacyDialectProvider.Instance : SqlServerDialectProvider.Instance;
                }
                case DbFactory.ProviderInvariantMysql:
                    return MySqlDialectProvider.Instance;
                case DbFactory.ProviderInvariantSqlite:
                    return SqliteDialectProvider.Instance;
                case DbFactory.ProviderInvariantOracle:
                    return OracleDialectProvider.Instance;
                case DbFactory.ProviderInvariantPostgres:
                    return PostgresDialectProvider.Instance;
                default:
                    throw new NotSupportedException();
            }
        }
    }
}
