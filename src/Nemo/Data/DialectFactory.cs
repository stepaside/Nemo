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
        public static DialectProvider GetProvider(string connectionName, INemoConfiguration config)
        {
#if NETSTANDARD2_0_OR_GREATER || NETCOREAPP
            var connectionStringsSettings = (config ?? ConfigurationFactory.DefaultConfiguration).SystemConfiguration?.ConnectionString(connectionName);

            if (connectionStringsSettings == null)
            {
                var connectionStringsSettingsSection = ConfigurationManager.ConnectionStrings[connectionName];
                var connection = DbFactory.CreateConnection(connectionStringsSettingsSection.ConnectionString, connectionStringsSettingsSection.ProviderName, config);
                return GetProvider(connection, connectionStringsSettingsSection.ProviderName);
            }
            else
            {
                var connection = DbFactory.CreateConnection(connectionStringsSettings.ConnectionString, connectionStringsSettings.ProviderName, config);
                return GetProvider(connection, connectionStringsSettings.ProviderName);
            }
#else
            var connectionStringsSettingsSection = ConfigurationManager.ConnectionStrings[connectionName];
            var connection = DbFactory.CreateConnection(connectionStringsSettingsSection.ConnectionString, connectionStringsSettingsSection.ProviderName, config);
            return GetProvider(connection, connectionStringsSettingsSection.ProviderName);
#endif
        }

        public static DialectProvider GetProvider(DbConnection connection, string providerName = null)
        {
            if (providerName == null)
            {
                providerName = DbFactory.GetProviderInvariantName(connection);
            }

            switch (providerName)
            {
                case DbFactory.ProviderInvariantSqlClient:
                {
                    if (connection.State != ConnectionState.Open)
                    {
                        connection.Open();
                    }

                    var version = new Version(connection.ServerVersion);
                    var isLegacy = version.Major <= 8;
                    var isLatest = version.Major >= 13;
                    return isLegacy ? SqlServerLegacyDialectProvider.Instance : (isLatest ? SqlServerLatestDialectProvider.Instance : SqlServerDialectProvider.Instance);
                }
                case DbFactory.ProviderInvariantMicrosoftSqlClient:
                    return SqlServerLatestDialectProvider.Instance;
                case DbFactory.ProviderInvariantMysql:
                case DbFactory.ProviderInvariantMysqlClient:
                    return MySqlDialectProvider.Instance;
                case DbFactory.ProviderInvariantSqlite:
                case DbFactory.ProviderInvariantMicrosoftSqlite:
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
