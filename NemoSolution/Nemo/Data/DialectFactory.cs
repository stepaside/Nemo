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
        public const string ProviderInvariantSql = "System.Data.SqlClient";
        public const string ProviderInvariantMysql = "MySql.Data.MySqlClient";
        public const string ProviderInvariantSqlite = "System.Data.SQLite";
        public const string ProviderInvariantOracle = "Oracle.DataAccess.Client";

        public static DialectProvider GetProvider(string connectionName)
        {
#if NETCOREAPP2_0
            var config = ConfigurationFactory.Default.SystemConfiguration?.ConnectionString(connectionName);
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
                providerName = DbFactory.GetProviderInvariantNameByConnectionString(connection.ConnectionString);
            }

            //var isSqlServer = string.Equals(providerName, ProviderInvariantSql, StringComparison.OrdinalIgnoreCase);

            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }

            switch (providerName)
            {
                case ProviderInvariantSql:
                {
                    var isLegacy = new Version(connection.ServerVersion).Major <= 8;
                    return isLegacy ? SqlServerLegacyDialectProvider.Instance : SqlServerDialectProvider.Instance;
                }
                case ProviderInvariantMysql:
                    return MySqlDialectProvider.Instance;
                case ProviderInvariantSqlite:
                    return SqliteDialectProvider.Instance;
                case ProviderInvariantOracle:
                    return OracleDialectProvider.Instance;
                default:
                    throw new NotSupportedException();
            }
        }
    }
}
