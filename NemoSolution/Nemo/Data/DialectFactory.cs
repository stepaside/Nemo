using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Common;
using System.Configuration;
using System.Data;

namespace Nemo.Data
{
    public static class DialectFactory
    {
        public const string PROVIDER_INVARIANT_SQL = "System.Data.SqlClient";
        public const string PROVIDER_INVARIANT_MYSQL = "MySql.Data.MySqlClient";
        public const string PROVIDER_INVARIANT_SQLITE = "System.Data.SQLite";
        public const string PROVIDER_INVARIANT_ORACLE = "Oracle.DataAccess.Client";

        public static DialectProvider GetProvider(string connectionName)
        {
            var config = ConfigurationManager.ConnectionStrings[connectionName];
            var connection = DbFactory.CreateConnection(config.ConnectionString, config.ProviderName);
            return GetProvider(connection, config.ProviderName);
        }

        public static DialectProvider GetProvider(DbConnection connection, string providerName = null)
        {
            if (providerName == null)
            {
                providerName = DbFactory.GetProviderInvariantNameByConnectionString(connection.ConnectionString);
            }

            var isSqlServer = string.Equals(providerName, PROVIDER_INVARIANT_SQL, StringComparison.OrdinalIgnoreCase);

            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }

            switch (providerName)
            {
                case PROVIDER_INVARIANT_SQL:
                    {
                        var isLegacy = new Version(connection.ServerVersion).Major <= 8;
                        if (isLegacy)
                        {
                            return SqlServerLegacyDialectProvider.Instance;
                        }
                        else
                        {
                            return SqlServerDialectProvider.Instance;
                        }
                    }
                case PROVIDER_INVARIANT_MYSQL:
                    return MySqlDialectProvider.Instance;
                default:
                    throw new NotSupportedException();
            }
        }
    }
}
