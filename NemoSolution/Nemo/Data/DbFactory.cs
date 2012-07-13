using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.Common;
using System.Linq;
using System.Text;
using Nemo.Attributes;
using Nemo.Extensions;
using Nemo.Reflection;
using Nemo.Utilities;

namespace Nemo.Data
{
    internal static class DbFactory
    {
        private static string GetDefaultConnectionName(Type objectType)
        {
            var attr = Reflector.GetAttribute<ConnectionAttribute>(objectType, false, true);
            if (attr != null)
            {
                return attr.Name;
            }
            else
            {
                attr = Reflector.GetAttribute<ConnectionAttribute>(objectType, true, false);
                if (attr != null)
                {
                    return attr.Name;
                }
            }
            return ObjectFactory.Configuration.DefaultConnectionName;
        }

        internal static string GetProviderInvariantName(string connectionName, Type objectType = null)
        {
            if (connectionName.NullIfEmpty() == null && objectType != null)
            {
                connectionName = GetDefaultConnectionName(objectType);
            }
            var config = Config.ConnectionStringSetting(connectionName);
            return config.ProviderName;
        }

        internal static string GetProviderInvariantNameByConnectionString(string connectionString)
        {
            for (int i = 0; i < ConfigurationManager.ConnectionStrings.Count; i++)
            {
                var config = ConfigurationManager.ConnectionStrings[i];
                if (config.ConnectionString == connectionString)
                {
                    return config.ProviderName;
                }
            }
            return null;
        }

        internal static DbConnection CreateConnection(string connectionString, string providerName)
        {
            var factory = DbProviderFactories.GetFactory(providerName);
            var connection = factory.CreateConnection();
            connection.ConnectionString = connectionString;
            return connection;
        }

        internal static DbConnection CreateConnection(string connectionName, Type objectType = null)
        {
            if (connectionName.NullIfEmpty() == null && objectType != null)
            {
                connectionName = GetDefaultConnectionName(objectType);
            }
            var config = ConfigurationManager.ConnectionStrings[connectionName];
            var factory = DbProviderFactories.GetFactory(config.ProviderName);
            var connection = factory.CreateConnection();
            connection.ConnectionString = config.ConnectionString;
            return connection;
        }

        internal static DbDataAdapter CreateDataAdapter(DbConnection connection)
        {
            string providerName = string.Empty;
            for (int i = 0; i < ConfigurationManager.ConnectionStrings.Count; i++)
            {
                var config = ConfigurationManager.ConnectionStrings[i];
                if (string.Equals(config.ConnectionString, connection.ConnectionString, StringComparison.OrdinalIgnoreCase))
                {
                    providerName = config.ProviderName;
                    break;
                }
            }
            return DbProviderFactories.GetFactory(providerName).CreateDataAdapter();
        }
    }
}
