using Nemo.Attributes;
using Nemo.Configuration;
using Nemo.Configuration.Mapping;
using Nemo.Extensions;
using Nemo.Reflection;
using Nemo.Utilities;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace Nemo.Data
{
    internal static class DbFactory
    {
        private static string GetDefaultConnectionName(Type objectType)
        {
            string connectionName = null;
            if (Reflector.IsEmitted(objectType))
            {
                objectType = Reflector.ExtractInterface(objectType);
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
                var attr = Reflector.GetAttribute<ConnectionAttribute>(objectType, true, false);
                if (attr != null)
                {
                    connectionName = attr.Name;
                }
            }

            return connectionName ?? ConfigurationFactory.Configuration.DefaultConnectionName;
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
                if (string.Equals(config.ConnectionString, connectionString, StringComparison.OrdinalIgnoreCase))
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
            var providerName = GetProviderInvariantNameByConnectionString(connection.ConnectionString);
            if (providerName != null)
            {
                return DbProviderFactories.GetFactory(providerName).CreateDataAdapter();
            }
            return null;
        }
    }
}
