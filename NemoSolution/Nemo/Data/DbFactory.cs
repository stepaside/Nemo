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
            if (connectionString == null) return null;

            var pos1 = connectionString.IndexOf("provider=", StringComparison.OrdinalIgnoreCase);
            if (pos1 > -1)
            {
                var pos2 = connectionString.IndexOf(";", pos1, StringComparison.Ordinal);
                return pos2 > -1 ? connectionString.Substring(pos1 + 9, pos2 - pos1 - 9) : connectionString.Substring(pos1 + 9);
            }

            for (var i = 0; i < ConfigurationManager.ConnectionStrings.Count; i++)
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
            return providerName != null ? DbProviderFactories.GetFactory(providerName).CreateDataAdapter() : null;
        }
    }
}
