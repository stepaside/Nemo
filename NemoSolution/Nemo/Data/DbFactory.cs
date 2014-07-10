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
            var config = Config.ConnectionStringSetting(connectionName);
            return config.ProviderName;
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
				for (var i = 0; i < ConfigurationManager.ConnectionStrings.Count; i++)
				{
					var config = ConfigurationManager.ConnectionStrings [i];
				    if (string.Equals(config.ConnectionString, connectionString, StringComparison.OrdinalIgnoreCase))
				    {
				        return config.ProviderName;
				    }
				}
			} 
			else
			{
				for (var i = 0; i < ConfigurationManager.ConnectionStrings.Count; i++)
				{
					var config = ConfigurationManager.ConnectionStrings[i];

                    var otherBuilder = new DbConnectionStringBuilder { ConnectionString = config.ConnectionString };
                    otherBuilder.Remove("pwd");
                    otherBuilder.Remove("password");

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
            var factory = DbProviderFactories.GetFactory(providerName);
            var connection = factory.CreateConnection();
            connection.ConnectionString = connectionString;
            return connection;
        }

        internal static DbConnection CreateConnection(string connectionName, Type objectType)
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

        public static DbConnection CreateConnection(string connectionStringOrName)
        {
            var providerName = GetProviderInvariantNameByConnectionString(connectionStringOrName);
            if (providerName != null)
            {
                return CreateConnection(connectionStringOrName, providerName);
            }
            return CreateConnection(connectionStringOrName, (Type)null);
        }

        internal static DbDataAdapter CreateDataAdapter(DbConnection connection)
        {
            var providerName = GetProviderInvariantNameByConnectionString(connection.ConnectionString);
            return providerName != null ? DbProviderFactories.GetFactory(providerName).CreateDataAdapter() : null;
        }
    }
}
