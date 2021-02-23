#if NETSTANDARD
using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Configuration;
using Nemo.Collections.Extensions;

namespace Nemo.Configuration
{
    public static class ConnectionStringSettingsExtensions
    {
        public static ConnectionStringSettingsCollection ConnectionStrings(this IConfigurationRoot configuration, string section = "ConnectionStrings")
        {
            var connectionStringCollection = configuration.GetSection(section).Get<ConnectionStringSettingsCollection>();
            if (connectionStringCollection == null || connectionStringCollection.Count == 0 || connectionStringCollection.All(c => c.Name == null))
            {
                var connectionStrings = configuration.GetSection(section).Get<Dictionary<string, ConnectionStringSettings>>();
                if (connectionStrings == null || connectionStrings.Count == 0)
                {
                    var connectionStringMap = configuration.GetSection(section).Get<Dictionary<string, string>>();
                    if (connectionStringMap == null || connectionStringMap.Count == 0)
                    {
                        return new ConnectionStringSettingsCollection();
                    }
                    else
                    {
                        return new ConnectionStringSettingsCollection(connectionStringMap.Select(p => new ConnectionStringSettings { Name = p.Key, ConnectionString = p.Value }));
                    }
                }
                else
                {
                    return new ConnectionStringSettingsCollection(connectionStrings.Do(p => p.Value.Name = p.Key).Select(p => p.Value));
                }
            }

            return connectionStringCollection;
        }

        public static ConnectionStringSettings ConnectionString(this IConfigurationRoot configuration, string name, string section = "ConnectionStrings")
        {
            ConnectionStringSettings connectionStringSettings;

            var connectionStringCollection = configuration.GetSection(section).Get<ConnectionStringSettingsCollection>();
            if (connectionStringCollection == null || connectionStringCollection.Count == 0 || connectionStringCollection.All(c => c.Name == null))
            {
                var connectionStrings = configuration.GetSection(section).Get<Dictionary<string, ConnectionStringSettings>>();
                if (connectionStrings == null || connectionStrings.Count == 0)
                {
                    var connectionStringMap = configuration.GetSection(section).Get<Dictionary<string, string>>();
                    if (connectionStringMap == null || connectionStringMap.Count == 0)
                    {
                        return null;
                    }
                    else if (!connectionStringMap.TryGetValue(name, out var connectionString))
                    {
                        return null;
                    }
                    else
                    {
                        connectionStringSettings = new ConnectionStringSettings { ConnectionString = connectionString, Name = name };
                    }
                }
                else if (!connectionStrings.TryGetValue(name, out connectionStringSettings))
                {
                    return null;
                }
            }
            else if (!connectionStringCollection.TryGetValue(name, out connectionStringSettings))
            {
                return null;
            }

            if (connectionStringSettings.Name == null)
            {
                connectionStringSettings.Name = name;
            }
            return connectionStringSettings;
        }
    }
}
#endif