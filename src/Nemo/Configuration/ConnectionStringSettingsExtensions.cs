#if NETSTANDARD2_0_OR_GREATER
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Configuration;
using Nemo.Collections.Extensions;

namespace Nemo.Configuration
{
    public static class ConnectionStringSettingsExtensions
    {
        private static readonly ConcurrentDictionary<string, ConnectionStringSettingsCollection> ConnectionStringSettingsCollectionCache = new ConcurrentDictionary<string, ConnectionStringSettingsCollection>();
        private static readonly ConcurrentDictionary<string, ConnectionStringSettings> ConnectionStringSettingsCache = new ConcurrentDictionary<string, ConnectionStringSettings>();

        public static ConnectionStringSettingsCollection ConnectionStrings(this Microsoft.Extensions.Configuration.IConfiguration configuration, string section = "ConnectionStrings")
        {
            return ConnectionStringSettingsCollectionCache.GetOrAdd(section, key =>
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
            });
        }

        public static ConnectionStringSettings ConnectionString(this Microsoft.Extensions.Configuration.IConfiguration configuration, string name, string section = "ConnectionStrings")
        {
            return ConnectionStringSettingsCache.GetOrAdd($"{section}:{name}", key =>
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
            });
        }
    }
}
#endif