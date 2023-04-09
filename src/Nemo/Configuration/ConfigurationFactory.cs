using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Nemo.Configuration.Mapping;
using Nemo.Reflection;

namespace Nemo.Configuration
{
    public class ConfigurationFactory
    {
        private static Lazy<INemoConfiguration> _configuration = new(() =>
        {
            MappingFactory.Initialize();
            return new DefaultNemoConfiguration();
        }, true);

        private static readonly ConcurrentDictionary<Type, INemoConfiguration> _typedConfigurations = new();

        public static string DefaultConnectionName
        {
            get { return DefaultConfiguration.DefaultConnectionName; }
        }

        public static INemoConfiguration DefaultConfiguration
        {
            get
            {
                return _configuration.Value;
            }
        }

        public static INemoConfiguration Configure(Func<INemoConfiguration> config = null)
        {
            if (_configuration.IsValueCreated) return null;

            MappingFactory.Initialize();
            if (config != null)
            {
                _configuration = new Lazy<INemoConfiguration>(config, true);
            }
            return _configuration.Value;
        }

        public static void RefreshEntityMappings()
        {
            MappingFactory.Initialize();
        }

        public static INemoConfiguration CloneCurrentConfiguration()
        {
            var clone = new DefaultNemoConfiguration();
            return clone.Merge(_configuration.Value);
        }

        public static INemoConfiguration CloneConfiguration(INemoConfiguration config)
        {
            if (config == null) return null;

            var clone = new DefaultNemoConfiguration();
            return clone.Merge(config);
        }

        public static INemoConfiguration Get<T>()
        {
            return Get(typeof(T));
        }

        public static INemoConfiguration Get(Type type)
        {
            var globalConfig = DefaultConfiguration;
            if (!IsConfigurable(type)) return globalConfig;

            if (_typedConfigurations.TryGetValue(type, out var typedConfig)) return typedConfig;    
            return globalConfig;
        }

        public static void Set<T>(INemoConfiguration configuration)
        {
            Set(typeof(T), configuration);
        }

        public static void Set(Type type, INemoConfiguration configuration)
        {
            if (configuration == null || !IsConfigurable(type)) return;

            var globalConfig = DefaultConfiguration;
            _typedConfigurations[type] =  configuration.Merge(globalConfig);
        }

        private static bool IsConfigurable(Type type)
        {
            var refletectType = Reflector.GetReflectedType(type);
            return !refletectType.IsSimpleType && !refletectType.IsSimpleList;
        }
    }
}
