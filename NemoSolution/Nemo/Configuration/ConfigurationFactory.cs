using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Nemo.Configuration.Mapping;

namespace Nemo.Configuration
{
    public class ConfigurationFactory
    {
        private static Lazy<IConfiguration> _configuration = new Lazy<IConfiguration>(DefaultConfiguration.New, true);

        public static string DefaultConnectionName
        {
            get { return Default.DefaultConnectionName; }
        }

        internal static IConfiguration Default
        {
            get
            {
                return _configuration.Value;
            }
        }

        public static IConfiguration Configure(Func<IConfiguration> config = null)
        {
            if (_configuration.IsValueCreated) return null;

            MappingFactory.Initialize();
            if (config != null)
            {
                _configuration = new Lazy<IConfiguration>(config, true);
            }
            return _configuration.Value;
        }

        public static IConfiguration Get<T>()
            where T : class
        {
            return Get(typeof(T));
        }

        public static IConfiguration Get(Type type)
        {
            var globalConfig = Default;
            var configurationKey = type.FullName + "/Configuration";
            var config = (IConfiguration)globalConfig.ExecutionContext.Get(configurationKey);
            return config != null ? config.Merge(globalConfig) : globalConfig;
        }

        public static void Set<T>(IConfiguration configuration)
            where T : class
        {
            Set(typeof(T), configuration);
        }

        public static void Set(Type type, IConfiguration configuration)
        {
            var configurationKey = type.FullName + "/Configuration";
            Default.ExecutionContext.Set(configurationKey, configuration);
        }

    }
}
