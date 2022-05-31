using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Nemo;
using Nemo.Configuration;
using Nemo.Serialization;
using Nemo.UnitOfWork;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NemoTestCore.Features
{
    internal class Config
    {
        private static readonly object _lock = new();

        public static void InitializeConfiguration()
        {
            // This call also initialize all entity mappings
            var nemoConfig = ConfigurationFactory.Configure();
        }

        public static void InitializeConfigurationUsingFactory(IServiceProvider provider)
        {
            // This call also initialize all entity mappings
            var nemoConfig = ConfigurationFactory.Configure(() => provider.GetService<INemoConfiguration>());
        }

        public static void UsingDefaultConfiguration()
        {
            // This call also initialize all entity mappings
            // Returns a singleton instance of DefaultNemoConfiguration
            var nemoConfig = ConfigurationFactory.DefaultConfiguration;
            // You can modify default condifuration values
            // Each method call is NOT thread-safe
            // It is recommended to initialize all default configuration settings together as an atomic action 
            lock (_lock)
            {
                nemoConfig.SetDefaultCacheRepresentation(CacheRepresentation.LazyList)
                    .SetPadListExpansion(true)
                    .SetAutoTypeCoercion(true)
                    .SetDefaultChangeTrackingMode(ChangeTrackingMode.Manual)
                    .SetDefaultMaterializationMode(MaterializationMode.Exact)
                    .SetDefaultSerializationMode(SerializationMode.Compact);
            }
        }

        public static void UsingDefaultConfigurationWithMicrosoftConfiguration(IConfiguration configuration )
        {
            var nemoConfig = ConfigurationFactory.DefaultConfiguration;
            // This is only available in .Net Core and above
            lock (_lock)
            {
                nemoConfig.SetSystemConfiguration(configuration);
            }
        }
    }
}
