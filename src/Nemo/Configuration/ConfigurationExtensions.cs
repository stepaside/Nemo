#if NETSTANDARD2_0_OR_GREATER
using System;
using Microsoft.Extensions.DependencyInjection;

namespace Nemo.Configuration
{
    public static class ConfigurationExtensions
    {
        public static IServiceCollection AddNemo(this IServiceCollection services, Action<INemoConfiguration> builder = null)
        {
            services.AddSingleton(new NemoConfigurationBuilder(builder));
            services.AddSingleton<NemoConfigurationService>();
            return services;
        }
    }

    public class NemoConfigurationService
    {
        public NemoConfigurationService(Microsoft.Extensions.Configuration.IConfigurationRoot configuration, NemoConfigurationBuilder builder)
        {
            var config = ConfigurationFactory.Configure();
            builder?.Build(config);
            config.SetSystemConfiguration(configuration);
        }
    }

    public class NemoConfigurationBuilder
    {
        private readonly Action<INemoConfiguration> _builder;

        public NemoConfigurationBuilder(Action<INemoConfiguration> builder)
        {
            _builder = builder;
        }

        public void Build(INemoConfiguration configuration)
        {
            _builder?.Invoke(configuration);

        }
    }
}
#endif