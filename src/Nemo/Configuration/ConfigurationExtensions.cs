#if NETSTANDARD
using System;
using Microsoft.Extensions.DependencyInjection;

namespace Nemo.Configuration
{
    public static class ConfigurationExtensions
    {
        public static IServiceCollection AddNemo(this IServiceCollection services, Action<IConfiguration> builder = null)
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
        private readonly Action<IConfiguration> _builder;

        public NemoConfigurationBuilder(Action<IConfiguration> builder)
        {
            _builder = builder;
        }

        public void Build(IConfiguration configuration)
        {
            _builder?.Invoke(configuration);

        }
    }
}
#endif