using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Nemo.Configuration.Mapping;
using Nemo.Linq;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace NemoTest
{
    class Program
    {
        private static IHostBuilder CreateHostBuilder(string[] args)
        {
            return Host.CreateDefaultBuilder(args)
            .ConfigureAppConfiguration((hostingContext, configuration) =>
            {
                configuration.Sources.Clear();
                configuration.AddJsonFile("appsettings.json", optional: false, reloadOnChange: false);
            }).ConfigureServices(services =>
            {
                services.AddHostedService<TestService>();
            });
        }

        static async Task Main(string[] args)
        {
            var host = CreateHostBuilder(args).Build();
            await host.RunAsync();
        }
    }
}
