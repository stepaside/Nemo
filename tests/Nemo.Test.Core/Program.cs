using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Json;
using Nemo;
using Nemo.Configuration;
using Nemo.Serialization;
using Nemo.UnitOfWork;
using System;
using System.IO;

namespace NemoTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory()).AddJsonFile("appsettings.json").Build();

            ConfigurationFactory.Configure()
                .SetDefaultChangeTrackingMode(ChangeTrackingMode.Debug)
                .SetDefaultFetchMode(FetchMode.Lazy)
                .SetDefaultMaterializationMode(MaterializationMode.Partial)
                .SetDefaultL1CacheRepresentation(L1CacheRepresentation.None)
                .SetDefaultSerializationMode(SerializationMode.Compact)
                .SetOperationNamingConvention(OperationNamingConvention.PrefixTypeName_Operation)
                .SetOperationPrefix("spDTO_")
                .SetLogging(false)
                .SetSystemConfiguration(config);

            var settings = ConfigurationFactory.Get(typeof(object)).SystemConfiguration.ConnectionString("DbConnection");

            Console.WriteLine(settings.ConnectionString);

        }
    }
}
