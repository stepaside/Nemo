using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Nemo;
using Nemo.Configuration;
using Nemo.Data;
using Nemo.Serialization;
using Nemo.UnitOfWork;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NemoTestCore.Features
{
    internal class Db
    {
        public static void GetProviderSpecificFactory()
        {
            var factory = DbFactory.GetDbProviderFactory(DataAccessProviderTypes.SqlServer);
        }

        public static void CreateProviderSpecificConnection()
        {
            var connection = DbFactory.CreateConnection("Data Source=.;Initial Catalog=Northwind;Uid=sa;Pwd=Passw0rd;", DataAccessProviderTypes.SqlServer);
        }
        
        public static void CreateConnectionBasedOnConfigurationFile()
        {
            // Connection string must configured with provider name.
            // In .Net framework provider is part of the confguration element.
            // However in .Net Core and above that is not the case.
            // In this case we can include provider name in the connection string (which is then removed)
            // or utilize Nemo specific JSON model for connection string configuration. This is fast and reliable way to specify data provider.
            // Unfortunately, both of these options reduce interoperability with other libraries.
            // If interoprability is important then Nemo will make an attempt to guess the provider by connection string
            // using brute force verification.
            // Then the discovered provider is cached by connection string.
            var guessed_connection = DbFactory.CreateConnection("Data Source=localhost;Initial Catalog=Northwind;Uid=sa;Pwd=Passw0rd;");

            var guessed_connection_by_name = DbFactory.CreateConnection("DbConnection");
        }
    }
}
