using Dapper;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Json;
using Nemo;
using Nemo.Configuration;
using Nemo.Configuration.Mapping;
using Nemo.Data;
using Nemo.Linq;
using Nemo.Serialization;
using Nemo.UnitOfWork;
using System;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Linq.Expressions;

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

            //var selected_customers_A_count = ObjectFactory.Count<Customer>(c => c.CompanyName.StartsWith("A"));
            //var linqCustomersAsync = new NemoQueryableAsync<Customer>().Where(c => c.Id == "ALFKI").Take(10).Skip(selected_customers_A_count).OrderBy(c => c.Id).FirstOrDefault().Result;

            RunNative(500);
            RunEF(500);
            RunExecute(500);
            RunDapper(500, false);
            RunRetrieve(500, false);
            RunNativeWithMapper(500);
            RunSelect(500, false);
        }

        private static void RunEF(int count)
        {
            // Warm-up
            using (var context = new EFContext())
            {
                context.ChangeTracker.AutoDetectChangesEnabled = false;
                context.ChangeTracker.QueryTrackingBehavior = Microsoft.EntityFrameworkCore.QueryTrackingBehavior.NoTracking;
                var customer = context.Customers.FirstOrDefault(c => c.Id == "ALFKI");
            }

            var timer = new Stopwatch();
            {
                using (var context = new EFContext())
                {
                    context.ChangeTracker.AutoDetectChangesEnabled = false;
                    context.ChangeTracker.QueryTrackingBehavior = Microsoft.EntityFrameworkCore.QueryTrackingBehavior.NoTracking;

                    timer.Start();
                    for (var i = 0; i < count; i++)
                    {
                        var customer = context.Customers.FirstOrDefault(c => c.Id == "ALFKI");
                    }
                    timer.Stop();
                }
            }

            Console.WriteLine("Entity Framework: " + timer.Elapsed.TotalMilliseconds);
        }

        private static void RunNativeWithMapper(int count)
        {
            var connection = DbFactory.CreateConnection("DbConnection");
            const string sql = @"select CustomerID, CompanyName from Customers where CustomerID = @CustomerID";

            connection.Open();

            // Warm-up
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = sql;
                cmd.CommandType = CommandType.Text;
                var param = cmd.CreateParameter();
                param.ParameterName = "CustomerId";
                param.Value = "ALFKI";
                cmd.Parameters.Add(param);
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var customer = ObjectFactory.Map<IDataReader, Customer>(reader);
                    }
                }
            }

            var timer = new Stopwatch();
            timer.Start();
            for (var i = 0; i < count; i++)
            {
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = sql;
                    cmd.CommandType = CommandType.Text;
                    var param = cmd.CreateParameter();
                    param.ParameterName = "CustomerId";
                    param.Value = "ALFKI";
                    cmd.Parameters.Add(param);
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var customer = ObjectFactory.Map<IDataReader, Customer>(reader);
                        };
                    }
                }
            }
            timer.Stop();
            connection.Close();

            Console.WriteLine("Native+Nemo.Mapper: " + timer.Elapsed.TotalMilliseconds);
        }

        private static void RunNative(int count)
        {
            var connection = DbFactory.CreateConnection("DbConnection");
            const string sql = @"select CustomerID, CompanyName from Customers where CustomerID = @CustomerID";

            connection.Open();

            // Warm-up
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = sql;
                cmd.CommandType = CommandType.Text;
                var param = cmd.CreateParameter();
                param.ParameterName = "CustomerId";
                param.Value = "ALFKI";
                cmd.Parameters.Add(param);
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read()) { }
                }
            }

            var timer = new Stopwatch();
            timer.Start();
            for (var i = 0; i < count; i++)
            {
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = sql;
                    cmd.CommandType = CommandType.Text;
                    var param = cmd.CreateParameter();
                    param.ParameterName = "CustomerId";
                    param.Value = "ALFKI";
                    cmd.Parameters.Add(param);
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read()) { };
                    }
                }
            }
            timer.Stop();
            connection.Close();

            Console.WriteLine("Native: " + timer.Elapsed.TotalMilliseconds);
        }

        private static void RunExecute(int count)
        {
            var connection = DbFactory.CreateConnection("DbConnection");

            connection.Open();

            var req = new OperationRequest { Operation = @"select CustomerID, CompanyName from Customers where CustomerID = @CustomerID", Parameters = new[] { new Param { Name = "CustomerId", Value = "ALFKI", DbType = DbType.String } }, ReturnType = OperationReturnType.SingleResult, OperationType = Nemo.OperationType.Sql, Connection = connection };

            // Warm-up
            var response = ObjectFactory.Execute<ICustomer>(req);
            using (var reader = (IDataReader)response.Value)
            {
                while (reader.Read()) { }
            }

            var timer = new Stopwatch();
            timer.Start();
            for (var i = 0; i < count; i++)
            {
                response = ObjectFactory.Execute<ICustomer>(req);
                using (var reader = (IDataReader)response.Value)
                {
                    while (reader.Read()) { }
                }
            }
            timer.Stop();
            connection.Close();

            Console.WriteLine("Nemo.Execute:" + timer.Elapsed.TotalMilliseconds);
        }

        private static void RunRetrieve(int count, bool cached)
        {
            var connection = DbFactory.CreateConnection("DbConnection");
            const string sql = @"select CustomerID, CompanyName from Customers where CustomerID = @CustomerID";
            var parameters = new[] { new Param { Name = "CustomerId", Value = "ALFKI", DbType = DbType.String } };

            connection.Open();

            // Warm-up
            var result = ObjectFactory.Retrieve<ICustomer>(connection: connection, sql: sql, parameters: parameters, cached: cached).FirstOrDefault();

            var timer = new Stopwatch();
            timer.Start();
            for (var i = 0; i < count; i++)
            {
                result = ObjectFactory.Retrieve<ICustomer>(connection: connection, sql: sql, parameters: parameters, cached: cached).FirstOrDefault();
            }
            timer.Stop();
            connection.Close();

            Console.WriteLine("Nemo.Retrieve: " + timer.Elapsed.TotalMilliseconds);
        }

        private static void RunSelect(int count, bool buffered = false)
        {
            var connection = DbFactory.CreateConnection("DbConnection");
            Expression<Func<ICustomer, bool>> predicate = c => c.Id == "ALFKI";

            connection.Open();

            // Warm-up
            var result = ObjectFactory.Select(predicate, connection: connection).FirstOrDefault();

            var timer = new Stopwatch();
            timer.Start();
            for (var i = 0; i < count; i++)
            {
                result = ObjectFactory.Select(predicate, connection: connection).FirstOrDefault();
            }
            timer.Stop();
            connection.Close();

            Console.WriteLine("Nemo.Select: " + timer.Elapsed.TotalMilliseconds);
        }

        private static void RunDapper(int count, bool buffered)
        {
            var connection = DbFactory.CreateConnection("DbConnection");
            var sql = @"select CustomerID as Id, CompanyName from Customers where CustomerID = @CustomerID";

            connection.Open();
            // Warm-up
            var result = connection.Query<Customer>(sql, new { CustomerID = "ALFKI" }, buffered: buffered).FirstOrDefault();
            var timer = new Stopwatch();
            timer.Start();
            for (var i = 0; i < count; i++)
            {
                result = connection.Query<Customer>(sql, new { CustomerID = "ALFKI" }, buffered: buffered).FirstOrDefault();
            }
            timer.Stop();

            connection.Close();

            Console.WriteLine("Dapper: " + timer.Elapsed.TotalMilliseconds);
        }
    }
}
