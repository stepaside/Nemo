using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Jobs;
using Dapper;
using Microsoft.Extensions.Configuration;
using Nemo.Benchmark.Entities;
using Nemo.Configuration;
using Nemo.Data;
using Nemo.Serialization;
using Nemo.UnitOfWork;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nemo.Benchmark
{
    //[SimpleJob(RunStrategy.ColdStart, RuntimeMoniker.Net472, baseline: true, warmupCount: 1)]
    [SimpleJob(RunStrategy.ColdStart, RuntimeMoniker.NetCoreApp31, warmupCount: 1, launchCount: 5)]
    //[SimpleJob(RunStrategy.ColdStart, RuntimeMoniker.NetCoreApp50, warmupCount: 1, invocationCount: 5)]
    [RPlotExporter, MemoryDiagnoser, AllStatisticsColumn]
    public class OrmBenchmark
    {
        private IConfigurationRoot _config;
        private Configuration.IConfiguration _nemoConfig;
        private System.Data.Common.DbConnection _connection;
        private List<object> _idList;

        const string sql = @"select CustomerID, CompanyName from Customers";
        const string sqlById = @"select CustomerID, CompanyName from Customers where CustomerID = @CustomerId";

        [GlobalSetup]
        public void Setup()
        {
            _config = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory()).AddJsonFile("appsettings.json").Build();

            _nemoConfig = (ConfigurationFactory.Configure() ?? ConfigurationFactory.DefaultConfiguration)
                .SetDefaultChangeTrackingMode(ChangeTrackingMode.Debug)
                .SetDefaultMaterializationMode(MaterializationMode.Exact)
                .SetDefaultCacheRepresentation(CacheRepresentation.None)
                .SetDefaultSerializationMode(SerializationMode.Compact)
                .SetOperationNamingConvention(OperationNamingConvention.Default)
                .SetOperationPrefix("spDTO_")
                .SetAutoTypeCoercion(true)
                .SetLogging(false)
                .SetSystemConfiguration(_config);
        }

        [IterationSetup]
        public void Warmup()
        {
            _connection = DbFactory.CreateConnection("DbConnection", _nemoConfig);
            _connection.Open();
        }

        [IterationCleanup]
        public void Cleanup()
        {
            _connection?.Close();
        }
                
        public IEnumerable<object> CustomerIdList
        {
            get
            {
                if (_idList != null) return _idList;

                _idList = new List<object>();

                var config = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory()).AddJsonFile("appsettings.json").Build();
                using (var connection = DbFactory.CreateConnection(config.GetConnectionString("DbConnection")))
                {
                    connection.Open();
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = "select top 20 CustomerID from Customers order by newid()";
                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                _idList.Add(reader.GetString(0));
                            }
                        }
                    }
                }

                return _idList;
            }
        }

        [Benchmark(Description = "EF Select All")]
        public void RunEF()
        {
            using (var context = new EFContext())
            {
                context.ChangeTracker.AutoDetectChangesEnabled = false;
                context.ChangeTracker.QueryTrackingBehavior = Microsoft.EntityFrameworkCore.QueryTrackingBehavior.NoTracking;
                var customer = context.Customers.ToList();
            }

        }

        [Benchmark(Description = "EF Select By Id")]
        [ArgumentsSource(nameof(CustomerIdList))]
        public void RunEF(string id)
        {
            using (var context = new EFContext())
            {
                context.ChangeTracker.AutoDetectChangesEnabled = false;
                context.ChangeTracker.QueryTrackingBehavior = Microsoft.EntityFrameworkCore.QueryTrackingBehavior.NoTracking;
                var customer = context.Customers.Find(id);
            }
        }

        [Benchmark(Description = "NativeWithMapper Select All")]
        public void RunNativeWithMapper()
        {
            using (var cmd = _connection.CreateCommand())
            {
                cmd.CommandText = sql;
                cmd.CommandType = CommandType.Text;
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var customer = new Customer();
                        ObjectFactory.Map(reader, customer);
                    }
                }
            }
        }

        [Benchmark(Description = "NativeWithMapper Select By Id")]
        [ArgumentsSource(nameof(CustomerIdList))]
        public void RunNativeWithMapper(string id)
        {
            using (var cmd = _connection.CreateCommand())
            {
                cmd.CommandText = sqlById;
                cmd.CommandType = CommandType.Text;
                var param = cmd.CreateParameter();
                param.ParameterName = "CustomerId";
                param.Value = id;
                cmd.Parameters.Add(param);
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var customer = new Customer();
                        ObjectFactory.Map(reader, customer);
                    }
                }
            }
        }

        [Benchmark(Description = "Native Select All")]
        public void RunNative()
        {
            using (var cmd = _connection.CreateCommand())
            {
                cmd.CommandText = sql;
                cmd.CommandType = CommandType.Text;
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read()) { };
                }
            }
        }

        [Benchmark(Description = "Native Select By Id")]
        [ArgumentsSource(nameof(CustomerIdList))]
        public void RunNative(string id)
        {
            using (var cmd = _connection.CreateCommand())
            {
                cmd.CommandText = sqlById;
                cmd.CommandType = CommandType.Text;
                var param = cmd.CreateParameter();
                param.ParameterName = "CustomerId";
                param.Value = id;
                cmd.Parameters.Add(param);
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read()) { };
                }
            }
        }

        [Benchmark(Description = "Execute Select All")]
        public void RunExecute()
        {
            var req = new OperationRequest { Operation = sql, ReturnType = OperationReturnType.SingleResult, OperationType = OperationType.Sql, Connection = _connection };
            var response = ObjectFactory.Execute(req);
            using (var reader = (IDataReader)response.Value)
            {
                while (reader.Read()) { }
            }
        }

        [Benchmark(Description = "Execute Select By Id")]
        [ArgumentsSource(nameof(CustomerIdList))]
        public void RunExecute(string id)
        {
            var req = new OperationRequest { Operation = sqlById, Parameters = new[] { new Param { Name = "CustomerId", Value = id, DbType = DbType.String } }, ReturnType = OperationReturnType.SingleResult, OperationType = OperationType.Sql, Connection = _connection };
            var response = ObjectFactory.Execute(req);
            using (var reader = (IDataReader)response.Value)
            {
                while (reader.Read()) { }
            }
        }

        [Benchmark(Description = "Retrieve Select All")]
        public void RunRetrieve()
        {
            var result = ObjectFactory.Retrieve<Customer>(connection: _connection, sql: sql, cached: false, config: _nemoConfig).ToList();
        }

        [Benchmark(Description = "Retrieve Select By Id")]
        [ArgumentsSource(nameof(CustomerIdList))]
        public void RunRetrieve(string id)
        {
            var parameters = new[] { new Param { Name = "CustomerId", Value = id, DbType = DbType.String } };
            var result = ObjectFactory.Retrieve<Customer>(connection: _connection, sql: sqlById, parameters: parameters, cached: false, config: _nemoConfig).ToList();
        }

        [Benchmark(Description = "Nemo Select All")]
        public void RunSelect()
        {
            var result = ObjectFactory.Select<Customer>(null, connection: _connection, cached: false, config: _nemoConfig).ToList();
        }

        [Benchmark(Description = "Nemo Select By Id")]
        [ArgumentsSource(nameof(CustomerIdList))]
        public void RunSelect(string id)
        {
            var result = ObjectFactory.Select<Customer>(c => c.Id == id, connection: _connection, cached: false, config: _nemoConfig).ToList();
        }

        [Benchmark(Description = "Dapper Select All")]
        public void RunDapper()
        {
            var result = _connection.Query<Customer>(sql, null, buffered: false).ToList();
        }

        [Benchmark(Description = "Dapper Select By Id")]
        [ArgumentsSource(nameof(CustomerIdList))]
        public void RunDapper(string id)
        {
            var result = _connection.Query<Customer>(sql, new { CustomerId = id }, buffered: false).ToList();
        }
    }
}
