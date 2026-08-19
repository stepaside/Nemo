using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Toolchains.InProcess.Emit;
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
    [Config(typeof(BenchmarkConfig))]
    [RPlotExporter, MemoryDiagnoser, AllStatisticsColumn]
    [CategoriesColumn]
    public class OrmBenchmark
    {
        // Warm in-process job: the primary, trustworthy measurement.
        // InvocationCount/UnrollFactor must be set explicitly because IterationSetup/IterationCleanup are used.
        public class BenchmarkConfig : ManualConfig
        {
            public BenchmarkConfig()
            {
                AddJob(Job.Default
                    .WithStrategy(RunStrategy.Throughput)
                    .WithWarmupCount(3)
                    .WithIterationCount(15)
                    .WithInvocationCount(100)
                    .WithUnrollFactor(1)
                    .WithToolchain(InProcessEmitToolchain.Instance));

                // Cold-start job: startup-cost indication only; its means/medians are dominated
                // by first-call effects. Uncomment to include it in a run.
                //AddJob(Job.Default
                //    .WithStrategy(RunStrategy.ColdStart)
                //    .WithWarmupCount(1)
                //    .WithInvocationCount(5)
                //    .WithUnrollFactor(1));
            }
        }

        private IConfigurationRoot _config;
        private Configuration.INemoConfiguration _nemoConfig;
        private System.Data.Common.DbConnection _connection;
        private List<object> _idList;

        const string sql = @"select CustomerID, CompanyName from Customers";
        const string sqlById = @"select CustomerID, CompanyName from Customers where CustomerID = @CustomerId";
        const string dapperSql = @"select CustomerID as Id, CompanyName from Customers";
        const string dapperSqlById = @"select CustomerID as Id, CompanyName from Customers where CustomerID = @CustomerId";

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
                .SetPadListExpansion(true)
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
                        command.CommandText = "select top 5 CustomerID from Customers order by CustomerID";
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
        [BenchmarkCategory("SelectAll")]
        public List<Customer> RunEF()
        {
            using (var context = new EFContext())
            {
                context.ChangeTracker.AutoDetectChangesEnabled = false;
                context.ChangeTracker.QueryTrackingBehavior = Microsoft.EntityFrameworkCore.QueryTrackingBehavior.NoTracking;
                return context.Customers.ToList();
            }
        }

        [Benchmark(Description = "EF Select By Id")]
        [BenchmarkCategory("SelectById")]
        [ArgumentsSource(nameof(CustomerIdList))]
        public Customer RunEF(string id)
        {
            using (var context = new EFContext())
            {
                context.ChangeTracker.AutoDetectChangesEnabled = false;
                context.ChangeTracker.QueryTrackingBehavior = Microsoft.EntityFrameworkCore.QueryTrackingBehavior.NoTracking;
                return context.Customers.Find(id);
            }
        }

        [Benchmark(Description = "NativeWithMapper Select All")]
        [BenchmarkCategory("SelectAll")]
        public Customer RunNativeWithMapper()
        {
            using (var cmd = _connection.CreateCommand())
            {
                cmd.CommandText = sql;
                cmd.CommandType = CommandType.Text;
                using (var reader = cmd.ExecuteReader())
                {
                    var map = ObjectFactory.CreateReaderMapper<Customer>(reader);
                    Customer customer = null;
                    while (reader.Read())
                    {
                        customer = new Customer();
                        map(reader, customer);
                    }
                    return customer;
                }
            }
        }

        [Benchmark(Description = "NativeWithMapper Select By Id")]
        [BenchmarkCategory("SelectById")]
        [ArgumentsSource(nameof(CustomerIdList))]
        public Customer RunNativeWithMapper(string id)
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
                    var map = ObjectFactory.CreateReaderMapper<Customer>(reader);
                    Customer customer = null;
                    while (reader.Read())
                    {
                        customer = new Customer();
                        map(reader, customer);
                    }
                    return customer;
                }
            }
        }

        [Benchmark(Description = "Handwritten Select All")]
        [BenchmarkCategory("SelectAll")]
        public Customer RunHandwritten()
        {
            using (var cmd = _connection.CreateCommand())
            {
                cmd.CommandText = sql;
                cmd.CommandType = CommandType.Text;
                using (var reader = cmd.ExecuteReader())
                {
                    Customer customer = null;
                    while (reader.Read())
                    {
                        customer = new Customer
                        {
                            Id = reader.GetString(0),
                            CompanyName = reader.IsDBNull(1) ? null : reader.GetString(1)
                        };
                    }
                    return customer;
                }
            }
        }

        [Benchmark(Description = "Handwritten Select By Id")]
        [BenchmarkCategory("SelectById")]
        [ArgumentsSource(nameof(CustomerIdList))]
        public Customer RunHandwritten(string id)
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
                    Customer customer = null;
                    while (reader.Read())
                    {
                        customer = new Customer
                        {
                            Id = reader.GetString(0),
                            CompanyName = reader.IsDBNull(1) ? null : reader.GetString(1)
                        };
                    }
                    return customer;
                }
            }
        }

        // Reader-only floor: measures the round trip without materializing objects,
        // so it is not comparable to the mapping benchmarks above.
        [Benchmark(Description = "Native (no mapping) Select All")]
        [BenchmarkCategory("SelectAll")]
        public int RunNative()
        {
            using (var cmd = _connection.CreateCommand())
            {
                cmd.CommandText = sql;
                cmd.CommandType = CommandType.Text;
                using (var reader = cmd.ExecuteReader())
                {
                    var count = 0;
                    while (reader.Read()) { count++; }
                    return count;
                }
            }
        }

        [Benchmark(Description = "Native (no mapping) Select By Id")]
        [BenchmarkCategory("SelectById")]
        [ArgumentsSource(nameof(CustomerIdList))]
        public int RunNative(string id)
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
                    var count = 0;
                    while (reader.Read()) { count++; }
                    return count;
                }
            }
        }

        [Benchmark(Description = "Execute (no mapping) Select All")]
        [BenchmarkCategory("SelectAll")]
        public int RunExecute()
        {
            var req = new OperationRequest { Operation = sql, ReturnType = OperationReturnType.SingleResult, OperationType = OperationType.Sql, Connection = _connection };
            var response = ObjectFactory.Execute(req);
            using (var reader = (IDataReader)response.Value)
            {
                var count = 0;
                while (reader.Read()) { count++; }
                return count;
            }
        }

        [Benchmark(Description = "Execute (no mapping) Select By Id")]
        [BenchmarkCategory("SelectById")]
        [ArgumentsSource(nameof(CustomerIdList))]
        public int RunExecute(string id)
        {
            var req = new OperationRequest { Operation = sqlById, Parameters = new[] { new Param { Name = "CustomerId", Value = id, DbType = DbType.String } }, ReturnType = OperationReturnType.SingleResult, OperationType = OperationType.Sql, Connection = _connection };
            var response = ObjectFactory.Execute(req);
            using (var reader = (IDataReader)response.Value)
            {
                var count = 0;
                while (reader.Read()) { count++; }
                return count;
            }
        }

        [Benchmark(Description = "Retrieve Select All")]
        [BenchmarkCategory("SelectAll")]
        public List<Customer> RunRetrieve()
        {
            return ObjectFactory.Retrieve<Customer>(connection: _connection, sql: sql, cached: false, config: _nemoConfig).ToList();
        }

        [Benchmark(Description = "Retrieve Select By Id")]
        [BenchmarkCategory("SelectById")]
        [ArgumentsSource(nameof(CustomerIdList))]
        public List<Customer> RunRetrieve(string id)
        {
            var parameters = new[] { new Param { Name = "CustomerId", Value = id, DbType = DbType.String } };
            return ObjectFactory.Retrieve<Customer>(connection: _connection, sql: sqlById, parameters: parameters, cached: false, config: _nemoConfig).ToList();
        }

        [Benchmark(Description = "Nemo Select All")]
        [BenchmarkCategory("SelectAll")]
        public List<Customer> RunSelect()
        {
            return ObjectFactory.Select<Customer>(null, connection: _connection, cached: false, config: _nemoConfig).ToList();
        }

        [Benchmark(Description = "Nemo Select By Id")]
        [BenchmarkCategory("SelectById")]
        [ArgumentsSource(nameof(CustomerIdList))]
        public List<Customer> RunSelect(string id)
        {
            return ObjectFactory.Select<Customer>(c => c.Id == id, connection: _connection, cached: false, config: _nemoConfig).ToList();
        }

        [Benchmark(Description = "Dapper Select All")]
        [BenchmarkCategory("SelectAll")]
        public List<Customer> RunDapper()
        {
            return _connection.Query<Customer>(dapperSql, null, buffered: false).ToList();
        }

        [Benchmark(Description = "Dapper Select By Id")]
        [BenchmarkCategory("SelectById")]
        [ArgumentsSource(nameof(CustomerIdList))]
        public List<Customer> RunDapper(string id)
        {
            return _connection.Query<Customer>(dapperSqlById, new { CustomerId = id }, buffered: false).ToList();
        }
    }
}
