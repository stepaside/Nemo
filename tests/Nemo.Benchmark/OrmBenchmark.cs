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
    [SimpleJob(RunStrategy.ColdStart, RuntimeMoniker.NetCoreApp31, baseline: true, warmupCount: 1)]
    //[SimpleJob(RunStrategy.ColdStart, RuntimeMoniker.NetCoreApp50, warmupCount: 1)]
    [RPlotExporter, MemoryDiagnoser, MinColumn, MaxColumn, MeanColumn, MedianColumn, RankColumn]
    public class OrmBenchmark
    {
        private IConfigurationRoot _config;
        private Configuration.IConfiguration _nemoConfig;
        private System.Data.Common.DbConnection _connection;

        const string sql = @"select CustomerID, CompanyName from Customers";

        [Params(500)]
        public int Count;

        [GlobalSetup]
        public void Setup()
        {
            _config = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory()).AddJsonFile("appsettings.json").Build();

            _nemoConfig = ConfigurationFactory.Configure()
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

        [Benchmark]
        [Arguments(true)]
        [Arguments(false)]
        public void RunEF(bool reuseContext)
        {
            if (reuseContext)
            {
                using (var context = new EFContext())
                {
                    context.ChangeTracker.AutoDetectChangesEnabled = false;
                    context.ChangeTracker.QueryTrackingBehavior = Microsoft.EntityFrameworkCore.QueryTrackingBehavior.NoTracking;

                    for (var i = 0; i < Count; i++)
                    {
                        var customer = context.Customers.ToList();//FirstOrDefault(c => c.Id == "ALFKI");
                    }
                }
            }
            else
            {
                for (var i = 0; i < Count; i++)
                {
                    using (var context = new EFContext())
                    {
                        context.ChangeTracker.AutoDetectChangesEnabled = false;
                        context.ChangeTracker.QueryTrackingBehavior = Microsoft.EntityFrameworkCore.QueryTrackingBehavior.NoTracking;

                        var customer = context.Customers.ToList();//FirstOrDefault(c => c.Id == "ALFKI");
                    }
                }
            }
        }

        [Benchmark]
        public void RunNativeWithMapper()
        {
            for (var i = 0; i < Count; i++)
            {
                using (var cmd = _connection.CreateCommand())
                {
                    cmd.CommandText = sql;
                    cmd.CommandType = CommandType.Text;
                    //var param = cmd.CreateParameter();
                    //param.ParameterName = "CustomerId";
                    //param.Value = "ALFKI";
                    //cmd.Parameters.Add(param);
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
        }

        [Benchmark]
        public void RunNative()
        {
            for (var i = 0; i < Count; i++)
            {
                using (var cmd = _connection.CreateCommand())
                {
                    cmd.CommandText = sql;
                    cmd.CommandType = CommandType.Text;
                    //var param = cmd.CreateParameter();
                    //param.ParameterName = "CustomerId";
                    //param.Value = "ALFKI";
                    //cmd.Parameters.Add(param);
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read()) { };
                    }
                }
            }
        }

        [Benchmark]
        public void RunExecute()
        {
            var req = new OperationRequest { Operation = sql, /*Parameters = new[] { new Param { Name = "CustomerId", Value = "ALFKI", DbType = DbType.String } },*/ ReturnType = OperationReturnType.SingleResult, OperationType = OperationType.Sql, Connection = _connection };
            for (var i = 0; i < Count; i++)
            {
                var response = ObjectFactory.Execute(req);
                using (var reader = (IDataReader)response.Value)
                {
                    while (reader.Read()) { }
                }
            }
        }

        [Benchmark]
        //[Arguments(true)]
        [Arguments(false)]
        public void RunRetrieve(bool cached)
        {
            for (var i = 0; i < Count; i++)
            {
                var result = ObjectFactory.Retrieve<Customer>(connection: _connection, sql: sql, /*parameters: parameters,*/ cached: cached, config: _nemoConfig).ToList();
            }
        }

        [Benchmark]
        //[Arguments(true)]
        [Arguments(false)]
        public void RunSelect(bool cached)
        {
            for (var i = 0; i < Count; i++)
            {
                var result = ObjectFactory.Select<Customer>(null, connection: _connection, cached: cached, config: _nemoConfig).ToList();
            }
        }

        [Benchmark]
        public void RunDapper()
        {
            for (var i = 0; i < Count; i++)
            {
                var result = _connection.Query<Customer>(sql, null /*new { CustomerID = "ALFKI" }*/, buffered: false).ToList();
            }
        }
    }
}
