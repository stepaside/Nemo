using Dapper;
using Nemo;
using Nemo.Collections;
using Nemo.Collections.Extensions;
using Nemo.Configuration;
using Nemo.Configuration.Mapping;
using Nemo.Data;
using Nemo.Extensions;
using Nemo.Fn;
using Nemo.Linq;
using Nemo.Serialization;
using Nemo.UnitOfWork;
using Nemo.Utilities;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Runtime.Serialization.Json;
using System.Text;
using System.Xml;

namespace NemoTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var nemoConfig = ConfigurationFactory.Configure()
                .SetDefaultChangeTrackingMode(ChangeTrackingMode.Debug)
                .SetDefaultMaterializationMode(MaterializationMode.Partial)
                .SetDefaultCacheRepresentation(CacheRepresentation.None)
                .SetDefaultSerializationMode(SerializationMode.Compact)
                .SetOperationNamingConvention(OperationNamingConvention.PrefixTypeName_Operation)
                .SetOperationPrefix("spDTO_")
                .SetAutoTypeCoercion(true)
                .SetIgnoreInvalidParameters(true)
                .SetPadListExpansion(true)
                .SetLogging(false);

            var factory = DbFactory.GetDbProviderFactory(DataAccessProviderTypes.SqlServer);

            var guessed_connection = DbFactory.CreateConnection("Data Source=localhost;Initial Catalog=Northwind;Uid=sa;Pwd=Passw0rd;");

            var connection = DbFactory.CreateConnection("Data Source=.;Initial Catalog=Northwind;Uid=sa;Pwd=Passw0rd;", DataAccessProviderTypes.SqlServer);

            //// Simple retrieve with dynamic parameters
            //var retrieve_customer_test = ObjectFactory.Retrieve<Customer>(parameters: new { CustomerID = "ALFKI" }).FirstOrDefault();

            var person_legacy = new PersonLegacy { person_id = 12345, name = "John Doe", DateOfBirth = new DateTime(1980, 1, 10) };
            var person_anonymous = new { person_id = 12345, name = "John Doe" };

            var dict = person_anonymous.ToDictionary();

            var company = new Company { Name = "Test Company" };
            company.Contacts.Add(new Manager { Name = "Manager 1", HireDate = new DateTime(1990, 12, 20) });
            company.Contacts.Add(new Manager { Name = "Manager 2", HireDate = new DateTime(1990, 12, 20) });

            var manager = (Manager)company.Contacts[0];
            manager.Employees.Add(new Employee { Name = "Employee 1.1", HireDate = new DateTime(1990, 12, 20) });
            manager.Employees.Add(new Employee { Name = "Employee 1.2", HireDate = new DateTime(1990, 12, 20) });

            //manager.Employees[0].Manager = manager;
            //manager.Employees[1].Manager = manager;

            var packedJson = company.ToJson();
            var unpackedJson = packedJson.FromJson<Company>();

            var packedXml = company.ToXml();
            var unpackedXml = packedXml.FromXml<Company>();

            var packed = company.Serialize();
            var unpacked = packed.Deserialize<Company>();

            // Create an instance
            var created = ObjectFactory.Create<ICustomer>();
            var created_new = ObjectFactory.Create<Customer>();

            // Binding to a legacy object
            var bound_legacy = ObjectFactory.Bind<IPerson>(person_legacy);

            // Binding to an anonymous object
            var bound_anonymous = ObjectFactory.Bind<IPersonReadOnly>(person_anonymous);

            // Mapping to a legacy object
            var mapped_legacy = ObjectFactory.Map<PersonLegacy, IPerson>(person_legacy);

            // Mapping from an anonymous object uses reflection
            var mapped_anonymous_null = ObjectFactory.Map<IPerson>(person_anonymous);

            // Mapping from an anonymous object via binding
            var mapped_anonymous = ObjectFactory.Map<IPerson>(ObjectFactory.Bind<IPersonReadOnly>(person_anonymous));

            // Dynamic select
            var selected_customers_10 = ObjectFactory.Select<Customer>(page: 1, pageSize: 10).ToList();
            //var selected_customers_10_repeat = ObjectFactory.Select<Customer>(page: 1, pageSize: 10).ToList();
            var selected_customers_A = ObjectFactory.Select<ICustomer>(c => c.CompanyName.StartsWith("A"), page: 1, pageSize: 2);

            var max1 = ObjectFactory.Max<Order, int>(o => o.OrderId, o => o.CustomerId == "ALFKI");
            var max2 = new NemoQueryable<Order>().Max(o => o.OrderId);
            var max3 = new NemoQueryable<Order>().Where(o => o.CustomerId == "ALFKI").Max(o => o.OrderId);

            var maxAsync = new NemoQueryableAsync<Order>().MaxAsync(o => o.OrderId).Result;

            var count1 = new NemoQueryable<Customer>().Count(c => c.Id == "ALFKI");
            var count2 = new NemoQueryable<Customer>().Where(c => c.Id == "ALFKI").Count();

            var selected_customers_A_count = ObjectFactory.Count<ICustomer>(c => c.CompanyName.StartsWith("A"));
            var linqCustomers = new NemoQueryable<Customer>().Where(c => c.Id == "ALFKI").Take(10).Skip(selected_customers_A_count).OrderBy(c => c.Id).ToList();
            var linqCustomersQuery = (from c in new NemoQueryable<Customer>()
                                        where c.Id == "ALFKI"
                                        orderby c.Id ascending
                                        select c).Take(10).Skip(selected_customers_A_count).ToList();

            var allCustomers = new NemoQueryable<Customer>().ToList();

            var linqCustomer = new NemoQueryable<Customer>().FirstOrDefault(c => c.Id == "ALFKI");

            var linqCustomersAsync = new NemoQueryableAsync<Customer>().Where(c => c.Id == "ALFKI").Take(10).Skip(selected_customers_A_count).OrderBy(c => c.Id).FirstOrDefaultAsync().Result;

            var selected_customers_with_orders = ObjectFactory.Select<ICustomer>(c => c.Orders.Count > 0);

            var selected_customers_and_orders_include = ObjectFactory.Select<ICustomer>(c => c.Orders.Count > 0).Include<ICustomer, IOrder>((c, o) => c.Id == o.CustomerId).ToList();

            // Simple retrieve with dynamic parameters
            var retrieve_customer_dyn = ObjectFactory.Retrieve<Customer>(parameters: new { CustomerID = "ALFKI" }).FirstOrDefault();

            // Simple retrieve with actual parameters
            var retrieve_customer = ObjectFactory.Retrieve<ICustomer>(parameters: new[] { new Param { Name = "CustomerID", Value = "ALFKI" } }).FirstOrDefault();

            // Simple retrieve with dynamic parameters and custom operation name
            var retrieve_customers_by_country = ObjectFactory.Retrieve<ICustomer>(operation: "RetrieveByCountry", parameters: new ParamList { Country => "USA", State => "PA" }).Memoize();

            if (retrieve_customers_by_country.Any())
            {
                var stream_customer_count = retrieve_customers_by_country.Count();
            }

            var stream_customer = retrieve_customers_by_country.FirstOrDefault();

            // Simple retrieve with sql statement operation
            var retrieve_customer_sql = ObjectFactory.Retrieve<ICustomer>(sql: "select * from Customers where CustomerID = @CustomerID", parameters: new ParamList { CustomerID => "ALFKI" });

            var retrieve_many_customer_sql = ObjectFactory.Retrieve<ICustomer>(sql: "select * from Customers where CustomerID in (@CustomerIDs)", parameters: new { CustomerIDs = new[] { "ALFKI", "ANTON" } });

            var retrieve_customer_orders_ids_sql = ObjectFactory.Retrieve<int>(sql: "select OrderID from Orders where CustomerID = @CustomerID", parameters: new ParamList { CustomerID => "ALFKI" });

            // Advanced!
            // Retrieve customers with orders as object graph
            var retrieve_customer_with_orders_graph = ((IMultiResult)ObjectFactory.Retrieve<Customer, Order>(
                                                                    sql: @"select * from Customers where CustomerID = @CustomerID; 
                                                                                        select * from Orders where CustomerID = @CustomerID",
                                                                    parameters: new ParamList { CustomerId => "ALFKI" })).Aggregate<Customer>();

            var customer = retrieve_customer_with_orders_graph.First();

            // Advanced!
            // Retrieve orders with customer as a single row mapping
            var retrieve_orders_with_customer = ObjectFactory.Retrieve<IOrder, ICustomer>(
                                                                    sql: @"select c.CustomerID, c.CompanyName, o.OrderID, o.ShipPostalCode from Customers c
                                                                                        left join Orders o on o.CustomerID = c.CustomerID 
                                                                                        where c.CustomerID = @CustomerID",
                                                                    parameters: new ParamList { CustomerId => "ALFKI" },
                                                                    map: (o, c) => { o.Customer = c; return o; });
            var orders = retrieve_orders_with_customer.ToList();
            var same = orders[0].Customer == orders[1].Customer;

            // Advanced!
            // Retrieve customers with orders as a single row mapping
            var aggregate_mapper = new DefaultAggregatePropertyMapper<ICustomer, IOrder>();
            var retrieve_customer_with_orders = ObjectFactory.Retrieve<ICustomer, IOrder>(
                                                                    sql: @"select c.CustomerID, c.CompanyName, o.OrderID, o.ShipPostalCode from Customers c
                                                                                        left join Orders o on o.CustomerID = c.CustomerID 
                                                                                        where c.CustomerID = @CustomerID",
                                                                    parameters: new ParamList { CustomerId => "ALFKI" },
                                                                    map: aggregate_mapper.Map).FirstOrDefault();

            // Advanced!
            // Retrieve customers with orders as multi-result
            var retrieve_customer_with_orders_lazy = ObjectFactory.Retrieve<ICustomer, IOrder>(
                                                                    sql: @"select * from Customers where CustomerID = @CustomerID;
                                                                                        select * from Orders where CustomerID = @CustomerID",
                                                                    parameters: new ParamList { CustomerId => "ALFKI" });

            var lazy_customer = ((IMultiResult)retrieve_customer_with_orders_lazy).Retrieve<ICustomer>().FirstOrDefault();
            //var lazy_customer = retrieve_customer_with_orders_lazy.FirstOrDefault();
            var lazy_orders = ((IMultiResult)retrieve_customer_with_orders_lazy).Retrieve<IOrder>().ToList();

            // Advanced!
            // Retrieve customers with orders as multi-result
            var retrieve_simple_lists = ObjectFactory.Retrieve<string, int, dynamic>(
                                                                    sql: @"select CustomerID from Customers;
                                                                            select OrderID from Orders;
                                                                            select CustomerID, OrderID from Orders",
                                                                    parameters: new ParamList { CustomerId => "ALFKI" });

            var customer_id_list = ((IMultiResult)retrieve_simple_lists).Retrieve<string>().ToList();
            var order_id_list = ((IMultiResult)retrieve_simple_lists).Retrieve<int>().ToList();
            var dynamic_item_list = ((IMultiResult)retrieve_simple_lists).Retrieve<dynamic>().ToList();

            // UnitOfWork example
            //customer.Orders.Do(o => o.Customer = null).Consume();
            using (ObjectScope.New(customer, autoCommit: false, config: nemoConfig))
            {
                customer.CompanyName += "Test";
                customer.Orders[0].ShipPostalCode = "11111";
                customer.Orders.RemoveAt(1);

                var o = ObjectFactory.Create<Order>();
                o.CustomerId = customer.Id;
                o.ShipPostalCode = "19115";
                o.GenerateKey(nemoConfig);
                customer.Orders.Add(o);

                //var previos = customer.Old();

                //customer_uow.Rollback();
                customer.Commit();
            }

            //// UnitOfWork example: manual change tracking
            //using (new ObjectScope(customer, mode: ChangeTrackingMode.Manual))
            //{
            //    item.CompanyName += "Test";
            //    item.Orders[0].ShipPostalCode = "11111";
            //    item.Orders[0].Update();

            //    var o1 = item.Orders[1];
            //    if (o1.Delete())
            //    {
            //        item.Orders.RemoveAt(1);
            //    }

            //    var o2 = ObjectFactory.Create<IOrder>();
            //    o2.CustomerId = item.Id;
            //    o2.ShipPostalCode = "19115";
            //    if (o2.Insert())
            //    {
            //        item.Orders.Add(o2);
            //    }

            //    item.Commit();
            //}

            // Passing open connection into the method
            using (var test_connection = DbFactory.CreateConnection(ConfigurationManager.ConnectionStrings[ConfigurationFactory.Get<ICustomer>().DefaultConnectionName].ConnectionString))
            {
                test_connection.Open();
                var retrieve_customer_sql_wth_open_connection = ObjectFactory.Retrieve<ICustomer>(connection: test_connection, sql: "select * from Customers where CustomerID = @CustomerID", parameters: new ParamList { CustomerID => "ALFKI" });
            }

            var read_only = retrieve_customer.AsReadOnly();
            var is_read_only = read_only.IsReadOnly();

            var json = retrieve_customer.ToJson();
            var customer_from_json = json.FromJson<ICustomer>();

            Console.WriteLine();
            Console.WriteLine("JSON DOM Parsing");

            RunJsonParser(json, 500);
            RunJsonNetParser(json, 500);
            RunSystemJsonParser(json, 500);
            // ServiceStack does not support DOM parsing
            // RunServiceStackJsonParser<Customer>(new Customer(customer), 500);

            var xsd = Xsd<ICustomer>.Text;
            var xml = retrieve_customer.ToXml();
            using (var reader = XmlReader.Create(new StringReader(xml)))
            {
                var customer_from_xml = reader.FromXml<ICustomer>();
            }

            Console.WriteLine();
            Console.WriteLine("Object Fetching and Materialization");

            //RunEF(500, false);
            RunNative(500);
            RunExecute(500);
            RunDapper(500);
            RunRetrieve(500, false, nemoConfig);
            RunNativeWithMapper(500);
            RunSelect(500, nemoConfig);
            RunRetrieveComplex(500, nemoConfig);

            return;

            //var buffer = customer.Serialize();
            //var new_customer = SerializationExtensions.Deserialize<ICustomer>(buffer);

            Console.WriteLine();
            Console.WriteLine("Simple Object Serialization Benchmark");

            var simpleObjectList = GenerateSimple(100000);

            var dcsSimple = new DataContractSerializer(typeof(SimpleObject));
            var dcjsSimple = new DataContractJsonSerializer(typeof(SimpleObject));
            var binform = new BinaryFormatter();

            RunSerializationBenchmark(simpleObjectList,
            s =>
            {
                using (var stream = new MemoryStream())
                {
                    ProtoBuf.Serializer.Serialize(stream, s);
                    var data = stream.ToArray();
                    return data;
                }
            },
            s => ProtoBuf.Serializer.Deserialize<SimpleObject>(new MemoryStream(s)), "ProtoBuf", s => s.Length);

            RunSerializationBenchmark(simpleObjectList, s => s.Serialize(SerializationMode.Compact), s => s.Deserialize<SimpleObject>(), "ObjectSerializer", s => s.Length);
            RunSerializationBenchmark(simpleObjectList,
            s =>
            {
                using (var stream = new MemoryStream())
                {
                    binform.Serialize(stream, s);
                    return stream.ToArray();
                }
            },
            s => (SimpleObject)binform.Deserialize(new MemoryStream(s)), "BinaryFormatter", s => s.Length);

            RunSerializationBenchmark(simpleObjectList, s => s.ToXml(), s => s.FromXml<SimpleObject>(), "ObjectXmlSerializer", s => s.Length);
            RunSerializationBenchmark(simpleObjectList,
            s =>
            {
                using (var stream = new MemoryStream())
                {
                    dcsSimple.WriteObject(stream, s);
                    stream.Position = 0;
                    using (var reader = new StreamReader(stream, Encoding.UTF8))
                    {
                        return reader.ReadToEnd();
                    }
                }
            },
            s =>
            {
                using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(s)))
                {
                    return (SimpleObject)dcsSimple.ReadObject(stream);
                }
            }, "DataContractSerializer", s => s.Length);

            RunSerializationBenchmark(simpleObjectList, s => s.ToJson(), s => s.FromJson<SimpleObject>(), "ObjectJsonSerializer", s => s.Length);
            RunSerializationBenchmark(simpleObjectList, ServiceStack.Text.JsonSerializer.SerializeToString,
                ServiceStack.Text.JsonSerializer.DeserializeFromString<SimpleObject>, "ServiceStack.Text", s => s.Length);
            RunSerializationBenchmark(simpleObjectList,
            s =>
            {
                using (var stream = new MemoryStream())
                {
                    dcjsSimple.WriteObject(stream, s);
                    stream.Position = 0;
                    using (var reader = new StreamReader(stream, Encoding.UTF8))
                    {
                        return reader.ReadToEnd();
                    }
                }
            },
            s =>
            {
                using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(s)))
                {
                    return (SimpleObject)dcjsSimple.ReadObject(stream);
                }
            }, "DataContractJsonSerializer", s => s.Length);
            
            Console.WriteLine();
            Console.WriteLine("Complex Object Serialization Benchmark");

            var complexObjectList = GenerateComplex(10000);
            var dcsComplex = new DataContractSerializer(typeof(ComplexObject));
            var dcjsComplex = new DataContractJsonSerializer(typeof(ComplexObject));

            RunSerializationBenchmark(complexObjectList,
            s =>
            {
                using (var stream = new MemoryStream())
                {
                    ProtoBuf.Serializer.Serialize<ComplexObject>(stream, s);
                    var data = stream.ToArray();
                    return data;
                }
            },
            s =>
            {
                using (var stream = new MemoryStream(s))
                {
                    return ProtoBuf.Serializer.Deserialize<ComplexObject>(stream);
                }
            }, "ProtoBuf", s => s.Length);

            RunSerializationBenchmark(complexObjectList, s => s.Serialize(SerializationMode.Compact), s => s.Deserialize<ComplexObject>(), "ObjectSerializer", s => s.Length);
            RunSerializationBenchmark(complexObjectList,
            s =>
            {
                using (var stream = new MemoryStream())
                {
                    binform.Serialize(stream, s);
                    return stream.ToArray();
                }
            },
            s => 
            {
                using (var stream = new MemoryStream(s))
                {
                    return (ComplexObject)binform.Deserialize(stream);
                }
            }, "BinaryFormatter", s => s.Length);

            RunSerializationBenchmark(complexObjectList, s => s.ToXml(), s => s.FromXml<ComplexObject>(), "ObjectXmlSerializer", s => s.Length);
            RunSerializationBenchmark(complexObjectList,
            s =>
            {
                using (var stream = new MemoryStream())
                {
                    dcsComplex.WriteObject(stream, s);
                    stream.Position = 0;
                    using (var reader = new StreamReader(stream, Encoding.UTF8))
                    {
                        return reader.ReadToEnd();
                    }
                }
            },
            s =>
            {
                using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(s)))
                {
                    return (ComplexObject)dcsComplex.ReadObject(stream);
                }
            }, "DataContractSerializer", s => s.Length);


            RunSerializationBenchmark(complexObjectList, s => s.ToJson(), s => s.FromJson<ComplexObject>(), "ObjectJsonSerializer", s => s.Length);
            RunSerializationBenchmark(complexObjectList, ServiceStack.Text.JsonSerializer.SerializeToString,
                ServiceStack.Text.JsonSerializer.DeserializeFromString<ComplexObject>, "ServiceStack.Text", s => s.Length);
            RunSerializationBenchmark(complexObjectList,
            s =>
            {
                using (var stream = new MemoryStream())
                {
                    dcjsComplex.WriteObject(stream, s);
                    stream.Position = 0;
                    using (var reader = new StreamReader(stream, Encoding.UTF8))
                    {
                        return reader.ReadToEnd();
                    }
                }
            },
            s =>
            {
                using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(s)))
                {
                    return (ComplexObject)dcjsComplex.ReadObject(stream);
                }
            }, "DataContractJsonSerializer", s => s.Length);

            Console.ReadLine();
        }

        private static void RunNative(int count)
        {
            var connection = DbFactory.CreateConnection(ConfigurationManager.ConnectionStrings[ConfigurationFactory.DefaultConnectionName]?.ConnectionString);
            //const string sql = @"select CustomerID, CompanyName from Customers where CustomerID = @CustomerID";
            const string sql = @"select CustomerID, CompanyName from Customers";

            connection.Open();

            // Warm-up
            using (var cmd = connection.CreateCommand())
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
                        for (var i = 0; i < reader.FieldCount; i++)
                        {
                            var item = reader.GetValue(i);
                        }
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
                    //var param = cmd.CreateParameter();
                    //param.ParameterName = "CustomerId";
                    //param.Value = "ALFKI";
                    //cmd.Parameters.Add(param);
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            for (var j = 0; j < reader.FieldCount; j++)
                            {
                                var item = reader.GetValue(j);
                            }
                        };
                    }
                }
            }
            timer.Stop();
            connection.Close();

            Console.WriteLine("Native: " + timer.Elapsed.TotalMilliseconds);
        }

        private static void RunExecute(int count)
        {
            var connection = DbFactory.CreateConnection(ConfigurationManager.ConnectionStrings[ConfigurationFactory.DefaultConnectionName]?.ConnectionString);

            connection.Open();

            //var req = new OperationRequest { Operation = @"select CustomerID, CompanyName from Customers where CustomerID = @CustomerID", Parameters = new[] { new Param { Name = "CustomerId", Value = "ALFKI", DbType = DbType.String } }, ReturnType = OperationReturnType.SingleResult, OperationType = Nemo.OperationType.Sql, Connection = connection };
            var req = new OperationRequest { Operation = @"select CustomerID, CompanyName from Customers", ReturnType = OperationReturnType.SingleResult, OperationType = Nemo.OperationType.Sql, Connection = connection };

            // Warm-up
            var response = ObjectFactory.Execute<Customer>(req);
            using (var reader = (IDataReader)response.Value)
            {
                while (reader.Read())
                {
                    for (var j = 0; j < reader.FieldCount; j++)
                    {
                        var item = reader.GetValue(j);
                    }
                }
            }

            var timer = new Stopwatch();
            timer.Start();
            for (var i = 0; i < count; i++)
            {
                response = ObjectFactory.Execute<Customer>(req);
                using (var reader = (IDataReader)response.Value)
                {
                    while (reader.Read())
                    {
                        for (var j = 0; j < reader.FieldCount; j++)
                        {
                            var item = reader.GetValue(j);
                        }
                    }
                }
            }
            timer.Stop();
            connection.Close();

            Console.WriteLine("Nemo.Execute:" + timer.Elapsed.TotalMilliseconds);
        }

        private static void RunRetrieve(int count, bool cached, INemoConfiguration config)
        {
            var connection = DbFactory.CreateConnection(ConfigurationManager.ConnectionStrings[ConfigurationFactory.DefaultConnectionName]?.ConnectionString);
            //const string sql = @"select CustomerID, CompanyName from Customers where CustomerID = @CustomerID";
            //var parameters = new[] { new Param { Name = "CustomerId", Value = "ALFKI", DbType = DbType.String } };
            const string sql = @"select CustomerID, CompanyName from Customers";
            
            connection.Open();

            // Warm-up
            var result = ObjectFactory.Retrieve<Customer>(connection: connection, sql: sql, cached: cached, config: config).ToList();

            var timer = new Stopwatch();
            timer.Start();
            for (var i = 0; i < count; i++)
            {
                result = ObjectFactory.Retrieve<Customer>(connection: connection, sql: sql, cached: cached, config: config).ToList();
            }
            timer.Stop();
            connection.Close();

            Console.WriteLine("Nemo.Retrieve: " + timer.Elapsed.TotalMilliseconds);
        }

        private static void RunSelect(int count, INemoConfiguration config)
        {
            var connection = DbFactory.CreateConnection(ConfigurationManager.ConnectionStrings[ConfigurationFactory.DefaultConnectionName]?.ConnectionString);
            //Expression<Func<ICustomer, bool>> predicate = c => c.Id == "ALFKI";

            connection.Open();

            // Warm-up
            var result = ObjectFactory.Select<Customer>(null, connection: connection, config: config).ToList();

            var timer = new Stopwatch();
            timer.Start();
            for (var i = 0; i < count; i++)
            {
                result = ObjectFactory.Select<Customer>(null, connection: connection, config: config).ToList();
            }
            timer.Stop();
            connection.Close();

            Console.WriteLine("Nemo.Select: " + timer.Elapsed.TotalMilliseconds);
        }

        private static void RunRetrieveComplex(int count, INemoConfiguration config)
        {
            var connection = DbFactory.CreateConnection(ConfigurationManager.ConnectionStrings[ConfigurationFactory.DefaultConnectionName]?.ConnectionString);
            var sql = @"select * from Customers where CustomerID = @CustomerID; select * from Orders where CustomerID = @CustomerID; 
                    select distinct d.ProductID, d.OrderID, p.ProductName, p.UnitsInStock, d.UnitPrice, d.Quantity, d.Discount 
                    from Orders o inner join [Order Details] d on o.OrderID = d.OrderID inner join Products p on d.ProductID = p.ProductID
                    where o.CustomerID = @CustomerID";

            connection.Open();

            var parameters = new Param[] { new Param { Name = "CustomerId", Value = "ALFKI" } };

            // Warm-up
            var result = ((IMultiResult)ObjectFactory.Retrieve<Customer, Order, OrderProduct>(sql: sql, parameters: parameters, connection: connection, config: config)).Aggregate<Customer>().FirstOrDefault();

            var timer = new Stopwatch();
            timer.Start();
            for (var i = 0; i < count; i++)
            {
                result = ((IMultiResult)ObjectFactory.Retrieve<Customer, Order, OrderProduct>(sql: sql, parameters: parameters, connection: connection, config: config)).Aggregate<Customer>().FirstOrDefault();
            }
            timer.Stop();
            connection.Close();

            Console.WriteLine("Nemo.Retrieve.Complex: " + timer.Elapsed.TotalMilliseconds);
        }

        private static void RunDapper(int count)
        {
            var connection = DbFactory.CreateConnection(ConfigurationManager.ConnectionStrings[ConfigurationFactory.DefaultConnectionName]?.ConnectionString);
            //var sql = @"select CustomerID as Id, CompanyName from Customers where CustomerID = @CustomerID";
            var sql = @"select CustomerID as Id, CompanyName from Customers";

            connection.Open();
            // Warm-up
            var result = connection.Query<Customer>(sql, new { CustomerID = "ALFKI" }, buffered: false).ToList();
            var timer = new Stopwatch();
            timer.Start();
            for (var i = 0; i < count; i++)
            {
                result = connection.Query<Customer>(sql, new { CustomerID = "ALFKI" }, buffered: false).ToList();
            }
            timer.Stop();

//            var result2 = connection.Query<Order, Customer, Order>(@"select c.CustomerID As Id, c.CompanyName, o.OrderID As OrderId from Customers c
//                                                                    left join Orders o on o.CustomerID = c.CustomerID where c.CustomerID = @CustomerID",
//                                                                    map: (o, c) => { o.Customer = c; return o; }, 
//                                                                    param: new { CustomerID = "ALFKI" }, 
//                                                                    splitOn: "OrderId").ToList();
            
            connection.Close();

            Console.WriteLine("Dapper: " + timer.Elapsed.TotalMilliseconds);
        }

        private static void RunNativeWithMapper(int count)
        {
            var connection = DbFactory.CreateConnection(ConfigurationManager.ConnectionStrings[ConfigurationFactory.DefaultConnectionName]?.ConnectionString);
            //const string sql = @"select CustomerID, CompanyName from Customers where CustomerID = @CustomerID";
            const string sql = @"select CustomerID, CompanyName from Customers";

            connection.Open();

            // Warm-up
            using (var cmd = connection.CreateCommand())
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

            var timer = new Stopwatch();
            timer.Start();
            for (var i = 0; i < count; i++)
            {
                using (var cmd = connection.CreateCommand())
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
                        };
                    }
                }
            }
            timer.Stop();
            connection.Close();

            Console.WriteLine("Native+Nemo.Mapper: " + timer.Elapsed.TotalMilliseconds);
        }

        private static void RunEF(int count, bool linq = false, bool noTracking = true)
        {
            //var sql = @"select CustomerID as Id, CompanyName from Customers where CustomerID = @p0";
            var sql = @"select CustomerID as Id, CompanyName from Customers";
            // Warm-up
            using (var context = new EFContext(ConfigurationFactory.DefaultConnectionName))
            {
                context.Configuration.AutoDetectChangesEnabled = false;

                if (linq)
                {
                    var customer = context.Customers.AsNoTracking().ToList();//FirstOrDefault(c => c.Id == "ALFKI");
                }
                else
                {
                    var parameters = new object[] { "ALFKI" };
                    var customer = context.Customers.SqlQuery(sql, parameters).AsNoTracking().ToList();
                }
            }

            var timer = new Stopwatch();
            if (linq)
            {
                using (var context = new EFContext(ConfigurationFactory.DefaultConnectionName))
                {
                    timer.Start();

                    for (var i = 0; i < count; i++)
                    {
                        var customer = context.Customers.AsNoTracking().ToList();//.FirstOrDefault(c => c.Id == "ALFKI");
                    }
                    timer.Stop();
                }
            }
            else
            {
                using (var context = new EFContext(ConfigurationFactory.DefaultConnectionName))
                {
                    timer.Start();
                    for (var i = 0; i < count; i++)
                    {
                        var parameters = new object[] { "ALFKI" };
                        var customer = context.Customers.SqlQuery(sql, parameters).AsNoTracking().ToList();
                    }
                    timer.Stop();
                }
            }

            Console.WriteLine("Entity Framework: " + timer.Elapsed.TotalMilliseconds);
        }
        
        public static void RunJsonParser(string json, int count)
        {
            // Warm-up
            var root = Json.Parse(json);

            var t = new Stopwatch();
            t.Start();
            for (var i = 0; i < count; i++)
            {
                root = Json.Parse(json);
            }
            t.Stop();
            var time = t.Elapsed.TotalMilliseconds * 1000;

            Console.WriteLine("Json Parser: {0}µs", time);
        }

        public static void RunJsonNetParser(string json, int count)
        {
            // Warm-up
            var root = Newtonsoft.Json.Linq.JToken.Parse(json);

            var t = new Stopwatch();
            t.Start();
            for (var i = 0; i < count; i++)
            {
                root = Newtonsoft.Json.Linq.JToken.Parse(json);
            }
            t.Stop();
            var time = t.Elapsed.TotalMilliseconds * 1000;

            Console.WriteLine("Json.NET Parser: {0}µs", time);
        }

        public static void RunSystemJsonParser(string json, int count)
        {
            // Warm-up
            var root = System.Text.Json.JsonDocument.Parse(json, new System.Text.Json.JsonDocumentOptions { AllowTrailingCommas = true });

            var t = new Stopwatch();
            t.Start();
            for (var i = 0; i < count; i++)
            {
                root = System.Text.Json.JsonDocument.Parse(json, new System.Text.Json.JsonDocumentOptions { AllowTrailingCommas = true });
            }
            t.Stop();
            var time = t.Elapsed.TotalMilliseconds * 1000;

            Console.WriteLine("System.Text.Json Parser: {0}µs", time);
        }

        public static void RunServiceStackJsonParser<T>(T item, int count)
            where T : class
        {
            // Warm-up
            var serializer = new ServiceStack.Text.JsonSerializer<T>();
            var json = ServiceStack.Text.JsonSerializer.SerializeToString(item);
            var root = serializer.DeserializeFromString(json);

            var t = new Stopwatch();
            t.Start();
            for (var i = 0; i < count; i++)
            {
                root = serializer.DeserializeFromString(json);
            }
            t.Stop();
            var time = t.Elapsed.TotalMilliseconds * 1000;

            Console.WriteLine("ServiceStack Parser: {0}µs", time);
        }

        public static List<SimpleObject> GenerateSimple(int count)
        {
            var list = new List<SimpleObject>();
            for (var i = 0; i < count; i++)
            {
                var item = new SimpleObject
                {
                    Id = i,
                    Name = ComputeRandomString(random.Next(10, 21)),
                    DateOfBirth = new DateTime(random.Next(1970, 1990), random.Next(1, 12), random.Next(1, 28)),
                    Income = random.NextDouble() * random.Next(100000, 200000)
                };
                list.Add(item);
            }
            return list;
        }

        public static List<ComplexObject> GenerateComplex(int count)
        {
            var list = new List<ComplexObject>();
            for (var i = 0; i < count; i++)
            {
                var item = new ComplexObject
                {
                    Id = i,
                    Name = ComputeRandomString(random.Next(10, 21)),
                    DateOfBirth = new DateTime(random.Next(1970, 1990), random.Next(1, 12), random.Next(1, 28)),
                    Income = random.NextDouble() * random.Next(100000, 200000),
                    Children = GenerateSimple(random.Next(15, 31))
                };
                list.Add(item);
            }
            return list;
        }

        public static void RunSerializationBenchmark<T, TResult>(List<T> objectList, Func<T, TResult> serialize, Func<TResult, T> deserialize, string name, Func<TResult, int> getLength)
        {
            // Warm-up
            var data = serialize(objectList[0]);
            var dataList = new List<TResult>();
            var stimeList = new List<double>();
            var dtimeList = new List<double>();
            var sizeList = new List<int>();

            var t = new Stopwatch();
            for (var i = 0; i < objectList.Count; i++)
            {
                t.Start();
                data = serialize(objectList[i]);
                t.Stop();
                dataList.Add(data);
                stimeList.Add(t.Elapsed.TotalMilliseconds * 1000);
                sizeList.Add(getLength(data));
                t.Reset();
            }

            // Warm-up
            var item = deserialize(dataList[0]);

            t.Reset();

            for (var i = 0; i < dataList.Count; i++)
            {
                t.Start();
                item = deserialize(dataList[i]);
                t.Stop();
                dtimeList.Add(t.Elapsed.TotalMilliseconds * 1000);
                t.Reset();
            }
            
            Console.WriteLine(name);
            Console.WriteLine("\tserialization: {0}µs", stimeList.Average());
            Console.WriteLine("\tdeserialization: {0}µs", dtimeList.Average());
            Console.WriteLine("\tsize: {0} bytes", sizeList.Average());
        }

        private static readonly Random random = new Random((int)DateTime.UtcNow.Ticks);
        private static string ComputeRandomString(int size)
        {
            var builder = new StringBuilder();
            char ch;
            for (var i = 0; i < size; i++)
            {
                ch = Convert.ToChar(Convert.ToInt32(Math.Floor(26 * random.NextDouble() + 65)));                 
                builder.Append(ch);
            }
            return builder.ToString();
        }

        private static IEnumerable<double> RemoveOutliers(IEnumerable<double> list, double stdDev, double avg)
        {
            return list.Where(i => i.SafeCast<double>() < avg + stdDev * 2 && i.SafeCast<double>() > avg - stdDev * 2);
        }

        private static double StandardDeviation(List<double> values)
        {
            var ret = 0.0;
            if (values.Count <= 0) return ret;
            //Compute the Average      
            var avg = values.Average();
            //Perform the Sum of (value-avg)_2_2      
            var sum = values.Sum(d => Math.Pow(d - avg, 2));
            //Put it all together      
            ret = Math.Sqrt((sum) / (values.Count() - 1));
            return ret;
        }

    }
}
