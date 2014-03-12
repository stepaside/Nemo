using Dapper;
using Nemo;
using Nemo.Collections;
using Nemo.Collections.Extensions;
using Nemo.Configuration;
using Nemo.Configuration.Mapping;
using Nemo.Extensions;
using Nemo.Fn;
using Nemo.Serialization;
using Nemo.UnitOfWork;
using Nemo.Utilities;
using System;
using System.Collections.Generic;
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
            ConfigurationFactory.Configure()
                .SetDefaultChangeTrackingMode(ChangeTrackingMode.Debug)
                .SetDefaultFetchMode(FetchMode.Lazy)
                .SetDefaultMaterializationMode(MaterializationMode.Partial)
                .SetDefaultL1CacheRepresentation(L1CacheRepresentation.None)
                .SetDefaultSerializationMode(SerializationMode.Compact)
                .SetOperationNamingConvention(OperationNamingConvention.PrefixTypeName_Operation)
                .SetOperationPrefix("spDTO_")
                .SetLogging(false);

            var person_legacy = new PersonLegacy { person_id = 12345, name = "John Doe", DateOfBirth = new DateTime(1980, 1, 10) };
            var person_anonymous = new { person_id = 12345, name = "John Doe" };

            // Create an instance
            var created = ObjectFactory.Create<ICustomer>();
            var created_new = ObjectFactory.Create<Customer>();

            // Binding to a legacy object
            var bound_legacy = ObjectFactory.Bind<PersonLegacy, IPerson>(person_legacy);
            
            // Binding to an anonymous object
            var bound_anonymous = ObjectFactory.Bind<IPersonReadOnly>(person_anonymous);

            // Mapping to a legacy object
            var mapped_legacy = ObjectFactory.Map<PersonLegacy, IPerson>(person_legacy);
            
            // Mapping to an anonymous object
            try
            {
                var mapped_anonymous = ObjectFactory.Map<IPerson>(person_anonymous);
            }
            catch { }

            // Dynamic select
            var selected_customers_10 = ObjectFactory.Select<Customer>(page: 1, pageSize: 10).ToList();
            //var selected_customers_10_repeat = ObjectFactory.Select<Customer>(page: 1, pageSize: 10).ToList();
            var selected_customers_A = ObjectFactory.Select<ICustomer>(c => c.CompanyName.StartsWith("A"), page: 1, pageSize: 2);
            var selected_customers_A_count = ObjectFactory.Count<ICustomer>(c => c.CompanyName.StartsWith("A"));

            var selected_customers_with_orders = ObjectFactory.Select<ICustomer>(c => c.Orders.Count > 0);

            // Simple retrieve with dynamic parameters
            var retrieve_customer_dyn = ObjectFactory.Retrieve<Customer>(parameters: new ParamList { CustomerID => "ALFKI" }).FirstOrDefault();
           
            // Simple retrieve with actual parameters
            var retrieve_customer = ObjectFactory.Retrieve<ICustomer>(parameters: new[] { new Param { Name = "CustomerID", Value = "ALFKI" } }).FirstOrDefault();

            // Simple retrieve with dynamic parameters and custom operation name
            var retrieve_customers_by_country = ObjectFactory.Retrieve<ICustomer>(operation: "RetrieveByCountry", parameters: new ParamList { Country => "USA" });

            // Simple retrieve with sql statement operation
            var retrieve_customer_sql = ObjectFactory.Retrieve<ICustomer>(sql: "select * from Customers where CustomerID = @CustomerID", parameters: new ParamList { CustomerID => "ALFKI" });

            // Advanced!
            // Retrieve customers with orders as object graph
            var retrieve_customer_with_orders_graph = ObjectFactory.Retrieve<ICustomer, IOrder>(
                                                                    sql: @"select * from Customers where CustomerID = @CustomerID; 
                                                                        select * from Orders where CustomerID = @CustomerID",
                                                                    parameters: new ParamList { CustomerId => "ALFKI" },
                                                                    mode: FetchMode.Eager);
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
            var retrieve_customer_with_orders = ObjectFactory.Retrieve<ICustomer, IOrder>(
                                                                    sql: @"select c.CustomerID, c.CompanyName, o.OrderID, o.ShipPostalCode from Customers c
                                                                            left join Orders o on o.CustomerID = c.CustomerID 
                                                                            where c.CustomerID = @CustomerID",
                                                                    parameters: new ParamList { CustomerId => "ALFKI" },
                                                                    map: new CustomerOrderMapper().Map);

            // Advanced!
            // Retrieve customers with orders as multi-result
            var retrieve_customer_with_orders_lazy = ObjectFactory.Retrieve<ICustomer, IOrder>(
                                                                    sql: @"select * from Customers where CustomerID = @CustomerID;
                                                                            select * from Orders where CustomerID = @CustomerID",
                                                                    parameters: new ParamList { CustomerId => "ALFKI" },
                                                                    mode: FetchMode.Lazy);

            var lazy_customer = retrieve_customer_with_orders_lazy.FirstOrDefault(); // ((IMultiResult)retrieve_customer_with_orders_lazy).Retrieve<ICustomer>().FirstOrDefault();
            var lazy_orders = ((IMultiResult)retrieve_customer_with_orders_lazy).Retrieve<IOrder>();
            
            // UnitOfWork example
            using (ObjectScope.New(customer, autoCommit: false))
            {
                customer.CompanyName += "Test";
                customer.Orders[0].ShipPostalCode = "11111";
                customer.Orders.RemoveAt(1);

                var o = ObjectFactory.Create<IOrder>();
                o.CustomerId = customer.Id;
                o.ShipPostalCode = "19115";
                o.GenerateKey();
                customer.Orders.Add(o);

                //customer.Rollback();
                customer.Commit();
            }

            //using (new CacheScope(buffered: true))
            //{
            //    var c1 = ObjectFactory.Retrieve<ICustomer>(parameters: new ParamList { CustomerID => "ALFKI" }).FirstOrDefault();
            //    var c2 = ObjectFactory.Retrieve<ICustomer>(parameters: new ParamList { CustomerID => "ALFKI" }).FirstOrDefault();
            //}

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
            using (var test_connection = new SqlConnection(Config.ConnectionString(ObjectFactory.DefaultConnectionName)))
            {
                test_connection.Open();
                var retrieve_customer_sql_wth_open_connection = ObjectFactory.Retrieve<ICustomer>(connection: test_connection, sql: "select * from Customers where CustomerID = @CustomerID", parameters: new ParamList { CustomerID => "ALFKI" });
            }

            var read_only = customer.AsReadOnly();
            var is_read_only = read_only.IsReadOnly();

            var json = customer.ToJson();
            var customer_from_json = json.FromJson<ICustomer>().FirstOrDefault();

            Console.WriteLine();
            Console.WriteLine("JSON DOM Parsing");
            
            RunJsonParser(json, 500);
            RunJsonNetParser(json, 500);
            // ServiceStack does not support DOM parsing
            // RunServiceStackJsonParser<Customer>(new Customer(customer), 500);

            var xsd = Xsd<ICustomer>.Text;
            var xml = customer.ToXml();
            using (var reader = XmlReader.Create(new StringReader(xml)))
            {
                var customer_from_xml = reader.FromXml<ICustomer>().FirstOrDefault();
            }

            Console.WriteLine();
            Console.WriteLine("Object Fetching and Materialization");

            RunEF(500, false);
            RunNative(500);
            RunExecute(500);
            RunDapper(500, false);
            RunRetrieve(500, false);
            RunNativeWithMapper(500);
            RunSelect(500, false);

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
                    ProtoBuf.Serializer.Serialize<SimpleObject>(stream, s);
                    var data = stream.ToArray();
                    return data;
                }
            },
            s => ProtoBuf.Serializer.Deserialize<SimpleObject>(new MemoryStream(s)), "ProtoBuf", s => s.Length);

            RunSerializationBenchmark(simpleObjectList, s => s.Serialize(), s => ObjectSerializer.Deserialize<SimpleObject>(s), "ObjectSerializer", s => s.Length);
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

            RunSerializationBenchmark(simpleObjectList, s => s.ToXml(), s => ObjectXmlSerializer.FromXml<SimpleObject>(s).FirstOrDefault(), "ObjectXmlSerializer", s => s.Length);
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

            RunSerializationBenchmark(simpleObjectList, s => s.ToJson(), s => ObjectJsonSerializer.FromJson<SimpleObject>(s).FirstOrDefault(), "ObjectJsonSerializer", s => s.Length);
            RunSerializationBenchmark(simpleObjectList, s => ServiceStack.Text.JsonSerializer.SerializeToString(s),
                s => ServiceStack.Text.JsonSerializer.DeserializeFromString<SimpleObject>(s), "ServiceStack.Text", s => s.Length);
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

            RunSerializationBenchmark(complexObjectList, s => s.Serialize(), s => ObjectSerializer.Deserialize<ComplexObject>(s), "ObjectSerializer", s => s.Length);
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

            RunSerializationBenchmark(complexObjectList, s => s.ToXml(), s => ObjectXmlSerializer.FromXml<ComplexObject>(s).FirstOrDefault(), "ObjectXmlSerializer", s => s.Length);
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


            RunSerializationBenchmark(complexObjectList, s => s.ToJson(), s => ObjectJsonSerializer.FromJson<ComplexObject>(s).FirstOrDefault(), "ObjectJsonSerializer", s => s.Length);
            RunSerializationBenchmark(complexObjectList, s => ServiceStack.Text.JsonSerializer.SerializeToString(s),
                s => ServiceStack.Text.JsonSerializer.DeserializeFromString<ComplexObject>(s), "ServiceStack.Text", s => s.Length);
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

            //Console.ReadLine();
        }

        private static void RunNative(int count)
        {
            var connection = new SqlConnection(Config.ConnectionString(ObjectFactory.DefaultConnectionName));
            var sql = @"select CustomerID, CompanyName from Customers where CustomerID = @CustomerID";

            connection.Open();

            // Warm-up
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = sql;
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.AddWithValue("CustomerId", "ALFKI");
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read()) { }
                }
            }

            var timer = new HiPerfTimer(true);
            timer.Start();
            for (int i = 0; i < count; i++)
            {
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = sql;
                    cmd.CommandType = CommandType.Text;
                    cmd.Parameters.AddWithValue("CustomerId", "ALFKI");
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read()) { };
                    }
                }
            }
            timer.Stop();
            connection.Close();

            Console.WriteLine("Native: " + timer.GetElapsedTimeInMicroseconds() / 1000);
        }

        private static void RunExecute(int count)
        {
            var connection = new SqlConnection(Config.ConnectionString(ObjectFactory.DefaultConnectionName));

            connection.Open();

            var req = new OperationRequest { Operation = @"select CustomerID, CompanyName from Customers where CustomerID = @CustomerID", Parameters = new[] { new Param { Name = "CustomerId", Value = "ALFKI", DbType = DbType.String } }, ReturnType = OperationReturnType.SingleResult, OperationType = Nemo.OperationType.Sql, Connection = connection };

            // Warm-up
            var response = ObjectFactory.Execute<ICustomer>(req);
            using (var reader = (IDataReader)response.Value)
            {
                while (reader.Read()) { }
            }

            var timer = new HiPerfTimer(true);
            timer.Start();
            for (int i = 0; i < count; i++)
            {
                response = ObjectFactory.Execute<ICustomer>(req);
                using (var reader = (IDataReader)response.Value)
                {
                    while (reader.Read()) { }
                }
            }
            timer.Stop();
            connection.Close();

            Console.WriteLine("Nemo.Execute:" + timer.GetElapsedTimeInMicroseconds() / 1000);
        }

        private static void RunRetrieve(int count, bool cached)
        {
            var connection = new SqlConnection(Config.ConnectionString(ObjectFactory.DefaultConnectionName));
            var sql = @"select CustomerID, CompanyName from Customers where CustomerID = @CustomerID";
            var parameters = new[] { new Param { Name = "CustomerId", Value = "ALFKI", DbType = DbType.String } };

            connection.Open();

            // Warm-up
            var result = ObjectFactory.Retrieve<ICustomer>(connection: connection, sql: sql, parameters: parameters, cached: cached).FirstOrDefault();

            var timer = new HiPerfTimer(true);
            timer.Start();
            for (int i = 0; i < count; i++)
            {
                result = ObjectFactory.Retrieve<ICustomer>(connection: connection, sql: sql, parameters: parameters, cached: cached).FirstOrDefault();
            }
            timer.Stop();
            connection.Close();

            Console.WriteLine("Nemo.Retrieve: " + timer.GetElapsedTimeInMicroseconds() / 1000);
        }

        private static void RunSelect(int count, bool buffered = false)
        {
            var connection = new SqlConnection(Config.ConnectionString(ObjectFactory.DefaultConnectionName));
            Expression<Func<ICustomer, bool>> predicate = c => c.Id == "ALFKI";

            connection.Open();

            // Warm-up
            var result = ObjectFactory.Select<ICustomer>(predicate, connection: connection).FirstOrDefault();

            var timer = new HiPerfTimer(true);
            timer.Start();
            for (int i = 0; i < count; i++)
            {
                result = ObjectFactory.Select<ICustomer>(predicate, connection: connection).FirstOrDefault();
            }
            timer.Stop();
            connection.Close();

            Console.WriteLine("Nemo.Select: " + timer.GetElapsedTimeInMicroseconds() / 1000);
        }

        private static void RunDapper(int count, bool buffered)
        {
            var connection = new SqlConnection(Config.ConnectionString(ObjectFactory.DefaultConnectionName));
            var sql = @"select CustomerID as Id, CompanyName from Customers where CustomerID = @CustomerID";
                
            connection.Open();
            // Warm-up
            var result = connection.Query<Customer>(sql, new { CustomerID = "ALFKI" }, buffered: buffered).FirstOrDefault();
            var timer = new HiPerfTimer(true);
            timer.Start();
            for (int i = 0; i < count; i++)
            {
                result = connection.Query<Customer>(sql, new { CustomerID = "ALFKI" }, buffered: buffered).FirstOrDefault();
            }
            timer.Stop();

//            var result2 = connection.Query<Order, Customer, Order>(@"select c.CustomerID As Id, c.CompanyName, o.OrderID As OrderId from Customers c
//                                                                    left join Orders o on o.CustomerID = c.CustomerID where c.CustomerID = @CustomerID",
//                                                                    map: (o, c) => { o.Customer = c; return o; }, 
//                                                                    param: new { CustomerID = "ALFKI" }, 
//                                                                    splitOn: "OrderId").ToList();
            
            connection.Close();

            Console.WriteLine("Dapper: " + timer.GetElapsedTimeInMicroseconds() / 1000);
        }

        private static void RunNativeWithMapper(int count)
        {
            var connection = new SqlConnection(Config.ConnectionString(ObjectFactory.DefaultConnectionName));
            var sql = @"select CustomerID, CompanyName from Customers where CustomerID = @CustomerID";

            connection.Open();

            // Warm-up
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = sql;
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.AddWithValue("CustomerId", "ALFKI");
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read()) 
                    {
                        var customer = ObjectFactory.Map<IDataReader, Customer>(reader);
                    }
                }
            }

            var timer = new HiPerfTimer(true);
            timer.Start();
            for (int i = 0; i < count; i++)
            {
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = sql;
                    cmd.CommandType = CommandType.Text;
                    cmd.Parameters.AddWithValue("CustomerId", "ALFKI");
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

            Console.WriteLine("Native+Nemo.Mapper: " + timer.GetElapsedTimeInMicroseconds() / 1000);
        }

        private static void RunEF(int count, bool linq = false, bool noTracking = true)
        {
            var sql = @"select CustomerID as Id, CompanyName from Customers where CustomerID = @CustomerID";
            // Warm-up
            using (var context = new EFContext(ObjectFactory.DefaultConnectionName))
            {
                if (linq)
                {
                    var customer = context.Customers/*.AsNoTracking()*/.Where(c => c.Id == "ALFKI").FirstOrDefault();
                }
                else
                {
                    var parameters = new object[] { new SqlParameter { ParameterName = "CustomerID", Value = "ALFKI", DbType = DbType.StringFixedLength, Size = 5 } };
                    var customer = context.Customers.SqlQuery(sql, parameters)/*.AsNoTracking()*/.FirstOrDefault();
                }
            }

            var timer = new HiPerfTimer(true);
            if (linq)
            {
                using (var context = new EFContext(ObjectFactory.DefaultConnectionName))
                {
                    timer.Start();

                    for (int i = 0; i < count; i++)
                    {
                        var customer = context.Customers/*.AsNoTracking()*/.Where(c => c.Id == "ALFKI").FirstOrDefault();
                    }
                    timer.Stop();
                }
            }
            else
            {
                using (var context = new EFContext(ObjectFactory.DefaultConnectionName))
                {
                    timer.Start();
                    for (int i = 0; i < count; i++)
                    {
                        var parameters = new object[] { new SqlParameter { ParameterName = "CustomerID", Value = "ALFKI", DbType = DbType.StringFixedLength, Size = 5 } };
                        var customer = context.Customers.SqlQuery(sql, parameters)/*.AsNoTracking()*/.FirstOrDefault();
                    }
                    timer.Stop();
                }
            }

            Console.WriteLine("Entity Framework: " + timer.GetElapsedTimeInMicroseconds() / 1000);
        }
        
        public static void RunJsonParser(string json, int count)
        {
            // Warm-up
            var root = Json.Parse(json);

            var t = new HiPerfTimer(true);
            t.Start();
            for (int i = 0; i < count; i++)
            {
                root = Json.Parse(json);
            }
            t.Stop();
            var time = t.GetElapsedTimeInMicroseconds();

            Console.WriteLine("Json Parser: {0}µs", time);
        }

        public static void RunJsonNetParser(string json, int count)
        {
            // Warm-up
            var root = Newtonsoft.Json.Linq.JToken.Parse(json);

            var t = new HiPerfTimer(true);
            t.Start();
            for (int i = 0; i < count; i++)
            {
                root = Newtonsoft.Json.Linq.JToken.Parse(json);
            }
            t.Stop();
            var time = t.GetElapsedTimeInMicroseconds();

            Console.WriteLine("Json.NET Parser: {0}µs", time);
        }

        public static void RunServiceStackJsonParser<T>(T item, int count)
            where T : class
        {
            // Warm-up
            var serializer = new ServiceStack.Text.JsonSerializer<T>();
            var json = ServiceStack.Text.JsonSerializer.SerializeToString(item);
            var root = serializer.DeserializeFromString(json);

            var t = new HiPerfTimer(true);
            t.Start();
            for (int i = 0; i < count; i++)
            {
                root = serializer.DeserializeFromString(json);
            }
            t.Stop();
            var time = t.GetElapsedTimeInMicroseconds();

            Console.WriteLine("ServiceStack Parser: {0}µs", time);
        }

        public static List<SimpleObject> GenerateSimple(int count)
        {
            var list = new List<SimpleObject>();
            for (int i = 0; i < count; i++)
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
            for (int i = 0; i < count; i++)
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

            var t = new HiPerfTimer(true);
            for (int i = 0; i < objectList.Count; i++)
            {
                t.Start();
                data = serialize(objectList[i]);
                t.Stop();
                dataList.Add(data);
                stimeList.Add(t.GetElapsedTimeInMicroseconds());
                sizeList.Add(getLength(data));
                t.Reset();
            }

            // Warm-up
            var item = deserialize(dataList[0]);

            t.Reset();

            for (int i = 0; i < dataList.Count; i++)
            {
                t.Start();
                item = deserialize(dataList[i]);
                t.Stop();
                dtimeList.Add(t.GetElapsedTimeInMicroseconds());
                t.Reset();
            }
            
            Console.WriteLine(name);
            Console.WriteLine("\tserialization: {0}µs", stimeList.Average());
            Console.WriteLine("\tdeserialization: {0}µs", dtimeList.Average());
            Console.WriteLine("\tsize: {0} bytes", sizeList.Average());
        }

        private static Random random = new Random((int)DateTime.UtcNow.Ticks);
        private static string ComputeRandomString(int size)
        {
            var builder = new StringBuilder();
            char ch;
            for (int i = 0; i < size; i++)
            {
                ch = Convert.ToChar(Convert.ToInt32(Math.Floor(26 * random.NextDouble() + 65)));                 
                builder.Append(ch);
            }
            return builder.ToString();
        }

        private static IEnumerable<double> RemoveOutliers(List<double> list, double stdDev, double avg)
        {
            return list.Where(i => i.SafeCast<double>() < avg + stdDev * 2 && i.SafeCast<double>() > avg - stdDev * 2);
        }

        private static double StandardDeviation(List<double> values)
        {
            var ret = 0.0;
            if (values.Count > 0)
            {
                //Compute the Average      
                var avg = values.Average();
                //Perform the Sum of (value-avg)_2_2      
                var sum = values.Sum(d => Math.Pow(d - avg, 2));
                //Put it all together      
                ret = Math.Sqrt((sum) / (values.Count() - 1));
            }
            return ret;
        }

    }
}
