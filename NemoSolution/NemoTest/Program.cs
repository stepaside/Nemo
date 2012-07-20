using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Runtime.Serialization.Json;
using System.Text;
using System.Xml;
using Dapper;
using Nemo;
using Nemo.Caching;
using Nemo.Collections;
using Nemo.Extensions;
using Nemo.Fn;
using Nemo.Serialization;
using Nemo.UnitOfWork;
using Nemo.Utilities;

namespace NemoTest
{
    class Program
    {
        static void Main(string[] args)
        {
            ObjectFactory.Configure()
                .SetCacheLifeTime(3600)
                .SetDefaultChangeTrackingMode(ChangeTrackingMode.Debug)
                .SetDefaultFetchMode(FetchMode.Lazy)
                .SetDefaultMaterializationMode(MaterializationMode.Partial)
                .SetContextLevelCache(ContextLevelCacheType.LazyList)
                .SetDefaultOperationNamingConvention(OperationNamingConvention.PrefixTypeName_Operation)
                .SetOperationPrefix("spDTO_")
                .SetDefaultHashAlgorithm(HashAlgorithmName.Jenkins)
                .ToggleDistributedLocking(false)
                .ToggleLogging(false)
                .ToggleCacheCollisionDetection(false)
                .ToggleDistributedLockVerification(false);

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

            // Simple retrieve with dynamic parameters
            var retrieve_customer_dyn = ObjectFactory.Retrieve<ICustomer>(parameters: new ParamList { CustomerID => "ALFKI" });

            // Simple retrieve with actual parameters
            var retrieve_customer = ObjectFactory.Retrieve<ICustomer>(parameters: new[] { new Param { Name = "CustomerID", Value = "ALFKI" } });

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
            using (new ObjectScope(customer, autoCommit: false))
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

            //// Test serialization of TypeUnion
            //customer.TypeUnionTest = new TypeUnion<int, string, double>(123);

            var json = customer.ToJson();
            var customer_from_json = ObjectJsonSerializer.FromJson<ICustomer>(json).FirstOrDefault();

            var xml = customer.ToXml();
            using(var reader = XmlReader.Create(new StringReader(xml)))
            {
                var customer_from_xml = ObjectXmlSerializer.FromXml<ICustomer>(reader).FirstOrDefault();
            }

            RunNative(500);
            RunExecute(500);
            RunDapper(500, false);
            RunRetrieve(500, false);
            RunNativeWithMapper(500);

            Console.WriteLine();
            
            var customer_legacy = CustomerLegacy.Make(customer);
            //item_legacy.Values = new List<int> { 1, 2, 3 };

            RunSerialization(customer, 1000);
            RunNativeSerialization(customer_legacy, 1000);
            RunProtocolBufferSerialization(customer_legacy, 1000);

            RunXmlSerialization(customer, 1000);
            RunDataContractSerialization(customer_legacy, 1000);
            
            RunJsonSerialization(customer, 1000);
            RunDataContractJsonSerialization(customer_legacy, 1000);
            RunServiceStackJsonSerialization(customer_legacy, 1000);

            RunJsonParser(json, 1000);
            RunJsonNetParser(json, 1000);
            RunServiceStackJsonParser(customer_legacy, 1000);

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

        private static void RunRetrieve(int count, bool buffered = false)
        {
            using (new CacheScope(buffered: buffered))
            {
                var connection = new SqlConnection(Config.ConnectionString(ObjectFactory.DefaultConnectionName));
                var sql = @"select CustomerID, CompanyName from Customers where CustomerID = @CustomerID";
                var parameters = new[] { new Param { Name = "CustomerId", Value = "ALFKI", DbType = DbType.String } };

                connection.Open();

                // Warm-up
                var result = ObjectFactory.Retrieve<ICustomer>(connection: connection, sql: sql, parameters: parameters).FirstOrDefault();

                var timer = new HiPerfTimer(true);
                timer.Start();
                for (int i = 0; i < count; i++)
                {
                    result = ObjectFactory.Retrieve<ICustomer>(connection: connection, sql: sql, parameters: parameters).FirstOrDefault();
                }
                timer.Stop();
                connection.Close();

                Console.WriteLine("Nemo.Retrieve: " + timer.GetElapsedTimeInMicroseconds() / 1000);
            }
        }

        private static void RunDapper(int count, bool buffered)
        {
            var connection = new SqlConnection(Config.ConnectionString(ObjectFactory.DefaultConnectionName));
            var sql = @"select CustomerID, CompanyName from Customers where CustomerID = @CustomerID";
                
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

        public static void RunSerialization<T>(T item, int count)
            where T : class, IBusinessObject
        {
            // Warm-up
            var buffer = item.Serialize();
            var size = buffer.Length;

            var t = new HiPerfTimer(true);
            t.Start();
            for (int i = 0; i < count; i++)
            {
                buffer = item.Serialize();
            }
            t.Stop();
            var stime = t.GetElapsedTimeInMicroseconds();

            // Warm-up
            var item_copy = SerializationExtensions.Deserialize<T>(buffer);
            
            t.Reset();
            t.Start();
            for (int i = 0; i < count; i++)
            {
                item_copy = SerializationExtensions.Deserialize<T>(buffer);
            }
            t.Stop();
            var dstime = t.GetElapsedTimeInMicroseconds();

            Console.WriteLine("ObjectSerializer");
            Console.WriteLine("\tserialization: {0}µs", stime);
            Console.WriteLine("\tdeserialization: {0}µs", dstime);
            Console.WriteLine("\tsize: {0} bytes", size);
        }

        public static void RunProtocolBufferSerialization<T>(T item, int count)
            where T : class, IBusinessObject
        {
            // Warm-up
            var stream = new MemoryStream();
            ProtoBuf.Serializer.Serialize<T>(stream, item);
            var buffer = stream.GetBuffer();
            var size = buffer.Length;

            var t = new HiPerfTimer(true);
            t.Start();
            for (int i = 0; i < count; i++)
            {
                stream = new MemoryStream();
                ProtoBuf.Serializer.Serialize<T>(stream, item);
                buffer = stream.GetBuffer();
            }
            t.Stop();
            var stime = t.GetElapsedTimeInMicroseconds();

            // Warm-up
            var item_copy = ProtoBuf.Serializer.Deserialize<T>(new MemoryStream(buffer));

            t.Reset();
            t.Start();
            for (int i = 0; i < count; i++)
            {
                item_copy = ProtoBuf.Serializer.Deserialize<T>(new MemoryStream(buffer));
            }
            t.Stop();
            var dstime = t.GetElapsedTimeInMicroseconds();

            Console.WriteLine("ProtoBuf.Serializer");
            Console.WriteLine("\tserialization: {0}µs", stime);
            Console.WriteLine("\tdeserialization: {0}µs", dstime);
            Console.WriteLine("\tsize: {0} bytes", size);
        }

        public static void RunNativeSerialization<T>(T item, int count)
            where T : class
        {
            // Warm-up
            var ms = new MemoryStream();
            var formatter = new BinaryFormatter();
            formatter.Serialize(ms, item);
            var buffer = ms.GetBuffer();
            var size = buffer.Length;

            var t = new HiPerfTimer(true);
            t.Start();
            for (int i = 0; i < count; i++)
            {
                ms = new MemoryStream();
                formatter.Serialize(ms, item);
                buffer = ms.GetBuffer();
            }
            t.Stop();
            var stime = t.GetElapsedTimeInMicroseconds();

            // Warm-up
            var item_copy = (T)formatter.Deserialize(new MemoryStream(buffer));
            
            t.Reset();
            t.Start();
            for (int i = 0; i < count; i++)
            {
                item_copy = (T)formatter.Deserialize(new MemoryStream(buffer));
            }
            t.Stop();
            var dstime = t.GetElapsedTimeInMicroseconds();

            Console.WriteLine("BinaryFormatter");
            Console.WriteLine("\tserialization: {0}µs", stime);
            Console.WriteLine("\tdeserialization: {0}µs", dstime);
            Console.WriteLine("\tsize: {0} bytes", size);
        }

        public static void RunXmlSerialization<T>(T item, int count)
           where T : class, IBusinessObject
        {
            // Warm-up
            var xml = item.ToXml();
            var size = xml.Length;

            var t = new HiPerfTimer(true);
            t.Start();
            for (int i = 0; i < count; i++)
            {
                xml = item.ToXml();
            }
            t.Stop();
            var stime = t.GetElapsedTimeInMicroseconds();

            // Warm-up
            var item_copy = ObjectXmlSerializer.FromXml<T>(xml).FirstOrDefault();

            t.Reset();
            t.Start();
            for (int i = 0; i < count; i++)
            {
                item_copy = ObjectXmlSerializer.FromXml<T>(xml).FirstOrDefault();
            }
            t.Stop();
            var dstime = t.GetElapsedTimeInMicroseconds();

            Console.WriteLine("ObjectXmlSerializer");
            Console.WriteLine("\tserialization: {0}µs", stime);
            Console.WriteLine("\tdeserialization: {0}µs", dstime);
            Console.WriteLine("\tsize: {0} bytes", size);
        }

        public static void RunJsonSerialization<T>(T item, int count)
           where T : class, IBusinessObject
        {
            // Warm-up
            var json = item.ToJson();
            var size = json.Length;

            var t = new HiPerfTimer(true);
            t.Start();
            for (int i = 0; i < count; i++)
            {
                json = item.ToJson();
            }
            t.Stop();
            var stime = t.GetElapsedTimeInMicroseconds();

            // Warm-up
            var item_copy = ObjectJsonSerializer.FromJson<T>(json).FirstOrDefault();

            t.Reset();
            t.Start();
            for (int i = 0; i < count; i++)
            {
                item_copy = ObjectJsonSerializer.FromJson<T>(json).FirstOrDefault();
            }
            t.Stop();
            var dstime = t.GetElapsedTimeInMicroseconds();

            Console.WriteLine("ObjectJsonSerializer");
            Console.WriteLine("\tserialization: {0}µs", stime);
            Console.WriteLine("\tdeserialization: {0}µs", dstime);
            Console.WriteLine("\tsize: {0} bytes", size);
        }

        public static void RunDataContractSerialization<T>(T item, int count)
           where T : class
        {
            // Warm-up
            var dcs = new DataContractSerializer(typeof(T));

            var sb = new StringBuilder();
            using (var writer = XmlWriter.Create(sb))
            {
                dcs.WriteObject(writer, item);
            }
            var xml = sb.ToString();
            var size = xml.Length;

            var t = new HiPerfTimer(true);
            t.Start();
            for (int i = 0; i < count; i++)
            {
                sb = new StringBuilder();
                using (var writer = XmlWriter.Create(sb))
                {
                    dcs.WriteObject(writer, item);
                }
                xml = sb.ToString();
            }
            t.Stop();
            var stime = t.GetElapsedTimeInMicroseconds();

            // Warm-up
            T item_copy;
            using (var reader = XmlReader.Create(new StringReader(xml)))
            {
                item_copy = (T)dcs.ReadObject(reader);
            }

            t.Reset();
            t.Start();
            for (int i = 0; i < count; i++)
            {
                using (var reader = XmlReader.Create(new StringReader(xml)))
                {
                    item_copy = (T)dcs.ReadObject(reader);
                }
            }
            t.Stop();
            var dstime = t.GetElapsedTimeInMicroseconds();

            Console.WriteLine("DataContractSerializer");
            Console.WriteLine("\tserialization: {0}µs", stime);
            Console.WriteLine("\tdeserialization: {0}µs", dstime);
            Console.WriteLine("\tsize: {0} bytes", size);
        }

        public static void RunDataContractJsonSerialization<T>(T item, int count)
           where T : class
        {
            // Warm-up
            var dcs = new DataContractJsonSerializer(typeof(T));

            var stream = new MemoryStream();
            dcs.WriteObject(stream, item);
            stream.Position = 0;
            var json = string.Empty;
            using (var reader = new StreamReader(stream))
            {
                json = reader.ReadToEnd();
            }
            var size = json.Length;

            var t = new HiPerfTimer(true);
            t.Start();
            for (int i = 0; i < count; i++)
            {
                stream = new MemoryStream();
                dcs.WriteObject(stream, item);
                stream.Position = 0;
                using (var reader = new StreamReader(stream))
                {
                    json = reader.ReadToEnd();
                }
            }
            t.Stop();
            var stime = t.GetElapsedTimeInMicroseconds();

            // Warm-up
            stream = new MemoryStream(Encoding.Default.GetBytes(json));
            var item_copy = (T)dcs.ReadObject(stream);

            t.Reset();
            t.Start();
            for (int i = 0; i < count; i++)
            {
                stream.Position = 0;
                item_copy = (T)dcs.ReadObject(stream);
            }
            t.Stop();
            var dstime = t.GetElapsedTimeInMicroseconds();

            Console.WriteLine("DataContractJsonSerializer");
            Console.WriteLine("\tserialization: {0}µs", stime);
            Console.WriteLine("\tdeserialization: {0}µs", dstime);
            Console.WriteLine("\tsize: {0} bytes", size);
        }

        public static void RunServiceStackJsonSerialization<T>(T item, int count)
           where T : class
        {
            // Warm-up
            var json = ServiceStack.Text.JsonSerializer.SerializeToString(item);
            var size = json.Length;

            var t = new HiPerfTimer(true);
            t.Start();
            for (int i = 0; i < count; i++)
            {
                json = ServiceStack.Text.JsonSerializer.SerializeToString(item);
            }
            t.Stop();
            var stime = t.GetElapsedTimeInMicroseconds();

            // Warm-up
            var item_copy = ServiceStack.Text.JsonSerializer.DeserializeFromString<T>(json);

            t.Reset();
            t.Start();
            for (int i = 0; i < count; i++)
            {
                item_copy = ServiceStack.Text.JsonSerializer.DeserializeFromString<T>(json);
            }
            t.Stop();
            var dstime = t.GetElapsedTimeInMicroseconds();

            Console.WriteLine("ServiceStack");
            Console.WriteLine("\tserialization: {0}µs", stime);
            Console.WriteLine("\tdeserialization: {0}µs", dstime);
            Console.WriteLine("\tsize: {0} bytes", size);
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
            var json = ServiceStack.Text.JsonSerializer.SerializeToString(item);
            var root = ServiceStack.Text.Json.JsonReader<T>.Parse(json);

            var t = new HiPerfTimer(true);
            t.Start();
            for (int i = 0; i < count; i++)
            {
                root = ServiceStack.Text.Json.JsonReader<T>.Parse(json);
            }
            t.Stop();
            var time = t.GetElapsedTimeInMicroseconds();

            Console.WriteLine("ServiceStack Parser: {0}µs", time);
        }
    }
}
