using Nemo;
using Nemo.Attributes;
using Nemo.Collections;
using Nemo.Configuration;
using Nemo.Configuration.Mapping;
using Nemo.Data;
using Nemo.Extensions;
using Nemo.Id;
using Nemo.Serialization;
using Nemo.UnitOfWork;
using Nemo.Validation;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NemoTestCore.Features
{
    internal partial class DbMappingProvidedSql
    {
        public static void RetrieveCustomersAndOrdersAsObjectGraph()
        {
            var retrieve_customer_with_orders_graph = ((IMultiResult)ObjectFactory.Retrieve<Customer, Order>(
                                                                   sql: @"select * from Customers where CustomerID = @CustomerID; 
                                                                                        select * from Orders where CustomerID = @CustomerID",
                                                                   parameters: new { CustomerId = "ALFKI" },
                                                                   connection: Connection)).Aggregate<Customer>();

            var customer = retrieve_customer_with_orders_graph.First();
        }

        public static void RetrieveOrdersAndCustomersSingleRow()
        {
            var retrieve_orders_with_customer = ObjectFactory.Retrieve<IOrder, ICustomer>(
                                                                   sql: @"select c.CustomerID, c.CompanyName, o.OrderID, o.ShipPostalCode from Customers c
                                                                                        left join Orders o on o.CustomerID = c.CustomerID 
                                                                                        where c.CustomerID = @CustomerID",
                                                                   parameters: new ParamList { CustomerId => "ALFKI" },
                                                                   map: (o, c) => { o.Customer = c; return o; },
                                                                   connection: Connection);
            var orders = retrieve_orders_with_customer.ToList();
            var same = orders[0].Customer == orders[1].Customer;
        }

        public static void RetrieveCustomersAndOrdersSingleRow()
        {
            var aggregate_mapper = new DefaultAggregatePropertyMapper<Customer, Order>();
            
            var retrieve_customer_with_orders = ObjectFactory.Retrieve<Customer, Order>(
                                                                    sql: @"select c.CustomerID, c.CompanyName, o.OrderID, o.ShipPostalCode from Customers c
                                                                                        left join Orders o on o.CustomerID = c.CustomerID 
                                                                                        where c.CustomerID = @CustomerID",
                                                                    parameters: new  { CustomerId = "ALFKI" },
                                                                    map: aggregate_mapper.Map, 
                                                                    connection: Connection).FirstOrDefault();
        }

        public static void RetrieveCustomersAndOrdersMultiResult()
        {
            var retrieve_customer_with_orders_lazy = ObjectFactory.Retrieve<Customer, Order>(
                                                                    sql: @"select * from Customers where CustomerID = @CustomerID;
                                                                                        select * from Orders where CustomerID = @CustomerID",
                                                                    parameters: new { CustomerId = "ALFKI" },
                                                                    connection: Connection);

            var lazy_customer = ((IMultiResult)retrieve_customer_with_orders_lazy).Retrieve<Customer>().FirstOrDefault();
            //var lazy_customer = retrieve_customer_with_orders_lazy.FirstOrDefault();
            var lazy_orders = ((IMultiResult)retrieve_customer_with_orders_lazy).Retrieve<Order>().ToList();
        }

        public static void RetrieveScalarAndDynamicDataMultiResult()
        {
            var retrieve_simple_lists = ObjectFactory.Retrieve<string, int, dynamic>(
                                                                    sql: @"select CustomerID from Customers;
                                                                            select OrderID from Orders;
                                                                            select CustomerID, OrderID from Orders",
                                                                    parameters: new ParamList { CustomerId => "ALFKI" },
                                                                    connection: Connection);

            var customer_id_list = ((IMultiResult)retrieve_simple_lists).Retrieve<string>().ToList();
            var order_id_list = ((IMultiResult)retrieve_simple_lists).Retrieve<int>().ToList();
            var dynamic_item_list = ((IMultiResult)retrieve_simple_lists).Retrieve<dynamic>().ToList();
        }

        public static void SimpleUnitOfWork()
        {
            var retrieve_customer_with_orders_graph = ((IMultiResult)ObjectFactory.Retrieve<Customer, Order>(
                                                                   sql: @"select * from Customers where CustomerID = @CustomerID; 
                                                                                        select * from Orders where CustomerID = @CustomerID",
                                                                   parameters: new ParamList { CustomerId => "ALFKI" },
                                                                   connection: Connection)).Aggregate<Customer>();

            var customer = retrieve_customer_with_orders_graph.First();

            using (ObjectScope.New(customer, autoCommit: false, connection: Connection, config: ConfigurationFactory.Get<Customer>()))
            {
                customer.CompanyName += "Test";
                customer.Orders[0].ShipPostalCode = "11111";
                customer.Orders.RemoveAt(1);

                var o = ObjectFactory.Create<Order>();
                o.CustomerId = customer.Id;
                o.ShipPostalCode = "19115";
                o.GenerateKey(ConfigurationFactory.Get<Order>());
                customer.Orders.Add(o);

                //var previos = customer.Old();

                //customer_uow.Rollback();
                customer.Commit();
            }
        }
    }
}
