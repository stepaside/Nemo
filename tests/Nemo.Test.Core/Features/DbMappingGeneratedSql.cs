using Nemo;
using Nemo.Attributes;
using Nemo.Configuration;
using Nemo.Configuration.Mapping;
using Nemo.Data;
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
    internal class DbMappingGeneratedSql
    {
        static DbMappingGeneratedSql()
        {
            var nemoConfig = ConfigurationFactory.CloneCurrentConfiguration();
          
            nemoConfig.SetDefaultCacheRepresentation(CacheRepresentation.LazyList)
                    .SetPadListExpansion(true)
                    .SetAutoTypeCoercion(true)
                    .SetIgnoreInvalidParameters(true)
                    .SetDefaultChangeTrackingMode(ChangeTrackingMode.Manual)
                    .SetDefaultMaterializationMode(MaterializationMode.Exact)
                    .SetDefaultSerializationMode(SerializationMode.Compact)
                    .SetGenerateInsertSql(true)
                    .SetGenerateDeleteSql(true)
                    .SetGenerateUpdateSql(true);

            ConfigurationFactory.Set<Customer>(nemoConfig);
            ConfigurationFactory.Set<Order>(nemoConfig);
            ConfigurationFactory.Set<ICustomer>(nemoConfig);
            ConfigurationFactory.Set<IOrder>(nemoConfig);
        }

        public interface ICustomer : IDataEntity
        {
            [MapColumn("CustomerID"), PrimaryKey]
            string Id { get; set; }
            [StringLength(50)]
            string CompanyName { get; set; }
            IList<IOrder> Orders { get; set; }
        }

        public interface IOrder : IDataEntity
        {
            [MapColumn("OrderID"), Generate.Using(typeof(UniqueNegativeNumberGenerator)), PrimaryKey]
            int OrderId { get; set; }
            [MapColumn("CustomerID"), References(typeof(ICustomer))]
            string CustomerId { get; set; }
            [DoNotPersist, DoNotSerialize]
            ICustomer Customer { get; set; }
            string ShipPostalCode { get; set; }
        }

        public class Customer
        {
            public string Id { get; set; }
            public string CompanyName { get; set; }
            public IList<Order> Orders { get; set; }
        }

        public class Order
        {
            public int OrderId { get; set; }
            public string CustomerId { get; set; }
            public Customer Customer { get; set; }
            public string ShipPostalCode { get; set; }
        }

        public class CustomerMap : EntityMap<Customer>
        {
            public CustomerMap()
            {
                TableName = "Customers";
                Property(c => c.Id).Column("CustomerID").Parameter("CustomerID").PrimaryKey();
            }
        }

        public class OrderMap : EntityMap<Order>
        {
            public OrderMap()
            {
                TableName = "Orders";
                Property(o => o.OrderId).Column("OrderID").Parameter("OrderID")
                    .PrimaryKey().Generated(typeof(UniqueNegativeNumberGenerator));
                Property(o => o.CustomerId).References<Customer>();
                Property(o => o.Customer).Not.Serializable().Not.Persistent();
            }
        }

        private static DbConnection Connection => DbFactory.CreateConnection("DbConnection", DataAccessProviderTypes.SqlServer);
            
        public static void SelectAllCustomers()
        {
            var customers = ObjectFactory.Select<Customer>(connection: Connection).ToList();
        }

        public static void SelectCustomersByPage()
        {
            var customers = ObjectFactory.Select<Customer>(page: 1, pageSize: 10, connection: Connection).ToList();
        }

        public static void SelectCustomersStartingWithAByPage()
        {
            var selected_customers_A = ObjectFactory.Select<Customer>(c => c.CompanyName.StartsWith("A"), page: 1, pageSize: 2, connection: Connection).ToList();
        }

        public static void CountCustomersStartingWithA()
        {
            var selected_customers_A_count = ObjectFactory.Count<Customer>(c => c.CompanyName.StartsWith("A"));
        }

        public static void SelectMaximumOrderIdForCustomer()
        {
            var max = ObjectFactory.Max<Order, int>(o => o.OrderId, o => o.CustomerId == "ALFKI", connection: Connection);
        }

        public static void SelectCustomersWithOrders()
        {
            var selected_customers_with_orders = ObjectFactory.Select<Customer>(c => c.Orders.Count > 0, connection: Connection);
        }

        public static void SelectCustomersWithOrdersIncludingOrders()
        {
            var selected_customers_and_orders_include = ObjectFactory.Select<Customer>(c => c.Orders.Count > 0, connection: Connection).Include<Customer, Order>((c, o) => c.Id == o.CustomerId).ToList();
        }
    }
}
