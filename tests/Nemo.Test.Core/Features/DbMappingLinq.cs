using Nemo;
using Nemo.Attributes;
using Nemo.Configuration;
using Nemo.Configuration.Mapping;
using Nemo.Data;
using Nemo.Id;
using Nemo.Linq;
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
    internal class DbMappingLinq
    {
        static DbMappingLinq()
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

        private static IQueryable<Customer> Customers => new NemoQueryable<Customer>(Connection);

        private static IQueryable<Order> Orders => new NemoQueryable<Order>(Connection);

        private static IAsyncQueryable<Customer> CustomersAsync => new NemoQueryableAsync<Customer>(Connection);

        private static IAsyncQueryable<Order> OrdersAsync => new NemoQueryableAsync<Order>(Connection);

        public static void SelectAllCustomers()
        {
            var allCustomers = Customers.ToList();
        }

        public static void SelectSingleCustomer()
        {
            var linqCustomer = Customers.FirstOrDefault(c => c.Id == "ALFKI");
        }

        public static void SelectSingleCustomerAsync()
        {
            var linqCustomer = CustomersAsync.FirstOrDefaultAsync(c => c.Id == "ALFKI").Result;
        }

        public static void SelectCustomersUsingLinqQuery()
        {
            var linqCustomersQuery = (from c in Customers
                                      where c.Id == "ALFKI"
                                      orderby c.Id descending
                                      select c).Take(10).Skip(5).ToList();
        }

        public static void GetMaxOrderId()
        {
            var maxId = Orders.Max(o => o.OrderId);
        }

        public static void GetMaxOrderIdOfCustomer()
        {
            var maxId = Orders.Where(o => o.CustomerId == "ALFKI").Max(o => o.OrderId);
        }

        public static void GetMaxOrderIdAsync()
        {
            var maxIdAsync = OrdersAsync.MaxAsync(o => o.OrderId).Result;
        }

        public static void CountCustomers()
        {
            var count1 = Customers.Count(c => c.Id == "ALFKI");
            var count2 = Customers.Where(c => c.Id == "ALFKI").Count();
        }
    }
}
