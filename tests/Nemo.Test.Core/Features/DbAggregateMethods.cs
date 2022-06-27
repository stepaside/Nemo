using Nemo;
using Nemo.Configuration;
using Nemo.Configuration.Mapping;
using Nemo.Data;
using Nemo.Id;
using Nemo.Serialization;
using Nemo.UnitOfWork;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NemoTestCore.Features
{
    public class DbAggregateMethods
    {
        private static readonly INemoConfiguration _nemoConfig;

        static DbAggregateMethods()
        {
            _nemoConfig = ConfigurationFactory.CloneCurrentConfiguration();

            _nemoConfig.SetDefaultCacheRepresentation(CacheRepresentation.LazyList)
                    .SetPadListExpansion(true)
                    .SetAutoTypeCoercion(true)
                    .SetIgnoreInvalidParameters(true)
                    .SetDefaultChangeTrackingMode(ChangeTrackingMode.Manual)
                    .SetDefaultMaterializationMode(MaterializationMode.Exact)
                    .SetDefaultSerializationMode(SerializationMode.Compact)
                    .SetGenerateInsertSql(true)
                    .SetGenerateDeleteSql(true)
                    .SetGenerateUpdateSql(true)
                    .SetOperationPrefix("spDTO_")
                    .SetOperationNamingConvention(OperationNamingConvention.PrefixTypeName_Operation);
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

        public static void GetMaxOrderId()
        {
            var maxOrderId = ObjectFactory.Max<Order, int>(o => o.OrderId, o => o.CustomerId == "ALFKI", connection: Connection);
        }

        public static void GetCountOfCustomersStartingWithA()
        {
            var selected_customers_A_count = ObjectFactory.Count<Customer>(c => c.CompanyName.StartsWith("A"), connection: Connection);
        }
    }
}
