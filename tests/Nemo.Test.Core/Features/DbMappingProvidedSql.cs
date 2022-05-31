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
    internal partial class DbMappingProvidedSql
    {
        static DbMappingProvidedSql()
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
                    .SetGenerateUpdateSql(true)
                    .SetOperationPrefix("spDTO_")
                    .SetOperationNamingConvention(OperationNamingConvention.PrefixTypeName_Operation);

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
            
        public static void RetrieveWithDynamicParameters()
        {
            var retrieve_customer_dyn = ObjectFactory.Retrieve<Customer>(parameters: new { CustomerID = "ALFKI" }, connection: Connection).FirstOrDefault();
        }

        public static void RetrieveWithNemoParameters()
        {
            var retrieve_customer = ObjectFactory.Retrieve<Customer>(parameters: new[] { new Param { Name = "CustomerID", Value = "ALFKI" } }, connection: Connection).FirstOrDefault();
        }

        public static void RetrieveWithNemoParametersUsingInterface()
        {
            var retrieve_customer = ObjectFactory.Retrieve<ICustomer>(parameters: new[] { new Param { Name = "CustomerID", Value = "ALFKI" } }, connection: Connection).FirstOrDefault();
        }

        public static void RetrieveMemoizedWithCustomOperationNameAndExpressionParametersUsingInterface()
        {
            var retrieve_customers_by_country = ObjectFactory.Retrieve<ICustomer>(operation: "RetrieveByCountry", parameters: new ParamList { Country => "USA", State => "PA" }, connection: Connection).Memoize();

            // Executes stpored procedure here
            if (retrieve_customers_by_country.Any())
            {
                var customer_count = retrieve_customers_by_country.Count();
            }

            // Doesn't execute stored procedure here since the enumerable has been memoized
            var customer = retrieve_customers_by_country.FirstOrDefault();
        }

        public static void RetrieveSingleCustomer()
        {
            // Simple retrieve with sql statement operation
            var retrieve_customer_sql = ObjectFactory.Retrieve<Customer>(sql: "select * from Customers where CustomerID = @CustomerID", parameters: new { CustomerID = "ALFKI" }, connection: Connection).ToList();
        }

        public static void RetrieveManyCustomers()
        {
            // Simple retrieve with sql statement operation
            var retrieve_many_customer_sql = ObjectFactory.Retrieve<Customer>(sql: "select * from Customers", connection: Connection).ToList();
        }

        public static void RetrieveManyCustomersFromList()
        {
            // Simple retrieve with sql statement operation
            var retrieve_many_customer_sql = ObjectFactory.Retrieve<Customer>(sql: "select * from Customers where CustomerID in (@CustomerIDs)", parameters: new { CustomerIDs = new[] { "ALFKI", "ANTON" } }, connection: Connection).ToList();
        }

        public static void RetrieveOrderIds()
        {
            // Simple retrieve with sql statement operation
            var retrieve_customer_orders_ids_sql = ObjectFactory.Retrieve<int>(sql: "select OrderID from Orders where CustomerID = @CustomerID", parameters: new { CustomerID = "ALFKI" }, connection: Connection);
        }
    }
}
