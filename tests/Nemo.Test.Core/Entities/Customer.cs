using Nemo.Attributes;
using System.Collections.Generic;

namespace NemoTest
{
    [Table("Customers")]
    // Used for Dapper performance test
    public class Customer
    {
        public Customer() { }

        public Customer(string id, string companyName)
        {
            Id = id;
            CompanyName = companyName;
        }

        public Customer(Customer customer)
        {
            Id = customer.Id;
            CompanyName = customer.CompanyName;
            if (customer.Orders != null)
            {
                Orders = new List<Order>();
                foreach (var order in customer.Orders)
                {
                    Orders.Add(new Order { OrderId = order.OrderId, CustomerId = order.CustomerId, ShipPostalCode = order.ShipPostalCode });
                }
            }
        }

        [PrimaryKey, MapColumn("CustomerID")]
        public string Id { get; set; }
        public string CompanyName { get; set; }
        public IList<Order> Orders { get; set; }
    }
}
