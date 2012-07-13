using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Xml.Serialization;
using Nemo;
using Nemo.Attributes;
using Nemo.Fn;
using Nemo.Id;
using Nemo.Validation;
using ProtoBuf;

namespace NemoTest
{
    [Table("Customers")]
    //[Cache]
    //[CacheLink(typeof(IOrder), DependentParameter = "CustomerId", ValueProperty = "Id")]
    public interface ICustomer : IBusinessObject
    {
        [PrimaryKey, MapColumn("CustomerID"), Parameter("CustomerID")]
        string Id { get; set; }
        [StringLength(50), Persistent(false), XmlAttribute]
        string CompanyName { get; set; }
        IList<IOrder> Orders { get; set; }
        [DoNotSerialize]
        TypeUnion<int, string, double> TypeUnionTest { get; set; }
    }

    [Table("Orders"), ProtoContract, ProtoInclude(50, typeof(OrderLegacy))]
    public interface IOrder : IBusinessObject
    {
        [MapColumn("OrderID"), Generate.Using(typeof(UniqueNegativeNumberGenerator)), PrimaryKey, ProtoMember(1)]
        int OrderId { get; set; }
        [MapColumn("CustomerID"), References(typeof(ICustomer)), ProtoMember(2)]
        string CustomerId { get; set; }
        [DoNotSerialize]
        ICustomer Customer { get; set; }
        [ProtoMember(3)]
        string ShipPostalCode { get; set; }
    }

    public class CustomerOrderMapper
    {
        public ICustomer _current;
        public ObjectEqualityComparer<ICustomer> _comparer = new ObjectEqualityComparer<ICustomer>();
        public ICustomer Map(ICustomer c, IOrder o)
        {
            // Terminating call.  Since we can return null from this function
            // we need to be ready for Nemo to callback later with null
            // parameters
            if (c == null)
                return _current;

            // Is this the same customer as the current one we're processing
            if (_current != null && _comparer.Equals(_current, c))
            {
                // Yes, just add this order to the current customer's collection of orders
                _current.Orders.Add(o);

                // Return null to indicate we're not done with this customer yet
                return null;
            }

            // This is a different customer to the current one, or this is the 
            // first time through and we don't have a customer yet

            // Save the current customer
            var prev = _current;

            // Setup the new current customer
            _current = c;
            _current.Orders = new List<IOrder>();
            _current.Orders.Add(o);

            // Return the now populated previous customer (or null if first time through)
            return prev;
        }
    }

    [Serializable]
    public class PersonLegacy
    {
        public int person_id { get; set; }
        public string name { get; set; }
        public DateTime DateOfBirth { get; set; }
    }

    [Serializable, DataContract, KnownType(typeof(OrderLegacy)), ProtoContract]
    public class CustomerLegacy : ICustomer
    {
        [DataMember, ProtoMember(1)]
        public string Id { get; set; }
        [DataMember, ProtoMember(2)]
        public string CompanyName { get; set; }
        [DataMember, ProtoMember(3)]
        public IList<IOrder> Orders { get; set; }
        [DataMember, ProtoIgnore]
        public List<int> Values { get; set; }
        [ProtoIgnore]
        public TypeUnion<int, string, double> TypeUnionTest { get; set; }
        public static CustomerLegacy Make(ICustomer customer)
        {
            var customer_legacy = new CustomerLegacy();
            customer_legacy.Id = customer.Id;
            customer_legacy.CompanyName = customer.CompanyName;
            if (customer.Orders != null && customer.Orders.Count > 0)
            {
                customer_legacy.Orders = new List<IOrder>();
                foreach (var order in customer.Orders)
                {
                    customer_legacy.Orders.Add(OrderLegacy.Make(order));
                }
            }
            return customer_legacy;
        }
    }

    [Serializable, DataContract, ProtoContract]
    public class OrderLegacy : IOrder
    {
        [DataMember, ProtoMember(1)]
        public int OrderId { get; set; }
        [DataMember, ProtoMember(2)]
        public string CustomerId { get; set; }
        [DataMember, ProtoMember(3)]
        public string ShipPostalCode { get; set; }
        [ProtoIgnore]
        public ICustomer Customer { get; set; }

        public static OrderLegacy Make(IOrder order)
        {
            var order_legacy = new OrderLegacy();
            order_legacy.OrderId = order.OrderId;
            order_legacy.CustomerId = order.CustomerId;
            order_legacy.ShipPostalCode = order.ShipPostalCode;
            return order_legacy;
        }
    }

    [Table("Employee")]
    public interface IPerson : IBusinessObject
    {
        [MapProperty("person_id"), MapColumn("EmployeeID")]
        int Id { get; set; }
        [MapProperty("name")]
        string Name { get; set; }
        DateTime DateOfBirth { get; set; }
    }

    public interface IPersonReadOnly : IBusinessObject
    {
        [MapProperty("person_id")]
        int Id { get; }
        [MapProperty("name")]
        string Name { get; }
        DateTime DateOfBirth { get; set; }
    }

    [Table("Customers")]
    // Used for Dapper performance test
    public class Customer : ICustomer
    {
        public Customer() { }
        public Customer(string id, string companyName) 
        {
            Id = id;
            CompanyName = companyName;
        }

        [PrimaryKey, MapColumn("CustomerID")]
        public string Id { get; set; }
        public string CompanyName { get; set; }
        public IList<IOrder> Orders { get; set; }
        public TypeUnion<int, string, double> TypeUnionTest { get; set; }
    }

    public class Order : IOrder
    {
        public int OrderId
        {
            get;
            set;
        }

        [References(typeof(Customer))]
        public string CustomerId
        {
            get;
            set;
        }

        public ICustomer Customer { get; set; }

        public string ShipPostalCode { get; set; }
    }
}