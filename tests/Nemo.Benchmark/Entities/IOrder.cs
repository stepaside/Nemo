using Nemo;
using Nemo.Attributes;
using Nemo.Id;

namespace Nemo.Benchmark.Entities
{
    [Nemo.Attributes.Table("Orders")]
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
}
