using Nemo;
using Nemo.Configuration.Mapping;
using System.Collections.Generic;

namespace NemoTest
{
    public interface ICustomer : IDataEntity
    {
        string Id { get; set; }
        [Nemo.Validation.StringLength(50)]
        string CompanyName { get; set; }
        //[Distinct]
        IList<IOrder> Orders { get; set; }
    }

    public class ICustomerMap : EntityMap<ICustomer>
    {
        public ICustomerMap()
        {
            TableName = "Customers";
            Property(c => c.Id).Column("CustomerID").Parameter("CustomerID").PrimaryKey();
        }
    }
}
