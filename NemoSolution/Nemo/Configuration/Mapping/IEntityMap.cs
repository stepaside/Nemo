using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nemo.Configuration.Mapping
{
    public interface IEntityMap
    {
        bool ReadOnly { get; }
        string TableName { get; }
        string SchemaName { get; }
        string DatabaseName { get; }
        string ConnectionStringName { get; }
        string SoftDeleteColumnName { get; }
        ICollection<IPropertyMap> Properties { get; }
    }
}
