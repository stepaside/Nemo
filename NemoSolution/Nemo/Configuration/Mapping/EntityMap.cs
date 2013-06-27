using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Nemo.Configuration.Mapping
{
    public abstract class EntityMap<T> : IEntityMap
        where T : class, IBusinessObject
    {
        private Dictionary<string, IPropertyMap> _properties;
           
        public EntityMap()
        {
            Cache = new CacheMap();
        }

        public PropertyMap<U> Property<U>(Expression<Func<U>> selector)
        {
            IPropertyMap map;
            if (!_properties.TryGetValue(selector.ToString(), out map))
            {
                map = new PropertyMap<U>(selector);
            }
            return (PropertyMap<U>)map;
        }

        public ICollection<IPropertyMap> Properties
        {
            get
            {
                return _properties.Values;
            }
        }

        public string TableName
        {
            get;
            private set;
        }

        public string SchemaName
        {
            get;
            private set;
        }

        public string DatabaseName
        {
            get;
            private set;
        }

        public string ConnectionStringName
        {
            get;
            private set;
        }

        ICacheMap IEntityMap.Cache
        {
            get
            {
                return this.Cache;
            }
        }

        public CacheMap Cache { get; private set; }

        public bool ReadOnly { get; protected set; }

        public void Table(string value)
        {
            TableName = value;
        }

        public void Schema(string value)
        {
            SchemaName = value;
        }

        public void Database(string value)
        {
            DatabaseName = value;
        }

        public void Connection(string value)
        {
            ConnectionStringName = value;
        }
    }
}
