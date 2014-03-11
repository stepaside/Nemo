using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Nemo.Configuration.Mapping
{
    public abstract class EntityMap<T> : IEntityMap
        where T : class, IDataEntity
    {
        private Dictionary<string, IPropertyMap> _properties;
           
        public EntityMap()
        {
            _properties = new Dictionary<string, IPropertyMap>();
            Cache = new CacheMap();
        }

        public PropertyMap<T, U> Property<U>(Expression<Func<T, U>> selector)
        {
            IPropertyMap map;
            var key = selector.ToString();
            if (!_properties.TryGetValue(key, out map))
            {
                map = new PropertyMap<T, U>(selector);
                _properties[key] = map;
            }
            return (PropertyMap<T, U>)map;
        }

        public ICollection<IPropertyMap> Properties
        {
            get
            {
                return _properties.Values;
            }
        }

        public string TableName { get; protected set; }

        public string SchemaName { get; protected set; }

        public string DatabaseName { get; protected set; }

        public string ConnectionStringName { get; protected set; }

        ICacheMap IEntityMap.Cache
        {
            get
            {
                return this.Cache;
            }
        }

        public CacheMap Cache { get; private set; }

        public bool ReadOnly { get; protected set; }

        public string SoftDeleteColumnName { get; protected set; }
    }
}
