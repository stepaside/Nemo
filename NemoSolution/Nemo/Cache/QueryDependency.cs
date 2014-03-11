using Nemo.Extensions;
using Nemo.Reflection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Cache
{
    public class QueryDependency
    {
        public QueryDependency(params string[] properties)
        {
            Properties = properties;
        }

        public string[] Properties { get; private set; }

        public IList<Param> GetParameters<T>(T item)
            where T : class, IDataEntity
        {
            var properties = Reflector.PropertyCache<T>.NameMap;
            var parameters = new List<Param>();
            var names = this.Properties.Where(p => !string.IsNullOrEmpty(p)).Select(p => p).Distinct();
            foreach (var name in names)
            {
                ReflectedProperty property;
                if (properties.TryGetValue(name, out property))
                {
                    parameters.Add(new Param { Name = property.ParameterName ?? name, Value = item.Property(name) });
                }
            }
            return parameters;
        }
    }
}
