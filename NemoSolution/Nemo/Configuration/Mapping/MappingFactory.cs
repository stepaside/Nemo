using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Nemo.Reflection;
using System.Reflection;
using Nemo.Attributes;

namespace Nemo.Configuration.Mapping
{
    internal static class MappingFactory
    {
        private static Lazy<Dictionary<Type, IEntityMap>> _types = new Lazy<Dictionary<Type, IEntityMap>>(MappingFactory.Scan, true);

        private static Dictionary<Type, IEntityMap> Scan()
        {
            var assemblies = AppDomain.CurrentDomain.GetAssemblies();
            var types = assemblies
                .Where(a => !a.IsDynamic && !a.ReflectionOnly)
                .Select(a => a.DefinedTypes)
                .SelectMany(_ => _)
                .Where(t => t.BaseType != null
                            && t.BaseType.IsAbstract
                            && t.BaseType.IsPublic
                            && t.BaseType.IsGenericType
                            && t.BaseType.GetGenericTypeDefinition() == typeof(EntityMap<>));
            var maps = types.GroupBy(t => t.BaseType.GetGenericArguments()[0]).ToDictionary(g => g.Key, g => (IEntityMap)g.First().New());
            return maps;
        }

        internal static void Initialize()
        {
            if (!_types.IsValueCreated)
            {
                var maps = _types.Value;
            }
        }

        internal static IEntityMap GetEntityMap<T>()
            where T : class, IBusinessObject
        {
            IEntityMap map;
            if (_types.Value.TryGetValue(typeof(T), out map))
            {
                return map;
            }
            return null;
        }

        internal static IEntityMap GetEntityMap(Type type)
        {
            IEntityMap map;
            if (Reflector.IsBusinessObject(type) && _types.Value.TryGetValue(type, out map))
            {
                return map;
            }
            return null;
        }

        internal static string GetPropertyOrColumnName(PropertyInfo property, bool ignoreMappings, IEntityMap entityMap, bool isColumn)
        {
            string propertyOrColumnName;
            if (ignoreMappings)
            {
                propertyOrColumnName = property.Name;
            }
            else if (entityMap != null)
            {
                var propertyMap = entityMap.Properties.FirstOrDefault(p => p.Property.PropertyName == property.Name);
                if (propertyMap != null && ((isColumn && propertyMap.Property.MappedColumnName != null) || propertyMap.Property.MappedPropertyName != null))
                {
                    propertyOrColumnName = isColumn ? propertyMap.Property.MappedColumnName : propertyMap.Property.MappedPropertyName;
                }
                else
                {
                    propertyOrColumnName = isColumn ? MapColumnAttribute.GetMappedColumnName(property) : MapPropertyAttribute.GetMappedPropertyName(property);
                }
            }
            else
            {
                propertyOrColumnName = isColumn ? MapColumnAttribute.GetMappedColumnName(property) : MapPropertyAttribute.GetMappedPropertyName(property);
            }
            return propertyOrColumnName;
        }
    }
}
