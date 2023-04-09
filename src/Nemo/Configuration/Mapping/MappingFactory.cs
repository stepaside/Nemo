using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Nemo.Attributes.Converters;
using Nemo.Reflection;
using System.Reflection;
using Nemo.Attributes;
using System.Data;
using Nemo.Fn;
using System.Collections.Concurrent;
using System.Runtime.InteropServices.ComTypes;

namespace Nemo.Configuration.Mapping
{
    internal static class MappingFactory
    {
        private static readonly ConcurrentDictionary<Type, IEntityMap> _types = new ConcurrentDictionary<Type, IEntityMap>();

        private static Dictionary<Type, IEntityMap> Scan()
        {
            var assemblies = AppDomain.CurrentDomain.GetAssemblies();
            var types = assemblies
                .Where(a => !a.IsDynamic && !a.ReflectionOnly)
                .Select(a =>
                {
                    try
                    {
                        return a.DefinedTypes;
                    }
                    catch (ReflectionTypeLoadException e)
                    {
                        return e.Types.Where(t => t != null).Select(t => t.GetTypeInfo()).ToArray();
                    }
                    catch
                    {
                        return new TypeInfo[] { };
                    }
                })
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
            var maps = Scan();
            foreach (var entry in maps)
            {
                _types[entry.Key] = entry.Value;
            }
        }

        internal static IEntityMap GetEntityMap<T>()
            where T : class
        {
            IEntityMap map;
            return _types.TryGetValue(typeof(T), out map) ? map : null;
        }

        internal static IEntityMap GetEntityMap(Type type)
        {
            IEntityMap map;
            if ((!Reflector.IsEmitted(type) || Reflector.IsDataEntity(type)) && _types.TryGetValue(type, out map))
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

        internal static Tuple<Type, Type> GetTypeConverter(Type fromType, PropertyInfo property, IEntityMap entityMap)
        {
            if (entityMap != null)
            {
                var propertyMap = entityMap.Properties.FirstOrDefault(p => p.Property.PropertyName == property.Name);
                if (propertyMap != null)
                {
                    var typeConverter = propertyMap.Property.Converter;
                    if (typeConverter != null)
                    {
                        TypeConverterAttribute.ValidateTypeConverterType(typeConverter, fromType, property.PropertyType);
                        return Tuple.Create(typeConverter, TypeConverterAttribute.GetExpectedConverterInterfaceType(fromType, property.PropertyType));
                    }
                }
            }

            return TypeConverterAttribute.GetTypeConverter(fromType, property);
        }

        internal static object GetItem(IDictionary<string, object> source, string name, object defaultValue)
        {
            if (source != null && source.TryGetValue(name, out var value))
            {
                return value ?? defaultValue;
            }

            return defaultValue;
        }

        internal static object GetItem(DataRow source, string name, object defaultValue)
        {
            if (source != null && source.Table != null && source.Table.Columns != null && source.Table.Columns.Contains(name))
            {
                return source[name] ?? defaultValue;
            }

            return defaultValue;
        }

        internal static object GetItem(IDataRecord source, string name, object defaultValue)
        {
            if (source != null)
            {
                if (source is WrappedRecord record)
                {
                    return record[name] ?? defaultValue;
                }

                if (source is WrappedReader reader)
                {
                    return reader[name] ?? defaultValue;
                }

                try
                {
                    return source[name] ?? defaultValue;
                }
                catch (IndexOutOfRangeException)
                {

                }
            }

            return defaultValue;
        }

        internal static bool IsIndexer<TSource>(TSource source) 
        {
            return source is IDictionary<string, object> || (source is IDataRecord) || source is DataRow;
        }

        internal static Type GetIndexerType(object source)
        {
            if (source is IDictionary<string, object>)
            {
                return typeof(IDictionary<string, object>);
            }
            if (source is IDataRecord)
            {
                return typeof(IDataRecord);
            }
            return typeof(DataRow);
        }
    }
}
