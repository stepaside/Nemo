using Nemo.Collections.Extensions;
using Nemo.Configuration;
using Nemo.Extensions;
using Nemo.Fn;
using Nemo.Reflection;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Nemo.Collections
{
    public interface IMultiResult
    {
        Type[] AllTypes { get; }
        IEnumerable<T> Retrieve<T>();
        bool Reset();
        bool IsCached { get; }
    }

    [Serializable]
    public class MultiResult<T1, T2> : IMultiResult, IEnumerable<T1>
        where T1 : class
        where T2 : class
    {
        private readonly IEnumerable<ITypeUnion> _source;
        private IEnumerator<ITypeUnion> _iter;
        private readonly bool _cached;
        private ITypeUnion _last;

        public MultiResult(IEnumerable<ITypeUnion> source, bool cached)
        {
            _cached = cached;
            if (cached)
            {
                if (ConfigurationFactory.Get<T1>().DefaultCacheRepresentation == CacheRepresentation.List)
                {
                    _source = source.ToList();
                }
                else
                {
                    _source = source.AsStream();
                }
            }
            else
            {
                _source = source;
            }
            _iter = _source.GetEnumerator();
        }

        IEnumerator<T1> IEnumerable<T1>.GetEnumerator()
        {
            return Retrieve<T1>().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return _iter;
        }

        public IEnumerable<T> Retrieve<T>()
        {
            if (typeof(T) == typeof(ObjectFactory.Fake))
            {
                yield break;
            }

            if (_last != null && _last.Is<T>())
            {
                yield return _last.As<T>(); ;
            }

            while (_iter.MoveNext())
            {
                _last = _iter.Current;
                if (_last.Is<T>())
                {
                    yield return _last.As<T>();
                }
                else
                {
                    yield break;
                }
            }
        }

        public virtual Type[] AllTypes => new[] { typeof(T1), typeof(T2) };
        
        public bool Reset()
        {
            if (!_cached) return false;
            _last = null;
            _iter = _source.GetEnumerator();
            return true;
        }

        public bool IsCached => _cached;
    }

    [Serializable]
    public class MultiResult<T1, T2, T3> : MultiResult<T1, T2>, IMultiResult
        where T1 : class
        where T2 : class
        where T3 : class
    {
        public MultiResult(IEnumerable<ITypeUnion> source, bool cached)
            : base(source, cached)
        { }

        public override Type[] AllTypes => new[] { typeof(T1), typeof(T2), typeof(T3) };
    }

    [Serializable]
    public class MultiResult<T1, T2, T3, T4> : MultiResult<T1, T2, T3>
        where T1 : class
        where T2 : class
        where T3 : class
        where T4 : class
    {
        public MultiResult(IEnumerable<ITypeUnion> source, bool cached)
            : base(source, cached)
        { }

        public override Type[] AllTypes => new[] { typeof(T1), typeof(T2), typeof(T3), typeof(T4) };
    }

    [Serializable]
    public class MultiResult<T1, T2, T3, T4, T5> : MultiResult<T1, T2, T3, T4>
        where T1 : class
        where T2 : class
        where T3 : class
        where T4 : class
        where T5 : class
    {
        public MultiResult(IEnumerable<ITypeUnion> source, bool cached)
            : base(source, cached)
        { }

        public override Type[] AllTypes => new[] { typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5) };
    }

    public static class MultiResult
    {
        private static readonly ConcurrentDictionary<TypeArray, Type> _types = new ConcurrentDictionary<TypeArray, Type>();
        private static readonly ConcurrentDictionary<TypeArray, List<MethodInfo>> _methods = new ConcurrentDictionary<TypeArray, List<MethodInfo>>();
        private static readonly ConcurrentDictionary<Type, List<ObjectRelation>> _relations = new ConcurrentDictionary<Type, List<ObjectRelation>>();

        public static IMultiResult Create(IList<Type> types, IEnumerable<ITypeUnion> source, bool cached)
        {
            if (types == null || source == null) return null;

            Type genericType = null;
            switch (types.Count)
            {
                case 2:
                    genericType = typeof(MultiResult<,>);
                    break;
                case 3:
                    genericType = typeof(MultiResult<,,>);
                    break;
                case 4:
                    genericType = typeof(MultiResult<,,,>);
                    break;
                case 5:
                    genericType = typeof(MultiResult<,,,,>);
                    break;
            }

            if (genericType == null) return null;

            var key = new TypeArray(types);
            var type = _types.GetOrAdd(key, t => genericType.MakeGenericType(t.Types is Type[] ? (Type[])t.Types : t.Types.ToArray()));
            var activator = Reflection.Activator.CreateDelegate(type, typeof(IEnumerable<ITypeUnion>), typeof(bool));
            var multiResult = (IMultiResult)activator(source, cached);
            return multiResult;
        }

        public static IEnumerable<IEnumerable<object>> AsEnumerable(this IMultiResult source)
        {
            if (source == null) yield break;

            var key = new TypeArray(source.AllTypes);

            var methods = _methods.GetOrAdd(key, t =>
            {
                var retrieve = source.GetType().GetMethod("Retrieve");
                return t.Types.Select(type => retrieve.MakeGenericMethod(type)).ToList();
            });

            foreach(var method in methods)
            {
                yield return ((IEnumerable)method.Invoke(source, null)).Cast<object>();
            }
        }

        public static IEnumerable<T> Aggregate<T>(this IMultiResult source)
            where T : class
        {
            return source.Aggregate<T>(ConfigurationFactory.Get<T>());
        }

        public static IEnumerable<T> Aggregate<T>(this IMultiResult source, IConfiguration config)
            where T : class
        {
            var results = source.AsEnumerable().Select(s => s.ToList()).ToList();
            
            var relations = InferRelations(source.AllTypes).ToList();

            var roots = new List<T>();

            for (var i = 0; i < source.AllTypes.Length; i++)
            {
                var identityMap = source.IsCached ? Identity.Get(source.AllTypes[i], config) : null;
                var propertyKey = source.IsCached ? ObjectFactory.GetPrimaryKeyProperties(source.AllTypes[i]) : null;
                var count = 0;
                foreach (var item in results[i])
                {
                    string hash = null;
                    var value = source.IsCached ? identityMap.GetEntityByKey<object, object>(item.GetKeySelector(propertyKey), out hash) : null;
                    if (value != null)
                    {
                        if (i == 0)
                        {
                            roots.Add((T)value);
                            count++;
                        }
                        continue;
                    }

                    if (i == 0)
                    {
                        roots.Add((T)item);
                    }

                    LoadRelatedData(item, source.AllTypes[i], relations, results, source.IsCached, config);

                    identityMap.WriteThrough(item, hash);
                }

                if (source.IsCached && i == 0 && count == roots.Count)
                {
                    return roots;
                }
            }

            return roots;
        }
        
        private static void LoadRelatedData(object value, Type objectType, List<ObjectRelation> relations, List<List<object>> set, bool cached, IConfiguration config)
        {
            var propertyMap = Reflector.GetPropertyMap(objectType);

            var primaryKey = value.GetPrimaryKey();
            
            foreach (var property in propertyMap)
            {
                // By convention each relation should end with the name of the property prefixed with underscore
                var relation = relations.FirstOrDefault(r => r.Name.EndsWith("_" + property.Key.Name));

                if (relation == null || !relation.IsValid()) continue;

                var items = set[relation.To.Index].Where(item => relation.To.Properties.Zip(relation.From.Properties, (to, from) => new { To = to, From = from }).All(p => object.Equals(item.Property(p.To.PropertyName), primaryKey[p.From.PropertyName]))).ToList();
                
                if (items.Count == 0) continue;
                
                object propertyValue = null;
                if (property.Value.IsDataEntity || property.Value.IsObject)
                {
                    var propertyKey = cached ? ObjectFactory.GetPrimaryKeyProperties(property.Key.PropertyType) : null;
                    var identityMap = cached ? Identity.Get(property.Key.PropertyType, config) : null;

                    propertyValue = cached ? identityMap.GetEntityByKey<object, object>(items[0].GetKeySelector(propertyKey), out var hash) ?? items[0] : items[0];

                    SetForeignKeys(property.Key.PropertyType, propertyValue, objectType, value);
                }
                else if (property.Value.IsDataEntityList || property.Value.IsObjectList)
                {
                    var elementType = property.Value.ElementType;
                    if (elementType != null)
                    {
                        var propertyKey = cached ? ObjectFactory.GetPrimaryKeyProperties(elementType) : null;;
                        var foreignKeys = Reflector.GetPropertyNameMap(elementType).Values.Where(p => p.PropertyType == objectType).ToArray();
                        var identityMap = cached ? Identity.Get(elementType, config) : null;

                        IList list;
                        if (!property.Value.IsListInterface)
                        {
                            list = (IList)property.Key.PropertyType.New();
                        }
                        else
                        {
                            list = List.Create(elementType, property.Value.Distinct, property.Value.Sorted);
                        }

                        foreach (var item in items)
                        {
                            var listItem = cached ? identityMap.GetEntityByKey<object, object>(item.GetKeySelector(propertyKey), out var hash) ?? item : item;
                            
                            SetForeignKeys(foreignKeys, listItem, value);

                            list.Add(listItem);
                        }

                        propertyValue = list;
                    }
                }
                
                Reflector.Property.Set(value.GetType(), value, property.Key.Name, propertyValue);
            }
        }

        private static void SetForeignKeys(Type propertyType, object propertyValue, Type parentType, object parentValue)
        {
            var foreignKeys = Reflector.GetPropertyNameMap(propertyType).Values.Where(p => p.PropertyType == parentType);

            SetForeignKeys(foreignKeys, propertyValue, parentValue);
        }

        private static void SetForeignKeys(IEnumerable<ReflectedProperty> foreignKeys, object propertyValue, object parentValue)
        {
            foreach (var foreignKey in foreignKeys)
            {
                propertyValue.Property(foreignKey.PropertyName, parentValue);
            }
        }

        private static IEnumerable<ObjectRelation> InferRelations(IList<Type> objectTypes)
        {
            foreach (var objectType in objectTypes)
            {
                foreach (var relation in _relations.GetOrAdd(objectType, t => InferRelations(t).ToList()))
                {
                    relation.To.Index = objectTypes.FindIndex(t => t == relation.To.Type);
                    yield return relation;
                }
            }
        }

        private static IEnumerable<ObjectRelation> InferRelations(Type objectType)
        {
            var propertyMap = Reflector.GetPropertyMap(objectType);

            var primaryKey = propertyMap.Where(p => p.Value.IsPrimaryKey).OrderBy(p => p.Value.KeyPosition).Select(p => p.Value).ToList();

            if (primaryKey.Count == 0) yield break;

            var fromVertex = new ObjectVertex { Type = objectType, Properties = primaryKey };

            var references = propertyMap.Where(p => p.Value.IsDataEntity || p.Value.IsDataEntityList || p.Value.IsObject || p.Value.IsObjectList).Select(p => p.Value);
            foreach (var reference in references)
            {
                var elementType = (reference.IsDataEntityList || reference.IsObjectList) ? reference.ElementType : reference.PropertyType;

                var referencedPropertyMap = Reflector.GetPropertyMap(elementType);
                var referencedProperties = referencedPropertyMap.Where(p => p.Value != null && p.Value.Parent == objectType).OrderBy(p => p.Value.RefPosition).Select(p => p.Value).ToList();
                if (referencedProperties.Count > 0)
                {
                    yield return new ObjectRelation { Name = "_" + reference.PropertyName, From = fromVertex, To = new ObjectVertex { Type = elementType, Properties = referencedProperties } };
                }
            }
        }

        private class ObjectRelation
        {
            public string Name { get; set; }
            public ObjectVertex From { get; set; }
            public ObjectVertex To { get; set; }

            public bool IsValid()
            {
                return From != null && To != null && From.Index >= 0 && To.Index >= 0;
            }
        }
        
        private class ObjectVertex
        {
            public Type Type { get; set; }
            public int Index { get; set; }
            public List<ReflectedProperty> Properties { get; set; }
        }
    }
}
