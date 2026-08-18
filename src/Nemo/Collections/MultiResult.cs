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
using System.Linq.Expressions;
using System.Reflection;

namespace Nemo.Collections
{
    public interface IMultiResult
    {
        Type[] AllTypes { get; }
        IEnumerable<T> Retrieve<T>();
        bool Reset();
        bool IsCached { get; }
        INemoConfiguration Configuration { get; }
    }

    [Serializable]
    internal class MultiResult<TFirst> : IMultiResult, IEnumerable<TFirst>
    {
        private readonly IEnumerable<MultiResultItem> _source;
        private IEnumerator<MultiResultItem> _iter;
        private MultiResultItem _last;

        public MultiResult(IList<Type> types, IEnumerable<MultiResultItem> source, bool cached, INemoConfiguration config)
        {
            if (types == null) throw new ArgumentNullException(nameof(types));
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (types.Count == 0 || (types.Count == 1 && types[0] == typeof(TFirst))) throw new ArgumentException("Insufficient number for types provided");

            if (types[0] != typeof(TFirst))
            {
                types = types.Prepend(typeof(TFirst)).ToArray();
            }

            AllTypes = types as Type[] ?? types.ToArray();
            IsCached = cached; 
            
            if (cached)
            {
                Configuration = config;
                if ((config ?? ConfigurationFactory.Get<TFirst>()).DefaultCacheRepresentation == CacheRepresentation.List)
                {
                    _source = source.ToList();
                }
                else
                {
                    _source = source.Memoize();
                }
            }
            else
            {
                _source = source;
            }
            _iter = _source.GetEnumerator();
        }

        public Type[] AllTypes { get; }

        public bool IsCached { get; }

        public INemoConfiguration Configuration { get; }

        public IEnumerable<T> Retrieve<T>()
        {
            if (typeof(T) == typeof(ObjectFactory.Fake) || !AllTypes.Any(t => t.IsAssignableFrom(typeof(T))))
            {
                yield break;
            }

            if (_last != null && _last.Item is T item1)
            {
                yield return item1;
            }

            while (_iter.MoveNext())
            {
                var current = _iter.Current;

                // if current item matches the type requested return the item
                if (current.Item is T item2)
                {
                    _last = current;
                    yield return item2;
                }
                else
                {
                    // the previous type items have been iterated through
                    // thus we can break out
                    if (_last != null && current.ItemTypeIndex != _last.ItemTypeIndex)
                    {
                        _last = current;
                        yield break;
                    }
                    else
                    {
                        // we haven't exhaused the type yet but are requesting next type
                        _last = null;
                        current.SkipNextCallback?.Invoke();
                    }
                }
            }
        }

        public bool Reset()
        {
            if (!IsCached) return false;
            _last = null;
            _iter = _source.GetEnumerator();
            return true;
        }

        IEnumerator<TFirst> IEnumerable<TFirst>.GetEnumerator()
        {
            return Retrieve<TFirst>().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return _iter;
        }
    }

    internal class MultiResultItem
    {
        public object Item { get; set; }
        public Type ItemType { get; set; }
        public int ItemTypeIndex { get; set; }
        public Action SkipNextCallback { get; set; }
    }

    public static class MultiResult
    {
        private static readonly ConcurrentDictionary<Type, Type> _types = new ConcurrentDictionary<Type, Type>();
        private static readonly ConcurrentDictionary<Type, Func<IMultiResult, IEnumerable<object>>> _retrievers = new ConcurrentDictionary<Type, Func<IMultiResult, IEnumerable<object>>>();
        private static readonly ConcurrentDictionary<Type, List<ObjectRelation>> _relations = new ConcurrentDictionary<Type, List<ObjectRelation>>();
        private static readonly ConcurrentDictionary<Type, List<RelationTemplate>> _templates = new ConcurrentDictionary<Type, List<RelationTemplate>>();
        private static readonly ConcurrentDictionary<TypeArray, List<RelationPlan>[]> _plans = new ConcurrentDictionary<TypeArray, List<RelationPlan>[]>();
        private static readonly MethodInfo RetrieveMethod = typeof(IMultiResult).GetMethod(nameof(IMultiResult.Retrieve));
        private static readonly MethodInfo CastMethod = typeof(Enumerable).GetMethods(BindingFlags.Static | BindingFlags.Public).First(m => m.Name == nameof(Enumerable.Cast));
        private static readonly List<object> EmptyResult = new List<object>();

        //  A key standing in for a null relation key, which a dictionary cannot store
        private static readonly object NullKey = new object();

        internal static IMultiResult Create(IList<Type> types, IEnumerable<MultiResultItem> source, bool cached, INemoConfiguration config)
        {
            if (types == null || source == null || types.Count < 2) return null;

            var type = _types.GetOrAdd(types[0], t => typeof(MultiResult<>).MakeGenericType(types[0]));
            var activator = Reflection.Activator.CreateDelegate(type, typeof(IList<Type>), typeof(IEnumerable<MultiResultItem>), typeof(bool), typeof(INemoConfiguration));
            var multiResult = (IMultiResult)activator(types, source, cached, config);
            return multiResult;
        }

        public static IEnumerable<IEnumerable<object>> AsEnumerable(this IMultiResult source)
        {
            if (source == null) yield break;

            var types = source.AllTypes;
            for (var i = 0; i < types.Length; i++)
            {
                yield return _retrievers.GetOrAdd(types[i], CreateRetriever)(source);
            }
        }

        private static Func<IMultiResult, IEnumerable<object>> CreateRetriever(Type type)
        {
            var parameter = Expression.Parameter(typeof(IMultiResult), "source");
            Expression body = Expression.Call(parameter, RetrieveMethod.MakeGenericMethod(type));

            //  A sequence of reference types is covariant with a sequence of objects, whereas value types have to be boxed
            body = type.IsValueType
                ? Expression.Call(CastMethod.MakeGenericMethod(typeof(object)), body)
                : (Expression)Expression.Convert(body, typeof(IEnumerable<object>));

            return Expression.Lambda<Func<IMultiResult, IEnumerable<object>>>(body, parameter).Compile();
        }

        public static IEnumerable<T> Aggregate<T>(this IMultiResult source)
            where T : class
        {
            return source.Aggregate<T>(source.Configuration ?? ConfigurationFactory.Get<T>());
        }

        public static IEnumerable<T> Aggregate<T>(this IMultiResult source, INemoConfiguration config)
            where T : class
        {
            var types = source.AllTypes;
            var results = new List<object>[types.Length];
            var resultIndex = 0;

            foreach (var set in source.AsEnumerable())
            {
                if (resultIndex == results.Length) break;

                var items = new List<object>();
                foreach (var item in set)
                {
                    items.Add(item);
                }
                results[resultIndex++] = items;
            }

            while (resultIndex < results.Length)
            {
                results[resultIndex++] = EmptyResult;
            }

            var plans = GetPlans(types);
            var relations = BuildRelations(plans, results);

            var roots = new List<T>();

            for (var i = 0; i < types.Length; i++)
            {
                var identityMap = source.IsCached ? Identity.Get(types[i], config) : null;
                var propertyKey = source.IsCached ? ObjectFactory.GetPrimaryKeyPropertiesCached(types[i]) : null;
                var typeRelations = relations?[i];
                var count = 0;
                foreach (var item in results[i])
                {
                    string hash = null;
                    if (source.IsCached)
                    {
                        hash = item.ComputeHash(propertyKey, typeof(object));
                    }
                    var value = source.IsCached ? identityMap.GetEntityByHash<object>(hash) : null;
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

                    if (typeRelations != null)
                    {
                        LoadRelatedData(item, typeRelations, source.IsCached, config);
                    }

                    identityMap.WriteThrough(item, hash);
                }

                if (source.IsCached && i == 0 && count == roots.Count)
                {
                    return roots;
                }
            }

            return roots;
        }

        private static void LoadRelatedData(object value, List<RelationState> relations, bool cached, INemoConfiguration config)
        {
            var valueType = value.GetType();

            foreach (var state in relations)
            {
                if (state.IsEmpty) continue;

                var template = state.Template;

                if (state.ParentType != valueType)
                {
                    var accessorType = GetAccessorType(valueType);
                    state.ParentKeyGetters = GetGetters(accessorType, template.ParentKeyProperties);
                    //  Matching the property assignment of the previous implementation which used the runtime type
                    state.PropertySetter = Reflector.Property.GetSetter(valueType, template.PropertyName);
                    state.ParentType = valueType;
                }

                var items = state.Find(value);
                if (items == null || items.Count == 0)
                {
                    continue;
                }

                var propertyKey = cached ? ObjectFactory.GetPrimaryKeyPropertiesCached(template.ElementType) : null;
                var identityMap = cached ? Identity.Get(template.ElementType, config) : null;

                object propertyValue;
                if (template.IsSingle)
                {
                    propertyValue = cached ? identityMap.GetEntityByHash<object>(items[0].ComputeHash(propertyKey, typeof(object))) ?? items[0] : items[0];

                    state.SetForeignKeys(propertyValue, value);
                }
                else
                {
                    var list = template.IsListInterface
                        ? List.Create(template.ElementType, template.Distinct, template.Sorted)
                        : (IList)template.PropertyType.New();

                    foreach (var item in items)
                    {
                        var listItem = cached ? identityMap.GetEntityByHash<object>(item.ComputeHash(propertyKey, typeof(object))) ?? item : item;

                        state.SetForeignKeys(listItem, value);

                        list.Add(listItem);
                    }

                    propertyValue = list;
                }

                state.PropertySetter?.Invoke(value, propertyValue);
            }
        }

        /// <summary>
        /// Returns the relations to load for each result set, cached for the combination of the result set types.
        /// </summary>
        private static List<RelationPlan>[] GetPlans(Type[] types)
        {
            return _plans.GetOrAdd(new TypeArray(types), key =>
            {
                var allTypes = key.Types;

                var typeIndexes = new Dictionary<Type, int>();
                for (var i = 0; i < allTypes.Count; i++)
                {
                    if (!typeIndexes.ContainsKey(allTypes[i]))
                    {
                        typeIndexes[allTypes[i]] = i;
                    }
                }

                var plans = new List<RelationPlan>[allTypes.Count];

                for (var i = 0; i < allTypes.Count; i++)
                {
                    List<RelationPlan> typePlans = null;

                    foreach (var template in _templates.GetOrAdd(allTypes[i], CreateTemplates))
                    {
                        if (!typeIndexes.TryGetValue(template.ElementType, out var childIndex)) continue;

                        typePlans ??= new List<RelationPlan>();
                        typePlans.Add(new RelationPlan { Template = template, ChildIndex = childIndex });
                    }

                    plans[i] = typePlans;
                }

                return plans;
            });
        }

        private static List<RelationState>[] BuildRelations(List<RelationPlan>[] plans, List<object>[] results)
        {
            List<RelationState>[] relations = null;

            for (var i = 0; i < plans.Length; i++)
            {
                var typePlans = plans[i];
                if (typePlans == null) continue;

                var states = new List<RelationState>(typePlans.Count);
                foreach (var plan in typePlans)
                {
                    var state = new RelationState { Template = plan.Template };
                    state.Index(results[plan.ChildIndex]);
                    states.Add(state);
                }

                relations ??= new List<RelationState>[plans.Length];
                relations[i] = states;
            }

            return relations;
        }

        /// <summary>
        /// Describes how a related property of a type is populated, cached for the lifetime of the process.
        /// </summary>
        private static List<RelationTemplate> CreateTemplates(Type objectType)
        {
            var templates = new List<RelationTemplate>();

            var relations = _relations.GetOrAdd(objectType, t => InferRelations(t).ToList());
            if (relations.Count == 0) return templates;

            var relationsByProperty = new Dictionary<string, ObjectRelation>(StringComparer.Ordinal);
            foreach (var relation in relations)
            {
                if (relation?.From == null || relation.To == null) continue;

                var relationName = relation.Name != null && relation.Name.StartsWith("_", StringComparison.Ordinal)
                    ? relation.Name.Substring(1)
                    : relation.Name;

                if (string.IsNullOrEmpty(relationName) || relationsByProperty.ContainsKey(relationName)) continue;

                relationsByProperty[relationName] = relation;
            }

            foreach (var property in Reflector.GetPropertyMap(objectType))
            {
                if (!relationsByProperty.TryGetValue(property.Key.Name, out var relation)) continue;

                Type elementType;
                bool isSingle;
                if (property.Value.IsDataEntity || property.Value.IsObject)
                {
                    elementType = property.Key.PropertyType;
                    isSingle = true;
                }
                else if (property.Value.IsDataEntityList || property.Value.IsObjectList)
                {
                    elementType = property.Value.ElementType;
                    isSingle = false;
                }
                else
                {
                    continue;
                }

                if (elementType == null) continue;

                templates.Add(new RelationTemplate
                {
                    PropertyName = property.Key.Name,
                    PropertyType = property.Key.PropertyType,
                    ElementType = elementType,
                    IsSingle = isSingle,
                    IsListInterface = property.Value.IsListInterface,
                    Distinct = property.Value.Distinct,
                    Sorted = property.Value.Sorted,
                    ParentKeyProperties = GetPropertyNames(relation.From.Properties),
                    ChildKeyProperties = GetPropertyNames(relation.To.Properties),
                    ForeignKeyProperties = Reflector.GetPropertyNameMap(elementType).Values.Where(p => p.PropertyType == objectType).Select(p => p.PropertyName).ToArray()
                });
            }

            return templates;
        }

        private static string[] GetPropertyNames(IList<ReflectedProperty> properties)
        {
            var names = new string[properties.Count];
            for (var i = 0; i < properties.Count; i++)
            {
                names[i] = properties[i].PropertyName;
            }
            return names;
        }

        private static Type GetAccessorType(Type entityType)
        {
            var reflectedType = Reflector.GetReflectedType(entityType);
            if (entityType == typeof(object) && reflectedType.IsEmitted && reflectedType.InterfaceTypeName != null)
            {
                return reflectedType.InterfaceType;
            }

            return reflectedType.IsMarkerInterface ? reflectedType.UnderlyingType : entityType;
        }

        private static Reflector.Property.GenericGetter[] GetGetters(Type accessorType, string[] propertyNames)
        {
            var getters = new Reflector.Property.GenericGetter[propertyNames.Length];
            for (var i = 0; i < propertyNames.Length; i++)
            {
                getters[i] = Reflector.Property.GetGetter(accessorType, propertyNames[i]);
            }
            return getters;
        }

        private static Action<object, object>[] GetSetters(Type accessorType, string[] propertyNames)
        {
            var setters = new Action<object, object>[propertyNames.Length];
            for (var i = 0; i < propertyNames.Length; i++)
            {
                setters[i] = Reflector.Property.GetSetter(accessorType, propertyNames[i]);
            }
            return setters;
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
        }
        
        private class ObjectVertex
        {
            public Type Type { get; set; }
            public List<ReflectedProperty> Properties { get; set; }
        }

        /// <summary>
        /// Cached description of a related property of a type, independent of the query it takes part in.
        /// </summary>
        private sealed class RelationTemplate
        {
            public string PropertyName;
            public Type PropertyType;
            public Type ElementType;
            public bool IsSingle;
            public bool IsListInterface;
            public Attributes.DistinctAttribute Distinct;
            public Attributes.SortedAttribute Sorted;
            public string[] ParentKeyProperties;
            public string[] ChildKeyProperties;
            public string[] ForeignKeyProperties;
        }

        /// <summary>
        /// A relation template bound to the result set providing its children.
        /// </summary>
        private sealed class RelationPlan
        {
            public RelationTemplate Template;
            public int ChildIndex;
        }

        /// <summary>
        /// Per-aggregation state of a relation holding the children indexed by their key and the property accessors
        /// resolved for the types encountered.
        /// </summary>
        private sealed class RelationState
        {
            public RelationTemplate Template;
            public Type ParentType;
            public Reflector.Property.GenericGetter[] ParentKeyGetters;
            public Action<object, object> PropertySetter;

            private Dictionary<object, List<object>> _childrenByKey;
            private Dictionary<object[], List<object>> _childrenByCompositeKey;
            private Type _childType;
            private Action<object, object>[] _foreignKeySetters;

            public bool IsEmpty
            {
                get { return _childrenByKey == null && _childrenByCompositeKey == null; }
            }

            public void Index(List<object> children)
            {
                if (children.Count == 0) return;

                var keyProperties = Template.ChildKeyProperties;
                if (keyProperties.Length == 0 || keyProperties.Length != Template.ParentKeyProperties.Length) return;

                var composite = keyProperties.Length > 1;
                if (composite)
                {
                    _childrenByCompositeKey = new Dictionary<object[], List<object>>(ObjectArrayComparer.Instance);
                }
                else
                {
                    _childrenByKey = new Dictionary<object, List<object>>();
                }

                Type childType = null;
                Reflector.Property.GenericGetter[] getters = null;

                foreach (var child in children)
                {
                    var type = child.GetType();
                    if (type != childType)
                    {
                        childType = type;
                        getters = GetGetters(GetAccessorType(type), keyProperties);
                    }

                    if (composite)
                    {
                        var key = new object[keyProperties.Length];
                        for (var i = 0; i < key.Length; i++)
                        {
                            key[i] = getters[i](child);
                        }

                        if (!_childrenByCompositeKey.TryGetValue(key, out var items))
                        {
                            items = new List<object>();
                            _childrenByCompositeKey.Add(key, items);
                        }
                        items.Add(child);
                    }
                    else
                    {
                        var key = getters[0](child) ?? NullKey;
                        if (!_childrenByKey.TryGetValue(key, out var items))
                        {
                            items = new List<object>();
                            _childrenByKey.Add(key, items);
                        }
                        items.Add(child);
                    }
                }
            }

            public List<object> Find(object parent)
            {
                if (_childrenByKey != null)
                {
                    return _childrenByKey.TryGetValue(ParentKeyGetters[0](parent) ?? NullKey, out var items) ? items : null;
                }

                if (_childrenByCompositeKey == null) return null;

                var key = new object[ParentKeyGetters.Length];
                for (var i = 0; i < key.Length; i++)
                {
                    key[i] = ParentKeyGetters[i](parent);
                }

                return _childrenByCompositeKey.TryGetValue(key, out var matches) ? matches : null;
            }

            public void SetForeignKeys(object child, object parent)
            {
                var foreignKeys = Template.ForeignKeyProperties;
                if (foreignKeys.Length == 0) return;

                var type = child.GetType();
                if (type != _childType)
                {
                    _childType = type;
                    _foreignKeySetters = GetSetters(GetAccessorType(type), foreignKeys);
                }

                for (var i = 0; i < _foreignKeySetters.Length; i++)
                {
                    _foreignKeySetters[i]?.Invoke(child, parent);
                }
            }
        }

        private sealed class ObjectArrayComparer : IEqualityComparer<object[]>
        {
            internal static readonly ObjectArrayComparer Instance = new ObjectArrayComparer();

            public bool Equals(object[] x, object[] y)
            {
                if (ReferenceEquals(x, y))
                {
                    return true;
                }

                if (x == null || y == null || x.Length != y.Length)
                {
                    return false;
                }

                for (var i = 0; i < x.Length; i++)
                {
                    if (!object.Equals(x[i], y[i]))
                    {
                        return false;
                    }
                }

                return true;
            }

            public int GetHashCode(object[] obj)
            {
                if (obj == null)
                {
                    return 0;
                }

                unchecked
                {
                    var hash = 17;
                    for (var i = 0; i < obj.Length; i++)
                    {
                        hash = (hash * 31) + (obj[i]?.GetHashCode() ?? 0);
                    }
                    return hash;
                }
            }
        }
    }
}
