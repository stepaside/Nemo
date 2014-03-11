using Nemo.Attributes;
using Nemo.Cache.Providers;
using Nemo.Collections;
using Nemo.Collections.Extensions;
using Nemo.Configuration;
using Nemo.Configuration.Mapping;
using Nemo.Extensions;
using Nemo.Fn;
using Nemo.Reflection;
using Nemo.Security.Cryptography;
using Nemo.Serialization;
using Nemo.Utilities;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nemo.Cache
{
    public static class ObjectCache
    {
        private static ConcurrentDictionary<string, object> _cacheLocks = new ConcurrentDictionary<string, object>();

        private static ConcurrentDictionary<string, HashSet<string>> _queryVariants = new ConcurrentDictionary<string, HashSet<string>>();

        private static ConcurrentDictionary<string, ulong> _queryVersions = new ConcurrentDictionary<string, ulong>();

        #region Helper Methods

        internal static bool IsCacheable<T>()
            where T : class, IDataEntity
        {
            return GetCacheType<T>() != null;
        }

        internal static bool IsCacheable(Type objectType)
        {
            return GetCacheType(objectType) != null;
        }

        private static bool IsTrackable<T>()
            where T : class, IDataEntity
        {
             var attribute = Reflector.GetAttribute<T, CacheAttribute>();
             if (attribute != null)
             {
                 return attribute.TrackKeys;
             }
             return false;
        }

        private static bool IsTrackable(Type objectType)
        {
            var attribute = Reflector.GetAttribute<CacheAttribute>(objectType);
            if (attribute != null)
            {
                return attribute.TrackKeys;
            }
            return false;
        }

        internal static Type GetCacheType<T>()
            where T : class, IDataEntity
        {
            if (CacheScope.Current != null)
            {
                return CacheScope.Current.Type;
            }

            var attribute = Reflector.GetAttribute<T, CacheAttribute>();
            if (attribute != null)
            {
                return attribute.Type ?? ConfigurationFactory.Configuration.DefaultCacheProvider;
            }
            return null;
        }

        internal static Type GetCacheType(Type objectType)
        {
            if (CacheScope.Current != null)
            {
                return CacheScope.Current.Type;
            }

            if (objectType != null)
            {
                if (Reflector.IsEmitted(objectType))
                {
                    objectType = Reflector.ExtractInterface(objectType);
                }

                var attribute = Reflector.GetAttribute<CacheAttribute>(objectType, false);
                if (attribute != null)
                {
                    return attribute.Type ?? ConfigurationFactory.Configuration.DefaultCacheProvider;
                }
            }
            return null;
        }
        
        internal static CacheOptions GetCacheOptions(Type objectType)
        {
            if (objectType != null)
            {
                if (Reflector.IsEmitted(objectType))
                {
                    objectType = Reflector.ExtractInterface(objectType);
                }

                var attribute = Reflector.GetAttribute<CacheAttribute>(objectType, false);
                if (attribute != null)
                {
                    return attribute.Options ?? new CacheOptions { Namespace = objectType.Name };
                }
            }
            return null;
        }

        public static Tuple<string, ulong, HashSet<string>> GetQueryKey<T>(string operation, IList<Param> parameters, OperationReturnType returnType, CacheProvider cache)
            where T : class, IDataEntity
        {
            var objectType = typeof(T);
        
            var parametersForCaching = new SortedDictionary<string, object>();
            if (parameters != null)
            {
                for (int i = 0; i < parameters.Count; i++)
                {
                    var parameter = parameters[i];
                    parametersForCaching.Add(parameter.Name, parameter.Value);
                }
            }

            var cacheKey = new CacheKey(parametersForCaching, objectType, operation, returnType);
            string queryKey = cacheKey.Value;
            var signature = 0ul;
            var variants = new HashSet<string>();

            // Use invalidation strategy only if it is a cache enabled call
            if (cache != null)
            {
                if (ConfigurationFactory.Configuration.CacheInvalidationStrategy == CacheInvalidationStrategy.InvalidateByVersion)
                {
                    signature = _queryVersions.GetOrAdd(queryKey, k => (ulong)UnixDateTime.GetTicks());
                    queryKey = cache.ComputeKey(queryKey, signature);
                }
                else
                {
                    variants = GetAllVariants<T>(parameters).ToHashSet();
                }
            }

            return Tuple.Create(queryKey, signature, variants);
        }

        internal static bool CanBeBuffered<T>()
            where T : class, IDataEntity
        {
            if (ConfigurationFactory.Configuration.DefaultContextLevelCache == ContextLevelCacheType.None)
            {
                return false;
            }

            if (CacheScope.Current != null)
            {
                return CacheScope.Current.Buffered;
            }

            var cacheType = GetCacheType<T>();
            return cacheType == null || CacheFactory.GetProvider(cacheType).IsOutOfProcess;
        }

        internal static void GetCacheInfo<T>(out bool canBeBuffered, out Type cacheType)
            where T : class, IDataEntity
        {
            canBeBuffered = false;
            cacheType = null;

            var cacheScope = CacheScope.Current;
            if (cacheScope != null)
            {
                canBeBuffered = cacheScope.Buffered;
                cacheType = cacheScope.Type;
            }
            else
            {
                var attribute = Reflector.GetAttribute<T, CacheAttribute>();
                if (attribute != null)
                {
                    cacheType = attribute.Type;
                }

                if (ConfigurationFactory.Configuration.DefaultContextLevelCache == ContextLevelCacheType.None)
                {
                    canBeBuffered = false;
                }
                else
                {
                    canBeBuffered = cacheType == null || CacheFactory.GetProvider(cacheType).ToMaybe().Let(c => c.HasValue ? c.Value.IsOutOfProcess : false);
                }
            }
        }

        #endregion

        #region Query Lookup Methods

        internal static string[] LookupKeys(string queryKey, CacheProvider cache, bool stale = false)
        {
            if (!string.IsNullOrEmpty(queryKey))
            {
                object value;
                if (stale && cache is IStaleCacheProvider)
                {
                    value = ((IStaleCacheProvider)cache).GetStale(queryKey);
                }
                else
                {
                    value = cache.Get(queryKey);
                }

                if (value != null)
                {
                    if (value is CacheIndex)
                    {
                        return ((CacheIndex)value).ToIndex();
                    }
                    else if (value is CacheValue)
                    {
                        return new CacheIndex(queryKey, (CacheValue)value).ToIndex();
                    }
                    else
                    {
                        return value as string[];
                    }
                }
            }
            return null;
        }

        #endregion

        #region Item Lookup Methods

        public static CacheItem Lookup<T>(string key, CacheProvider cache, bool stale = false)
            where T : class, IDataEntity
        {
            object value;
            CacheItem item = null;
            if (cache != null)
            {
                if (cache.IsOutOfProcess)
                {
                    if (stale && cache is IStaleCacheProvider)
                    {
                        value = ((IStaleCacheProvider)cache).GetStale(key);
                    }
                    else
                    {
                        value = cache.Get(key);
                    }

                    if (value != null)
                    {
                        item = value is CacheDataObject ? (CacheDataObject)value : new CacheDataObject(key, (CacheValue)value);
                    }
                }
                else
                {
                    value = cache.Get(key);
                    if (value != null)
                    {
                        item = new CacheDataObject(key, (T)value);
                    }
                }
            }

            return item;
        }

        public static CacheDataObject[] Lookup<T>(IEnumerable<string> keys, CacheProvider cache, bool stale = false)
            where T : class, IDataEntity
        {
            CacheDataObject[] items = null;
            var keyCount = keys.Count();
            if (cache != null)
            {
                if (keyCount == 0)
                {
                    items = new CacheDataObject[] { };
                }
                else
                {
                    if (cache.IsOutOfProcess)
                    {
                        if (keyCount == 1)
                        {
                            var key = keys.First();
                            object value;
                            if (stale && cache is IStaleCacheProvider)
                            {
                                value = ((IStaleCacheProvider)cache).GetStale(key);
                            }
                            else
                            {
                                value = cache.Get(key);
                            }

                            if (value != null)
                            {
                                var item = value is CacheDataObject ? (CacheDataObject)value : new CacheDataObject(key, (CacheValue)value);
                                return new[] { item };
                            }
                        }
                        else
                        {
                            IDictionary<string, object> map;
                            if (stale && cache is IStaleCacheProvider)
                            {
                                map = ((IStaleCacheProvider)cache).GetStale(keys);
                            }
                            else
                            {
                                map = cache.Get(keys);
                            }

                            if (map != null)
                            {
                                // Need to enforce original order on multiple items returned from memcached using multi-get                            
                                items = map.Where(p => p.Value != null).Select(p => p.Value is CacheDataObject ? (CacheDataObject)p.Value : new CacheDataObject(p.Key, (CacheValue)p.Value)).Arrange(keys, i => i.Key).ToArray();
                            }
                        }
                    }
                    else
                    {
                        IDictionary<string, object> map;
                        if (stale && cache is IStaleCacheProvider)
                        {
                            map = ((IStaleCacheProvider)cache).GetStale(keys);
                        }
                        else
                        {
                            map = cache.Get(keys);
                        }

                        if (map != null)
                        {
                            items = map.Where(p => p.Value != null).Select(p => new CacheDataObject(p.Key, (T)p.Value)).ToArray();
                        }
                    }

                    if (items != null)
                    {
                        if (items.Any())
                        {
                            // Don't care if the counts are the same for the stale data; 
                            // however if none of the stale items are available we have to consider it as a cache miss
                            if (stale && cache is IStaleCacheProvider)
                            {
                                return items;
                            }
                            else if (items.Length == keyCount)
                            {
                                return items;
                            }
                        }
                        items = null;
                    }
                }
            }
            return items;
        }

        public static IEnumerable<T> Deserialize<T>(this IList<CacheDataObject> items)
            where T : class, IDataEntity
        {
            return items.Select(i => i.ToObject<T>());
        }

        #endregion

        #region Key Tracking Methods

        private static void TrackKeys<T>(string queryKey, IEnumerable<string> itemKeys, HashSet<string> variants)
            where T : class, IDataEntity
        {
            var cacheScope = CacheScope.Current;
            if (cacheScope != null)
            {
                cacheScope.Track(queryKey);
                foreach (var key in itemKeys)
                {
                    cacheScope.Track(key);
                }
            }
            else if (IsTrackable<T>())
            {
                if (ConfigurationFactory.Configuration.CacheInvalidationStrategy == CacheInvalidationStrategy.InvalidateByParameters)
                {
                    foreach (var variant in variants)
                    {
                        _queryVariants.AddOrUpdate(variant, k => new HashSet<string>(new[] { queryKey }), (k, s) => { s.Add(queryKey); return s; });
                    }
                }
            }
        }

        #region Query Variant Methods

        private static IEnumerable<string> GetAllVariants<T>(IList<Param> parameters)
        {
            return (parameters ?? new List<Param>()).PowerSet().Select(p => GetSingleVariant<T>(p));
        }

        private static string GetSingleVariant(Type type, IEnumerable<Param> parameters)
        {
            return new CacheKey(parameters.ToDictionary(_ => _.Name, _ => _.Value), type).Value;
        }

        private static string GetSingleVariant<T>(IEnumerable<Param> parameters)
        {
            return GetSingleVariant(typeof(T), parameters);
        }

        #endregion

        #endregion

        #region Add Methods

        internal static Tuple<bool, IEnumerable<T>, string[], bool> Add<T>(string queryKey, HashSet<string> queryVariants, IList<Param> parameters, Func<IEnumerable<T>> retrieveItems, bool forceRetrieve, CacheProvider cache)
            where T : class, IDataEntity
        {
            var result = false;
            var values = Enumerable.Empty<T>();
            string[] keys = null;
            var stale = false;

            if (cache != null)
            {
                if (cache.IsDistributed)
                {
                    if (cache is IStaleCacheProvider)
                    {
                        if (((DistributedCacheProvider)cache).TryAcquireLock(queryKey))
                        {
                            try
                            {
                                AddToCache<T>(queryKey, queryVariants, parameters, retrieveItems, forceRetrieve, cache, ref keys, ref values, ref result);
                            }
                            finally
                            {
                                ((DistributedCacheProvider)cache).ReleaseLock(queryKey);
                            }
                        }
                        else
                        {
                            keys = LookupKeys(queryKey, cache, true);
                            stale = true;
                        }
                    }
                    else
                    {
                        AddToCache<T>(queryKey, queryVariants, parameters, retrieveItems, forceRetrieve, cache, ref keys, ref values, ref result);
                    }
                }
                else
                {
                    // Threads are blocked if there is a contention on a key
                    var cacheLock = _cacheLocks.GetOrAdd(queryKey, k => new object());

                    lock (cacheLock)
                    {
                        AddToCache<T>(queryKey, queryVariants, parameters, retrieveItems, forceRetrieve, cache, ref keys, ref values, ref result);
                    }
                }
            }
            return Tuple.Create(result, values, keys, stale);
        }
        
        public static bool Add<T>(T item, CacheProvider cache = null)
            where T : class, IDataEntity
        {
            if (item != null)
            {
                DateTime expiresAt = DateTime.MinValue;
                if (cache != null)
                {
                    var key = new CacheKey(item).Value;
                    if (cache.IsOutOfProcess)
                    {
                        return cache.Add(key, new CacheDataObject(key, item).Value);
                    }
                    else
                    {
                        return cache.Add(key, item);
                    }
                }
            }
            return false;
        }

        private static void AddToCache<T>(string queryKey, 
                                    HashSet<string> queryVariants,
                                    IList<Param> parameters, 
                                    Func<IEnumerable<T>> retrieveItems, 
                                    bool forceRetrieve, 
                                    CacheProvider cache,
                                    ref string[] keys, 
                                    ref IEnumerable<T> values,
                                    ref bool result)
            where T : class, IDataEntity
        {
            keys = ObjectCache.LookupKeys(queryKey, cache);

            if (keys == null || forceRetrieve)
            {
                var items = retrieveItems();

                int count = 0;
                var keyMap = new Dictionary<string, object>();
                var valueList = new List<T>();

                foreach (var item in items)
                {
                    count++;
                    valueList.Add(item);
                    keyMap[new CacheKey(item).Value] = item;
                }

                // Cache key collision is not detected
                if (count == keyMap.Count)
                {
                    values = keyMap.Values.Cast<T>();

                    result = true;
                    if (cache.IsOutOfProcess)
                    {
                        var keyMapSerialized = keyMap.ToDictionary(kvp => kvp.Key, kvp => (object)new CacheDataObject(kvp.Key, (T)kvp.Value).Value);
                        result = cache.Set(keyMapSerialized);
                    }
                    else
                    {
                        result = cache.Set(keyMap);
                    }
                }

                if (result)
                {
                    // Store a query and corresponding keys
                    if (cache.IsOutOfProcess)
                    {
                        result = result && cache.Set(queryKey, new CacheIndex(queryKey, keyMap.Keys.ToArray()).Value);
                    }
                    else
                    {
                        result = result && cache.Set(queryKey, keyMap.Keys.ToArray());
                    }

                    TrackKeys<T>(queryKey, keyMap.Keys, queryVariants);
                }
                else
                {
                    values = valueList;
                }
            }
        }

        #endregion

        #region Remove Methods

        internal static bool Remove<T>(string queryKey, IList<Param> parameters, CacheProvider cache)
            where T : class, IDataEntity
        {
            var success = false;
            if (cache != null)
            {
                var strategy = ConfigurationFactory.Configuration.CacheInvalidationStrategy;

                if (strategy != CacheInvalidationStrategy.CacheProvider)
                {
                    success = Invalidate<T>(parameters);
                }
                else
                {
                    success = cache.Remove(queryKey);
                }

                if (success)
                {
                    ExecutionContext.Pop(queryKey);
                }
            }
            return success;
        }

        public static bool Remove<T>(T item, CacheProvider cache = null)
            where T : class, IDataEntity
        {
            var success = false;
            Type cacheType = null;
            if (item != null)
            {
                if (cache == null)
                {
                    cacheType = GetCacheType<T>();
                    if (cacheType != null)
                    {
                        cache = CacheFactory.GetProvider(cacheType, GetCacheOptions(typeof(T)));
                    }
                }

                if (cache != null)
                {
                    var key = new CacheKey(item).Value;
                    var strategy = ConfigurationFactory.Configuration.CacheInvalidationStrategy;

                    if (strategy != CacheInvalidationStrategy.CacheProvider)
                    {
                        success = Invalidate<T>(item);

                        // Purely for cleanup purposes
                        if (!cache.IsDistributed && strategy == CacheInvalidationStrategy.InvalidateByVersion)
                        {
                            success = cache.Remove(key) && success;
                        }
                    }
                    else
                    {
                        success = cache.Remove(key);
                    }

                    if (success)
                    {
                        ExecutionContext.Remove(key);
                        RemoveDependencies<T>(item);
                    }
                }
            }
            return success;
        }
        
        #endregion

        #region Modify Methods

        public static bool Modify<T>(T item, CacheProvider cache = null)
            where T : class, IDataEntity
        {
            if (item != null)
            {
                if (cache != null)
                {
                    var key = new CacheKey(item).Value;
                    if (cache.IsOutOfProcess)
                    {
                        return cache.Set(key, new CacheDataObject(key, item).Value);
                    }
                    else
                    {
                        return cache.Set(key, item);
                    }
                }
            }
            return false;
        }

        #endregion

        #region Invalidate Methods

        public static bool Invalidate(Type objectType, IList<Param> parameters)
        {
            return InvalidateImplementation(objectType, parameters);
        }

        public static bool Invalidate<T>(IList<Param> parameters)
            where T : class, IDataEntity
        {
            return InvalidateImplementation<T>(parameters);
        }

        public static bool Invalidate<T>(T entity)
            where T : class, IDataEntity
        {
            var nameMap = Reflector.PropertyCache<T>.NameMap;
            var parameters = entity.GetPrimaryKey<T>(true).Select(pk => new Param { Name = nameMap[pk.Key].ParameterName ?? pk.Key, Value = pk.Value }).ToList();
            return InvalidateImplementation<T>(parameters);
        }

        private static bool InvalidateImplementation(Type objectType, IList<Param> parameters)
        {
            var strategy = ConfigurationFactory.Configuration.CacheInvalidationStrategy;
            var variant = GetSingleVariant(objectType, parameters);
            if (strategy == CacheInvalidationStrategy.InvalidateByParameters)
            {
                try
                {
                    HashSet<string> queries;
                    return _queryVariants.TryRemove(variant, out queries);
                }
                finally
                {
                    _publishQueryInvalidationEvent(null, new PublishQueryInvalidationEventArgs { Variant = variant });
                }
            }
            else if (strategy == CacheInvalidationStrategy.InvalidateByVersion)
            {
                HashSet<string> queries;
                if (_queryVariants.TryRemove(variant, out queries))
                {
                    foreach (var query in queries)
                    {
                        var version = _queryVersions.AddOrUpdate(query, q => (ulong)UnixDateTime.GetTicks(), (q, v) => v + 1ul);
                        _publishQueryInvalidationEvent(null, new PublishQueryInvalidationEventArgs { Key = query, Version = version });
                    }
                    return true;
                }
                else
                {
                    return false;
                }
            }
            else
            {
                return false;
            }
        }

        private static bool InvalidateImplementation<T>(IList<Param> parameters)
            where T : class, IDataEntity
        {
            return InvalidateImplementation(typeof(T), parameters);
        }

        #endregion

        #region Dependency Invalidation Methods

        private static IList<QueryDependency> GetQueryDependencies<T>()
           where T : class, IDataEntity
        {
            if (CacheScope.Current != null)
            {
                return CacheScope.Current.Dependencies;
            }

            IList<QueryDependency> dependencies = new List<QueryDependency>();

            var map = MappingFactory.GetEntityMap<T>();

            if (map != null)
            {
                dependencies = map.Cache.QueryDependencies;
            }

            if (dependencies.Count == 0)
            {
                var attrList = Reflector.GetAttributeList<T, QueryDependencyAttribute>();
                for (int i = 0; i < attrList.Count; i++)
                {
                    var attr = attrList[i];
                    dependencies.Add(new QueryDependency(attr.Properties));
                }
            }

            return dependencies;
        }

        internal static void RemoveDependencies<T>(T item)
            where T : class, IDataEntity
        {
            var properties = Reflector.PropertyCache<T>.NameMap;
            var cacheType = GetCacheType<T>();
            var cache = CacheFactory.GetProvider(cacheType);

            var strategy = ConfigurationFactory.Configuration.CacheInvalidationStrategy;
            var queryInvalidation = strategy == CacheInvalidationStrategy.InvalidateByParameters || strategy == CacheInvalidationStrategy.InvalidateByVersion;

            var dependencies = GetQueryDependencies<T>();
            if (dependencies == null || dependencies.Count == 0)
            {
                var validProperties = properties.Values.Where(p => (p.IsPrimaryKey || p.IsCacheKey || p.IsCacheParameter)
                                                    && (p.IsSimpleType || p.IsSimpleList)
                                                    && !p.IsAutoGenerated);
                dependencies = validProperties.Select(p => new QueryDependency(p.PropertyName)).ToList();
            }

            if (cache != null)
            {
                foreach (var dependency in dependencies)
                {
                    if (queryInvalidation)
                    {
                        Invalidate(typeof(T), dependency.GetParameters<T>(item));
                    }
                    else
                    {
                        var variant = GetSingleVariant<T>(dependency.GetParameters<T>(item));
                        HashSet<string> queries;
                        if (_queryVariants.TryRemove(variant, out queries))
                        {
                            foreach (var query in queries)
                            {
                                cache.Remove(query);
                            }
                        }
                    }
                }
            }
        }

        #endregion

        #region Signalling Methods

        private static EventHandler<PublishQueryInvalidationEventArgs> _publishQueryInvalidationEvent = delegate { };

        public static event EventHandler<PublishQueryInvalidationEventArgs> PublishInvalidation
        {
            add
            {
                _publishQueryInvalidationEvent += value.MakeWeak(eh => _publishQueryInvalidationEvent -= eh);
            }
            remove 
            {
            }
        }

        #endregion
    }
}
