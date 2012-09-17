using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Nemo.Attributes;
using Nemo.Collections.Extensions;
using Nemo.Extensions;
using Nemo.Reflection;
using Nemo.Serialization;

namespace Nemo.Caching
{
    public static class ObjectCache
    {
        private static ConcurrentDictionary<string, object> _cacheLocks = new ConcurrentDictionary<string, object>();
        private static Lazy<CacheProvider> _trackingCache = new Lazy<CacheProvider>(() => CacheFactory.GetProvider(CacheType.Redis), true);

        #region Helper Methods

        internal static bool IsCacheable<T>()
            where T : class,IBusinessObject
        {
            return GetCacheType<T>() != CacheType.None;
        }

        internal static bool IsCacheable(Type objectType)
        {
            return GetCacheType(objectType) != CacheType.None;
        }

        internal static CacheType GetCacheType<T>()
            where T : class, IBusinessObject
        {
            if (CacheScope.Current != null)
            {
                return CacheScope.Current.Type;
            }

            var attribute = Reflector.GetAttribute<T, CacheAttribute>();
            if (attribute != null)
            {
                return attribute.Type;
            }
            return CacheType.None;
        }

        internal static CacheType GetCacheType(Type objectType)
        {
            if (CacheScope.Current != null)
            {
                return CacheScope.Current.Type;
            }

            if (objectType != null)
            {
                if (!Reflector.IsInterface(objectType))
                {
                    objectType = Reflector.ExtractInterface(objectType);
                }

                var attribute = Reflector.GetAttribute<CacheAttribute>(objectType, false);
                if (attribute != null)
                {
                    return attribute.Type;
                }
            }
            return CacheType.None;
        }

        internal static IList<CacheLink> GetCacheLinks<T>()
           where T : class, IBusinessObject
        {
            if (CacheScope.Current != null)
            {
                return CacheScope.Current.Links;
            }

            var links = new List<CacheLink>();
            var attrList = Reflector.GetAttributeList<T, CacheLinkAttribute>();
            for (int i = 0; i < attrList.Count; i++ )
            {
                var attr = attrList[i];
                links.Add(new CacheLink { DependentType = attr.DependentType, DependentParameter = attr.DependentParameter, ValueProperty = attr.ValueProperty });
            }
            return links;
        }

        internal static HashAlgorithmName GetHashAlgorithm(Type objectType)
        {
            if (CacheScope.Current != null)
            {
                return CacheScope.Current.HashAlgorithm;
            }

            if (objectType != null)
            {
                if (!Reflector.IsInterface(objectType))
                {
                    objectType = Reflector.ExtractInterface(objectType);
                }

                var attribute = Reflector.GetAttribute<CacheAttribute>(objectType, false);
                if (attribute != null && attribute.Options != null && attribute.Options.HashAlgorithm.HasValue)
                {
                    return attribute.Options.HashAlgorithm.Value;
                }
            }
            return HashAlgorithmName.Default;
        }

        private static CacheOptions GetCacheOptions(Type objectType)
        {
             if (objectType != null)
            {
                if (!Reflector.IsInterface(objectType))
                {
                    objectType = Reflector.ExtractInterface(objectType);
                }

                var attribute = Reflector.GetAttribute<CacheAttribute>(objectType, false);
                if (attribute != null )
                {
                    return attribute.Options;
                }
            }
            return null;
        }

        internal static List<CacheKey> ComputeKeys<T>(this IEnumerable<T> items)
           where T : class, IBusinessObject
        {
            return items.Select(i => new CacheKey(i)).ToList();
        }

        public static string GetCacheKey<T>(IList<Param> parameters)
            where T : class, IBusinessObject
        {
            return GetCacheKey<T>(null, parameters, OperationReturnType.Guess);
        }

        public static string GetCacheKey<T>(IList<Param> parameters, OperationReturnType returnType)
            where T : class, IBusinessObject
        {
            return GetCacheKey<T>(ObjectFactory.OPERATION_RETRIEVE, parameters, returnType);
        }

        public static string GetCacheKey<T>(string operation, IList<Param> parameters, OperationReturnType returnType)
            where T : class, IBusinessObject
        {
            return GetCacheKey(typeof(T), operation, parameters, returnType);
        }

        public static string GetCacheKey(Type objectType, string operation, IList<Param> parameters, OperationReturnType returnType)
        {
            var parametersForCaching = new SortedDictionary<string, object>();
            if (parameters != null)
            {
                for (int i = 0; i < parameters.Count; i++)
                {
                    var parameter = parameters[i];
                    parametersForCaching.Add(parameter.Name, parameter.Value);
                }
            }
            var cacheKey = new CacheKey(parametersForCaching, objectType, operation, returnType, GetHashAlgorithm(objectType));
            return cacheKey.Value;
        }

        internal static bool CanBeBuffered<T>()
            where T : class, IBusinessObject
        {
            if (ObjectFactory.Configuration.ContextLevelCache == ContextLevelCacheType.None)
            {
                return false;
            }

            if (CacheScope.Current != null)
            {
                return CacheScope.Current.Buffered;
            }

            var cacheType = GetCacheType<T>();
            return cacheType == CacheType.None || CacheFactory.GetProvider(cacheType).IsOutOfProcess;
        }

        #endregion

        #region Query Lookup Methods

        internal static List<string> LookupKeys(Type objectType, string queryKey, bool allowStale = false)
        {
            if (!string.IsNullOrEmpty(queryKey))
            {
                var cacheType = GetCacheType(objectType);
                if (cacheType != CacheType.None)
                {
                    var cache = CacheFactory.GetProvider(cacheType);
                    if (cache.IsDistributed && allowStale)
                    {
                        return ((IDistributedCacheProvider)cache).RetrieveStale(queryKey) as List<string>;
                    }
                    else
                    {
                        return cache.Retrieve(queryKey) as List<string>;
                    }
                }
            }
            return null;
        }

        internal static List<string> LookupKeys<T>(string operation, IList<Param> parameters, OperationReturnType returnType, bool allowStale = false)
            where T : class, IBusinessObject
        {
            var queryKey = ObjectCache.GetCacheKey<T>(operation, parameters, returnType);
            return ObjectCache.LookupKeys(typeof(T), queryKey, allowStale);
        }
        
        #endregion

        #region Item Lookup Methods

        internal static T Lookup<T>(CacheKey key, bool allowStale = false)
            where T : class, IBusinessObject
        {
            return Lookup<T>(key.Value, allowStale);
        }

        public static T Lookup<T>(string key, bool allowStale = false)
            where T : class, IBusinessObject
        {
            var cacheType = GetCacheType(typeof(T));
            CacheItem item = null;
            if (cacheType != CacheType.None)
            {
                var cache = CacheFactory.GetProvider(cacheType);
                if (cache.IsDistributed && allowStale)
                {
                    item = (CacheItem)((IDistributedCacheProvider)cache).RetrieveStale(key);
                }
                else
                {
                    item = (CacheItem)cache.Retrieve(key);
                }
            }

            if (item != null)
            {
                return item.GetDataObject<T>();
            }
            return default(T);
        }

        internal static IList<CacheItem> Lookup<T>(IEnumerable<CacheKey> keys, bool allowStale = false)
            where T : class, IBusinessObject
        {
            return Lookup<T>(keys.Select(k => k.Value), allowStale);
        }

        public static IList<CacheItem> Lookup<T>(IEnumerable<string> keys, bool allowStale = false)
            where T : class, IBusinessObject
        {
            var cacheType = GetCacheType(typeof(T));
            var keyCount = keys.Count();
            if (cacheType != CacheType.None)
            {
                var cache = CacheFactory.GetProvider(cacheType, GetCacheOptions(typeof(T)));
                if (cache.IsOutOfProcess)
                {
                    if (keyCount == 1)
                    {
                        object item;
                        if (cache.IsDistributed && allowStale)
                        {
                            item = ((IDistributedCacheProvider)cache).RetrieveStale(keys.First());
                        }
                        else
                        {
                            item = cache.Retrieve(keys.First());
                        }
                        
                        if (item != null)
                        {
                            return new List<CacheItem> { (CacheItem)item };
                        }
                    }
                    else
                    {
                        IDictionary<string, object> map;
                        if (cache.IsDistributed && allowStale)
                        {
                            map = ((IDistributedCacheProvider)cache).RetrieveStale(keys);
                        }
                        else
                        {
                            map = cache.Retrieve(keys);
                        }
                    
                        if (map != null && map.Count == keyCount)
                        {
                            // Enforce original order on multiple items returned from memcached using multi-get
                            return map.Values.Cast<CacheItem>().Arrange(keys, i => i.GetKey<T>()).ToList();
                        }
                    }
                }
                else
                {
                    IDictionary<string, object> map;
                    if (cache.IsDistributed && allowStale)
                    {
                        map = ((IDistributedCacheProvider)cache).RetrieveStale(keys);
                    }
                    else
                    {
                        map = cache.Retrieve(keys);
                    }

                    if (map != null && map.Count == keyCount)
                    {
                        return map.Values.Where(i => i != null).Cast<CacheItem>().ToList();
                    }
                }
            }
            return null;
        }

        public static IEnumerable<T> Deserialize<T>(this IList<CacheItem> items)
            where T : class, IBusinessObject
        {
            return items.Select(i => i.GetDataObject<T>());
        }

        #endregion

        #region Key Lookup Methods

        public static IEnumerable<string> LookupQueriesByParameterName(Type type, IEnumerable<string> parameters)
        {
            var cacheType = GetCacheType(type);
            if (cacheType != CacheType.None)
            {
                var typeKey = GetTypeKey(type, false, false);
                var keys = parameters.Select(p => string.Concat(typeKey, "::", p.ToUpper()));

                var cachedQueries = _trackingCache.Value.Retrieve(keys);
                var result = cachedQueries
                    .Select(q => q.Value as Dictionary<string, HashSet<string>>)
                    .WhereNotNull()
                    .SelectMany(q => q.Values)
                    .SelectMany(v => v);
                return result;
            }
            return Enumerable.Empty<string>();
        }

        public static IEnumerable<string> LookupQueriesByParameter(Type type, IList<Param> parameters)
        {
            var cacheType = GetCacheType(type);
            if (cacheType != CacheType.None)
            {
                var typeKey = GetTypeKey(type, false, false);
                var parameterSet = parameters.GroupBy(p => p.Name).ToDictionary(g => string.Concat(typeKey, "::", g.Key.ToUpper()), g => g.First(), StringComparer.OrdinalIgnoreCase);

                var cachedQueriesResult = _trackingCache.Value.Retrieve(parameterSet.Keys).Cast<KeyValuePair<string, object>>();
                if (cachedQueriesResult != null)
                {
                    var cachedQueries = cachedQueriesResult.ToDictionary(i => i.Key, i => (Dictionary<string, HashSet<string>>)i.Value, StringComparer.OrdinalIgnoreCase);
                    var allQueries = new HashSet<string>();
                    foreach (var parameterKey in parameterSet.Keys)
                    {
                        Dictionary<string, HashSet<string>> valueSet;
                        if (cachedQueries.TryGetValue(parameterKey, out valueSet))
                        {
                            var valueKey = parameterSet[parameterKey].Value.SafeCast<string>();
                            HashSet<string> queries;
                            if (valueSet.TryGetValue(valueKey, out queries))
                            {
                                if (allQueries.Count == 0)
                                {
                                    allQueries = queries;
                                }
                                else
                                {
                                    allQueries.IntersectWith(queries);
                                }
                            }
                        }
                    }
                    return allQueries;
                }
            }
            return Enumerable.Empty<string>();
        }

        public static IEnumerable<string> LookupQueries(Type type)
        {
            return LookupByType(type, true);
        }

        public static IEnumerable<string> LookupItemsByType(Type type)
        {
            return LookupByType(type, false);
        }

        private static IEnumerable<string> LookupByType(Type type, bool queriesOnly)
        {
            var cacheType = GetCacheType(type);
            var typeKey = GetTypeKey(type, queriesOnly, false);
            if (cacheType != CacheType.None)
            {
                var typeKeySet = _trackingCache.Value.Retrieve(typeKey) as HashSet<string>;
                return typeKeySet;
            }
            return Enumerable.Empty<string>();
        }

        #endregion

        #region Add Methods

        private static void TrackKeys<T>(string queryKey, IEnumerable<string> itemKeys, IList<Param> parameters)
            where T : class, IBusinessObject
        {
            if (CacheScope.Current != null)
            {
                CacheScope.Current.Track(queryKey);
                foreach (var key in itemKeys)
                {
                    CacheScope.Current.Track(key);
                }
            }
            else
            {
                var typeKey = GetTypeKey(typeof(T), false, false);
                var typeQueryKey = GetTypeKey(typeof(T), true, false);

                var cache = _trackingCache.Value;
                // Keys associated with the given type
                var typeKeySet = cache.Retrieve(typeKey) as HashSet<string>;
                if (typeKeySet == null)
                {
                    typeKeySet = new HashSet<string>(itemKeys);
                }
                else
                {
                    typeKeySet.UnionWith(itemKeys);
                }
                cache.Save(typeKey, typeKeySet);

                // Query keys associated with the given type
                var typeQueryKeySet = cache.Retrieve(typeQueryKey) as HashSet<string>;
                if (typeQueryKeySet == null)
                {
                    typeQueryKeySet = new HashSet<string>();
                }
                typeQueryKeySet.Add(queryKey);
                cache.Save(typeQueryKey, typeQueryKeySet);

                // Queries associated with the given type and parameters
                if (parameters != null)
                {
                    var parameterSet = parameters.ToDictionary(p => string.Concat(typeKey, "::", p.Name.ToUpper()), p => p, StringComparer.OrdinalIgnoreCase);
                    var cachedQueriesResult = cache.Retrieve(parameterSet.Keys);
                    var cachedQueries = cachedQueriesResult != null
                                        ? cachedQueriesResult.ToDictionary(i => i.Key, i => (Dictionary<string, HashSet<string>>)i.Value, StringComparer.OrdinalIgnoreCase)
                                        : new Dictionary<string, Dictionary<string, HashSet<string>>>(StringComparer.OrdinalIgnoreCase);
                    foreach (var parameterKey in parameterSet.Keys)
                    {
                        var valueKey = parameterSet[parameterKey].Value.SafeCast<string>() ?? string.Empty;
                        Dictionary<string, HashSet<string>> valueSet;
                        if (!cachedQueries.TryGetValue(parameterKey, out valueSet))
                        {
                            valueSet = new Dictionary<string, HashSet<string>>();
                        }

                        HashSet<string> queries;
                        if (!valueSet.TryGetValue(valueKey, out queries))
                        {
                            queries = new HashSet<string>();
                            valueSet.Add(valueKey, queries);
                        }
                        queries.Add(queryKey);
                        cache.Save(parameterKey, valueSet);
                    }
                }
            }
        }

        internal static Tuple<bool, IEnumerable<T>, List<string>> Add<T>(string queryKey, IList<Param> parameters, Func<IEnumerable<T>> retrieveItems, bool forceRetrieve)
            where T : class, IBusinessObject
        {
            var result = false;
            var values = Enumerable.Empty<T>();
            List<string> keys = null;
            var cacheType = GetCacheType(typeof(T));

            if (cacheType != CacheType.None)
            {
                var cache = CacheFactory.GetProvider(cacheType, GetCacheOptions(typeof(T)));

                Action addToCache = () =>
                {
                    keys = ObjectCache.LookupKeys(typeof(T), queryKey);

                    if (keys == null || forceRetrieve)
                    {
                        var items = retrieveItems();

                        int count = 0;
                        var keyMap = new Dictionary<string, T>();
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
                            values = keyMap.Values;

                            result = true;
                            foreach (var key in keyMap.Keys)
                            {
                                CacheItem cacheItem;

                                if (cache.IsOutOfProcess)
                                {
                                    cacheItem = new CacheItem(key);
                                    cacheItem.Data = keyMap[key].Serialize();
                                }
                                else
                                {
                                    cacheItem = new CacheItem(keyMap[key]);
                                }
                                result = result && cache.Save(key, cacheItem);
                            }
                        }

                        if (result)
                        {
                            // Store a query and corresponding keys
                            result = result && cache.Save(queryKey, keyMap.Keys.ToList());

                            TrackKeys<T>(queryKey, keyMap.Keys, parameters);
                        }
                        else
                        {
                            values = valueList;
                        }
                    }
                };

                if (cache.IsDistributed)
                {
                    var cacheContentMitigation = ObjectFactory.Configuration.CacheContentionMitigation;
                    if (cacheContentMitigation == CacheContentionMitigationType.None || ((IDistributedCacheProvider)cache).TryAcquireLock(queryKey))
                    {
                        try
                        {
                            addToCache();
                        }
                        finally
                        {
                            if (cacheContentMitigation != CacheContentionMitigationType.None)
                            {
                                ((IDistributedCacheProvider)cache).ReleaseLock(queryKey);
                            }
                        }
                    }
                    else if (cacheContentMitigation == CacheContentionMitigationType.UseStaleCache)
                    {
                        keys = ((IDistributedCacheProvider)cache).RetrieveStale(queryKey) as List<string>;
                    }
                    else if (cacheContentMitigation == CacheContentionMitigationType.DistributedLocking)
                    {
                        keys = ((IDistributedCacheProvider)cache).WaitForItems(queryKey) as List<string>;
                    }
                }
                else
                {
                    // Threads are blocked if there is a contention on a key
                    var cacheLock = _cacheLocks.GetOrAdd(queryKey, k => new object());

                    lock (cacheLock)
                    {
                        addToCache();
                    }
                }
            }
            return Tuple.Create(result, values, keys);
        }

        internal static Tuple<bool, IEnumerable<T>, List<string>> AddOperation<T>(string operation, IList<Param> parameters, OperationReturnType returnType, Func<IEnumerable<T>> retrieveItems, bool forceRetrieve)
            where T : class, IBusinessObject
        {
            var queryKey = ObjectCache.GetCacheKey<T>(operation, parameters, returnType);
            return Add<T>(queryKey, parameters, retrieveItems, forceRetrieve);
        }

        internal static Tuple<bool, IEnumerable<T>, List<string>> AddOperation<T>(IList<Param> parameters, OperationReturnType returnType, Func<IEnumerable<T>> retrieveItems, bool forceRetrieve)
           where T : class, IBusinessObject
        {
            var queryKey = ObjectCache.GetCacheKey<T>(parameters, returnType);
            return Add<T>(queryKey, parameters, retrieveItems, forceRetrieve);
        }

        internal static bool Add<T>(T item)
            where T : class, IBusinessObject
        {
            if (item != null)
            {
                DateTime expiresAt = DateTime.MinValue;
                var cacheType = GetCacheType(typeof(T));
                var key = new CacheKey(item).Value;
                if (cacheType != CacheType.None)
                {
                    var cache = CacheFactory.GetProvider(cacheType, GetCacheOptions(typeof(T)));

                    if (cache.IsOutOfProcess)
                    {
                        return cache.AddNew(key, new CacheItem(key) { Data = item.Serialize() });
                    }
                    else
                    {
                        return cache.AddNew(key, new CacheItem(item));
                    }
                }
            }
            return false;
        }

        #endregion

        #region Remove Methods

        internal static bool Remove(Type objectType, string queryKey)
        {
            var cacheType = GetCacheType(objectType);
            if (cacheType != CacheType.None)
            {
                var cache = CacheFactory.GetProvider(cacheType, GetCacheOptions(objectType));
                var success = cache.Clear(queryKey);
                if (success)
                {
                    ExecutionContext.Remove(queryKey);
                }
                return success;
            }
            return false;
        }

        internal static bool Remove<T>(string queryKey)
            where T : class, IBusinessObject
        {
            return Remove(typeof(T), queryKey);
        }

        internal static bool Remove<T>(T item)
            where T : class, IBusinessObject
        {
            var success = false;
            if (item != null)
            {
                var cacheType = GetCacheType<T>();
                var key = new CacheKey(item).Value;
                if (cacheType != CacheType.None)
                {
                    var cache = CacheFactory.GetProvider(cacheType, GetCacheOptions(typeof(T)));
                    success = cache.Clear(key);
                    if (success)
                    {
                        RemoveLinks<T>(item);
                    }
                }
            }
            return success;
        }

        internal static void RemoveLinks<T>(T item)
            where T : class, IBusinessObject
        {
            // Clear cache dependencies if there are any
            var dependencies = GetCacheLinks<T>();
            if (dependencies != null && dependencies.Count > 0)
            {
                var validDependecies = dependencies.Where(d => d.DependentType != null && d.DependentParameter.NullIfEmpty() != null && d.ValueProperty.NullIfEmpty() != null);
                var parameterValuesByType = validDependecies.GroupBy(d => d.DependentType).ToDictionary(g => g.Key, g => g.Select(d => new Param { Name = d.DependentParameter, Value = item.Property(d.ValueProperty) }).ToList());

                parameterValuesByType.AsParallel().ForAll(p => 
                {
                    var targetCacheType = GetCacheType(p.Key);
                    if (targetCacheType != CacheType.None)
                    {
                        var keys = LookupQueriesByParameter(p.Key, p.Value);
                        var targetCache = CacheFactory.GetProvider(targetCacheType);
                        foreach (var targetKey in keys)
                        {
                            targetCache.Remove(targetKey);
                        }
                    }
                });
            }
        }

        internal static bool Clear(Type type)
        {
            return ClearByType(type, false);
        }

        internal static bool ClearQueries(Type type)
        {
            return ClearByType(type, true);
        }

        internal static bool ClearQueries(Type type, IList<Param> parameters)
        {
            var cacheType = GetCacheType(type);
            if (cacheType != CacheType.None)
            {
                var cache = CacheFactory.GetProvider(cacheType, GetCacheOptions(type));
                var queries = LookupQueriesByParameter(type, parameters);

                queries.Run(queryKey =>
                {
                    cache.Clear(queryKey);
                    ExecutionContext.Remove(queryKey);
                });

                return true;
            }
            return false;
        }

        internal static bool ClearQueries(Type type, IEnumerable<string> parameters)
        {
            var cacheType = GetCacheType(type);
            if (cacheType != CacheType.None)
            {
                var cache = CacheFactory.GetProvider(cacheType, GetCacheOptions(type));
                var queries = LookupQueriesByParameterName(type, parameters);
                queries.Run(k =>
                {
                    cache.Clear(k);
                    ExecutionContext.Remove(k);
                });
                return true;
            }
            return false;
        }

        internal static bool ClearByType(Type type, bool queriesOnly)
        {
            var cacheType = GetCacheType(type);
            if (cacheType != CacheType.None)
            {
                var cache = CacheFactory.GetProvider(cacheType, GetCacheOptions(type));
                var result = true;
                
                var keys = LookupByType(type, queriesOnly);
                if (keys.Any())
                {
                    foreach (var key in keys)
                    {
                        result = cache.Clear(key) && result;
                        ExecutionContext.Remove(key);
                    }
                }
                return result;
            }
            return false;
        }

        private static string GetTypeKey(Type type, bool queries, bool parameters)
        {
            var typeKey = type.FullName;
            if (queries)
            {
                typeKey += "::Queries";
                if (parameters)
                {
                    typeKey += "::Parameters";
                }
            }
            return typeKey;
        }

        #endregion

        #region Modify Methods

        internal static bool Modify<T>(T item)
            where T : class, IBusinessObject
        {
            if (item != null)
            {
                var cacheType = GetCacheType(typeof(T));
                var key = new CacheKey(item).Value;
                if (cacheType != CacheType.None)
                {
                    var cache = CacheFactory.GetProvider(cacheType, GetCacheOptions(typeof(T)));

                    if (cache.IsOutOfProcess)
                    {
                        return cache.Save(key, new CacheItem(key) { Data = item.Serialize() });
                    }
                    else 
                    {
                        return cache.Save(key, new CacheItem(item));
                    }
                }
            }
            return false;
        }

        #endregion
    }
}
