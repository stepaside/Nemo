using Nemo.Attributes;
using Nemo.Caching.Providers;
using Nemo.Collections;
using Nemo.Collections.Extensions;
using Nemo.Configuration;
using Nemo.Extensions;
using Nemo.Fn;
using Nemo.Reflection;
using Nemo.Serialization;
using Nemo.Utilities;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nemo.Caching
{
    public static class ObjectCache
    {
        private static ConcurrentDictionary<string, object> _cacheLocks = new ConcurrentDictionary<string, object>();
        private static Lazy<CacheProvider> _trackingCache = new Lazy<CacheProvider>(() => CacheFactory.GetProvider(ConfigurationFactory.Configuration.TrackingCacheProvider), true);

        #region Helper Methods

        internal static bool IsCacheable<T>()
            where T : class, IBusinessObject
        {
            return GetCacheType<T>() != null;
        }

        internal static bool IsCacheable(Type objectType)
        {
            return GetCacheType(objectType) != null;
        }

        private static bool IsTrackable<T>()
            where T : class, IBusinessObject
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
            where T : class, IBusinessObject
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

        private static IList<CacheDependency> GetCacheDependencies<T>()
           where T : class, IBusinessObject
        {
            if (CacheScope.Current != null)
            {
                return CacheScope.Current.Dependencies;
            }

            var dependencies = new List<CacheDependency>();
            var attrList = Reflector.GetAttributeList<T, CacheDependencyAttribute>();
            for (int i = 0; i < attrList.Count; i++ )
            {
                var attr = attrList[i];
                if (IsTrackable(attr.DependentType))
                {
                    dependencies.Add(new CacheDependency { DependentType = attr.DependentType, DependentProperty = attr.DependentProperty, ValueProperty = attr.ValueProperty });
                }
            }
            return dependencies;
        }

        private static HashAlgorithmName GetHashAlgorithm(Type objectType)
        {
            if (CacheScope.Current != null)
            {
                return CacheScope.Current.HashAlgorithm;
            }

            var options = GetCacheOptions(objectType);
            if (options != null && options.HashAlgorithm.HasValue)
            {
                return options.HashAlgorithm.Value;
            }
            return HashAlgorithmName.Default;
        }

        private static CacheOptions GetCacheOptions(Type objectType)
        {
             if (objectType != null)
            {
                if (Reflector.IsEmitted(objectType))
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
            where T : class, IBusinessObject
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

        internal static string[] LookupKeys(Type objectType, string queryKey)
        {
            if (!string.IsNullOrEmpty(queryKey))
            {
                var cacheType = GetCacheType(objectType);
                if (cacheType != null)
                {
                    var cache = CacheFactory.GetProvider(cacheType);
                    return LookupKeys(cache, queryKey, false);
                }
            }
            return null;
        }

        internal static string[] LookupKeys<T>(string operation, IList<Param> parameters, OperationReturnType returnType, bool stale)
            where T : class, IBusinessObject
        {
            var queryKey = ObjectCache.GetCacheKey<T>(operation, parameters, returnType);
            return ObjectCache.LookupKeys(typeof(T), queryKey);
        }

        private static string[] LookupKeys(CacheProvider cache, string queryKey, bool stale)
        {
            object value;
            if (stale && cache is IStaleCacheProvider)
            {
                value = ((IStaleCacheProvider)cache).RetrieveStale(queryKey);
            }
            else
            {
                value = cache.Retrieve(queryKey);
            }

            if (value != null)
            {
                if (value is CacheItem)
                {
                    return ((CacheItem)value).ToIndex();
                }
                else if (value is byte[])
                {
                    return new CacheItem((byte[])value).ToIndex();
                }
                else
                {
                    return value as string[];
                }
            }
            return null;
        }

        public static IEnumerable<string> LookupKeysByParameterValues(Type objectType, IList<Param> parameters)
        {
            var cacheType = GetCacheType(objectType);
            if (cacheType != null && parameters != null && parameters.Count > 0)
            {
                var typeKey = objectType.FullName;
                var parameterSet = parameters.GroupBy(p => p.Name).ToDictionary(g => string.Concat(typeKey, "::", g.Key.ToUpper()), g => g.First(), StringComparer.OrdinalIgnoreCase);

                var cachedQueriesResult = _trackingCache.Value.Retrieve(parameterSet.Keys).Cast<KeyValuePair<string, object>>();
                if (cachedQueriesResult != null)
                {
                    var cachedQueries = cachedQueriesResult.ToDictionary(i => i.Key, i => (string)i.Value, StringComparer.OrdinalIgnoreCase);
                    HashSet<string> allQueries = null;
                    foreach (var parameterKey in parameterSet.Keys)
                    {
                        string valueSetJson;
                        if (cachedQueries.TryGetValue(parameterKey, out valueSetJson))
                        {
                            var json = Json.Parse(valueSetJson);
                            var valueSet = (Dictionary<string, string[]>)JsonSerializationReader.ReadObject(json, typeof(Dictionary<string, string[]>));
                            //var valueSet = Newtonsoft.Json.JsonConvert.DeserializeObject<Dictionary<string, string[]>>(valueSetJson);
                            var valueKey = FixEmptyValueKey(parameterSet[parameterKey].Value.SafeCast<string>());
                            string[] queries;
                            if (valueSet.TryGetValue(valueKey, out queries))
                            {
                                if (allQueries == null)
                                {
                                    allQueries = new HashSet<string>(queries);
                                }
                                else
                                {
                                    allQueries.IntersectWith(queries);
                                }
                            }
                        }
                    }

                    if (allQueries != null)
                    {
                        return allQueries;
                    }
                }
            }
            return Enumerable.Empty<string>();
        }

        #endregion

        #region Item Lookup Methods

        internal static T Lookup<T>(CacheKey key, bool stale = false)
            where T : class, IBusinessObject
        {
            return Lookup<T>(key.Value, stale);
        }

        public static T Lookup<T>(string key, bool stale = false)
            where T : class, IBusinessObject
        {
            var cacheType = GetCacheType(typeof(T));
            object value;
            CacheItem item = null;
            if (cacheType != null)
            {
                var cache = CacheFactory.GetProvider(cacheType);
                if (cache.IsOutOfProcess)
                {
                    if (stale && cache is IStaleCacheProvider)
                    {
                        value = ((IStaleCacheProvider)cache).RetrieveStale(key);
                    }
                    else
                    {
                        value = cache.Retrieve(key);
                    }

                    if (value != null)
                    {
                        item = value is CacheItem ? (CacheItem)value : new CacheItem(key, (byte[])value);
                    }
                }
                else
                {
                    value = cache.Retrieve(key);
                    if (value != null)
                    {
                        item = new CacheItem(key, (T)value);
                    }
                }
            }

            if (item != null)
            {
                return item.ToObject<T>();
            }
            return default(T);
        }

        internal static CacheItem[] Lookup<T>(IEnumerable<CacheKey> keys, bool stale = false)
            where T : class, IBusinessObject
        {
            return Lookup<T>(keys.Select(k => k.Value), stale);
        }

        public static CacheItem[] Lookup<T>(IEnumerable<string> keys, bool stale = false)
            where T : class, IBusinessObject
        {
            CacheItem[] items = null;
            var cacheType = GetCacheType(typeof(T));
            var keyCount = keys.Count();
            if (cacheType != null)
            {
                if (keyCount == 0)
                {
                    items = new CacheItem[] { };
                }
                else
                {
                    var cache = CacheFactory.GetProvider(cacheType, GetCacheOptions(typeof(T)));
                    if (cache.IsOutOfProcess)
                    {
                        if (keyCount == 1)
                        {
                            var key = keys.First();
                            object value;
                            if (stale && cache is IStaleCacheProvider)
                            {
                                value = ((IStaleCacheProvider)cache).RetrieveStale(key);
                            }
                            else
                            {
                                value = cache.Retrieve(key);
                            }

                            if (value != null)
                            {
                                var item = value is CacheItem ? (CacheItem)value : new CacheItem(key, (byte[])value);
                                return new[] { item };
                            }
                        }
                        else
                        {
                            IDictionary<string, object> map;
                            if (stale && cache is IStaleCacheProvider)
                            {
                                map = ((IStaleCacheProvider)cache).RetrieveStale(keys);
                            }
                            else
                            {
                                map = cache.Retrieve(keys);
                            }

                            if (map != null)
                            {
                                // Need to enforce original order on multiple items returned from memcached using multi-get                            
                                items = map.Where(p => p.Value != null).Select(p => p.Value is CacheItem ? (CacheItem)p.Value : new CacheItem(p.Key, (byte[])p.Value)).Arrange(keys, i => i.Key).ToArray();
                            }
                        }
                    }
                    else
                    {
                        IDictionary<string, object> map;
                        if (stale && cache is IStaleCacheProvider)
                        {
                            map = ((IStaleCacheProvider)cache).RetrieveStale(keys);
                        }
                        else
                        {
                            map = cache.Retrieve(keys);
                        }

                        if (map != null)
                        {
                            items = map.Where(p => p.Value != null).Select(p => new CacheItem(p.Key, (T)p.Value)).ToArray();
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

        public static IEnumerable<T> Deserialize<T>(this IList<CacheItem> items)
            where T : class, IBusinessObject
        {
            return items.Select(i => i.ToObject<T>());
        }

        #endregion

        #region Key Tracking Methods

        private static void TrackKeys<T>(string queryKey, IEnumerable<string> itemKeys, IList<Param> parameters)
            where T : class, IBusinessObject
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
                var cache = _trackingCache.Value;
                if (cache != null)
                {
                    Task.Run(() => TrackCacheProviderKeys<T>(queryKey, itemKeys, parameters, cache));
                }
            }
        }

        private static void TrackCacheProviderKeys<T>(string queryKey, IEnumerable<string> itemKeys, IList<Param> parameters, CacheProvider cache)
            where T : class, IBusinessObject
        {
            if (parameters != null && parameters.Count > 0 && cache.IsDistributed && cache != null && cache is IPersistentCacheProvider)
            {
                var typeKey = typeof(T).FullName;
                var parameterSet = parameters.ToDictionary(p => string.Concat(typeKey, "::", p.Name.ToUpper()), p => p, StringComparer.OrdinalIgnoreCase);
                var cachedQueriesResult = cache.Retrieve(parameterSet.Keys);
                var cachedQueries = cachedQueriesResult != null
                                    ? cachedQueriesResult.ToDictionary(i => i.Key, i => (string)i.Value, StringComparer.OrdinalIgnoreCase)
                                    : new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                foreach (var parameterKey in parameterSet.Keys)
                {
                    var valueKey = FixEmptyValueKey(parameterSet[parameterKey].Value.SafeCast<string>());
                    string valueSetJson;
                    Dictionary<string, string[]> valueSet;
                    if (!cachedQueries.TryGetValue(parameterKey, out valueSetJson))
                    {
                        valueSet = new Dictionary<string, string[]>();
                    }
                    else
                    {
                        var json = Json.Parse(valueSetJson);
                        valueSet = (Dictionary<string, string[]>)JsonSerializationReader.ReadObject(json, typeof(Dictionary<string, string[]>));
                        //valueSet = Newtonsoft.Json.JsonConvert.DeserializeObject<Dictionary<string, string[]>>(valueSetJson);
                    }

                    HashSet<string> querySet;
                    string[] queries;
                    if (valueSet.TryGetValue(valueKey, out queries))
                    {
                        querySet = new HashSet<string>(queries);
                    }
                    else
                    {
                        querySet = new HashSet<string>();
                    }

                    querySet.Add(queryKey);
                    valueSet[valueKey] = querySet.ToArray();

                    var jsonBuffer = new StringBuilder();
                    using (var writer = new StringWriter(jsonBuffer))
                    {
                        JsonSerializationWriter.WriteObject(valueSet, null, writer);
                    }
                    cache.Save(parameterKey, jsonBuffer.ToString());

                    //valueSetJson = Newtonsoft.Json.JsonConvert.SerializeObject(valueSet);
                    //cache.Save(parameterKey, valueSetJson);
                }
            }
        }

        private static string FixEmptyValueKey(string valueKey)
        {
            if (valueKey == null || valueKey.Trim().Length == 0)
            {
                valueKey = "_EMPTY";
            }
            return valueKey;
        }

        #endregion

        #region Add Methods

        internal static Tuple<bool, IEnumerable<T>, string[], bool> Add<T>(string queryKey, IList<Param> parameters, Func<IEnumerable<T>> retrieveItems, bool forceRetrieve)
            where T : class, IBusinessObject
        {
            var result = false;
            var values = Enumerable.Empty<T>();
            string[] keys = null;
            var cacheType = GetCacheType(typeof(T));
            var stale = false;

            if (cacheType != null)
            {
                var cache = CacheFactory.GetProvider(cacheType, GetCacheOptions(typeof(T)));
                
                if (cache.IsDistributed)
                {
                    if (cache is IStaleCacheProvider)
                    {
                        if (((DistributedCacheProvider)cache).TryAcquireLock(queryKey))
                        {
                            try
                            {
                                AddToCache<T>(queryKey, parameters, retrieveItems, forceRetrieve, cache, ref keys, ref values, ref result);
                            }
                            finally
                            {
                                ((DistributedCacheProvider)cache).ReleaseLock(queryKey);
                            }
                        }
                        else
                        {
                            keys = LookupKeys(cache, queryKey, true);
                            stale = true;
                        }
                    }
                    else
                    {
                        AddToCache<T>(queryKey, parameters, retrieveItems, forceRetrieve, cache, ref keys, ref values, ref result);
                    }
                }
                else
                {
                    // Threads are blocked if there is a contention on a key
                    var cacheLock = _cacheLocks.GetOrAdd(queryKey, k => new object());

                    lock (cacheLock)
                    {
                        AddToCache<T>(queryKey, parameters, retrieveItems, forceRetrieve, cache, ref keys, ref values, ref result);
                    }
                }
            }
            return Tuple.Create(result, values, keys, stale);
        }

        internal static Tuple<bool, IEnumerable<T>, string[], bool> Add<T>(string operation, IList<Param> parameters, OperationReturnType returnType, Func<IEnumerable<T>> retrieveItems, bool forceRetrieve)
            where T : class, IBusinessObject
        {
            var queryKey = ObjectCache.GetCacheKey<T>(operation, parameters, returnType);
            return Add<T>(queryKey, parameters, retrieveItems, forceRetrieve);
        }

        internal static Tuple<bool, IEnumerable<T>, string[], bool> Add<T>(IList<Param> parameters, OperationReturnType returnType, Func<IEnumerable<T>> retrieveItems, bool forceRetrieve)
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
                if (cacheType != null)
                {
                    var cache = CacheFactory.GetProvider(cacheType, GetCacheOptions(typeof(T)));

                    if (cache.IsOutOfProcess)
                    {
                        if (cache.IsDistributed)
                        {
                            cache.AddNew(key, new CacheItem(key, item));
                        }
                        else
                        {
                            return cache.AddNew(key, item.Serialize());
                        }
                    }
                    else
                    {
                        return cache.AddNew(key, item);
                    }
                }
            }
            return false;
        }

        private static void AddToCache<T>(string queryKey, 
                                    IList<Param> parameters, 
                                    Func<IEnumerable<T>> retrieveItems, 
                                    bool forceRetrieve, 
                                    CacheProvider cache,
                                    ref string[] keys, 
                                    ref IEnumerable<T> values,
                                    ref bool result)
            where T : class, IBusinessObject
        {
            keys = ObjectCache.LookupKeys(typeof(T), queryKey);

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
                        var keyMapSerialized = keyMap.AsParallel().ToDictionary(kvp => kvp.Key, kvp => (object)new CacheItem(kvp.Key, (T)kvp.Value));
                        result = cache.Save(keyMapSerialized);
                    }
                    else
                    {
                        result = cache.Save(keyMap);
                    }
                }

                if (result)
                {
                    // Store a query and corresponding keys
                    result = result && cache.Save(queryKey, new CacheItem(queryKey, keyMap.Keys.ToArray()));

                    TrackKeys<T>(queryKey, keyMap.Keys, parameters);
                }
                else
                {
                    values = valueList;
                }
            }
        }

        #endregion

        #region Remove Methods

        internal static bool Remove(Type objectType, string queryKey)
        {
            var cacheType = GetCacheType(objectType);
            if (cacheType != null)
            {
                var cache = CacheFactory.GetProvider(cacheType, GetCacheOptions(objectType));
                var success = cache.Clear(queryKey);
                if (success)
                {
                    ExecutionContext.Pop(queryKey);
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
                if (cacheType != null)
                {
                    var cache = CacheFactory.GetProvider(cacheType, GetCacheOptions(typeof(T)));
                    success = cache.Clear(key);
                    if (success)
                    {
                        RemoveDependencies<T>(item);
                    }
                }
            }
            return success;
        }

        internal static void RemoveDependencies<T>(T item)
            where T : class, IBusinessObject
        {
            // Clear cache dependencies if there are any
            var dependencies = GetCacheDependencies<T>();
            if (dependencies != null && dependencies.Count > 0)
            {
                var validDependecies = dependencies.Where(d => d.DependentType != null && !string.IsNullOrEmpty(d.DependentProperty) && !string.IsNullOrEmpty(d.ValueProperty));
                var parameterValuesByType = validDependecies.GroupBy(d => d.DependentType).ToDictionary(g => g.Key, g => g.Select(d => new Param { Name = d.DependentProperty, Value = item.Property(d.ValueProperty) }).ToList());

                parameterValuesByType.AsParallel().ForAll(p => 
                {
                    var targetCacheType = GetCacheType(p.Key);
                    if (targetCacheType != null)
                    {
                        var keys = LookupKeysByParameterValues(p.Key, p.Value);
                        var targetCache = CacheFactory.GetProvider(targetCacheType);
                        foreach (var targetKey in keys)
                        {
                            targetCache.Remove(targetKey);
                        }
                    }
                });
            }
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
                if (cacheType != null)
                {
                    var cache = CacheFactory.GetProvider(cacheType, GetCacheOptions(typeof(T)));

                    if (cache.IsOutOfProcess)
                    {
                        if (cache.IsDistributed)
                        {
                            return cache.Save(key, new CacheItem(key, item));
                        }
                        else
                        {
                            return cache.Save(key, item.Serialize());
                        }
                    }
                    else
                    {
                        return cache.Save(key, item);
                    }
                }
            }
            return false;
        }

        #endregion
    }
}
