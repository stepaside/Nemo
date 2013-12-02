using Nemo.Attributes;
using Nemo.Caching.Providers;
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

namespace Nemo.Caching
{
    public static class ObjectCache
    {
        private static ConcurrentDictionary<string, object> _cacheLocks = new ConcurrentDictionary<string, object>();

        private static Lazy<IPersistentCacheProvider> _trackingCache = new Lazy<IPersistentCacheProvider>(() => (IPersistentCacheProvider)CacheFactory.GetProvider(ConfigurationFactory.Configuration.TrackingCacheProvider), true);

        private static Lazy<IRevisionProvider> _revisionCache = new Lazy<IRevisionProvider>(() => (IRevisionProvider)CacheFactory.GetProvider(ConfigurationFactory.Configuration.TrackingCacheProvider), true);

        private static ConcurrentDictionary<string, ulong> _revisions = new ConcurrentDictionary<string,ulong>();

        public static void Initialize() 
        {
            if (!_revisionCache.IsValueCreated && _revisionCache.Value != null)
            {
                _revisions = new ConcurrentDictionary<string, ulong>(_revisionCache.Value.GetAllRevisions());
            }
        }

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

        public static Tuple<string, ulong[]> GetQueryKey<T>(string operation, IList<Param> parameters, OperationReturnType returnType, CacheProvider cache)
            where T : class, IBusinessObject
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
            string queryKey = null;
            ulong[] signature = null;

            // Use invalidation strategy only if it is a cache enabled call
            if (cache != null)
            {
                var strategy = ConfigurationFactory.Configuration.CacheInvalidationStrategy;
                if (parameters != null && (strategy == CacheInvalidationStrategy.QuerySignature || strategy == CacheInvalidationStrategy.DelayedQuerySignature))
                {
                    // Compute subspaces
                    var subspaces = GetQuerySubscpaces<T>(parameters);
                    var revisions = new Dictionary<string, ulong>();
                    if (strategy == CacheInvalidationStrategy.QuerySignature)
                    {
                        var missingSubspaces = new List<string>();
                        // Look locally to see if revision is available
                        foreach (var subspace in subspaces)
                        {
                            ulong revision;
                            if (!_revisions.TryGetValue(subspace, out revision))
                            {
                                // No revision found: fetch it in bulk from the revision provider after the loop
                                missingSubspaces.Add(subspace);
                            }
                            else
                            {
                                // Found it! Add to the list of revisions
                                revisions.Add(subspace, revision);
                            }
                        }

                        // Fetch only missing revisions
                        if (missingSubspaces.Count > 0)
                        {
                            var missinRevisions = _revisionCache.Value.GetRevisions(missingSubspaces);
                            foreach (var subspace in missingSubspaces)
                            {
                                ulong revision;
                                // Check if the revision has been added while we were retrieving from the revision provider
                                if (_revisions.TryGetValue(subspace, out revision))
                                {
                                    // Check if the value retrieved is greater than the value stored locally
                                    if (missinRevisions[subspace] > revision)
                                    {
                                        revisions.Add(subspace, missinRevisions[subspace]);
                                    }
                                    else
                                    {
                                        revisions.Add(subspace, revision);
                                    }
                                }
                                else
                                {
                                    // The revision has not been added, thus proceed with the retrieved value
                                    revisions.Add(subspace, missinRevisions[subspace]);
                                }
                            }
                        }
                    }
                    else
                    {
                        foreach (var subspace in subspaces)
                        {
                            var added = false;

                            var revision = _revisions.GetOrAdd(subspace, key =>
                            {
                                var value = _revisionCache.Value.GenerateRevision();
                                added = true;
                                return value;
                            });

                            if (added)
                            {
                                _publishRevisionEvent(null, new PublisheRevisionIncrementEventArgs { Cache = cache, Key = subspace, Revision = revision });
                            }

                            revisions.Add(subspace, revision);
                        }
                    }

                    queryKey = cacheKey.Value;
                    signature = revisions.Arrange(subspaces, p => p.Key).Select(p => p.Value).ToArray();
                }
                else if (strategy == CacheInvalidationStrategy.TrackAndIncrement)
                {
                    var computedKey = cache.ComputeKey(cacheKey.Value);
                    var revision = _revisions.GetOrAdd(computedKey, key => _revisionCache.Value.GetRevision(cache.CleanKey(computedKey)));
                    if (revision > 0ul)
                    {
                        computedKey = cache.ComputeKey(cacheKey.Value, revision);
                    }
                    queryKey = computedKey;
                }
            }

            return Tuple.Create(queryKey, signature);
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

        internal static string[] LookupKeys(string queryKey, CacheProvider cache, bool stale = false)
        {
            if (!string.IsNullOrEmpty(queryKey))
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
                    else if (value is CacheValue)
                    {
                        return new CacheItem(queryKey, (CacheValue)value).ToIndex();
                    }
                    else
                    {
                        return value as string[];
                    }
                }
            }
            return null;
        }

        public static IEnumerable<string> LookupQueries(Type objectType, IList<Param> parameters)
        {
            var cacheType = GetCacheType(objectType);
            if (cacheType != null && parameters != null && parameters.Count > 0)
            {
                var parameterKeys = parameters.Select(p => new CacheKey(new SortedDictionary<string, object> { { p.Name, p.Value } }, objectType).Compute().Item1).ToList();

                var cache = (IPersistentCacheProvider)_trackingCache.Value;
                if (cache != null)
                {
                    IDictionary<string, object> versions;
                    var cachedQueries = cache.Retrieve(parameterKeys, out versions);
                    if (cachedQueries != null)
                    {
                        HashSet<string> allQueries = null;
                        foreach (var parameterKey in parameterKeys)
                        {
                            object value;
                            if (cachedQueries.TryGetValue(parameterKey, out value) && value != null)
                            {
                                IEnumerable<string> queries = (((string)value)[0] == ',' ? ((string)value).Substring(1) : (string)value).Split(',');

                                var count = ((string[])queries).Length;
                                var uniqueCount = queries.Distinct().Count();

                                // Before the string grows too much try to compact it
                                if ((count - uniqueCount) / (double)count > .3)
                                {
                                    queries = CompactQueryKeys(cache, parameterKey, values: queries, version: versions[parameterKey]);
                                }

                                if (allQueries == null)
                                {
                                    allQueries = queries as HashSet<string>;
                                    if (allQueries == null)
                                    {
                                        allQueries = new HashSet<string>(queries);
                                    }
                                }
                                else
                                {
                                    allQueries.IntersectWith(queries);
                                }
                            }
                        }

                        if (allQueries != null)
                        {
                            return allQueries;
                        }
                    }
                }
            }
            return Enumerable.Empty<string>();
        }

        #endregion

        #region Item Lookup Methods

        public static CacheItem Lookup<T>(string key, CacheProvider cache, bool stale = false)
            where T : class, IBusinessObject
        {
            object value;
            CacheItem item = null;
            if (cache != null)
            {
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
                        item = value is CacheItem ? (CacheItem)value : new CacheItem(key, (CacheValue)value);
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

            return item;
        }

        public static CacheItem[] Lookup<T>(IEnumerable<string> keys, CacheProvider cache, bool stale = false)
            where T : class, IBusinessObject
        {
            CacheItem[] items = null;
            var keyCount = keys.Count();
            if (cache != null)
            {
                if (keyCount == 0)
                {
                    items = new CacheItem[] { };
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
                                value = ((IStaleCacheProvider)cache).RetrieveStale(key);
                            }
                            else
                            {
                                value = cache.Retrieve(key);
                            }

                            if (value != null)
                            {
                                var item = value is CacheItem ? (CacheItem)value : new CacheItem(key, (CacheValue)value);
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
                                items = map.Where(p => p.Value != null).Select(p => p.Value is CacheItem ? (CacheItem)p.Value : new CacheItem(p.Key, (CacheValue)p.Value)).Arrange(keys, i => i.Key).ToArray();
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
                    Task.Run(() => TrackQuery<T>(queryKey, parameters, cache));
                }
            }
        }

        private static void TrackQuery<T>(string queryKey, IList<Param> parameters, IPersistentCacheProvider cache)
            where T : class, IBusinessObject
        {
            var strategy = ConfigurationFactory.Configuration.CacheInvalidationStrategy;
            if (parameters != null && parameters.Count > 0 && cache != null 
                && strategy != CacheInvalidationStrategy.QuerySignature && strategy != CacheInvalidationStrategy.DelayedQuerySignature)
            {
                var typeKey = typeof(T).FullName;
                var parameterKeys = parameters.Select(p => new CacheKey(new SortedDictionary<string, object> { { p.Name, p.Value } }, typeof(T)).Compute().Item1).ToList();
                foreach (var parameterKey in parameterKeys)
                {
                    // In case append fails try to compact the value
                    if (!cache.Append(parameterKey, "," + queryKey))
                    {
                        CompactQueryKeys(cache, parameterKey, queryKey: queryKey);
                    }
                }
            }
        }

        private static HashSet<string> CompactQueryKeys(IPersistentCacheProvider cache, string parameterKey, ushort retries = 1, string queryKey = null, IEnumerable<string> values = null, object version = null)
        {
            // If values and version were provided try to compact right away
            // unless version is no longer valid
            HashSet<string> queries = null;
            if (values != null && version != null)
            {
                 queries = new HashSet<string>(values.Where(v => !string.IsNullOrWhiteSpace(v)));
                 if (cache.Save(parameterKey, string.Join(",", queries), version))
                 {
                     return queries;
                 }
            }

            int index = 0;
            while (index <= retries)
            {
                // Retrieve comma-delimited list of query keys with the version number;
                object v;
                var value = cache.Retrieve(parameterKey, out v);
                // Compact the list
                if (value == null)
                {
                    queries = new HashSet<string>();
                }
                else
                {
                    queries = new HashSet<string>(((string)value).Split(',').Where(q => !string.IsNullOrWhiteSpace(q)));
                }
                // Add current query key if available
                if (queryKey != null)
                {
                    queries.Add(queryKey);
                }
                // Try to save compacted value using optimistic concurrency control
                if (cache.Save(parameterKey, string.Join(",", queries), v))
                {
                    break;
                }

                index++;
            }
            return queries;
        }

        #region Query Subspace Methods

        private static IEnumerable<string> GetQuerySubscpaces<T>(IList<Param> parameters)
        {
            return GetQuerySubscpacesImplementation<T>(parameters, true, value => Tuple.Create("?", value != "*"));
        }

        private static IEnumerable<string> GetQuerySubscpacesForInvalidation<T>(IList<Param> parameters)
        {
            return GetQuerySubscpacesImplementation<T>(parameters, false, value => Tuple.Create("?", value == "*"), value => Tuple.Create("*", value != "*"));
        }

        private static IEnumerable<string> GetQuerySubscpacesForInvalidation(Type objectType, IList<Param> parameters)
        {
            return GetQuerySubscpacesImplementation(objectType, parameters, false, value => Tuple.Create("?", value == "*"), value => Tuple.Create("*", value != "*"));
        }

        private static IEnumerable<string> GetQuerySubscpacesImplementation<T>(IList<Param> parameters, bool generate, params Func<string, Tuple<string, bool>>[] rules)
        {
            var names = Reflector.PropertyCache<T>.NameMap.Values.Where(p => (p.IsPrimaryKey || p.IsCacheKey || p.IsCacheParameter) && (p.IsSimpleType || p.IsSimpleList)).Select(p => p.ParameterName ?? p.PropertyName).OrderBy(_ => _).ToList();
            return GetQuerySubscpacesImplementation(typeof(T).FullName, names, parameters, generate, rules);
        }

        private static IEnumerable<string> GetQuerySubscpacesImplementation(Type objectType, IList<Param> parameters, bool generate, params Func<string, Tuple<string, bool>>[] rules)
        {
            var names = Reflector.GetPropertyMap(objectType).Values.Where(p => (p.IsPrimaryKey || p.IsCacheKey || p.IsCacheParameter) && (p.IsSimpleType || p.IsSimpleList)).Select(p => p.ParameterName ?? p.PropertyName).OrderBy(_ => _).ToList();
            return GetQuerySubscpacesImplementation(objectType.FullName, names, parameters, generate, rules);
        }

        private static IEnumerable<string> GetQuerySubscpacesImplementation(string typeName, List<string> names, IList<Param> parameters, bool generate, params Func<string, Tuple<string, bool>>[] rules)
        {
            var query = names.GroupJoin(parameters, n => n, p => p.Name, (name, args) => args.FirstOrDefault().ToMaybe().Select(p => p.Value == null ? string.Empty : p.Value.ToString()).Value ?? "*").ToList();
            List<List<string>> variants;
            if (generate || query.Contains("*"))
            {
                variants = AllVariantsOf(query, query.Count, rules);
            }
            else
            {
                variants = new List<List<string>> { query };
            }
            return variants.Select(s => typeName + "::" + Jenkins96Hash.Compute(Encoding.UTF8.GetBytes(string.Join("::", s))));
        }

        private static List<List<T>> AllVariantsOf<T>(List<T> source, int length, params Func<T, Tuple<T, bool>>[] rules)
        {
            if (length == 0)
            {
                return new List<List<T>>();
            }

            var prefixes = AllVariantsOf(source, length - 1, rules);

            List<List<T>> extended = null;
            if (prefixes.Count == 0)
            {
                extended = new List<List<T>> { new List<T> { source[length - 1] } };
            }
            else
            {
                extended = prefixes.Select(p =>
                {
                    var e = new List<T>(p);
                    e.Add(source[length - 1]);
                    return e;
                }).ToList();
            }

            var found = false;
            foreach (var rule in rules)
            {
                found = rule(source[length - 1]).Item2;
                if (found) break;
            }

            if (!found)
            {
                return extended;
            }

            var alternative = new List<List<T>>();
            foreach (var rule in rules)
            {
                foreach (var prefix in extended)
                {
                    var res = rule(source[length - 1]);
                    if (res.Item2)
                    {
                        var e = new List<T>(prefix);
                        e[e.Count - 1] = res.Item1;
                        alternative.Add(e);
                    }
                }
            }

            extended.AddRange(alternative);
            return extended;
        }

        #endregion

        #endregion

        #region Add Methods

        internal static Tuple<bool, IEnumerable<T>, string[], bool> Add<T>(string queryKey, IList<Param> parameters, Func<IEnumerable<T>> retrieveItems, bool forceRetrieve, CacheProvider cache)
            where T : class, IBusinessObject
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
                                AddToCache<T>(queryKey, parameters, retrieveItems, forceRetrieve, cache, ref keys, ref values, ref result);
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
        
        public static bool Add<T>(T item, CacheProvider cache = null)
            where T : class, IBusinessObject
        {
            if (item != null)
            {
                DateTime expiresAt = DateTime.MinValue;
                if (cache != null)
                {
                    var key = new CacheKey(item).Value;
                    if (cache.IsOutOfProcess)
                    {
                        return cache.AddNew(key, new CacheItem(key, item).Value);
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
                        var keyMapSerialized = keyMap.AsParallel().ToDictionary(kvp => kvp.Key, kvp => (object)new CacheItem(kvp.Key, (T)kvp.Value).Value);
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
                    if (cache.IsOutOfProcess)
                    {
                        result = result && cache.Save(queryKey, new CacheItem(queryKey, keyMap.Keys.ToArray()).Value);
                    }
                    else
                    {
                        result = result && cache.Save(queryKey, keyMap.Keys.ToArray());
                    }

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

        internal static bool Remove<T>(string queryKey, IList<Param> parameters, CacheProvider cache)
            where T : class, IBusinessObject
        {
            var success = false;
            if (cache != null)
            {
                var strategy = ConfigurationFactory.Configuration.CacheInvalidationStrategy;

                if (strategy != CacheInvalidationStrategy.TrackAndRemove)
                {
                    if (strategy == CacheInvalidationStrategy.TrackAndIncrement)
                    {
                        var revision = _revisionCache.Value.IncrementRevision(queryKey);
                        success = revision > 0ul;
                        if (success)
                        {
                            UpdateRevision(queryKey, revision, cache);
                            _publishRevisionEvent(null, new PublisheRevisionIncrementEventArgs { Cache = cache, Key = queryKey, Revision = revision });
                        }
                    }
                    else
                    {
                        success = Invalidate<T>(parameters);
                    }
                }
                else
                {
                    success = cache.Clear(queryKey);
                }

                if (success)
                {
                    ExecutionContext.Pop(queryKey);
                }
            }
            return success;
        }

        public static bool Remove<T>(T item, CacheProvider cache = null)
            where T : class, IBusinessObject
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

                    if (strategy == CacheInvalidationStrategy.QuerySignature || strategy == CacheInvalidationStrategy.DelayedQuerySignature)
                    {
                        success = Invalidate<T>(item);
                        if (!cache.IsDistributed)
                        {
                            success = cache.Clear(key) && success;
                        }
                    }
                    else
                    {
                        success = cache.Clear(key);
                    }

                    if (success)
                    {
                        ExecutionContext.Pop(key);
                        RemoveDependencies<T>(item);
                    }
                }
            }
            return success;
        }
        
        #endregion

        #region Modify Methods

        public static bool Modify<T>(T item, CacheProvider cache = null)
            where T : class, IBusinessObject
        {
            if (item != null)
            {
                if (cache != null)
                {
                    var key = new CacheKey(item).Value;
                    if (cache.IsOutOfProcess)
                    {
                        return cache.Save(key, new CacheItem(key, item).Value);
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

        #region Invalidate Methods

        internal static bool Invalidate(Type objectType, IList<Param> parameters)
        {
            var strategy = ConfigurationFactory.Configuration.CacheInvalidationStrategy;
            if (strategy == CacheInvalidationStrategy.QuerySignature || strategy == CacheInvalidationStrategy.DelayedQuerySignature)
            {
                InvalidateImplementation(objectType, parameters, strategy == CacheInvalidationStrategy.DelayedQuerySignature);
                return true;
            }
            return false;
        }

        internal static bool Invalidate<T>(IList<Param> parameters)
            where T : class, IBusinessObject
        {
            var strategy = ConfigurationFactory.Configuration.CacheInvalidationStrategy;
            if (strategy == CacheInvalidationStrategy.QuerySignature || strategy == CacheInvalidationStrategy.DelayedQuerySignature)
            {
                InvalidateImplementation<T>(parameters, strategy == CacheInvalidationStrategy.DelayedQuerySignature);
                return true;
            }
            return false;
        }

        internal static bool Invalidate<T>(T businessObject)
            where T : class, IBusinessObject
        {
            var strategy = ConfigurationFactory.Configuration.CacheInvalidationStrategy;
            if (strategy == CacheInvalidationStrategy.QuerySignature || strategy == CacheInvalidationStrategy.DelayedQuerySignature)
            {
                var nameMap = Reflector.PropertyCache<T>.NameMap;
                var parameters = businessObject.GetPrimaryKey<T>(true).Select(pk => new Param { Name = nameMap[pk.Key].ParameterName ?? pk.Key, Value = pk.Value }).ToList();
                InvalidateImplementation<T>(parameters, strategy == CacheInvalidationStrategy.DelayedQuerySignature);
                return true;
            }
            return false;
        }

        private static void InvalidateImplementation(Type objectType, IList<Param> parameters, bool delayed)
        {
            var variants = GetQuerySubscpacesForInvalidation(objectType, parameters);
            foreach (var variant in variants)
            {
                if (delayed)
                {
                    var revision = _revisions.AddOrUpdate(variant, _revisionCache.Value.GenerateRevision(), (k, v) => v + 1ul);
                    _publishRevisionEvent(null, new PublisheRevisionIncrementEventArgs { Cache = CacheFactory.GetProvider(GetCacheType(objectType)), Key = variant, Revision = revision });
                }
                else
                {
                    var revision = _revisionCache.Value.IncrementRevision(variant);
                    if (revision > 0ul)
                    {
                        UpdateRevision(variant, revision, null);
                        _publishRevisionEvent(null, new PublisheRevisionIncrementEventArgs { Cache = CacheFactory.GetProvider(GetCacheType(objectType)), Key = variant, Revision = revision });
                    }
                }
            }
        }

        private static void InvalidateImplementation<T>(IList<Param> parameters, bool delayed)
            where T : class, IBusinessObject
        {
            var variants = GetQuerySubscpacesForInvalidation<T>(parameters);
            foreach (var variant in variants)
            {
                if (delayed)
                {
                    var revision = _revisions.AddOrUpdate(variant, _revisionCache.Value.GenerateRevision(), (k, v) => v + 1ul);
                    _publishRevisionEvent(null, new PublisheRevisionIncrementEventArgs { Cache = CacheFactory.GetProvider(GetCacheType<T>()), Key = variant, Revision = revision });
                }
                else
                {
                    var revision = _revisionCache.Value.IncrementRevision(variant);
                    if (revision > 0ul)
                    {
                        UpdateRevision(variant, revision, null);
                        _publishRevisionEvent(null, new PublisheRevisionIncrementEventArgs { Cache = CacheFactory.GetProvider(GetCacheType<T>()), Key = variant, Revision = revision });
                    }
                }
            }
        }

        #endregion

        #region Dependency Invalidation Methods

        private static IList<QueryDependency> GetQueryDependencies<T>()
           where T : class, IBusinessObject
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
            where T : class, IBusinessObject
        {
            var properties = Reflector.PropertyCache<T>.NameMap;
            var cacheType = GetCacheType<T>();
            var cache = CacheFactory.GetProvider(cacheType);

            var strategy = ConfigurationFactory.Configuration.CacheInvalidationStrategy;
            var queryInvalidationBySignature = strategy == CacheInvalidationStrategy.QuerySignature || strategy == CacheInvalidationStrategy.DelayedQuerySignature;

            var dependencies = GetQueryDependencies<T>();
            if (dependencies == null || dependencies.Count == 0)
            {
                var validProperties = properties.Values.Where(p => (p.IsPrimaryKey || p.IsCacheKey || p.IsCacheParameter)
                                                    && (p.IsSimpleType || p.IsSimpleList)
                                                    && !p.IsAutoGenerated);
                if (queryInvalidationBySignature)
                {
                    dependencies = new List<QueryDependency> { new QueryDependency(validProperties.Select(p => p.PropertyName).ToArray()) };
                }
                else
                {
                    dependencies = validProperties.Select(p => new QueryDependency(p.PropertyName)).ToList();
                }
            }
            
            if (cache != null)
            {
                foreach (var dependency in dependencies)
                {
                    var parameters = new List<Param>();
                    var names = dependency.Properties.Where(p => !string.IsNullOrEmpty(p)).Select(p => p).Distinct();
                    foreach (var name in names)
                    {
                        ReflectedProperty property;
                        if (properties.TryGetValue(name, out property))
                        {
                            parameters.Add(new Param { Name = property.ParameterName ?? name, Value = item.Property(name) });
                        }
                    }

                    if (queryInvalidationBySignature)
                    {
                        Invalidate(typeof(T), parameters);
                    }
                    else
                    {
                        var removeByIncrement = strategy == CacheInvalidationStrategy.TrackAndIncrement;
                        var keys = LookupQueries(typeof(T), parameters);
                        foreach (var queryKey in keys)
                        {
                            if (removeByIncrement)
                            {
                                var revision = _revisionCache.Value.IncrementRevision(queryKey);
                                if (revision > 0ul)
                                {
                                    UpdateRevision(queryKey, revision, cache);
                                    _publishRevisionEvent(null, new PublisheRevisionIncrementEventArgs { Cache = cache, Key = queryKey, Revision = revision });
                                }
                            }
                            else
                            {
                                cache.Remove(queryKey);
                            }
                        }
                    }
                }
            }
        }

        #endregion

        #region Signalling Methods
        
        private static EventHandler<PublisheRevisionIncrementEventArgs> _publishRevisionEvent = delegate { };

        public static event EventHandler<PublisheRevisionIncrementEventArgs> PublishRevisionIncrement
        {
            add
            {
                _publishRevisionEvent += value.MakeWeak(eh => _publishRevisionEvent -= eh);
            }
            remove 
            {
            }
        }

        public static void UpdateRevision(string key, ulong revision, CacheProvider cache)
        {
            if (cache != null)
            {
                key = cache.ComputeKey(key, revision);
            }
            _revisions.AddOrUpdate(key, revision, (k, r) => revision);
        }

        #endregion
    }
}
