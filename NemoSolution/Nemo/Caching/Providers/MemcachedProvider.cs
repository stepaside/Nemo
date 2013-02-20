using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Enyim.Caching;
using Enyim.Caching.Memcached;
using Nemo.Collections.Extensions;
using Nemo.Extensions;
using Nemo.Utilities;

namespace Nemo.Caching.Providers
{
    public class MemcachedProvider : DistributedCacheProviderWithLockManager<MemcachedProvider>
                                    , IDistributedCounter
                                    , IOptimisticConcurrencyProvider
                                    , IStaleCacheProvider
    {
        #region Static Declarations

        private static ConcurrentDictionary<string, MemcachedClient> _memcachedClientList = new ConcurrentDictionary<string, MemcachedClient>();

        public static MemcachedClient GetMemcachedClient(string clusterName)
        {
            MemcachedClient memcachedClient = null;
            clusterName = !string.IsNullOrEmpty(clusterName) ? clusterName : DefaultClusterName;
            if (clusterName.NullIfEmpty() != null)
            {
                memcachedClient = _memcachedClientList.GetOrAdd(clusterName, n => new MemcachedClient(n));
            }
            return memcachedClient;
        }

        public static string DefaultClusterName
        {
            get
            {
                return Config.AppSettings("MemcachedProvider.DefaultClusterName", "enyim.com/memcached");
            }
        }
        
        #endregion

        private MemcachedClient _memcachedClient;

        #region Constructors

        // This one is used to provide lock management
        internal MemcachedProvider(string clusterName) : base(null, null) 
        {
            _memcachedClient = GetMemcachedClient(clusterName);
        }

        public MemcachedProvider(CacheOptions options = null)
            : base(new MemcachedProvider(options != null ? options.ClusterName : DefaultClusterName), options)
        {
            _memcachedClient = GetMemcachedClient(options != null ? options.ClusterName : DefaultClusterName);
        }

        #endregion

        public override bool IsOutOfProcess
        {
            get
            {
                return true;
            }
        }

        public bool CheckAndSave(string key, object val, ulong cas)
        {
            key = ComputeKey(key);
            val = ComputeValue(val, DateTimeOffset.Now);
            CasResult<bool> result;
            switch (ExpirationType)
            {
                case CacheExpirationType.TimeOfDay:
                    result = _memcachedClient.Cas(StoreMode.Set, key, val, ExpiresAtSpecificTime.Value.DateTime, cas);
                    break;
                case CacheExpirationType.DateTime:
                    result = _memcachedClient.Cas(StoreMode.Set, key, val, ExpiresAt.DateTime, cas);
                    break;
                case CacheExpirationType.TimeSpan:
                    result = _memcachedClient.Cas(StoreMode.Set, key, val, LifeSpan, cas);
                    break;
                default:
                    result = _memcachedClient.Cas(StoreMode.Set, key, val, cas);
                    break;
            }
            return result.Result;
        }

        public Tuple<object, ulong> RetrieveWithCas(string key)
        {
            key = ComputeKey(key);
            var casResult = _memcachedClient.GetWithCas(key);
            var result = casResult.Result;
            if (result != null && result is TemporalValue)
            {
                var staleValue = (TemporalValue)result;
                if (staleValue.IsValid())
                {
                    result = staleValue.Value;
                }
                else
                {
                    result = null;
                }
            }
            return Tuple.Create(result, casResult.Cas);
        }

        public IDictionary<string, Tuple<object, ulong>> RetrieveWithCas(IEnumerable<string> keys)
        {
            var computedKeys = ComputeKey(keys);
            var items = _memcachedClient.GetWithCas(computedKeys.Keys);
            var result = items
                            .Where(i => !(i.Value.Result is TemporalValue) || ((TemporalValue)i.Value.Result).IsValid())
                            .ToDictionary
                            (
                                i => computedKeys[i.Key],
                                i => Tuple.Create(i.Value.Result is TemporalValue 
                                                    ? ((TemporalValue)i.Value.Result).Value
                                                    : i.Value.Result, i.Value.Cas)
                            );
            return result;
        }

        public override void RemoveAll()
        {
            _memcachedClient.FlushAll();
        }

        public override void RemoveByNamespace()
        {
            if (!string.IsNullOrEmpty(_cacheNamespace))
            {
                _namespaceVersion = _memcachedClient.Increment(_cacheNamespace, 1, 1);
            }
        }

        public override object Remove(string key)
        {
            key = ComputeKey(key);
            var result = _memcachedClient.Get(key);
            _memcachedClient.Remove(key);
            return result;
        }

        public override bool Clear(string key)
        {
            key = ComputeKey(key);
            return _memcachedClient.Remove(key);
        }

        public override bool AddNew(string key, object val)
        {
            key = ComputeKey(key);
            var success = Store(StoreMode.Add, key, val, DateTimeOffset.Now);
            return success;
        }

        public override bool Save(string key, object val)
        {
            key = ComputeKey(key);
            var success = Store(StoreMode.Set, key, val, DateTimeOffset.Now);
            return success; ;
        }

        public override bool Save(IDictionary<string, object> items)
        {
            var keys = ComputeKey(items.Keys);
            var success = true;
            var currentDateTime = DateTimeOffset.Now;
            foreach (var k in keys)
            {
                success = success && Store(StoreMode.Set, k.Key, items[k.Value], currentDateTime);
            }
            return success;
        }

        public override object Retrieve(string key)
        {
            key = ComputeKey(key);
            return RetrieveUsingRawKey(key);
        }

        public override IDictionary<string, object> Retrieve(IEnumerable<string> keys)
        {
            var computedKeys = ComputeKey(keys);
            var items = _memcachedClient.Get(computedKeys.Keys);
            items = items.Where(i => !(i.Value is TemporalValue) || ((TemporalValue)i.Value).IsValid()).ToDictionary(i => computedKeys[i.Key], i => i.Value is TemporalValue ? ((TemporalValue)i.Value).Value : i.Value);
            //if (SlidingExpiration && items.Count > 0)
            //{
            //    items.Run(delegate(KeyValuePair<string, object> kvp) 
            //    {
            //        Store(StoreMode.Replace, kvp.Key, kvp.Value);
            //    });
            //}
            return items;
        }

        public override bool Touch(string key, TimeSpan lifeSpan)
        {
            var success = false;
            key = ComputeKey(key);
            var result = _memcachedClient.Get(key);
            if (result != null)
            {
                success = _memcachedClient.Store(StoreMode.Replace, key, result, lifeSpan);
            }
            return success;
        }

        public object RetrieveStale(string key)
        {
            key = ComputeKey(key);
            var result = _memcachedClient.Get(key);
            return ((TemporalValue)result).Value;
        }

        public IDictionary<string, object> RetrieveStale(IEnumerable<string> keys)
        {
            var computedKeys = ComputeKey(keys);
            var items = _memcachedClient.Get(computedKeys.Keys);
            return items.ToDictionary(i => computedKeys[i.Key], i => ((TemporalValue)i.Value).Value);
        }

        public ulong Increment(string key, ulong delta = 1)
        {
            return _memcachedClient.Increment(key, 1, delta == 0 ? 1 : delta);
        }

        public ulong Decrement(string key, ulong delta = 1)
        {
            return _memcachedClient.Decrement(key, 1, delta == 0 ? 1 : delta);
        }

        private bool Store(StoreMode mode, string key, object val, DateTimeOffset currentDateTime)
        {
            var success = false;
            val = ComputeValue(val, currentDateTime);
            switch (ExpirationType)
            {
                case CacheExpirationType.TimeOfDay:
                    success = _memcachedClient.Store(mode, key, val, ExpiresAtSpecificTime.Value.DateTime);
                    break;
                case CacheExpirationType.DateTime:
                    success = _memcachedClient.Store(mode, key, val, ExpiresAt.DateTime);
                    break;
                case CacheExpirationType.TimeSpan:
                    success = _memcachedClient.Store(mode, key, val, LifeSpan);
                    break;
                default:
                    success = _memcachedClient.Store(mode, key, val);
                    break;
            }
            return success;
        }

        public override object RetrieveUsingRawKey(string key)
        {
            var result = _memcachedClient.Get(key);
            if (result != null && result is TemporalValue)
            {
                var staleValue = (TemporalValue)result;
                if (staleValue.IsValid())
                {
                    return staleValue.Value;
                }
                else
                {
                    return null;
                }
            }
            else if (SlidingExpiration && result != null)
            {
                Store(StoreMode.Replace, key, result, DateTimeOffset.Now);
            }
            return result;
        }
    }
}
