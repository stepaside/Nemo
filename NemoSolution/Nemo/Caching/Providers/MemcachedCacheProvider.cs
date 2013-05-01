using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Enyim.Caching;
using Enyim.Caching.Memcached;
using Nemo.Collections.Extensions;
using Nemo.Configuration;
using Nemo.Extensions;
using Nemo.Utilities;

namespace Nemo.Caching.Providers
{
    public class MemcachedCacheProvider : DistributedCacheProvider, IDistributedCounter, IStaleCacheProvider
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

        public MemcachedCacheProvider(CacheOptions options = null)
            : base(options)
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

        public override void RemoveAll()
        {
            _memcachedClient.FlushAll();
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

        public override bool TryAcquireLock(string key)
        {
            var originalKey = key;
            key = "STALE::" + ComputeKey(key);
            
            var value = Guid.NewGuid().ToString();
            var stored = _memcachedClient.Store(StoreMode.Add, key, value);
            if (stored)
            {
                stored = string.CompareOrdinal(value, _memcachedClient.Get<string>(key)) == 0;
            }

            if (stored)
            {
                Log.Capture(() => string.Format("Acquired lock for {0}", originalKey));
            }
            else
            {
                Log.Capture(() => string.Format("Failed to acquire lock for {0}", originalKey));
            }
            return stored;
        }

        public override object WaitForItems(string key, int count = -1)
        {
            return null;
        }

        public override bool ReleaseLock(string key)
        {
            var originalKey = key;
            key = "STALE::" + ComputeKey(key);
            
            var removed = _memcachedClient.Remove(key);
            if (removed)
            {
                Log.Capture(() => string.Format("Removed lock for {0}", originalKey));
            }
            else
            {
                Log.Capture(() => string.Format("Failed to remove lock for {0}", originalKey));
            }
            return removed;
        }
    }
}
