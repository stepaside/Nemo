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
using System.Threading.Tasks;
using System.Diagnostics;
using Nemo.Serialization;

namespace Nemo.Cache.Providers.Generic
{
    public abstract class MemcachedCacheProvider<T> : DistributedCacheProvider, IStaleCacheProvider
        where T : MemcachedClient
    {
        protected T _client;

        #region Constructors

        public MemcachedCacheProvider(CacheOptions options = null)
            : base(options)
        {
            _client = CreateClient(options);
        }
        
        #endregion

        protected abstract T CreateClient(CacheOptions options);
        
        public override bool IsOutOfProcess
        {
            get
            {
                return true;
            }
        }

        public override void Clear()
        {
            _client.FlushAll();
            LocalCache.Clear();
        }

        public override object Pop(string key)
        {
            key = ComputeKey(key);
            object result;
            if(!LocalCache.TryGetValue(key, out result))
            {
                result = _client.Get(key);
            }
            _client.Remove(key);
            return result;
        }

        public override bool Remove(string key)
        {
            key = ComputeKey(key);
            LocalCache.Remove(key);
            return _client.Remove(key);
        }

        public override bool Add(string key, object val)
        {
            key = ComputeKey(key);
            var now = DateTimeOffset.Now;
            if (LocalCache.ContainsKey(key))
            {
                LocalCache.Add(key, val);
            }
            var success = Store(StoreMode.Add, key, (CacheValue)val, now);
            return success;
        }

        public override bool Set(string key, object val)
        {
            key = ComputeKey(key);
            LocalCache[key] = val;
            var now = DateTimeOffset.Now;
#if DEBUG
            Store(StoreMode.Set, key, (CacheValue)val, now);
#else
            Task.Run(() => Store(StoreMode.Set, key, (CacheValue)val, now));
#endif
            return true;
        }

        public override bool Set(IDictionary<string, object> items)
        {
            var keys = ComputeKey(items.Keys);
            var now = DateTimeOffset.Now;
            
            foreach (var k in keys)
            {
                LocalCache[k.Key] = items[k.Value];
#if DEBUG
                Store(StoreMode.Set, k.Key, (CacheValue)items[k.Value], now);
#else
                Task.Run(() => Store(StoreMode.Set, k.Key, (CacheValue)items[k.Value], now));
#endif
            }

            return true;
        }
        
        public override object Get(string key)
        {
            var originalKey = key;
            key = ComputeKey(key);
            object result;
            if (!LocalCache.TryGetValue(key, out result))
            {
                var value = _client.Get<byte[]>(key);
                if (value != null)
                {
                    result = ProcessRetrieve(value, key, originalKey);
                }
            }
            return result;
        }

        public override IDictionary<string, object> Get(IEnumerable<string> keys)
        {
            IDictionary<string, object> items = new ConcurrentDictionary<string, object>();
            if (keys.Any())
            {
                var computedKeys = ComputeKey(keys);
                var localCache = LocalCache;
                
                Parallel.ForEach(computedKeys.Keys, k =>
                {
                    object value;
                    if (localCache.TryGetValue(k, out value))
                    {
                        items[k] = value;
                    }
                });

                if (items.Count < computedKeys.Count)
                {
                    var missingItems = _client.Get(items.Count == 0 ? computedKeys.Keys : computedKeys.Keys.Except(items.Keys));

                    Parallel.ForEach(computedKeys.Keys, k =>
                    {
                        object item;
                        if (missingItems.TryGetValue(k, out item))
                        {
                            var originalKey = computedKeys[k];
                            item = ProcessRetrieve((byte[])item, k, originalKey);
                            if (item != null)
                            {
                                items[originalKey] = item;
                            }
                        }
                    });
                }
            }
            return items;
        }
        
        public object GetStale(string key)
        {
            var originalKey = key;
            key = ComputeKey(key);
            object result;
            if (!LocalCache.TryGetValue(key, out result))
            {
                var item = _client.Get<byte[]>(key);
                if (item != null && this is IStaleCacheProvider)
                {
                    var temporal = CacheValue.FromBytes(item);
                    result = temporal.Buffer;
                }

                if (result != null)
                {
                    var value = CacheValue.FromBytes((byte[])result);
                    LocalCache[key] = value.QueryKey ? (CacheItem)new CacheIndex(originalKey, value) : (CacheItem)new CacheDataObject(originalKey, value);
                }
            }
            return result;
        }

        public IDictionary<string, object> GetStale(IEnumerable<string> keys)
        {
            IDictionary<string, object> items = new ConcurrentDictionary<string, object>();
            if (keys.Any())
            {
                var computedKeys = ComputeKey(keys);
                var localCache = LocalCache;
                
                Parallel.ForEach(computedKeys.Keys, k =>
                {
                    object value;
                    if (localCache.TryGetValue(k, out value))
                    {
                        items[k] = value;
                    }
                });

                if (items.Count < computedKeys.Count)
                {
                    var missingItems = _client.Get(items.Count == 0 ? computedKeys.Keys : computedKeys.Keys.Except(items.Keys));
                    
                    Parallel.ForEach(computedKeys.Keys, k =>
                    {
                        var originalKey = computedKeys[k];
                        object item;
                        missingItems.TryGetValue(k, out item);
                        
                        if (item != null && this is IStaleCacheProvider)
                        {
                            var temporal = CacheValue.FromBytes((byte[])item);
                            item = temporal.Buffer;
                        }

                        if (item != null)
                        {
                            items[originalKey] = item;
                            var value = CacheValue.FromBytes((byte[])item);
                            localCache[k] = value.QueryKey ? (CacheItem)new CacheIndex(originalKey, value) : (CacheItem)new CacheDataObject(originalKey, value);
                        }
                    });
                }
            }
            return items;
        }
        
        private CacheValue ProcessRetrieve(byte[] result, string key, string originalKey)
        {
            var cacheValue = CacheValue.FromBytes(result);
            if (this is IStaleCacheProvider && !cacheValue.IsValid())
            {
                cacheValue = null;
            }

            if (cacheValue != null)
            {
                LocalCache[key] = cacheValue.QueryKey ? (CacheItem)new CacheIndex(originalKey, cacheValue) : (CacheItem)new CacheDataObject(originalKey, cacheValue);
            }

            if (cacheValue != null && SlidingExpiration)
            {
                var now = DateTimeOffset.Now;
#if DEBUG
                Store(StoreMode.Replace, key, cacheValue, now);
#else
                Task.Run(() => Store(StoreMode.Replace, key, cacheValue, now));
#endif
            }

            return cacheValue;
        }

        protected bool Store(StoreMode mode, string key, CacheValue cacheValue, DateTimeOffset currentDateTime)
        {
            var value = ComputeValue(cacheValue, currentDateTime);
            return Store(mode, key, value);
        }

        protected bool Store(StoreMode mode, string key, object value)
        {
            var success = false;
            switch (ExpirationType)
            {
                case CacheExpirationType.TimeOfDay:
                    success = _client.Store(mode, key, value, ExpiresAtSpecificTime.Value.DateTime);
                    break;
                case CacheExpirationType.Absolute:
                    success = _client.Store(mode, key, value, ExpiresAt.DateTime);
                    break;
                case CacheExpirationType.Sliding:
                    success = _client.Store(mode, key, value, LifeSpan);
                    break;
                default:
                    success = _client.Store(mode, key, value);
                    break;
            }
            return success;
        }

        public override bool TryAcquireLock(string key)
        {
            var originalKey = key;
            key = "STALE::" + ComputeKey(key);
            
            var value = Guid.NewGuid().ToString();
            var stored = _client.Store(StoreMode.Add, key, value, TimeSpan.FromSeconds(ConfigurationFactory.Configuration.DistributedLockTimeout));
            if (stored)
            {
                stored = string.CompareOrdinal(value, _client.Get<string>(key)) == 0;
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
            
            var removed = _client.Remove(key);
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
