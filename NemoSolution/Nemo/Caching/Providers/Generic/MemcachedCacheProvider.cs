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

namespace Nemo.Caching.Providers.Generic
{
    public abstract class MemcachedCacheProvider<T> : DistributedCacheProvider, IRevisionProvider, IStaleCacheProvider
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

        public override void RemoveAll()
        {
            _client.FlushAll();
            LocalCache.Clear();
        }

        public override object Remove(string key)
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

        public override bool Clear(string key)
        {
            key = ComputeKey(key);
            LocalCache.Remove(key);
            return _client.Remove(key);
        }

        public override bool AddNew(string key, object val)
        {
            key = ComputeKey(key);
            var now = DateTimeOffset.Now;
            if (LocalCache.ContainsKey(key))
            {
                LocalCache.Add(key, val);
            }
            var success = Store(StoreMode.Add, key, val, now);
            return success;
        }

        public override bool Save(string key, object val)
        {
            key = ComputeKey(key);
            LocalCache[key] = val;
            var now = DateTimeOffset.Now;
#if DEBUG
            Store(StoreMode.Set, key, val, now);
#else
            Task.Run(() => Store(StoreMode.Set, key, val, now));
#endif
            return true;
        }

        public override bool Save(IDictionary<string, object> items)
        {
            var keys = ComputeKey(items.Keys);
            var now = DateTimeOffset.Now;
            
            foreach (var k in keys)
            {
                LocalCache[k.Key] = items[k.Value];
#if DEBUG
                Store(StoreMode.Set, k.Key, items[k.Value], now);
#else
                Task.Run(() => Store(StoreMode.Set, k.Key, items[k.Value], now));
#endif
            }

            return true;
        }

        private byte[] ProcessRetrieve(byte[] result, string key, string originalKey, IDictionary<string, object> localCache)
        {
            if (this is IStaleCacheProvider)
            {
                var temporal = TemporalValue.FromBytes(result);
                if (temporal.IsValid())
                {
                    result = temporal.Value;
                }
                else
                {
                    result = null;
                }
            }

            if (result != null)
            {
                localCache[key] = new CacheItem(originalKey, result);
            }

            if (result != null && SlidingExpiration)
            {
                var now = DateTimeOffset.Now;
#if DEBUG
                Store(StoreMode.Replace, key, result, now);
#else
                Task.Run(() => Store(StoreMode.Replace, key, result, now));
#endif
            }

            return result;
        }

        public override object Retrieve(string key)
        {
            var originalKey = key;
            key = ComputeKey(key);
            object result;
            if (!LocalCache.TryGetValue(key, out result))
            {
                var value = _client.Get<byte[]>(key);
                if (value != null)
                {
                    result = ProcessRetrieve(value, key, originalKey, LocalCache);
                }
            }
            return result;
        }

        public override IDictionary<string, object> Retrieve(IEnumerable<string> keys)
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
                            item = ProcessRetrieve((byte[])item, k, originalKey, localCache);
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
        
        public object RetrieveStale(string key)
        {
            var originalKey = key;
            key = ComputeKey(key);
            object result;
            if (!LocalCache.TryGetValue(key, out result))
            {
                var item = _client.Get<byte[]>(key);
                if (item != null && this is IStaleCacheProvider)
                {
                    var temporal = TemporalValue.FromBytes(item);
                    result = temporal.Value;
                }

                if (result != null)
                {
                    LocalCache[key] = new CacheItem(originalKey, (byte[])result);
                }
            }
            return result;
        }

        public IDictionary<string, object> RetrieveStale(IEnumerable<string> keys)
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
                            var temporal = TemporalValue.FromBytes((byte[])item);
                            item = temporal.Value;
                        }

                        if (item != null)
                        {
                            items[originalKey] = item;
                            localCache[k] = new CacheItem(originalKey, (byte[])item);
                        }
                    });
                }
            }
            return items;
        }

        #region IRevisionProvider Members

        public ulong GetRevision(string key)
        {
            key = "REVISION::" + key;
            var value = _client.Get(key);
            if (value == null)
            {
                return _client.Store(StoreMode.Add, key, 1) ? 1 : GetRevision(key);
            }
            else
            {
                return (ulong)value;
            }
        }

        public ulong IncrementRevision(string key, ulong delta = 1)
        {
            key = "REVISION::" + key;
            return _client.Increment(key, 1, delta == 0 ? 1 : delta);
        }

        #endregion

        private bool Store(StoreMode mode, string key, object val, DateTimeOffset currentDateTime)
        {
            var success = false;
            val = ComputeValue((byte[])ExtractValue(val), currentDateTime);
            switch (ExpirationType)
            {
                case CacheExpirationType.TimeOfDay:
                    success = _client.Store(mode, key, val, ExpiresAtSpecificTime.Value.DateTime);
                    break;
                case CacheExpirationType.DateTime:
                    success = _client.Store(mode, key, val, ExpiresAt.DateTime);
                    break;
                case CacheExpirationType.TimeSpan:
                    success = _client.Store(mode, key, val, LifeSpan);
                    break;
                default:
                    success = _client.Store(mode, key, val);
                    break;
            }
            return success;
        }

        public override bool TryAcquireLock(string key)
        {
            var originalKey = key;
            key = "STALE::" + ComputeKey(key);
            
            var value = Guid.NewGuid().ToString();
            var stored = _client.Store(StoreMode.Add, key, value);
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
