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
            var success = Store(StoreMode.Add, key, (CacheValue)val, now);
            return success;
        }

        public override bool Save(string key, object val)
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

        public override bool Save(IDictionary<string, object> items)
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
                    result = ProcessRetrieve(value, key, originalKey, LocalCache, this.ExpectedVersion);
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
                var expectedVersion = this.ExpectedVersion;
                
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
                    var revisions = GetRevisions(missingItems.Keys);

                    Parallel.ForEach(computedKeys.Keys, k =>
                    {
                        object item;
                        if (missingItems.TryGetValue(k, out item))
                        {
                            var originalKey = computedKeys[k];
                            item = ProcessRetrieve((byte[])item, k, originalKey, localCache, expectedVersion);
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
                    var temporal = CacheValue.FromBytes(item);
                    result = temporal.Buffer;
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
                            var temporal = CacheValue.FromBytes((byte[])item);
                            item = temporal.Buffer;
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
                var ticks = (ulong)GetTicks();
                return _client.Store(StoreMode.Add, key, ticks) ? ticks : GetRevision(key);
            }
            else
            {
                return (ulong)value;
            }
        }

        public IDictionary<string, ulong> GetRevisions(IEnumerable<string> keys)
        {
            var keyArray = keys.Select(k => "REVISION::" + k).ToArray();
            var values = _client.Get(keyArray);
            if (values != null)
            {
                var missingKeys = new List<string>();
                var items = new Dictionary<string, ulong>();
                for (int i = 0; i < keyArray.Length; i++ )
                {
                    var key = keyArray[i];
                    object value;
                    if (values.TryGetValue(key, out value) && value != null)
                    {
                        items.Add(key, Convert.ToUInt64(value));
                    }
                    else
                    {
                        missingKeys.Add(key);
                    }
                }

                foreach (var key in missingKeys)
                {
                    var ticks = (ulong)GetTicks();
                    items.Add(key, _client.Store(StoreMode.Add, key, ticks) ? ticks : GetRevision(key));
                }

                return items;
            }
            return null;
        }

        public ulong IncrementRevision(string key, ulong delta = 1)
        {
            key = "REVISION::" + key;
            return _client.Increment(key, 1, delta == 0 ? 1 : delta);
        }

        public ulong[] ExpectedVersion { get; set; }

        #endregion

        private CacheValue ProcessRetrieve(byte[] result, string key, string originalKey, IDictionary<string, object> localCache, ulong[] expectedRevision)
        {
            var cacheValue = CacheValue.FromBytes(result);
            if (this is IStaleCacheProvider && !cacheValue.IsValid())
            {
                cacheValue = null;
            }

            if (cacheValue != null && !cacheValue.IsValidVersion(expectedRevision))
            {
                cacheValue = null;
            }

            if (cacheValue != null)
            {
                localCache[key] = new CacheItem(originalKey, cacheValue);
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

        private bool Store(StoreMode mode, string key, CacheValue val, DateTimeOffset currentDateTime)
        {
            var success = false;
            var value = ComputeValue(val, currentDateTime);
            switch (ExpirationType)
            {
                case CacheExpirationType.TimeOfDay:
                    success = _client.Store(mode, key, value, ExpiresAtSpecificTime.Value.DateTime);
                    break;
                case CacheExpirationType.DateTime:
                    success = _client.Store(mode, key, value, ExpiresAt.DateTime);
                    break;
                case CacheExpirationType.TimeSpan:
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
