using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nemo.Extensions;
using Enyim.Caching;
using Enyim.Caching.Memcached;
using System.Runtime.Caching;

namespace Nemo.Caching.Providers
{
    public class DistributedLocalCacheProvider : DistributedCacheProvider<MemcachedProvider>
    {
        private MemoryCache MemoryCache = MemoryCache.Default;
        private MemcachedClient _memcachedClient;

        private const string TIMESTAMP_GLOBAL = "TIMESTAMP_GLOBAL::";
        private const string TIMESTAMP_LOCAL = "TIMESTAMP_LOCAL::";

        public DistributedLocalCacheProvider(CacheOptions options = null)
            : base(new MemcachedProvider(options != null ? options.ClusterName : MemcachedProvider.DefaultClusterName), CacheType.DistributedLocal, options)
        {
            _memcachedClient = MemcachedProvider.GetMemcachedClient(options != null ? options.ClusterName : MemcachedProvider.DefaultClusterName);
        }

        public override bool CheckAndSave(string key, object val, ulong cas)
        {
            throw new NotSupportedException();
        }

        public override Tuple<object, ulong> RetrieveWithCas(string key)
        {
            throw new NotSupportedException();
        }

        public override IDictionary<string, Tuple<object, ulong>> RetrieveWithCas(IEnumerable<string> keys)
        {
            throw new NotSupportedException();
        }

        public override void RemoveAll()
        {
            throw new NotSupportedException();
        }

        public override object Remove(string key)
        {
            key = ComputeKey(key);
            MemoryCache.Remove(TIMESTAMP_LOCAL + key);
            var result = MemoryCache.Remove(key);
            _memcachedClient.Remove(TIMESTAMP_GLOBAL + key);
            return result;
        }

        public override bool Clear(string key)
        {
            key = ComputeKey(key);
            var success = MemoryCache.Remove(TIMESTAMP_LOCAL + key) != null;
            success = MemoryCache.Remove(key) != null && success;
            success = _memcachedClient.Remove(TIMESTAMP_GLOBAL + key) && success;
            return success;
        }

        public override bool AddNew(string key, object val)
        {
            key = ComputeKey(key);
            var success = true;
            var globalTimestamp = _memcachedClient.Get(TIMESTAMP_GLOBAL + key);
            var localTimestamp = MemoryCache.Get(TIMESTAMP_LOCAL + key);
            // if global timestamp exists and local timestamp either null or less than global timestamp
            // then update local cache
            if (globalTimestamp != null && (DateTimeOffset)globalTimestamp >= DateTimeOffset.Now && (localTimestamp == null || (DateTimeOffset)localTimestamp <= (DateTimeOffset)globalTimestamp))
            {
                success = MemoryCache.Add(key, ComputeValue(val, DateTimeOffset.Now), (DateTimeOffset)globalTimestamp);
                MemoryCache[TIMESTAMP_LOCAL + key] = globalTimestamp;
            }
            else
            {
                var timestamp = ComputeTimestamp();
                success = _memcachedClient.Store(StoreMode.Set, TIMESTAMP_GLOBAL + key, timestamp);
                success = success && MemoryCache.Add(key, ComputeValue(val, DateTimeOffset.Now), timestamp);
                MemoryCache[TIMESTAMP_LOCAL + key] = timestamp;
            }
            return success;
        }

        public override bool Save(string key, object val)
        {
            key = ComputeKey(key);
            var success = true;
            var globalTimestamp = _memcachedClient.Get(TIMESTAMP_GLOBAL + key);
            var localTimestamp = MemoryCache.Get(TIMESTAMP_LOCAL + key);
            // if global timestamp exists and local timestamp either null or less than global timestamp
            // then update local cache
            if (globalTimestamp != null && (DateTimeOffset)globalTimestamp >= DateTimeOffset.Now && (localTimestamp == null || (DateTimeOffset)localTimestamp <= (DateTimeOffset)globalTimestamp))
            {
                MemoryCache.Set(key, ComputeValue(val, DateTimeOffset.Now), (DateTimeOffset)globalTimestamp);
                MemoryCache[TIMESTAMP_LOCAL + key] = globalTimestamp;
            }
            else
            {
                var timestamp = ComputeTimestamp();
                success = _memcachedClient.Store(StoreMode.Set, TIMESTAMP_GLOBAL + key, timestamp);
                MemoryCache.Set(key, ComputeValue(val, DateTimeOffset.Now), timestamp);
                MemoryCache[TIMESTAMP_LOCAL + key] = timestamp;
            }
            return success;
        }

        public override bool Save(IDictionary<string, object> items)
        {
            var computedKeys = ComputeKey(items.Keys);
            var success = true;
            var currentDateTime = DateTimeOffset.Now;
            var timestamp = ComputeTimestamp();
            var globalTimestamps = _memcachedClient.Get(computedKeys.Keys.Select(key => TIMESTAMP_GLOBAL + key));
            foreach (var item in items)
            {
                object globalTimestamp = null;
                globalTimestamps.TryGetValue(TIMESTAMP_GLOBAL + item.Key, out globalTimestamp);
                var localTimestamp = MemoryCache.Get(TIMESTAMP_LOCAL + item.Key);
                if (globalTimestamp != null && (DateTimeOffset)globalTimestamp >= DateTimeOffset.Now && (localTimestamp == null || (DateTimeOffset)localTimestamp <= (DateTimeOffset)globalTimestamp))
                {
                    MemoryCache.Set(item.Key, ComputeValue(item.Value, currentDateTime), (DateTimeOffset)globalTimestamp);
                    MemoryCache[TIMESTAMP_LOCAL + item.Key] = globalTimestamp;
                }
                else
                {
                    success = success && _memcachedClient.Store(StoreMode.Set, TIMESTAMP_GLOBAL + item.Key, timestamp);
                    MemoryCache.Set(item.Key, ComputeValue(item.Value, currentDateTime), timestamp);
                    MemoryCache[TIMESTAMP_LOCAL + item.Key] = timestamp;
                }
            }
            return success;
        }

        public override object Retrieve(string key)
        {
            return Retrieve(key, false);
        }

        public override IDictionary<string, object> Retrieve(IEnumerable<string> keys)
        {
            return Retrieve(keys, false);
        }

        public override bool Touch(string key, TimeSpan lifeSpan)
        {
            throw new NotImplementedException();
        }

        public override object RetrieveStale(string key)
        {
            return Retrieve(key, true);
        }

        public override IDictionary<string, object> RetrieveStale(IEnumerable<string> keys)
        {
            return Retrieve(keys, true);
        }

        private DateTimeOffset ComputeTimestamp()
        {
            switch (ExpirationType)
            {
                case CacheExpirationType.TimeOfDay:
                    return ExpiresAtSpecificTime;
                case CacheExpirationType.DateTime:
                    return ExpiresAt;
                case CacheExpirationType.TimeSpan:
                    return DateTimeOffset.Now.Add(LifeSpan);
                default:
                    return DateTimeOffset.MaxValue;
            }
        }

        private object Retrieve(string key, bool allowStale)
        {
            key = ComputeKey(key);
            object result = null;
            var globalTimestamp = _memcachedClient.Get(TIMESTAMP_GLOBAL + key);
            var localTimestamp = MemoryCache.Get(TIMESTAMP_LOCAL + key);
            if (localTimestamp != null && globalTimestamp != null && (DateTimeOffset)localTimestamp == (DateTimeOffset)globalTimestamp)
            {
                result = MemoryCache.Get(key);
                // if local cache expires we need to remove the global timestamp
                // thus expiring all local caches
                if (result == null)
                {
                    _memcachedClient.Remove(TIMESTAMP_GLOBAL + key);
                }
                else
                {
                    if (!allowStale && result is TemporalValue)
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
                }
            }
            else
            {
                MemoryCache.Remove(key);
            }

            return result;
        }

        private IDictionary<string, object> Retrieve(IEnumerable<string> keys, bool allowStale)
        {
            var computedKeys = ComputeKey(keys);
            var items = new Dictionary<string, object>();
            var globalTimestamps = _memcachedClient.Get(computedKeys.Keys.Select(n => TIMESTAMP_GLOBAL + n));
            var globalClear = false;
            foreach (var key in computedKeys.Keys)
            {
                object result = null;
                object globalTimestamp = null;
                globalTimestamps.TryGetValue(TIMESTAMP_GLOBAL + key, out globalTimestamp);
                var localTimestamp = MemoryCache.Get(TIMESTAMP_LOCAL + key);
                if (!globalClear && localTimestamp != null && globalTimestamp != null && (DateTimeOffset)localTimestamp == (DateTimeOffset)globalTimestamp)
                {
                    result = MemoryCache.Get(key);
                    // if local cache expires we need to remove the global timestamp
                    // thus expiring all local caches
                    if (result == null)
                    {
                        globalClear = true;
                    }
                    else
                    {
                        if (!allowStale && result is TemporalValue)
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
                    }
                }
                else
                {
                    MemoryCache.Remove(key);
                }

                if (globalClear)
                {
                    _memcachedClient.Remove(TIMESTAMP_GLOBAL + key);
                }
                else
                {
                    if (result != null)
                    {
                        items[key] = result;
                    }
                }
            }

            if (globalClear)
            {
                items.Clear();
            }

            items = items.ToDictionary(i => computedKeys[i.Key], i => i.Value);
            return items;
        }
    }
}
