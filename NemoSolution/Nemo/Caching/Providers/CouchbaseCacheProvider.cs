using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Nemo.Extensions;
using Nemo.Utilities;
using Couchbase;
using Enyim.Caching.Memcached;

namespace Nemo.Caching.Providers
{
    public class CouchbaseCacheProvider : DistributedCacheProvider, IDistributedCounter, IStaleCacheProvider, IPersistentCacheProvider
    {
        #region Static Declarations

        private static ConcurrentDictionary<Tuple<string, string>, CouchbaseClient> _couchbaseClientList = new ConcurrentDictionary<Tuple<string, string>, CouchbaseClient>();

        public static CouchbaseClient GetCouchbaseClient(string bucketName, string bucketPassword = null)
        {
            CouchbaseClient memcachedClient = null;
            bucketName = !string.IsNullOrEmpty(bucketName) ? bucketName : DefaultBucketName;
            if (bucketName.NullIfEmpty() != null)
            {
                memcachedClient = _couchbaseClientList.GetOrAdd(Tuple.Create(bucketName, bucketPassword), t => new CouchbaseClient(t.Item1, t.Item2));
            }
            return memcachedClient;
        }

        public static string DefaultBucketName
        {
            get
            {
                return Config.AppSettings("CouchbaseCacheProvider.DefaultBucketName", "default");
            }
        }
        
        #endregion

        private CouchbaseClient _couchbaseClient;

        #region Constructors

        public CouchbaseCacheProvider(CacheOptions options = null)
            : base(options)
        {
            _couchbaseClient = GetCouchbaseClient(options != null ? options.ClusterName : DefaultBucketName, options != null ? options.ClusterPassword : null);
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
            _couchbaseClient.FlushAll();
        }

        public override object Remove(string key)
        {
            key = ComputeKey(key);
            var result = _couchbaseClient.Get(key);
            _couchbaseClient.Remove(key);
            return result;
        }

        public override bool Clear(string key)
        {
            key = ComputeKey(key);
            return _couchbaseClient.Remove(key);
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
            var result = _couchbaseClient.Get(key);
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
            var items = _couchbaseClient.Get(computedKeys.Keys);
            items = items.Where(i => !(i.Value is TemporalValue) || ((TemporalValue)i.Value).IsValid()).ToDictionary(i => computedKeys[i.Key], i => i.Value is TemporalValue ? ((TemporalValue)i.Value).Value : i.Value);
            return items;
        }

        public override bool Touch(string key, TimeSpan lifeSpan)
        {
            _couchbaseClient.Touch(key, lifeSpan);
            return true;
        }

        public object RetrieveStale(string key)
        {
            key = ComputeKey(key);
            var result = _couchbaseClient.Get(key);
            return ((TemporalValue)result).Value;
        }

        public IDictionary<string, object> RetrieveStale(IEnumerable<string> keys)
        {
            var computedKeys = ComputeKey(keys);
            var items = _couchbaseClient.Get(computedKeys.Keys);
            return items.ToDictionary(i => computedKeys[i.Key], i => ((TemporalValue)i.Value).Value);
        }

        public ulong Increment(string key, ulong delta = 1)
        {
            return _couchbaseClient.Increment(key, 1, delta == 0 ? 1 : delta);
        }

        public ulong Decrement(string key, ulong delta = 1)
        {
            return _couchbaseClient.Decrement(key, 1, delta == 0 ? 1 : delta);
        }

        private bool Store(StoreMode mode, string key, object val, DateTimeOffset currentDateTime)
        {
            var success = false;
            val = ComputeValue(val, currentDateTime);
            switch (ExpirationType)
            {
                case CacheExpirationType.TimeOfDay:
                    success = _couchbaseClient.Store(mode, key, val, ExpiresAtSpecificTime.Value.DateTime);
                    break;
                case CacheExpirationType.DateTime:
                    success = _couchbaseClient.Store(mode, key, val, ExpiresAt.DateTime);
                    break;
                case CacheExpirationType.TimeSpan:
                    success = _couchbaseClient.Store(mode, key, val, LifeSpan);
                    break;
                default:
                    success = _couchbaseClient.Store(mode, key, val);
                    break;
            }
            return success;
        }

        public override bool TryAcquireLock(string key)
        {
            var originalKey = key;
            key = "STALE::" + ComputeKey(key);

            var value = Guid.NewGuid().ToString();
            var stored = _couchbaseClient.Store(StoreMode.Add, key, value);
            if (stored)
            {
                stored = string.CompareOrdinal(value, _couchbaseClient.Get<string>(key)) == 0;
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

            var removed = _couchbaseClient.Remove(key);
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
