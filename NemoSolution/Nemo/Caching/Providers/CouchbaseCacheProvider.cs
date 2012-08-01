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
    public class CouchbaseCacheProvider : DistributedCacheProvider<CouchbaseCacheProvider>, IDistributedCounter
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

        // This one is used to provide lock management
        internal CouchbaseCacheProvider(string bucketName, string bucketPassword)
            : base(null, CacheType.Couchbase, null) 
        {
            _couchbaseClient = GetCouchbaseClient(bucketName, bucketPassword);
        }

        public CouchbaseCacheProvider(CacheOptions options = null)
            : base(new CouchbaseCacheProvider(options != null ? options.ClusterName : DefaultBucketName, options != null ? options.ClusterPassword : null), CacheType.Couchbase, options)
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

        public override bool CheckAndSave(string key, object val, ulong cas)
        {
            key = ComputeKey(key);
            val = ComputeValue(val, DateTimeOffset.Now);
            CasResult<bool> result;
            switch (ExpirationType)
            {
                case CacheExpirationType.TimeOfDay:
                    result = _couchbaseClient.Cas(StoreMode.Set, key, val, ExpiresAtSpecificTime.Value.DateTime, cas);
                    break;
                case CacheExpirationType.DateTime:
                    result = _couchbaseClient.Cas(StoreMode.Set, key, val, ExpiresAt.DateTime, cas);
                    break;
                case CacheExpirationType.TimeSpan:
                    result = _couchbaseClient.Cas(StoreMode.Set, key, val, LifeSpan, cas);
                    break;
                default:
                    result = _couchbaseClient.Cas(StoreMode.Set, key, val, cas);
                    break;
            }
            return result.Result;
        }

        public override Tuple<object, ulong> RetrieveWithCas(string key)
        {
            key = ComputeKey(key);
            var casResult = _couchbaseClient.GetWithCas(key);
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

        public override IDictionary<string, Tuple<object, ulong>> RetrieveWithCas(IEnumerable<string> keys)
        {
            var computedKeys = ComputeKey(keys);
            var items = _couchbaseClient.GetWithCas(computedKeys.Keys);
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
            _couchbaseClient.FlushAll();
        }

        public override void RemoveByNamespace()
        {
            if (!string.IsNullOrEmpty(_cacheNamespace))
            {
                _namespaceVersion = _couchbaseClient.Increment(_cacheNamespace, 1, 1);
            }
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

        public override object RetrieveStale(string key)
        {
            key = ComputeKey(key);
            var result = _couchbaseClient.Get(key);
            return ((TemporalValue)result).Value;
        }

        public override IDictionary<string, object> RetrieveStale(IEnumerable<string> keys)
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
    }
}
