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
    public class CouchbaseCacheProvider : DistributedCacheProvider<CouchbaseCacheProvider>
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
            var result = _couchbaseClient.GetWithCas(key);
            return Tuple.Create(result.Result, result.Cas);
        }

        public override IDictionary<string, Tuple<object, ulong>> RetrieveWithCas(IEnumerable<string> keys)
        {
            var computedKeys = ComputeKey(keys);
            var items = _couchbaseClient.GetWithCas(computedKeys.Keys);
            var result = items.ToDictionary(i => computedKeys[i.Key], i => Tuple.Create(i.Value.Result, i.Value.Cas));
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
            var success = Store(StoreMode.Add, key, val);
            return success;
        }

        public override bool Save(string key, object val)
        {
            key = ComputeKey(key);
            var success = Store(StoreMode.Set, key, val);
            return success; ;
        }

        public override bool Save(IDictionary<string, object> items)
        {
            var keys = ComputeKey(items.Keys);
            var success = true;
            foreach (var k in keys)
            {
                success = success && Store(StoreMode.Set, k.Key, items[k.Value]);
            }
            return success;
        }

        public override object Retrieve(string key)
        {
            key = ComputeKey(key);
            var result = _couchbaseClient.Get(key);
            if (SlidingExpiration)
            {
                Store(StoreMode.Replace, key, result);
            }
            return result;
        }

        public override IDictionary<string, object> Retrieve(IEnumerable<string> keys)
        {
            var computedKeys = ComputeKey(keys);
            var items = _couchbaseClient.Get(computedKeys.Keys);
            items = items.ToDictionary(i => computedKeys[i.Key], i => i.Value);
            return items;
        }

        public override bool Touch(string key, TimeSpan lifeSpan)
        {
            _couchbaseClient.Touch(key, lifeSpan);
            return true;
        }

        private bool Store(StoreMode mode, string key, object val)
        {
            var success = false;
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
