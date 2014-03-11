using Couchbase;
using Enyim.Caching.Memcached;
using Nemo.Cache.Providers.Generic;
using Nemo.Extensions;
using Nemo.Utilities;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Cache.Providers
{
    public class CouchbaseCacheProvider : MemcachedCacheProvider<CouchbaseClient>, IPersistentCacheProvider
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

        #region Constructors

        public CouchbaseCacheProvider(CacheOptions options = null)
            : base(options)
        { }

        #endregion

        protected override CouchbaseClient CreateClient(CacheOptions options)
        {
            return GetCouchbaseClient(options != null ? options.ClusterName : DefaultBucketName, options != null ? options.ClusterPassword : null);
        }

        public override bool Touch(string key, TimeSpan lifeSpan)
        {
            key = ComputeKey(key);
            _client.Touch(key, lifeSpan);
            return true;
        }

        #region IPersistentCacheProvider Methods

        bool IPersistentCacheProvider.Append(string key, string value)
        {
            key = ComputeKey(key);
            var data = new ArraySegment<byte>(Encoding.UTF8.GetBytes(value));
            var result = _client.Append(key, data);
            if (!result)
            {
                result = Store(StoreMode.Add, key, value);
            }
            if (!result)
            {
                result = _client.Append(key, data);
            }
            return result;
        }

        bool IPersistentCacheProvider.Set(string key, object value, object version)
        {
            key = ComputeKey(key);
            var buffer = ((CacheValue)value).ToBytes();
            CasResult<bool> result;
            switch (ExpirationType)
            {
                case CacheExpirationType.TimeOfDay:
                    result = _client.Cas(StoreMode.Set, key, buffer, ExpiresAtSpecificTime.Value.DateTime, (ulong)version);
                    break;
                case CacheExpirationType.Absolute:
                    result = _client.Cas(StoreMode.Set, key, buffer, ExpiresAt.DateTime, (ulong)version);
                    break;
                case CacheExpirationType.Sliding:
                    result = _client.Cas(StoreMode.Set, key, buffer, LifeSpan, (ulong)version);
                    break;
                default:
                    result = _client.Cas(StoreMode.Set, key, buffer, (ulong)version);
                    break;
            }
            return result.Result;
        }

        object IPersistentCacheProvider.Get(string key, out object version)
        {
            key = ComputeKey(key);
            var result = _client.GetWithCas<byte[]>(key);
            version = result.Cas;
            return CacheValue.FromBytes(result.Result);
        }

        IDictionary<string, object> IPersistentCacheProvider.Get(IEnumerable<string> keys, out IDictionary<string, object> versions)
        {
            var items = new Dictionary<string, object>();
            versions = null;
            if (keys.Any())
            {
                var computedKeys = ComputeKey(keys);
                var result = _client.GetWithCas(computedKeys.Keys);
                versions = new Dictionary<string, object>();
                foreach (var item in result)
                {
                    var itemKey = computedKeys[item.Key];
                    items.Add(itemKey, CacheValue.FromBytes((byte[])item.Value.Result));
                    versions.Add(itemKey, item.Value.Cas);
                }
            }
            return items;
        }

        #endregion
    }
}
