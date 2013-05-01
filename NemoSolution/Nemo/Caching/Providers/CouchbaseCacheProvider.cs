using Couchbase;
using Nemo.Caching.Providers.Generic;
using Nemo.Extensions;
using Nemo.Utilities;
using System;
using System.Collections.Concurrent;

namespace Nemo.Caching.Providers
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
            _client.Touch(key, lifeSpan);
            return true;
        }
    }
}
