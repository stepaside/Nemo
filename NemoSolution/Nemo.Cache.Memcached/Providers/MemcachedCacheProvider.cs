using Enyim.Caching;
using Enyim.Caching.Memcached;
using Nemo.Cache.Providers.Generic;
using Nemo.Extensions;
using Nemo.Utilities;
using System;
using System.Collections.Concurrent;

namespace Nemo.Cache.Providers
{
    public class MemcachedCacheProvider : MemcachedCacheProvider<MemcachedClient>
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

        #region Constructors

        public MemcachedCacheProvider(CacheOptions options = null)
            : base(options)
        { }

        #endregion

        protected override MemcachedClient CreateClient(CacheOptions options)
        {
            return GetMemcachedClient(options != null ? options.ClusterName : DefaultClusterName);
        }

        public override bool Touch(string key, TimeSpan lifeSpan)
        {
            var success = false;
            key = ComputeKey(key);
            var result = _client.Get(key);
            if (result != null)
            {
                success = _client.Store(StoreMode.Replace, key, result, lifeSpan);
            }
            return success;
        }
    }
}
