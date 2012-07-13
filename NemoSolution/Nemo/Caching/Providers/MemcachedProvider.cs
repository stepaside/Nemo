using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Enyim.Caching;
using Enyim.Caching.Memcached;
using Nemo.Extensions;
using Nemo.Utilities;

namespace Nemo.Caching.Providers
{
    public class MemcachedProvider : DistributedCacheProvider<MemcachedProvider>
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

        private MemcachedClient _memcachedClient;

        #region Constructors

        // This one is used to provide lock management
        internal MemcachedProvider(string clusterName) : base(null, CacheType.Memcached, null) 
        {
            _memcachedClient = GetMemcachedClient(clusterName);
        }

        public MemcachedProvider(CacheOptions options = null)
            : base(new MemcachedProvider(options != null ? options.ClusterName : DefaultClusterName), CacheType.Memcached, options)
        {
            _memcachedClient = GetMemcachedClient(options != null ? options.ClusterName : DefaultClusterName);
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
                    result = _memcachedClient.Cas(StoreMode.Set, key, val, ExpiresAtSpecificTime.Value.DateTime, cas);
                    break;
                case CacheExpirationType.DateTime:
                    result = _memcachedClient.Cas(StoreMode.Set, key, val, ExpiresAt.DateTime, cas);
                    break;
                case CacheExpirationType.TimeSpan:
                    result = _memcachedClient.Cas(StoreMode.Set, key, val, LifeSpan, cas);
                    break;
                default:
                    result = _memcachedClient.Cas(StoreMode.Set, key, val, cas);
                    break;
            }
            return result.Result;
        }

        public override Tuple<object, ulong> RetrieveWithCas(string key)
        {
            key = ComputeKey(key);
            var result = _memcachedClient.GetWithCas(key);
            return Tuple.Create(result.Result, result.Cas);
        }

        public override IDictionary<string, Tuple<object, ulong>> RetrieveWithCas(IEnumerable<string> keys)
        {
            var computedKeys = ComputeKey(keys);
            var items = _memcachedClient.GetWithCas(computedKeys.Keys);
            var result = items.ToDictionary(i => computedKeys[i.Key], i => Tuple.Create(i.Value.Result, i.Value.Cas));
            return result;
        }

        public override void RemoveAll()
        {
            _memcachedClient.FlushAll();
        }

        public override void RemoveByNamespace()
        {
            if (!string.IsNullOrEmpty(_cacheNamespace))
            {
                _namespaceVersion = _memcachedClient.Increment(_cacheNamespace, 1, 1);
            }
        }

        public override object Remove(string key)
        {
            key = ComputeKey(key);
            var result = _memcachedClient.Get(key);
            _memcachedClient.Remove(key);
            return result;
        }

        public override bool Clear(string key)
        {
            key = ComputeKey(key);
            return _memcachedClient.Remove(key);
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
            var result = _memcachedClient.Get(key);
            if (SlidingExpiration)
            {
                Store(StoreMode.Replace, key, result);
            }
            return result;
        }

        public override IDictionary<string, object> Retrieve(IEnumerable<string> keys)
        {
            var computedKeys = ComputeKey(keys);
            var items = _memcachedClient.Get(computedKeys.Keys);
            items = items.ToDictionary(i => computedKeys[i.Key], i => i.Value);
            return items;
        }

        public override bool Touch(string key, TimeSpan lifeSpan)
        {
            throw new NotImplementedException();
        }

        private bool Store(StoreMode mode, string key, object val)
        {
            var success = false;
            switch (ExpirationType)
            {
                case CacheExpirationType.TimeOfDay:
                    success = _memcachedClient.Store(mode, key, val, ExpiresAtSpecificTime.Value.DateTime);
                    break;
                case CacheExpirationType.DateTime:
                    success = _memcachedClient.Store(mode, key, val, ExpiresAt.DateTime);
                    break;
                case CacheExpirationType.TimeSpan:
                    success = _memcachedClient.Store(mode, key, val, LifeSpan);
                    break;
                default:
                    success = _memcachedClient.Store(mode, key, val);
                    break;
            }
            return success;
        }
    }
}
