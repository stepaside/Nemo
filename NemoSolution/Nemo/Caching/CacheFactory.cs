using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nemo.Caching.Providers;

namespace Nemo.Caching
{
    public enum CacheType { None, ExecutionContext, Local, Memcached, File, DistributedLocal, Redis, Couchbase }

    public enum CacheExpirationType { Never, DateTime, TimeSpan, TimeOfDay };

    public class CacheFactory
    {
        private static HashSet<string> _cacheTypes = new HashSet<string>(Enum.GetNames(typeof(CacheType)), StringComparer.OrdinalIgnoreCase);

        public static CacheType CacheTypeFromString(string cacheType)
        {
            if (_cacheTypes.Contains(cacheType))
            {
                return (CacheType)Enum.Parse(typeof(CacheType), cacheType, true);
            }
            return CacheType.None;
        }

        public static CacheProvider GetProvider(CacheType cacheType, CacheOptions options = null)
        {
            switch (cacheType)
            {
                case CacheType.File:
                    return new FileCacheProvider(options);
                case CacheType.ExecutionContext:
                    return new ExecutionContextCacheProvider(options);
                case CacheType.Memcached:
                    return new MemcachedProvider(options);
                case CacheType.Local:
                    return new LocalCacheProvider(options);
                case CacheType.DistributedLocal:
                    return new DistributedLocalCacheProvider(options);
                case CacheType.Redis:
                    return new RedisCacheProvider(options);
                case CacheType.Couchbase:
                    return new CouchbaseCacheProvider(options);
            }
            return null;
        }
    }
}
