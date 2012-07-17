using System.Collections.Generic;
using System.Runtime.Caching;
using Nemo.Collections.Extensions;

namespace Nemo.Caching.Providers
{
    public class LocalCacheProvider : CacheProvider
    {
        private static MemoryCache MemoryCache
        {
            get
            {
                return MemoryCache.Default;
            }
        }

        public LocalCacheProvider(CacheOptions options = null)
            : base(CacheType.Local, options)
        { }

        public override void RemoveAll()
        {
            var keys = new List<string>();
            var iter = ((IEnumerable<KeyValuePair<string, object>>)MemoryCache).GetEnumerator();
            while (iter.MoveNext())
            {
                keys.Add(iter.Current.Key.ToString());
            }
            keys.Run(key => MemoryCache.Remove(key));
        }

        public override object Remove(string key)
        {
            return MemoryCache.Remove(key);
        }

        public override bool Clear(string key)
        {
            return Remove(key) != null;
        }

        public override bool AddNew(string key, object val)
        {
            key = ComputeKey(key);
            var success = false;
            switch (ExpirationType)
            {
                case CacheExpirationType.TimeOfDay:
                    success = MemoryCache.Add(key, val, ExpiresAtSpecificTime);
                    break;
                case CacheExpirationType.DateTime:
                    success = MemoryCache.Add(key, val, ExpiresAt);
                    break;
                case CacheExpirationType.TimeSpan:
                    success = MemoryCache.Add(key, val, new CacheItemPolicy { SlidingExpiration = LifeSpan });
                    break;
                default:
                    success = MemoryCache.Add(key, val, new CacheItemPolicy { SlidingExpiration = System.Runtime.Caching.MemoryCache.NoSlidingExpiration, AbsoluteExpiration = System.Runtime.Caching.MemoryCache.InfiniteAbsoluteExpiration });
                    break;
            }
            return success;
        }

        public override bool Save(string key, object val)
        {
            key = ComputeKey(key);
            var success = SaveImplementation(key, val);
            return success;
        }

        public override bool Save(IDictionary<string, object> items)
        {
            var keys = ComputeKey(items.Keys);
            var success = true;
            foreach (var k in keys)
            {
                success = SaveImplementation(k.Key, items[k.Value]) && success;
            }
            return success;
        }

        private bool SaveImplementation(string key, object val)
        {
            var success = true;
            switch (ExpirationType)
            {
                case CacheExpirationType.TimeOfDay:
                    MemoryCache.Set(key, val, ExpiresAtSpecificTime);
                    break;
                case CacheExpirationType.DateTime:
                    MemoryCache.Set(key, val, ExpiresAt);
                    break;
                case CacheExpirationType.TimeSpan:
                    MemoryCache.Set(key, val, new CacheItemPolicy { SlidingExpiration = LifeSpan });
                    break;
                default:
                    MemoryCache.Set(key, val, new CacheItemPolicy { SlidingExpiration = System.Runtime.Caching.MemoryCache.NoSlidingExpiration, AbsoluteExpiration = System.Runtime.Caching.MemoryCache.InfiniteAbsoluteExpiration });
                    break;
            }
            return success;
        }

        public override object Retrieve(string key)
        {
            key = ComputeKey(key);
            return MemoryCache.Get(key);
        }

        public override IDictionary<string, object> Retrieve(IEnumerable<string> keys)
        {
            var computedKeys = ComputeKey(keys);
            return MemoryCache.GetValues(keys);
        }
    }
}
