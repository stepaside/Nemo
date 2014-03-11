using System.Collections.Generic;
using System.Runtime.Caching;
using Nemo.Collections.Extensions;
using System;

namespace Nemo.Cache.Providers
{
    public class MemoryCacheProvider : CacheProvider
    {
        private static MemoryCache MemoryCache
        {
            get
            {
                return MemoryCache.Default;
            }
        }

        public MemoryCacheProvider(CacheOptions options = null)
            : base(options)
        { }

        public override void Clear()
        {
            var keys = new List<string>();
            var iter = ((IEnumerable<KeyValuePair<string, object>>)MemoryCache).GetEnumerator();
            while (iter.MoveNext())
            {
                keys.Add(iter.Current.Key.ToString());
            }
            keys.Run(key => MemoryCache.Remove(key));
        }

        public override object Pop(string key)
        {
            return MemoryCache.Remove(key);
        }

        public override bool Remove(string key)
        {
            return Pop(key) != null;
        }

        public override bool Add(string key, object val)
        {
            key = ComputeKey(key);
            var success = false;
            switch (ExpirationType)
            {
                case CacheExpirationType.TimeOfDay:
                    try
                    {
                        success = MemoryCache.Add(key, val, ExpiresAtSpecificTime.Value);
                    }
                    catch (Exception ex)
                    {
                        success = false;
                    }
                    break;
                case CacheExpirationType.Absolute:
                    success = MemoryCache.Add(key, val, ExpiresAt);
                    break;
                case CacheExpirationType.Sliding:
                    success = MemoryCache.Add(key, val, new CacheItemPolicy { SlidingExpiration = LifeSpan });
                    break;
                default:
                    success = MemoryCache.Add(key, val, new CacheItemPolicy { SlidingExpiration = MemoryCache.NoSlidingExpiration, AbsoluteExpiration = MemoryCache.InfiniteAbsoluteExpiration });
                    break;
            }
            return success;
        }

        public override bool Set(string key, object val)
        {
            key = ComputeKey(key);
            var success = SetImplementation(key, val);
            return success;
        }

        public override bool Set(IDictionary<string, object> items)
        {
            var keys = ComputeKey(items.Keys);
            var success = true;
            foreach (var k in keys)
            {
                success = SetImplementation(k.Key, items[k.Value]) && success;
            }
            return success;
        }

        private bool SetImplementation(string key, object val)
        {
            var success = true;
            switch (ExpirationType)
            {
                case CacheExpirationType.TimeOfDay:
                    try
                    {
                        MemoryCache.Set(key, val, ExpiresAtSpecificTime.Value);
                    }
                    catch (Exception ex)
                    {
                        success = false;
                    }
                    break;
                case CacheExpirationType.Absolute:
                    MemoryCache.Set(key, val, ExpiresAt);
                    break;
                case CacheExpirationType.Sliding:
                    MemoryCache.Set(key, val, new CacheItemPolicy { SlidingExpiration = LifeSpan });
                    break;
                default:
                    MemoryCache.Set(key, val, new CacheItemPolicy { SlidingExpiration = MemoryCache.NoSlidingExpiration, AbsoluteExpiration = MemoryCache.InfiniteAbsoluteExpiration });
                    break;
            }
            return success;
        }

        public override object Get(string key)
        {
            key = ComputeKey(key);
            return MemoryCache.Get(key);
        }

        public override IDictionary<string, object> Get(IEnumerable<string> keys)
        {
            var computedKeys = ComputeKey(keys);
            return MemoryCache.GetValues(keys);
        }

        public override bool Touch(string key, TimeSpan lifeSpan)
        {
            return false;
        }
    }
}
