using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nemo.Caching.Providers;

namespace Nemo.Caching
{
    public enum CacheExpirationType { Never, DateTime, TimeSpan, TimeOfDay };

    public enum CacheContentionMitigationType { None, DistributedLocking, UseStaleCache }

    public class CacheFactory
    {
        public static CacheProvider GetProvider(Type cacheType, CacheOptions options = null)
        {
            if (cacheType != null && typeof(CacheProvider).IsAssignableFrom(cacheType))
            {
                var activator = Nemo.Reflection.Activator.CreateDelegate(cacheType, typeof(CacheOptions));
                if (activator != null)
                {
                    return (CacheProvider)activator(options);
                }
            }
            return null;
        }
    }
}
