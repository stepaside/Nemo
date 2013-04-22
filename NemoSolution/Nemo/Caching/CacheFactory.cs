using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nemo.Caching.Providers;

namespace Nemo.Caching
{
    public class CacheFactory
    {
        public static CacheProvider GetProvider(Type cacheType, CacheOptions options = null)
        {
            if (cacheType != null && typeof(CacheProvider).IsAssignableFrom(cacheType))
            {
                return (CacheProvider)Nemo.Reflection.Activator.New(cacheType, typeof(CacheOptions));
            }
            return null;
        }
    }
}
