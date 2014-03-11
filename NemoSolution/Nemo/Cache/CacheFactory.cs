using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nemo.Cache.Providers;

namespace Nemo.Cache
{
    public class CacheFactory
    {
        public static CacheProvider GetProvider(Type cacheType, CacheOptions options = null)
        {
            if (cacheType != null && typeof(CacheProvider).IsAssignableFrom(cacheType))
            {
                return (CacheProvider)Nemo.Reflection.Activator.CreateDelegate(cacheType, typeof(CacheOptions))(options);
            }
            return null;
        }
    }
}
