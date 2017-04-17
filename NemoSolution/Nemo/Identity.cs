using Nemo.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nemo
{
    internal static class Identity
    {
        internal static IIdentityMap Get(Type objectType)
        {
            var executionContext = ConfigurationFactory.Get(objectType).ExecutionContext;
            IIdentityMap identityMap;
            var identityMapKey = objectType.FullName + "/IdentityMap";
            if (!executionContext.Exists(identityMapKey))
            {
                identityMap = (IIdentityMap)Activator.CreateInstance(typeof(IdentityMap<>).MakeGenericType(objectType));
                executionContext.Set(identityMapKey, identityMap);
            }
            else
            {
                identityMap = (IIdentityMap)executionContext.Get(identityMapKey);
            }
            return identityMap;
        }

        internal static IdentityMap<T> Get<T>()
            where T : class
        {
            var executionContext = ConfigurationFactory.Get<T>().ExecutionContext;
            IdentityMap<T> identityMap;
            var identityMapKey = typeof(T).FullName + "/IdentityMap";
            if (!executionContext.Exists(identityMapKey))
            {
                identityMap = new IdentityMap<T>();
                executionContext.Set(identityMapKey, identityMap);
            }
            else
            {
                identityMap = (IdentityMap<T>)executionContext.Get(identityMapKey);
            }
            return identityMap;
        }
    }
}
