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
