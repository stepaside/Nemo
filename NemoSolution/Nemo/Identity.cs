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
            where T : class, IDataEntity
        {
            IdentityMap<T> identityMap;
            var identityMapKey = typeof(T).FullName + "/IdentityMap";
            if (!ConfigurationFactory.Configuration.ExecutionContext.Exists(identityMapKey))
            {
                identityMap = new IdentityMap<T>();
                ConfigurationFactory.Configuration.ExecutionContext.Set(identityMapKey, identityMap);
            }
            else
            {
                identityMap = (IdentityMap<T>)ConfigurationFactory.Configuration.ExecutionContext.Get(identityMapKey);
            }
            return identityMap;
        }
    }
}
