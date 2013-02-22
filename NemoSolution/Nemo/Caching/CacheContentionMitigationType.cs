using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Caching
{
    public enum CacheContentionMitigationType
    {
        None, 
        DistributedLocking, 
        UseStaleCache
    }
}
