using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nemo.Caching
{
    public enum CacheInvalidationStrategy
    {
        TrackAndRemove,
        TrackAndIncrement,
        QuerySignature,
        DelayedQuerySignature
    }
}
