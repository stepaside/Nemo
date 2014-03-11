using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nemo.Cache
{
    public enum CacheInvalidationStrategy
    {
        CacheProvider,
        InvalidateByParameters,
        InvalidateByVersion
    }
}
