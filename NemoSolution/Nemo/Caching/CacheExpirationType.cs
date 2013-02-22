using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Caching
{
    public enum CacheExpirationType 
    { 
        Never, 
        DateTime, 
        TimeSpan, 
        TimeOfDay 
    };
}
