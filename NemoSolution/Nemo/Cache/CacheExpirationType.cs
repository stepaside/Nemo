using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Cache
{
    public enum CacheExpirationType 
    { 
        Never, 
        Absolute, 
        Sliding, 
        TimeOfDay 
    };
}
