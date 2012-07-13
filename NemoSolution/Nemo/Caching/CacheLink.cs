using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Caching
{
    public class CacheLink
    {
        public Type DependentType { get; set; }
        public string DependentParameter { get; set; }
        public string ValueProperty { get; set; }
    }
}
