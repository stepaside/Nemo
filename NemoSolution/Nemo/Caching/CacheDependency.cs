using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Caching
{
    public class CacheDependency
    {
        public Type DependentType { get; set; }
        public string DependentProperty { get; set; }
        public string ValueProperty { get; set; }
    }
}
