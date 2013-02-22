using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Attributes
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Interface, AllowMultiple = true, Inherited = true)]
    public class CacheDependencyAttribute : Attribute
    {
        public CacheDependencyAttribute(Type type) : base() 
        {
            DependentType = type;
        }

        public Type DependentType { get; private set; }
        public string DependentProperty { get; set; }
        public string ValueProperty { get; set; }
    }
}
