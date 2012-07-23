using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Attributes
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = false)]
    public class DistinctAttribute : Attribute
    {
        public Type EqualityComparerType
        {
            get;
            set;
        }
    }
}
