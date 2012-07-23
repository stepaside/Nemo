using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Attributes
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = false)]
    public class SortedAttribute : Attribute
    {
        public Type ComparerType
        {
            get;
            set;
        }
    }
}
