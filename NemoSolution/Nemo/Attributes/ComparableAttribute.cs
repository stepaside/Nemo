using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Attributes
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
    public sealed class ComparableAttribute : PropertyAttribute
    {
        public Type CustomComparer { get; set; }
    }
}
