using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Attributes
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
    public class SelectableAttribute : PropertyAttribute
    {
        public SelectableAttribute(bool value)
        {
            Value = value;
        }

        public bool Value
        {
            get;
            private set;
        }
    }
}
