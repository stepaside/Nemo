using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Attributes
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
    public sealed class ReferencesAttribute : PropertyAttribute
    {
        public ReferencesAttribute(Type parent)
        {
            Parent = parent;
        }

        public Type Parent
        {
            get;
            private set;
        }
    }
}
