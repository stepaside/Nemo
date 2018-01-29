using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Attributes
{
    public sealed class Generate
    {
        [AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
        public sealed class NativeAttribute : PropertyAttribute
        { }

        [AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
        public sealed class UsingAttribute : PropertyAttribute
        {
            public UsingAttribute(Type generator)
                : base()
            {
                Type = generator;
            }

            public Type Type { get; private set; }
        }
    }
}
