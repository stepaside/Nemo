using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Attributes
{
    [AttributeUsage(AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
    public class DoNotSerializeAttribute : PropertyAttribute
    { }
}
