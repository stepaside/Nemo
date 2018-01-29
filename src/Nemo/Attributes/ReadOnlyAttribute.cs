using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Attributes
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
    public class ReadOnlyAttribute : Attribute
    {
    }
}
