using Nemo.Reflection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nemo.Configuration.Mapping
{
    public interface IPropertyMap
    {
        ReflectedProperty Property { get; }
    }
}
