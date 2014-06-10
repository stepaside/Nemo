using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Reflection
{
    internal static class FastImplementor<T>
    {
        static FastImplementor()
        {
            Instance = Adapter.InternalImplement<T>();
        }
        internal static Activator.ObjectActivator Instance;
    }
}
