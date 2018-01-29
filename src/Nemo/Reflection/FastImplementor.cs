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
        // ReSharper disable once StaticMemberInGenericType
        internal static readonly Activator.ObjectActivator Instance;
    }
}
