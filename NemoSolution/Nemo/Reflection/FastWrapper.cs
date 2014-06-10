using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Reflection
{
    internal class FastWrapper<T>
    {
        static FastWrapper()
        {
            Instance = Adapter.InternalWrap(typeof(IDictionary<string, object>), typeof(T), false, false);
        }
        internal static Activator.ObjectActivator Instance;
    }

    internal class FastComplexWrapper<T>
    {
        static FastComplexWrapper()
        {
            Instance = Adapter.InternalWrap(typeof(IDictionary<string, object>), typeof(T), false, true);
        }
        internal static Activator.ObjectActivator Instance;
    }

    internal class FastExactWrapper<T>
    {
        static FastExactWrapper()
        {
            Instance = Adapter.InternalWrap(typeof(IDictionary<string, object>), typeof(T), true, false);
        }
        internal static Activator.ObjectActivator Instance;
    }

    internal class FastExactComplexWrapper<T>
    {
        static FastExactComplexWrapper()
        {
            Instance = Adapter.InternalWrap(typeof(IDictionary<string, object>), typeof(T), true, true);
        }
        internal static Activator.ObjectActivator Instance;
    }
}
