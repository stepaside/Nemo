using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Reflection
{
    internal class FastActivator<T>
    {
        static FastActivator()
        {
            Instance = Activator.CreateDelegate(typeof(T));
        }
        internal static Nemo.Reflection.Activator.ObjectActivator Instance;
    }

    internal class FastActivator<T1, T2>
    {
        static FastActivator()
        {
            Instance = Activator.CreateDelegate(typeof(T1), typeof(T2));
        }
        internal static Nemo.Reflection.Activator.ObjectActivator Instance;
    }

    internal class FastActivator<T1, T2, T3>
    {
        static FastActivator()
        {
            Instance = Activator.CreateDelegate(typeof(T1), typeof(T2), typeof(T3));
        }
        internal static Nemo.Reflection.Activator.ObjectActivator Instance;
    }
}
