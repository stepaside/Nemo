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
            InstanceBuilder = Activator.CreateDelegate(typeof(T));
        }

        internal static T New()
        {
            return (T)InstanceBuilder();
        }

        private static Nemo.Reflection.Activator.ObjectActivator InstanceBuilder;
    }

    internal class FastActivator<T1, T2>
    {
        static FastActivator()
        {
            InstanceBuilder = Activator.CreateDelegate(typeof(T1), typeof(T2));
        }

        internal static T1 New(T2 p)
        {
            return (T1)InstanceBuilder(p);
        }

        private static Nemo.Reflection.Activator.ObjectActivator InstanceBuilder;
    }

    internal class FastActivator<T1, T2, T3>
    {
        static FastActivator()
        {
            InstanceBuilder = Activator.CreateDelegate(typeof(T1), typeof(T2), typeof(T3));
        }

        internal static T1 New(T2 p1, T3 p2)
        {
            return (T1)InstanceBuilder(p1, p2);
        }

        private static Nemo.Reflection.Activator.ObjectActivator InstanceBuilder;
    }
}
