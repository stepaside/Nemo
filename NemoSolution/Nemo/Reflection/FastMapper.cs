using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Reflection
{
    internal class FastMapper<T1, T2>
    {
        static FastMapper()
        {
            Instance = Mapper.CreateDelegate(typeof(T1), typeof(T2), false, false); 
        }
        internal static Nemo.Reflection.Mapper.PropertyMapper Instance;
    }

    internal class FastExactMapper<T1, T2>
    {
        static FastExactMapper()
        {
            Instance = Mapper.CreateDelegate(typeof(T1), typeof(T2), false, true);
        }
        internal static Nemo.Reflection.Mapper.PropertyMapper Instance;
    }
}
