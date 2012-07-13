using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Reflection
{
    internal class FastIndexerMapper<T1, T2>
    {
        static FastIndexerMapper()
        {
            Instance = Mapper.CreateDelegate(typeof(T1), typeof(T2), true, false);
        }
        internal static Nemo.Reflection.Mapper.PropertyMapper Instance;
    }

    internal class FastExactIndexerMapper<T1, T2>
    {
        static FastExactIndexerMapper()
        {
            Instance = Mapper.CreateDelegate(typeof(T1), typeof(T2), true, true);
        }
        internal static Nemo.Reflection.Mapper.PropertyMapper Instance;
    }
}
