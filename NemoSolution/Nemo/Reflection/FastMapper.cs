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
            InstanceMapper = Mapper.CreateDelegate(typeof(T1), typeof(T2), false, false); 
        }

        internal static void Map(T1 source, T2 target)
        {
            InstanceMapper(source, target);
        }

        private static Nemo.Reflection.Mapper.PropertyMapper InstanceMapper;
    }

    internal class FastExactMapper<T1, T2>
    {
        static FastExactMapper()
        {
            InstanceMapper = Mapper.CreateDelegate(typeof(T1), typeof(T2), false, true);
        }

        internal static void Map(T1 source, T2 target)
        {
            InstanceMapper(source, target);
        }

        private static Nemo.Reflection.Mapper.PropertyMapper InstanceMapper;
    }
}
