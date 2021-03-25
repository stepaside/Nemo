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
            InstanceMapper = Mapper.CreateDelegate(typeof(T1), typeof(T2), false); 
        }

        internal static void Map(T1 source, T2 target)
        {
            InstanceMapper(source, target);
        }

        // ReSharper disable once StaticMemberInGenericType
        private static readonly Mapper.PropertyMapper InstanceMapper;
    }
}
