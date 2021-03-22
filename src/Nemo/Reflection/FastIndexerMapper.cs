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
            IndexerMapper = Mapper.CreateDelegate(typeof(T1), typeof(T2), true, false);
        }

        internal static void Map(T1 source, T2 target)
        {
            IndexerMapper(source, target);
        }

        // ReSharper disable once StaticMemberInGenericType
        private static readonly Mapper.PropertyMapper IndexerMapper;
    }

    internal class FastStrictIndexerMapper<T1, T2>
    {
        static FastStrictIndexerMapper()
        {
            IndexerMapper = Mapper.CreateDelegate(typeof(T1), typeof(T2), true, true);
        }

        internal static void Map(T1 source, T2 target)
        {
            IndexerMapper(source, target);
        }

        // ReSharper disable once StaticMemberInGenericType
        private static readonly Mapper.PropertyMapper IndexerMapper;
    }
}
