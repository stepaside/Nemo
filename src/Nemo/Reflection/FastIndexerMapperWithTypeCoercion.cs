namespace Nemo.Reflection
{
    internal class FastIndexerMapperWithTypeCoercion<T1, T2>
    {
        static FastIndexerMapperWithTypeCoercion()
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
