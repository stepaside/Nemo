using System;
using Enyim.Caching.Memcached;
using Nemo.Collections.Extensions;
using Nemo.Serialization;

namespace Nemo.Caching
{
    public class NemoTranscoder : DefaultTranscoder
    {
        protected override object DeserializeObject(ArraySegment<byte> value)
        {
            var data = value.Array.Slice(value.Offset, value.Offset + value.Count);
            SerializationReader reader = SerializationReader.CreateReader(data);
            var result = reader.ReadObject();
            reader.Close();
            return result;
        }

        protected override ArraySegment<byte> SerializeObject(object value)
        {
            var data = ((IBusinessObject)value).Serialize();
            return new ArraySegment<byte>(data, 0, data.Length);
        }
    }
}
