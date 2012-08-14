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
            var data = value.ToArray();
            var result = SerializationReader.ReadObjectWithType(data);
            return result;
        }

        protected override ArraySegment<byte> SerializeObject(object value)
        {
            var data = SerializationWriter.WriteObjectWithType(value);
            return new ArraySegment<byte>(data, 0, data.Length);
        }
    }
}
