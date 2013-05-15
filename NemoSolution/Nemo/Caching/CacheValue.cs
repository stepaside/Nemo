using Nemo.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Nemo.Caching
{
    [Serializable]
    public class CacheValue
    {
        public DateTime ExpiresAt { get; set; }
        public byte[] Value { get; set; }
        public ulong Revision { get; set; }

        public bool IsValid()
        {
            return this.ExpiresAt >= DateTimeOffset.Now.DateTime;
        }

        public byte[] ToBytes()
        {
            using (var writer = SerializationWriter.CreateWriter(SerializationMode.Manual))
            {
                writer.Write(ExpiresAt);
                writer.Write(Revision);
                writer.Write(Value);
                return writer.GetBytes();
            }
        }

        public static CacheValue FromBytes(byte[] buffer)
        {
            var result = new CacheValue();
            using (var reader = SerializationReader.CreateReader(buffer))
            {
                result.ExpiresAt = reader.ReadDateTime();
                result.Revision = reader.ReadUInt64();
                result.Value = reader.ReadBytes();
            }
            return result;
        }
    }
}
