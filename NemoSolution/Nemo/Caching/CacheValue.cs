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
        public bool QueryKey { get; set; }
        public byte[] Buffer { get; set; }
        public ulong[] Signature { get; set; }

        public bool IsValid()
        {
            return this.ExpiresAt >= DateTimeOffset.Now.DateTime;
        }

        public bool IsValidVersion(ulong[] expectedVersion)
        {
            if (Signature != null && expectedVersion != null)
            {
                return Signature.Zip(expectedVersion, (v, ev) => v >= ev).All(r => r);
            }
            return true;
        }

        public byte[] ToBytes()
        {
            using (var writer = SerializationWriter.CreateWriter(SerializationMode.Manual))
            {
                writer.Write(ExpiresAt);
                writer.Write(QueryKey);
                writer.WriteList<ulong>(Signature);
                writer.Write(Buffer);
                return writer.GetBytes();
            }
        }

        public static CacheValue FromBytes(byte[] buffer)
        {
            var result = new CacheValue();
            using (var reader = SerializationReader.CreateReader(buffer))
            {
                result.ExpiresAt = reader.ReadDateTime();
                result.QueryKey = reader.ReadBoolean();
                result.Signature = reader.ReadList<ulong>().ToArray();
                result.Buffer = reader.ReadBytes();
            }
            return result;
        }
    }
}
