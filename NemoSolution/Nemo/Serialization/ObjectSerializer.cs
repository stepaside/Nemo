using Nemo.Configuration;
using Nemo.Reflection;
using System;
using System.Collections.Generic;

namespace Nemo.Serialization
{
    public enum ObjectTypeCode : byte
    {
        Empty = TypeCode.Empty,
        Object = TypeCode.Object,
        DBNull = TypeCode.DBNull,
        Boolean = TypeCode.Boolean,
        Char = TypeCode.Char,
        SByte = TypeCode.SByte,
        Byte = TypeCode.Byte,
        Int16 = TypeCode.Int16,
        UInt16 = TypeCode.UInt16,
        Int32 = TypeCode.Int32,
        UInt32 = TypeCode.UInt32,
        Int64 = TypeCode.Int64,
        UInt64 = TypeCode.UInt64,
        Single = TypeCode.Single,
        Double = TypeCode.Double,
        Decimal = TypeCode.Decimal,
        DateTime = TypeCode.DateTime,
        String = TypeCode.String,
        TimeSpan = 32,
        DateTimeOffset = 33,
        Guid = 34,
        Version = 35,
        Uri = 36,
        ByteArray = 64,
        CharArray = 65,
        ObjectList = 66,
        ObjectMap = 67,
        TypeUnion = 128,
        BusinessObject = 129,
        BusinessObjectList = 130,
        ProtocolBuffer = 255
    }

    public enum ListAspectType : byte
    {
        None = 1,
        Distinct = 2,
        Sorted = 4
    }

    public enum SerializationMode : byte
    {
        Compact = 1,
        SerializeAll = 2,
        IncludePropertyNames = 4,
        Manual = 8
    }

    public enum TemporalScale : byte
    {
        Days = 0,
        Hours = 1,
        Minutes = 2,
        Seconds = 3,
        Milliseconds = 4,
        Ticks = 5,
        MinMax = 15
    }

    public static class UnixDateTime
    {
        public static readonly DateTime Epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    }

    public static class ObjectSerializer
    {
        #region Serialize Methods

        public static byte[] Serialize<T>(this T businessObject)
            where T : class, IBusinessObject
        {
            return Serialize(businessObject, ConfigurationFactory.Configuration.DefaultSerializationMode);
        }

        internal static byte[] Serialize<T>(this T businessObject, SerializationMode mode)
            where T : class, IBusinessObject
        {
            byte[] buffer = null;

            using (var writer = SerializationWriter.CreateWriter(mode))
            {
                writer.WriteObject(businessObject, ObjectTypeCode.BusinessObject);
                buffer = writer.GetBytes();
            }

            return buffer;
        }

        public static IEnumerable<byte[]> Serialize<T>(this IEnumerable<T> businessObjectCollection)
            where T : class, IBusinessObject
        {
            return Serialize(businessObjectCollection, ConfigurationFactory.Configuration.DefaultSerializationMode);
        }

        internal static IEnumerable<byte[]> Serialize<T>(this IEnumerable<T> businessObjectCollection, SerializationMode mode)
            where T : class, IBusinessObject
        {
            foreach (T businessObject in businessObjectCollection)
            {
                if (businessObject != null)
                {
                    yield return businessObject.Serialize(mode);
                }
            }
        }

        #endregion

        #region Deserialize Methods

        public static T Deserialize<T>(this byte[] data)
            where T : class, IBusinessObject
        {
            T result = default(T);
            using (var reader = SerializationReader.CreateReader(data))
            {
                result = (T)reader.ReadObject(typeof(T), ObjectTypeCode.BusinessObject, default(T) is IConvertible);
            }
            return result;
        }

        public static IEnumerable<T> Deserialize<T>(this IEnumerable<byte[]> dataCollection)
            where T : class, IBusinessObject
        {
            foreach (byte[] data in dataCollection)
            {
                if (data != null)
                {
                    yield return Deserialize<T>(data);
                }
            }
        }

        internal static bool CheckType<T>(byte[] data)
            where T : class, IBusinessObject
        {
            var objectTypeHash = SerializationReader.GetObjectTypeHash(data);
            var type = Reflector.TypeCache<T>.Type;
            if (type.ElementType != null)
            {
                return type.ElementType.FullName.GetHashCode() == objectTypeHash;
            }
            else
            {
                return type.FullTypeName.GetHashCode() == objectTypeHash;
            }
        }

        #endregion
    }
}
