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
        DataEntity = 128,
        DataEntityList = 129,
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
    
    public static class ObjectSerializer
    {
        #region Serialize Methods

        public static byte[] Serialize<T>(this T dataEntity)
            where T : class, IDataEntity
        {
            return Serialize(dataEntity, ConfigurationFactory.Configuration.DefaultSerializationMode);
        }

        internal static byte[] Serialize<T>(this T dataEntity, SerializationMode mode)
            where T : class, IDataEntity
        {
            byte[] buffer = null;

            using (var writer = SerializationWriter.CreateWriter(mode))
            {
                writer.WriteObject(dataEntity, ObjectTypeCode.DataEntity);
                buffer = writer.GetBytes();
            }

            return buffer;
        }

        public static IEnumerable<byte[]> Serialize<T>(this IEnumerable<T> dataEntityCollection)
            where T : class, IDataEntity
        {
            return Serialize(dataEntityCollection, ConfigurationFactory.Configuration.DefaultSerializationMode);
        }

        internal static IEnumerable<byte[]> Serialize<T>(this IEnumerable<T> dataEntityCollection, SerializationMode mode)
            where T : class, IDataEntity
        {
            foreach (T dataEntity in dataEntityCollection)
            {
                if (dataEntity != null)
                {
                    yield return dataEntity.Serialize(mode);
                }
            }
        }

        #endregion

        #region Deserialize Methods

        public static T Deserialize<T>(this byte[] data)
            where T : class, IDataEntity
        {
            T result = default(T);
            using (var reader = SerializationReader.CreateReader(data))
            {
                result = (T)reader.ReadObject(typeof(T), ObjectTypeCode.DataEntity, default(T) is IConvertible);
            }
            return result;
        }

        public static IEnumerable<T> Deserialize<T>(this IEnumerable<byte[]> dataCollection)
            where T : class, IDataEntity
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
            where T : class, IDataEntity
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
