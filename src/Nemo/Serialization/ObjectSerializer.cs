using System.Linq;
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
        SimpleList = 66,
        ObjectMap = 67,
        ObjectList = 68,
        PolymorphicObjectList = 69
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
            where T : class
        {
            return Serialize(dataEntity, ConfigurationFactory.Get<T>().DefaultSerializationMode);
        }

        public static byte[] Serialize<T>(this T dataEntity, SerializationMode mode)
            where T : class
        {
            byte[] buffer;

            using (var writer = SerializationWriter.CreateWriter(mode))
            {
                writer.WriteObject(dataEntity, ObjectTypeCode.Object, null);
                buffer = writer.GetBytes();
            }

            return buffer;
        }

        public static IEnumerable<byte[]> Serialize<T>(this IEnumerable<T> dataEntityCollection)
            where T : class
        {
            return Serialize(dataEntityCollection, ConfigurationFactory.Get<T>().DefaultSerializationMode);
        }

        public static IEnumerable<byte[]> Serialize<T>(this IEnumerable<T> dataEntityCollection, SerializationMode mode)
            where T : class
        {
            return dataEntityCollection.Where(e => e != null).Select(e => e.Serialize(mode));
        }

        #endregion

        #region Deserialize Methods

        public static T Deserialize<T>(this byte[] data)
            where T : class
        {
            T result;
            using (var reader = SerializationReader.CreateReader(data))
            {
                result = (T)reader.ReadObject(typeof(T), ObjectTypeCode.Object, false);
            }
            return result;
        }

        public static object Deserialize(this byte[] data, Type objectType)
        {
            object result;
            using (var reader = SerializationReader.CreateReader(data))
            {
                result = reader.ReadObject(objectType, ObjectTypeCode.Object, false);
            }
            return result;
        }

        public static IEnumerable<T> Deserialize<T>(this IEnumerable<byte[]> dataCollection)
            where T : class
        {
            return dataCollection.Where(data => data != null).Select(Deserialize<T>);
        }

        public static IEnumerable<object> Deserialize(this IEnumerable<byte[]> dataCollection, Type objectType)
        {
            return dataCollection.Where(data => data != null).Select(data => Deserialize(data, objectType));
        }

        #endregion
    }
}
