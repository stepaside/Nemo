using System.Collections.Generic;
using Nemo.Reflection;

namespace Nemo.Serialization
{
    public static class SerializationExtensions
    {
        #region Serialize Methods

        public static byte[] Serialize<T>(this T businessObject)
            where T : class, IBusinessObject
        {
            return Serialize(businessObject, ObjectFactory.Configuration.DefaultSerializationMode);
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
            return Serialize(businessObjectCollection, ObjectFactory.Configuration.DefaultSerializationMode);
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

        public static T Deserialize<T>(byte[] data)
            where T : class, IBusinessObject
        {
            T result = default(T);
            using (var reader = SerializationReader.CreateReader(data))
            {
                result = (T)reader.ReadObject(typeof(T), ObjectTypeCode.BusinessObject);
            }
            return result;
        }

        public static IEnumerable<T> Deserialize<T>(IEnumerable<byte[]> dataCollection)
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
