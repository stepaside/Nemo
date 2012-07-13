using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text;
using System.Xml.XPath;
using Nemo.Reflection;

namespace Nemo.Serialization
{
    public static class SerializationExtensions
    {
        #region Serialize Methods

        public static byte[] Serialize<T>(this T businessObject)
            where T : class, IBusinessObject
        {
            return Serialize(businessObject, false);
        }

        internal static byte[] Serialize<T>(this T businessObject, bool serializeAll)
            where T : class, IBusinessObject
        {
            byte[] buffer = null;

            SerializationWriter writer = SerializationWriter.CreateWriter(serializeAll);
            writer.WriteObject(businessObject, ObjectTypeCode.BusinessObject);
            buffer = writer.GetBytes();
            writer.Close();

            return buffer;
        }

        public static IEnumerable<byte[]> Serialize<T>(this IEnumerable<T> businessObjectCollection)
            where T : class, IBusinessObject
        {
            return Serialize(businessObjectCollection, false);
        }

        internal static IEnumerable<byte[]> Serialize<T>(this IEnumerable<T> businessObjectCollection, bool serializeAll)
            where T : class, IBusinessObject
        {
            foreach (T businessObject in businessObjectCollection)
            {
                if (businessObject != null)
                {
                    yield return businessObject.Serialize(serializeAll);
                }
            }
        }

        #endregion

        #region Deserialize Methods

        public static T Deserialize<T>(byte[] data)
            where T : class, IBusinessObject
        {
            T result = default(T);
            SerializationReader reader = SerializationReader.CreateReader(data);
            result = (T)reader.ReadObject();
            reader.Close();
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
            string objectTypeName = SerializationReader.GetObjectType(data);
            if (!string.IsNullOrEmpty(objectTypeName))
            {
                var type = Reflector.TypeCache<T>.Type;
                if (type.ElementType != null)
                {
                    return type.ElementType.Name == objectTypeName;
                }
                else
                {
                    return type.TypeName == objectTypeName;
                }
            }
            return true;
        }

        #endregion
    }
}
