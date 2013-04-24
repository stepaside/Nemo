using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection.Emit;
using System.Text;
using System.Xml;
using Nemo.Attributes;
using Nemo.Collections;
using Nemo.Collections.Extensions;
using Nemo.Extensions;
using Nemo.Fn;
using Nemo.Reflection;
using Nemo.Utilities;

namespace Nemo.Serialization
{
    public static class ObjectJsonSerializer
    {
        public static string ToJson<T>(this T businessObject)
            where T : class, IBusinessObject
        {
            var output = new StringBuilder(1024);
            using (var writer = new StringWriter(output))
            {
                JsonSerializationWriter.WriteObject(businessObject, null, writer);
            }
            return output.ToString();
        }

        public static void ToJson<T>(this T businessObject, TextWriter writer)
            where T : class, IBusinessObject
        {
            JsonSerializationWriter.WriteObject(businessObject, null, writer);
        }

        public static string ToJson<T>(this IEnumerable<T> businessObjects)
            where T : class, IBusinessObject
        {
            var output = new StringBuilder(1024);
            using (var writer = new StringWriter(output))
            {
                JsonSerializationWriter.WriteObject(businessObjects.ToList(), null, writer);
            }
            return output.ToString();
        }

        public static void ToJson<T>(this IEnumerable<T> businessObjects, TextWriter writer)
            where T : class, IBusinessObject
        {
            JsonSerializationWriter.WriteObject(businessObjects.ToList(), null, writer);
        }

        public static IEnumerable<T> FromJson<T>(this string json)
            where T : class, IBusinessObject
        {
            var value = Json.Parse(json);
            var result = JsonSerializationReader.ReadObject(value, typeof(T));
            if (value.Type == JsonType.Array)
            {
                return ((IList)result).Cast<T>();
            }
            else
            {
                return ((T)result).Return();
            }
        }
    }
}
