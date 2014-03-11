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
        public static string ToJson<T>(this T dataEntity)
            where T : class, IDataEntity
        {
            var output = new StringBuilder(1024);
            using (var writer = new StringWriter(output))
            {
                JsonSerializationWriter.WriteObject(dataEntity, null, writer);
            }
            return output.ToString();
        }

        public static void ToJson<T>(this T dataEntity, TextWriter writer)
            where T : class, IDataEntity
        {
            JsonSerializationWriter.WriteObject(dataEntity, null, writer);
        }

        public static string ToJson<T>(this IEnumerable<T> dataEntitys)
            where T : class, IDataEntity
        {
            var output = new StringBuilder(1024);
            using (var writer = new StringWriter(output))
            {
                JsonSerializationWriter.WriteObject(dataEntitys.ToList(), null, writer);
            }
            return output.ToString();
        }

        public static void ToJson<T>(this IEnumerable<T> dataEntitys, TextWriter writer)
            where T : class, IDataEntity
        {
            JsonSerializationWriter.WriteObject(dataEntitys.ToList(), null, writer);
        }

        public static IEnumerable<T> FromJson<T>(this string json)
            where T : class, IDataEntity
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
