using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;

namespace Nemo.Serialization
{
    public static class ObjectJsonSerializer
    {
        /// <summary>
        /// Options used by ToJson/FromJson when no options are provided. Nemo specific behavior
        /// (property selection, data entity activation, interface materialization) is preserved
        /// by <see cref="JsonSerializationOptions.Create"/>.
        /// </summary>
        public static JsonSerializerOptions Options { get; set; } = JsonSerializationOptions.Default;

        public static string ToJson<T>(this T dataEntity)
            where T : class
        {
            return dataEntity.ToJson((JsonSerializerOptions)null);
        }

        public static string ToJson<T>(this T dataEntity, JsonSerializerOptions options)
            where T : class
        {
            if (dataEntity == null) return null;
            return JsonSerializer.Serialize(dataEntity, JsonSerializationOptions.GetWriteType(dataEntity, typeof(T)), options ?? Options);
        }

        public static void ToJson<T>(this T dataEntity, TextWriter writer)
            where T : class
        {
            dataEntity.ToJson(writer, null);
        }

        public static void ToJson<T>(this T dataEntity, TextWriter writer, JsonSerializerOptions options)
            where T : class
        {
            if (writer == null) throw new ArgumentNullException(nameof(writer));
            writer.Write(dataEntity.ToJson(options));
        }

        public static string ToJson<T>(this IEnumerable<T> dataEntitys)
            where T : class
        {
            return dataEntitys.ToJson((JsonSerializerOptions)null);
        }

        public static string ToJson<T>(this IEnumerable<T> dataEntitys, JsonSerializerOptions options)
            where T : class
        {
            if (dataEntitys == null) return null;
            return JsonSerializer.Serialize(dataEntitys, options ?? Options);
        }

        public static void ToJson<T>(this IEnumerable<T> dataEntitys, TextWriter writer)
            where T : class
        {
            dataEntitys.ToJson(writer, null);
        }

        public static void ToJson<T>(this IEnumerable<T> dataEntitys, TextWriter writer, JsonSerializerOptions options)
            where T : class
        {
            if (writer == null) throw new ArgumentNullException(nameof(writer));
            writer.Write(dataEntitys.ToJson(options));
        }

        public static T FromJson<T>(this string json)
            where T : class
        {
            return (T)json.FromJson(typeof(T), null);
        }

        public static T FromJson<T>(this string json, JsonSerializerOptions options)
            where T : class
        {
            return (T)json.FromJson(typeof(T), options);
        }

        public static object FromJson(this string json, Type objectType)
        {
            return json.FromJson(objectType, null);
        }

        public static object FromJson(this string json, Type objectType, JsonSerializerOptions options)
        {
            if (objectType == null) throw new ArgumentNullException(nameof(objectType));
            if (json == null) return null;
            return JsonSerializer.Deserialize(json, objectType, options ?? Options);
        }

        public static T FromJson<T>(this TextReader reader)
            where T : class
        {
            return (T)reader.FromJson(typeof(T), null);
        }

        public static object FromJson(this TextReader reader, Type objectType, JsonSerializerOptions options)
        {
            if (reader == null) throw new ArgumentNullException(nameof(reader));
            return reader.ReadToEnd().FromJson(objectType, options);
        }
    }
}
