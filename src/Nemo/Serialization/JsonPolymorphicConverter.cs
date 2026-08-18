using System;
using System.Collections;
using System.Collections.Concurrent;
using System.IO;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Nemo.Serialization
{
    /// <summary>
    /// Serializes values whose declared type is an abstract class by their runtime type, writing the
    /// Nemo type discriminator (<c>"$type": "Namespace.Type,AssemblyName"</c>) as the first property
    /// so that the value can be materialized again. Without it System.Text.Json writes only the
    /// properties declared by the abstract type and fails to deserialize it at all.
    /// </summary>
    internal sealed class JsonPolymorphicConverter : JsonConverterFactory
    {
        internal const string TypeDiscriminator = "$type";

        public override bool CanConvert(Type typeToConvert)
        {
            return typeToConvert.IsClass && typeToConvert.IsAbstract && !typeof(IEnumerable).IsAssignableFrom(typeToConvert);
        }

        public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
        {
            return (JsonConverter)Activator.CreateInstance(typeof(PolymorphicConverter<>).MakeGenericType(typeToConvert));
        }

        private sealed class PolymorphicConverter<T> : JsonConverter<T>
            where T : class
        {
            private static readonly ConcurrentDictionary<string, Type> ResolvedTypes = new ConcurrentDictionary<string, Type>();

            public override T Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                if (reader.TokenType == JsonTokenType.Null) return null;

                using (var document = JsonDocument.ParseValue(ref reader))
                {
                    if (!document.RootElement.TryGetProperty(TypeDiscriminator, out var discriminator)
                        || discriminator.ValueKind != JsonValueKind.String)
                    {
                        throw new JsonException($"Unable to deserialize abstract type '{typeToConvert.FullName}': the payload does not contain a \"{TypeDiscriminator}\" property.");
                    }

                    var typeName = discriminator.GetString();
                    var runtimeType = ResolvedTypes.GetOrAdd(typeName, ResolveType);
                    if (runtimeType == null || !typeToConvert.IsAssignableFrom(runtimeType))
                    {
                        throw new JsonException($"Type '{typeName}' cannot be resolved as '{typeToConvert.FullName}'.");
                    }

                    return (T)JsonSerializer.Deserialize(document.RootElement.GetRawText(), runtimeType, options);
                }
            }

            public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options)
            {
                var runtimeType = value.GetType();
                writer.WriteStartObject();
                writer.WriteString(TypeDiscriminator, $"{runtimeType.FullName},{runtimeType.Assembly.GetName().Name}");
                using (var document = JsonSerializer.SerializeToDocument(value, runtimeType, options))
                {
                    foreach (var property in document.RootElement.EnumerateObject())
                    {
                        property.WriteTo(writer);
                    }
                }
                writer.WriteEndObject();
            }

            /// <summary>
            /// Resolves a discriminator without loading every type in the process: the qualified name
            /// is tried first, then assemblies are probed one at a time so a single unloadable
            /// assembly cannot break resolution.
            /// </summary>
            private static Type ResolveType(string typeName)
            {
                var type = Type.GetType(typeName, false);
                if (type != null) return type;

                var separator = typeName.IndexOf(',');
                var fullName = separator > 0 ? typeName.Substring(0, separator) : typeName;

                foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
                {
                    try
                    {
                        type = assembly.GetType(fullName, false);
                    }
                    catch (Exception e) when (e is ReflectionTypeLoadException || e is TypeLoadException || e is FileLoadException || e is BadImageFormatException)
                    {
                        continue;
                    }
                    if (type != null) return type;
                }
                return null;
            }
        }
    }
}
