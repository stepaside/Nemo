using System;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Nemo.Attributes;
using Nemo.Reflection;

namespace Nemo.Serialization
{
    /// <summary>
    /// Builds System.Text.Json options that reproduce Nemo's serialization semantics:
    /// only readable and writable properties are emitted, properties marked with
    /// <see cref="DoNotSerializeAttribute"/> and interface indexers are skipped, nulls are omitted,
    /// reference cycles are ignored, data entities are activated through <see cref="ObjectFactory"/>,
    /// and data entity interfaces are materialized as Nemo adapters.
    /// </summary>
    public static class JsonSerializationOptions
    {
        public static JsonSerializerOptions Default { get; } = Create();

        public static JsonSerializerOptions Create()
        {
            var options = new JsonSerializerOptions
            {
                DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
                ReferenceHandler = ReferenceHandler.IgnoreCycles,
                TypeInfoResolver = new DefaultJsonTypeInfoResolver
                {
                    Modifiers = { ApplyPropertyRules, ApplyDataEntityActivator }
                }
            };
            return options;
        }

        /// <summary>
        /// Nemo serializes an emitted adapter using the interface it implements rather than the
        /// generated type, and otherwise uses the runtime type of the value.
        /// </summary>
        internal static Type GetWriteType(object value, Type declaredType)
        {
            var objectType = value.GetType();
            if (Reflector.IsEmitted(objectType))
            {
                return declaredType.IsInterface ? declaredType : Reflector.GetInterface(objectType) ?? objectType;
            }
            return objectType;
        }

        private static void ApplyPropertyRules(JsonTypeInfo typeInfo)
        {
            if (typeInfo.Kind != JsonTypeInfoKind.Object) return;

            for (var i = typeInfo.Properties.Count - 1; i >= 0; i--)
            {
                var property = typeInfo.Properties[i];
                if (property.Get == null || property.Set == null || property.Name == "Indexer" || IsNotSerializable(property))
                {
                    typeInfo.Properties.RemoveAt(i);
                }
            }
        }

        private static bool IsNotSerializable(JsonPropertyInfo property)
        {
            return property.AttributeProvider is PropertyInfo propertyInfo
                && propertyInfo.GetCustomAttributes(typeof(DoNotSerializeAttribute), false).Length > 0;
        }

        /// <summary>
        /// System.Text.Json cannot instantiate an interface or apply Nemo's object state tracking;
        /// data entities (including interfaces, which are materialized as Nemo adapters) are created
        /// through <see cref="ObjectFactory"/> instead.
        /// </summary>
        private static void ApplyDataEntityActivator(JsonTypeInfo typeInfo)
        {
            if (typeInfo.Kind != JsonTypeInfoKind.Object) return;

            var objectType = typeInfo.Type;
            if (objectType.IsInterface)
            {
                if (!Reflector.GetReflectedType(objectType).IsDataEntity) return;

                var implement = Adapter.InternalImplement(objectType);
                typeInfo.CreateObject = () =>
                {
                    var value = implement();
                    ObjectFactory.TrySetObjectState(value);
                    return value;
                };
            }
            else if (typeof(ITrackableDataEntity).IsAssignableFrom(objectType))
            {
                var create = typeInfo.CreateObject;
                if (create == null) return;

                typeInfo.CreateObject = () =>
                {
                    var value = create();
                    ObjectFactory.TrySetObjectState(value);
                    return value;
                };
            }
        }
    }
}
