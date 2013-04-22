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

        public static IEnumerable<T> FromJson<T>(string json)
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

    public static class JsonSerializationWriter
    {
        internal delegate void JsonObjectSerializer(object values, TextWriter output, bool compact);

        private static ConcurrentDictionary<string, JsonObjectSerializer> _serializers = new ConcurrentDictionary<string, JsonObjectSerializer>();

        public static void Write(string value, TextWriter output)
        {
            output.Write(value);
        }

        private static void WriteString(string value, TextWriter output)
        {
            var hexSeqBuffer = new char[4];
            var len = value.Length;
            for (var i = 0; i < len; i++)
            {
                switch (value[i])
                {
                    case '\n':
                        output.Write("\\n");
                        continue;

                    case '\r':
                        output.Write("\\r");
                        continue;

                    case '\t':
                        output.Write("\\t");
                        continue;

                    case '"':
                    case '\\':
                        output.Write('\\');
                        output.Write(value[i]);
                        continue;

                    case '\f':
                        output.Write("\\f");
                        continue;

                    case '\b':
                        output.Write("\\b");
                        continue;
                }

                //Is printable char?
                if (value[i] >= 32 && value[i] <= 126)
                {
                    output.Write(value[i]);
                    continue;
                }

                var isValidSequence = value[i] < 0xD800 || value[i] > 0xDFFF;
                if (isValidSequence)
                {
                    // Default, turn into a \uXXXX sequence
                    Json.IntegerToHex(value[i], hexSeqBuffer);
                    output.Write("\\u");
                    output.Write(hexSeqBuffer);
                }
            }
        }

        /// <summary> Writes a generic ICollection (such as an IList<T>) to the buffer. </summary>
        internal static void WriteList(IList items, TextWriter output)
        {
            for (int i = 0; i < items.Count; i++)
            {
                WriteObject(items[i], null, output, i != items.Count - 1);
            }
        }

        internal static void WriteList<T>(IList<T> items, TextWriter output)
        {
            WriteList((IList)items, output);
        }

        internal static void WriteDictionary<T, U>(IDictionary<T, U> map, string name, TextWriter output)
        {
            foreach (var pair in map)
            {
                WriteObject(pair.Key, "Key", output);
                WriteObject(pair.Value, "Value", output);
            }
        }

        public static void WriteObject(object value, string name, TextWriter output, bool hasMore = false, bool compact = true)
        {
            if (value != null)
            {
                var objectType = value.GetType();
                var typeCode = Type.GetTypeCode(objectType);
                var isText = false;
                if (typeCode != TypeCode.DBNull)
                {
                    string jsonValue = null;
                    switch (typeCode)
                    {
                        case TypeCode.Boolean:
                            jsonValue = ((bool)value).ToString().ToLower();
                            break;
                        case TypeCode.String:
                            jsonValue = (string)value;
                            isText = true;
                            break;
                        case TypeCode.DateTime:
                            jsonValue = XmlConvert.ToString((DateTime)value, XmlDateTimeSerializationMode.Utc);
                            isText = true;
                            break;
                        case TypeCode.Byte:
                        case TypeCode.UInt16:
                        case TypeCode.UInt32:
                        case TypeCode.UInt64:
                        case TypeCode.SByte:
                        case TypeCode.Int16:
                        case TypeCode.Int32:
                        case TypeCode.Int64:
                        case TypeCode.Char:
                        case TypeCode.Single:
                        case TypeCode.Double:
                        case TypeCode.Decimal:
                            jsonValue = Convert.ToString(value);
                            break;
                        default:
                            if (objectType == typeof(DateTimeOffset))
                            {
                                jsonValue = XmlConvert.ToString((DateTimeOffset)value);
                                isText = true;
                            }
                            else if (objectType == typeof(TimeSpan))
                            {
                                jsonValue = XmlConvert.ToString((TimeSpan)value);
                                isText = true;
                            }
                            else if (objectType == typeof(Guid))
                            {
                                jsonValue = ((Guid)value).ToString("D");
                                isText = true;
                            }
                            else if (value is IBusinessObject)
                            {
                                var serializer = CreateDelegate(objectType);
                                serializer(value, output, compact);
                                if (hasMore)
                                {
                                    Write(",", output);
                                }
                            }
                            else if (value is IList)
                            {
                                Write(string.Format("\"{0}\":[", name), output);
                                var items = ((IList)value).Cast<object>().ToArray();
                                if (items.Length > 0 && items[0] is IBusinessObject)
                                {
                                    var elementType = items[0].GetType();
                                    var serializer = CreateDelegate(elementType);
                                    for (int i = 0; i < items.Length; i++)
                                    {
                                        serializer(items[i], output, compact);
                                        if (i < items.Length - 1)
                                        {
                                            Write(",", output);
                                        }
                                    }
                                }
                                else
                                {
                                    WriteList((IList)value, output);
                                }
                                Write("]", output);
                                if (hasMore)
                                {
                                    Write(",", output);
                                }
                            }
                            else if (value is ITypeUnion)
                            {
                                Write(string.Format("\"{0}\":[", name), output);
                                var typeUnion = (ITypeUnion)value;
                                var elementType = typeUnion.UnionType;
                                Write(string.Format("\"{0}\",", elementType.FullName), output);
                                if (Reflector.IsSimpleType(typeUnion.UnionType))
                                {
                                    WriteObject(typeUnion.GetObject(), null, output, false, compact);
                                }
                                else
                                {
                                    var serializer = CreateDelegate(elementType);
                                    serializer(typeUnion.GetObject(), output, compact);
                                }
                                Write("]", output);
                                if (hasMore)
                                {
                                    Write(",", output);
                                }
                            }
                            break;
                    }

                    if (jsonValue != null)
                    {
                        if (name != null)
                        {
                            Write(string.Format("\"{0}\":", name), output);
                        }

                        if (isText)
                        {
                            Write(string.Format("\"", name), output);
                        }

                        if (typeCode == TypeCode.String)
                        {
                            WriteString(jsonValue, output);
                        }
                        else
                        {
                            Write(jsonValue, output);
                        }

                        if (isText)
                        {
                            Write("\"", output);
                        }

                        if (hasMore)
                        {
                            Write(",", output);
                        }
                    }
                }
            }
            else if (!compact && !string.IsNullOrEmpty(name))
            {
                Write(string.Format("\"{0}\":null", name), output);
                if (hasMore)
                {
                    Write(",", output);
                }
            }
        }

        private static JsonObjectSerializer CreateDelegate(Type objectType)
        {
            var reflectedType = Reflector.GetReflectedType(objectType);
            return _serializers.GetOrAdd(reflectedType.InterfaceTypeName ?? reflectedType.FullTypeName, k => GenerateDelegate(k, objectType));
        }

        private static JsonObjectSerializer GenerateDelegate(string key, Type objectType)
        {
            var method = new DynamicMethod("JsonSerialize_" + key, null, new[] { typeof(object), typeof(TextWriter), typeof(bool)}, typeof(JsonSerializationWriter).Module);
            var il = method.GetILGenerator();

            var writeObject = typeof(JsonSerializationWriter).GetMethod("WriteObject");
            var write = typeof(JsonSerializationWriter).GetMethod("Write");

            var interfaceType = objectType;
            if (!interfaceType.IsInterface)
            {
                interfaceType = Reflector.ExtractInterface(objectType);
                if (interfaceType == null)
                {
                    interfaceType = objectType;
                }
            }

            var properties = Reflector.GetPropertyMap(interfaceType).Where(p => p.Key.CanRead && p.Key.CanWrite && p.Key.Name != "Indexer" && !p.Key.GetCustomAttributes(typeof(DoNotSerializeAttribute), false).Any()).ToArray();

            il.Emit(OpCodes.Ldstr, "{");
            il.Emit(OpCodes.Ldarg_1);
            il.Emit(OpCodes.Call, write);

            var index = 0;

            foreach (var property in properties)
            {
                // Write property value
                il.Emit(OpCodes.Ldarg_0);
                il.EmitCastToReference(interfaceType);
                il.EmitCall(OpCodes.Callvirt, property.Key.GetGetMethod(), null);
                il.BoxIfNeeded(property.Key.PropertyType);
                il.Emit(OpCodes.Ldstr, property.Key.Name);
                il.Emit(OpCodes.Ldarg_1);
                if (++index == properties.Length)
                {
                    il.Emit(OpCodes.Ldc_I4_0);
                }
                else
                {
                    il.Emit(OpCodes.Ldc_I4_1);
                }
                il.Emit(OpCodes.Ldarg_2);
                il.Emit(OpCodes.Call, writeObject);
            }
           
            il.Emit(OpCodes.Ldstr, "}");
            il.Emit(OpCodes.Ldarg_1);
            il.Emit(OpCodes.Call, write);
                
            il.Emit(OpCodes.Ret);

            var serializer = (JsonObjectSerializer)method.CreateDelegate(typeof(JsonObjectSerializer));
            return serializer;
        }
    }

    public static class JsonSerializationReader
    {
        public static object ReadObject(JsonValue root, Type objectType)
        {
            JsonValue current = root.FirstChild ?? root;

            object result = null;
            Type elementType = null;
            var reflectedType = Reflector.GetReflectedType(objectType);

            if (root.Type == JsonType.Object)
            {
                result = ObjectFactory.Create(objectType);
                elementType = objectType;
            }
            else if (root.Type == JsonType.Array)
            {
                if (reflectedType.IsBusinessObject)
                {
                    result = List.Create(objectType);
                    elementType = objectType;
                }
                else if (reflectedType.IsBusinessObjectList)
                {
                    elementType = reflectedType.ElementType;
                    if (!objectType.IsInterface)
                    {
                        result = (IList)Nemo.Reflection.Activator.New(objectType);
                    }
                    else
                    {
                        result = List.Create(elementType);
                    }
                }
            }

            if (elementType == null && root.Parent == null)
            {
                return result;
            }

            IDictionary<string, ReflectedProperty> propertyMap = null;
            if (elementType != null)
            {
                propertyMap = Reflector.GetPropertyNameMap(elementType);
            }
                
            while (current != null)
            {
                ReflectedProperty property = null;
                if (propertyMap == null || (current.Name != null && propertyMap.TryGetValue(current.Name, out property)))
                {
                    switch (current.Type)
                    {
                        case JsonType.Boolean:
                            if (property != null && root.Type == JsonType.Object)
                            {
                                ((IBusinessObject)result).Property(property.PropertyName, current.Value.As<bool>());
                            }
                            else
                            {
                                result = current.Value.As<bool>();
                            }
                            break;
                        case JsonType.Decimal:
                            if (property != null && root.Type == JsonType.Object)
                            {
                                var value = current.Value.As<decimal>();
                                var typeCode = Type.GetTypeCode(property.PropertyType);
                                checked
                                {
                                    switch (typeCode)
                                    {
                                        case TypeCode.Double:
                                            ((IBusinessObject)result).Property(property.PropertyName, (double)value);
                                            break;
                                        case TypeCode.Single:
                                            ((IBusinessObject)result).Property(property.PropertyName, (float)value);
                                            break;
                                        case TypeCode.Decimal:
                                            ((IBusinessObject)result).Property(property.PropertyName, value);
                                            break;
                                    }
                                }
                            }
                            else
                            {
                                var value = current.Value.As<decimal>();
                                var typeCode = Type.GetTypeCode(objectType);
                                checked
                                {
                                    switch (typeCode)
                                    {
                                        case TypeCode.Double:
                                            result = (double)value;
                                            break;
                                        case TypeCode.Single:
                                            result = (float)value;
                                            break;
                                        case TypeCode.Decimal:
                                            result = value;
                                            break;
                                    }
                                }
                            }
                            break;
                        case JsonType.Integer:
                            if (property != null && root.Type == JsonType.Object)
                            {
                                var value = current.Value.As<long>();
                                var typeCode = Type.GetTypeCode(objectType);
                                checked
                                {
                                    switch (typeCode)
                                    {
                                        case TypeCode.Byte:
                                            ((IBusinessObject)result).Property(property.PropertyName, (byte)value);
                                            break;
                                        case TypeCode.SByte:
                                            ((IBusinessObject)result).Property(property.PropertyName, (sbyte)value);
                                            break;
                                        case TypeCode.Int16:
                                            ((IBusinessObject)result).Property(property.PropertyName, (short)value);
                                            break;
                                        case TypeCode.Int32:
                                            ((IBusinessObject)result).Property(property.PropertyName, (int)value);
                                            break;
                                        case TypeCode.Int64:
                                            ((IBusinessObject)result).Property(property.PropertyName, value);
                                            break;
                                        case TypeCode.UInt16:
                                            ((IBusinessObject)result).Property(property.PropertyName, (ushort)value);
                                            break;
                                        case TypeCode.UInt32:
                                            ((IBusinessObject)result).Property(property.PropertyName, (uint)value);
                                            break;
                                        case TypeCode.UInt64:
                                            ((IBusinessObject)result).Property(property.PropertyName, (ulong)value);
                                            break;
                                    }
                                }
                            }
                            else
                            {
                                var value = current.Value.As<long>();
                                var typeCode = Type.GetTypeCode(objectType);
                                checked
                                {
                                    switch (typeCode)
                                    {
                                        case TypeCode.Byte:
                                            result = (byte)value;
                                            break;
                                        case TypeCode.SByte:
                                            result = (sbyte)value;
                                            break;
                                        case TypeCode.Int16:
                                            result = (short)value;
                                            break;
                                        case TypeCode.Int32:
                                            result = (int)value;
                                            break;
                                        case TypeCode.Int64:
                                            result = value;
                                            break;
                                        case TypeCode.UInt16:
                                            result = (ushort)value;
                                            break;
                                        case TypeCode.UInt32:
                                            result = (uint)value;
                                            break;
                                        case TypeCode.UInt64:
                                            result = (ulong)value;
                                            break;
                                    }
                                }
                            }
                            break;
                        case JsonType.String:
                            if (property != null && root.Type == JsonType.Object)
                            {
                                var value = current.Value.As<string>();
                                if (property.PropertyType == typeof(string))
                                {
                                    ((IBusinessObject)result).Property(property.PropertyName, value);
                                }
                                else if (property.PropertyType == typeof(DateTime))
                                {
                                    DateTime date;
                                    if (DateTime.TryParse(value, out date))
                                    {
                                        ((IBusinessObject)result).Property(property.PropertyName, date);
                                    }
                                }
                                else if (property.PropertyType == typeof(TimeSpan))
                                {
                                    TimeSpan time;
                                    if (TimeSpan.TryParse(value, out time))
                                    {
                                        ((IBusinessObject)result).Property(property.PropertyName, time);
                                    }
                                }
                                else if (property.PropertyType == typeof(DateTimeOffset))
                                {
                                    DateTimeOffset date;
                                    if (DateTimeOffset.TryParse(value, out date))
                                    {
                                        ((IBusinessObject)result).Property(property.PropertyName, date);
                                    }
                                }
                                else if (property.PropertyType == typeof(Guid))
                                {
                                    Guid guid;
                                    if (Guid.TryParse(value, out guid))
                                    {
                                        ((IBusinessObject)result).Property(property.PropertyName, guid);
                                    }
                                }
                                else if (property.PropertyType == typeof(char) && !string.IsNullOrEmpty(value))
                                {
                                    ((IBusinessObject)result).Property(property.PropertyName, value[0]);
                                }
                            }
                            else
                            {
                                var value = current.Value.As<string>();
                                if (objectType == typeof(string))
                                {
                                    result = value;
                                }
                                else if (objectType == typeof(DateTime))
                                {
                                    DateTime date;
                                    if (DateTime.TryParse(value, out date))
                                    {
                                        result = date;
                                    }
                                }
                                else if (objectType == typeof(TimeSpan))
                                {
                                    TimeSpan time;
                                    if (TimeSpan.TryParse(value, out time))
                                    {
                                        result = time;
                                    }
                                }
                                else if (objectType == typeof(DateTimeOffset))
                                {
                                    DateTimeOffset date;
                                    if (DateTimeOffset.TryParse(value, out date))
                                    {
                                        result = date;
                                    }
                                }
                                else if (objectType == typeof(Guid))
                                {
                                    Guid guid;
                                    if (Guid.TryParse(value, out guid))
                                    {
                                       result = guid;
                                    }
                                }
                                else if (objectType == typeof(char) && !string.IsNullOrEmpty(value))
                                {
                                    result = value[0];
                                }
                            }
                            break;
                        case JsonType.Object:
                            {
                                var item = (IBusinessObject)ReadObject(current, property.PropertyType);
                                if (root.Type == JsonType.Object)
                                {
                                    ((IBusinessObject)result).Property(property.PropertyName, item);
                                }
                                else if (root.Type == JsonType.Array)
                                {
                                    ((IList)result).Add(item);
                                }
                            }
                            break;
                        case JsonType.Array:
                            {
                                if (root.Type == JsonType.Object)
                                {
                                    if (property.IsTypeUnion)
                                    {
                                        var allTypes = property.PropertyType.GetGenericArguments();
                                        var child = current.FirstChild;
                                        var unionTypeName = child.Value.As<string>();
                                        var unionType = allTypes.FirstOrDefault(t => t.FullName == unionTypeName);
                                        if (unionType != null)
                                        {
                                            var item = ReadObject(child.NexSibling, unionType);
                                            var typeUnion = TypeUnion.Create(allTypes, item);
                                            ((IBusinessObject)result).Property(property.PropertyName, typeUnion);
                                        }
                                    }
                                    else
                                    {
                                        IList list;
                                        if (!property.IsListInterface)
                                        {
                                            list = (IList)Nemo.Reflection.Activator.New(property.PropertyType);
                                        }
                                        else
                                        {
                                            list = List.Create(property.ElementType, property.Distinct, property.Sorted);
                                        }
                                        var child = current.FirstChild;
                                        while (child != null)
                                        {
                                            var item = (IBusinessObject)ReadObject(child, property.ElementType);
                                            list.Add(item);
                                            child = child.NexSibling;
                                        }
                                        ((IBusinessObject)result).Property(property.PropertyName, list);
                                    }
                                }
                                break;
                            }
                    }
                }
                current = current.NexSibling;
            }
            return result;
        }
    }
}
