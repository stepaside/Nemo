using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection.Emit;
using System.Security;
using System.Text;
using System.Xml;
using System.Xml.Serialization;
using Nemo.Attributes;
using Nemo.Collections;
using Nemo.Collections.Extensions;
using Nemo.Extensions;
using Nemo.Fn;
using Nemo.Reflection;
using Nemo.Utilities;

namespace Nemo.Serialization
{
    public static class ObjectXmlSerializer
    {
        /// <summary>
        /// ToXml method provides an ability to convert an object to XML string. 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="businessObject"></param>
        /// <returns></returns>
        private static void ToXml<T>(this T businessObject, string documentElement, TextWriter output, bool addSchemaDeclaration)
            where T : class, IBusinessObject
        {
            var documentElementName = documentElement ?? Xml.GetElementNameFromType<T>();
            XmlSerializationWriter.WriteObject(businessObject, documentElementName, output, addSchemaDeclaration);
        }

        private static string ToXml<T>(this T businessObject, string documentElement, bool addSchemaDeclaration)
            where T : class, IBusinessObject
        {
            var output = new StringBuilder(1024);
            using (var writer = new StringWriter(output))
            {
                businessObject.ToXml(documentElement, writer, addSchemaDeclaration);
            }
            return output.ToString();
        }

        public static string ToXml<T>(this T businessObject)
            where T : class, IBusinessObject
        {
            return businessObject.ToXml(null, true);
        }

        public static void ToXml<T>(this T businessObject, TextWriter writer)
            where T : class, IBusinessObject
        {
            businessObject.ToXml(null, writer, true);
        }

        public static string ToXml<T>(this IEnumerable<T> businessObjects)
           where T : class, IBusinessObject
        {
            var output = new StringBuilder(1024);
            using (var writer = new StringWriter(output))
            {
                businessObjects.ToXml(writer);
            }
            return output.ToString();
        }

        public static void ToXml<T>(this IEnumerable<T> businessObjects, TextWriter writer)
            where T : class, IBusinessObject
        {
            var documentElementName = string.Empty;
            var addSchemaDeclaration = true;

            foreach (var businessObject in businessObjects)
            {
                if (string.IsNullOrEmpty(documentElementName))
                {
                    documentElementName = Xml.GetElementNameFromType<T>();
                    if (!string.IsNullOrEmpty(documentElementName))
                    {
                        writer.Write(string.Format("<ArrayOf{0} xmlns:xs=\"http://www.w3.org/2001/XMLSchema\">", documentElementName));
                        addSchemaDeclaration = false;
                    }
                }
                businessObject.ToXml(null, addSchemaDeclaration);
            }

            if (!string.IsNullOrEmpty(documentElementName))
            {
                writer.Write(string.Format("</ArrayOf{0}>", documentElementName));
            }
        }

        public static IEnumerable<T> FromXml<T>(string xml)
            where T : class, IBusinessObject
        {
            return FromXml<T>(new StringReader(xml));
        }

        public static IEnumerable<T> FromXml<T>(Stream stream)
            where T : class, IBusinessObject
        {
            using (var reader = new StreamReader(stream))
            {
                return FromXml<T>(reader);
            }
        }

        public static IEnumerable<T> FromXml<T>(TextReader textReader)
           where T : class, IBusinessObject
        {
            using (var reader = XmlReader.Create(textReader))
            {
                return FromXml<T>(reader);
            }
        }

        public static IEnumerable<T> FromXml<T>(XmlReader reader)
            where T : class, IBusinessObject
        {
            bool isArray;
            var result = XmlSerializationReader.ReadObject(reader, typeof(T), out isArray);
            if (isArray)
            {
                return ((IList)result).Cast<T>();
            }
            else
            {
                return ((T)result).Return();
            }
        }
    }

    public static class XmlSerializationWriter
    {
        internal delegate void XmlObjectSerializer(object value, TextWriter output, bool addSchemaDeclaration);

        private static ConcurrentDictionary<string, XmlObjectSerializer> _serializers = new ConcurrentDictionary<string, XmlObjectSerializer>();
        
        public static void WriteStartElement(string name, TextWriter output, bool addSchemaDeclaration, IDictionary<string, string> attributes)
        {
            output.Write(string.Format("<{0}", name));
            if (addSchemaDeclaration)
            {
                 output.Write(" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\"");
            }
            if (attributes != null)
            {
                foreach (var attr in attributes)
                {
                    WriteAttribute(attr.Key, attr.Value, output);
                }
            }
            output.Write(">");
        }

        public static void WriteEndElement(string name, TextWriter output)
        {
            output.Write(string.Format("</{0}>", name));
        }

        public static void WriteEmptyElement(string name, TextWriter output, bool addSchemaDeclaration, IDictionary<string, string> attributes)
        {
            output.Write(string.Format("<{0}", name));
            if (addSchemaDeclaration)
            {
                output.Write(" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\"");
            }
            if (attributes != null)
            {
                foreach (var attr in attributes)
                {
                    WriteAttribute(attr.Key, attr.Value, output);
                }
            }
            output.Write(" />");
        }

        public static void WriteAttribute(string name, string value, TextWriter output)
        {
            if (!string.IsNullOrEmpty(value))
            {
                output.Write(" {0}=\"{1}\"", name, value);
            }
        }

        public static void WriteElement(string name, string value, TextWriter output, bool addSchemaDeclaration, IDictionary<string, string> attributes)
        {
            if (!string.IsNullOrEmpty(value))
            {
                WriteStartElement(name, output, addSchemaDeclaration, attributes);
                output.Write(value);
                WriteEndElement(name, output);
            }
        }

        public static void Write(string value, TextWriter output)
        {
            if (!string.IsNullOrEmpty(value))
            {
                output.Write(value);
            }
        }

        public static void WriteList(IList items, bool isSimpleList, TextWriter output)
        {
            string name = null;
            if (isSimpleList && items.Count > 0)
            {
                name = Reflector.SimpleClrToXmlType(items[0].GetType());
            }

            for (int i = 0; i < items.Count; i++)
            {
                if (isSimpleList)
                {
                    WriteObject(items[i], name, output);
                }
                else
                {
                    WriteObject(items[i], Xml.GetElementNameFromType(items[i].GetType()), output);
                }
            }
        }

        public static void WriteList<T>(IList<T> items, bool isSimpleList, TextWriter output)
        {
            WriteList((IList)items, isSimpleList, output);
        }

        public static void WriteDictionary<T, U>(IDictionary<T, U> map, string name, TextWriter output)
        {
            foreach (var pair in map)
            {
                WriteObject(pair.Key, "Key", output);
                WriteObject(pair.Value, "Value", output);
            }
        }

        public static void WriteObject(object value, string name, TextWriter output, bool addSchemaDeclaration = true, IDictionary<string, string> attributes = null, Type objectType = null)
        {
            if (value != null)
            {
                objectType = objectType ?? value.GetType();
                var typeCode = Type.GetTypeCode(objectType);
                if (typeCode != TypeCode.DBNull)
                {
                    string xmlValue = null;
                    switch (typeCode)
                    {
                        case TypeCode.Boolean:
                            xmlValue = XmlConvert.ToString((bool)value);
                            break;
                        case TypeCode.String:
                            xmlValue = SecurityElement.Escape((string)value);
                            break;
                        case TypeCode.DateTime:
                            xmlValue = XmlConvert.ToString((DateTime)value, XmlDateTimeSerializationMode.Utc);
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
                            xmlValue = Convert.ToString(value);
                            break;
                        default:
                            if (objectType == typeof(byte[]))
                            {
                                xmlValue = Convert.ToBase64String((byte[])value);
                            }
                            else if (objectType == typeof(DateTimeOffset))
                            {
                                xmlValue = XmlConvert.ToString((DateTimeOffset)value);
                            }
                            else if (objectType == typeof(TimeSpan))
                            {
                                xmlValue = XmlConvert.ToString((TimeSpan)value);
                            }
                            else if (objectType == typeof(Guid))
                            {
                                xmlValue = ((Guid)value).ToString("D");
                            }
                            else if (value is IBusinessObject)
                            {
                                var serializer = CreateDelegate(objectType);
                                serializer(value, output, addSchemaDeclaration);
                            }
                            else if (value is IList)
                            {
                                WriteStartElement(name, output, false, null);
                                var items = ((IList)value).Cast<object>().ToArray();
                                if (items.Length > 0 && items[0] is IBusinessObject)
                                {
                                    var elementType = items[0].GetType();
                                    var serializer = CreateDelegate(elementType);
                                    foreach (var item in items)
                                    {
                                        serializer(item, output, false);
                                    }
                                }
                                else
                                {
                                    WriteList((IList)value, true, output);
                                }
                                WriteEndElement(name, output);
                            }
                            else if (value is ITypeUnion)
                            {
                                var typeUnion = (ITypeUnion)value;
                                WriteStartElement(name, output, false, new Dictionary<string, string> { { "__type", typeUnion.UnionType.FullName } });
                                var reflectedUnionType = Reflector.GetReflectedType(typeUnion.UnionType);
                                for (int i = 0; i < typeUnion.AllTypes.Length; i++)
                                {
                                    var itemName = "Item" + (i + 1);
                                    if (typeUnion.AllTypes[i] == typeUnion.UnionType)
                                    {
                                        WriteStartElement(itemName, output, false, null);
                                        if (reflectedUnionType.IsSimpleType)
                                        {
                                            WriteObject(typeUnion.GetObject(), Reflector.SimpleClrToXmlType(typeUnion.UnionType), output, false);
                                        }
                                        else
                                        {
                                            var serializer = CreateDelegate(typeUnion.UnionType);
                                            serializer(typeUnion.GetObject(), output, addSchemaDeclaration);
                                        }
                                        WriteEndElement(itemName, output);
                                    }
                                    else
                                    {
                                        WriteEmptyElement(itemName, output, false, null);
                                    }
                                }
                                WriteEndElement(name, output);
                            }
                            else
                            {
                                WriteStartElement(name, output, false, null);
                                new XmlSerializer(objectType).Serialize(output, value);
                                WriteEndElement(name, output);
                            }
                            break;
                    }

                    if (xmlValue != null)
                    {
                        WriteElement(name, xmlValue, output, false, null);
                    }
                }
            }
        }
        
        public static string GetXmlValue(object value, Type objectType)
        {
            string xmlValue = null;
            if (value != null)
            {
                var typeCode = Type.GetTypeCode(objectType);
                if (typeCode != TypeCode.DBNull)
                {
                    switch (typeCode)
                    {
                        case TypeCode.Boolean:
                            xmlValue = XmlConvert.ToString((bool)value);
                            break;
                        case TypeCode.String:
                            xmlValue = SecurityElement.Escape((string)value);
                            break;
                        case TypeCode.DateTime:
                            xmlValue = XmlConvert.ToString((DateTime)value, XmlDateTimeSerializationMode.Utc);
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
                            xmlValue = Convert.ToString(value);
                            break;
                        default:
                            if (objectType == typeof(byte[]))
                            {
                                xmlValue = Convert.ToBase64String((byte[])value);
                            }
                            else if (objectType == typeof(DateTimeOffset))
                            {
                                xmlValue = XmlConvert.ToString((DateTimeOffset)value);
                            }
                            else if (objectType == typeof(TimeSpan))
                            {
                                xmlValue = XmlConvert.ToString((TimeSpan)value);
                            }
                            else if (objectType == typeof(Guid))
                            {
                                xmlValue = ((Guid)value).ToString("D");
                            }
                            break;
                    }
                }
            }
            return xmlValue;
        }
        
        private static XmlObjectSerializer CreateDelegate(Type objectType)
        {
            var reflectedType = Reflector.GetReflectedType(objectType);
            return _serializers.GetOrAdd(reflectedType.InterfaceTypeName ?? reflectedType.FullTypeName, k => GenerateDelegate(k, objectType));
        }

        private static XmlObjectSerializer GenerateDelegate(string key, Type objectType)
        {
            var method = new DynamicMethod("XmlSerialize_" + key, null, new[] { typeof(object), typeof(TextWriter), typeof(bool) }, typeof(XmlSerializationWriter).Module);
            var il = method.GetILGenerator();

            var writeObject = typeof(XmlSerializationWriter).GetMethod("WriteObject");
            var writeStart = typeof(XmlSerializationWriter).GetMethod("WriteStartElement");
            var writeEnd = typeof(XmlSerializationWriter).GetMethod("WriteEndElement");
            var getXmlValue = typeof(XmlSerializationWriter).GetMethod("GetXmlValue");
            var getTypeFromHandle = typeof(Type).GetMethod("GetTypeFromHandle");

            var interfaceType = objectType;
            if (!interfaceType.IsInterface)
            {
                interfaceType = Reflector.ExtractInterface(objectType);
                if (interfaceType == null)
                {
                    interfaceType = objectType;
                }
            }

            var properties = Reflector.GetPropertyMap(interfaceType).Where(p => p.Key.CanRead && p.Key.CanWrite && p.Key.Name != "Indexer" && !p.Key.GetCustomAttributes(typeof(DoNotSerializeAttribute), false).Any())
                            .Partition(p => (p.Value.IsSimpleType || (p.Value.IsSimpleList && p.Value.ElementType == typeof(byte))) && p.Key.GetCustomAttributes(typeof(XmlAttributeAttribute), false).Any());

            var elementName = Xml.GetElementNameFromType(interfaceType);

            if (properties.Item1.Any())
            {
                var attributeSetType = typeof(Dictionary<string, string>);
                var addItem = attributeSetType.GetMethod("Add");
                il.DeclareLocal(attributeSetType);
                il.Emit(OpCodes.Newobj, attributeSetType.GetConstructor(Type.EmptyTypes));
                il.Emit(OpCodes.Stloc_0);

                foreach (var property in properties.Item1)
                {
                    il.Emit(OpCodes.Ldloc_0);
                    il.Emit(OpCodes.Ldstr, property.Value.PropertyName);
                    il.Emit(OpCodes.Ldarg_0);
                    il.EmitCastToReference(interfaceType);
                    il.EmitCall(OpCodes.Callvirt, property.Key.GetGetMethod(), null);
                    il.BoxIfNeeded(property.Value.PropertyType);
                    il.Emit(OpCodes.Ldtoken, property.Value.PropertyType);
                    il.Emit(OpCodes.Call, getTypeFromHandle);
                    il.Emit(OpCodes.Call, getXmlValue);
                    il.EmitCall(OpCodes.Callvirt, addItem, null);
                }
            }

            il.Emit(OpCodes.Ldstr, elementName);
            il.Emit(OpCodes.Ldarg_1);
            il.Emit(OpCodes.Ldarg_2);
            if (properties.Item1.Any())
            {
                il.Emit(OpCodes.Ldloc_0);
            }
            else
            {
                il.Emit(OpCodes.Ldnull);
            }
            il.Emit(OpCodes.Call, writeStart);
            
            foreach (var property in properties.Item2)
            {
                // Write property value
                il.Emit(OpCodes.Ldarg_0);
                il.EmitCastToReference(interfaceType);
                il.EmitCall(OpCodes.Callvirt, property.Key.GetGetMethod(), null);
                il.BoxIfNeeded(property.Key.PropertyType);
                il.Emit(OpCodes.Ldstr, Xml.GetElementName(property.Key, property.Value.IsSimpleList));
                il.Emit(OpCodes.Ldarg_1);
                il.Emit(OpCodes.Ldc_I4_0);
                if (properties.Item1.Any())
                {
                    il.Emit(OpCodes.Ldloc_0);
                }
                else
                {
                    il.Emit(OpCodes.Ldnull);
                }
                il.Emit(OpCodes.Ldtoken, property.Value.PropertyType);
                il.Emit(OpCodes.Call, getTypeFromHandle);
                il.Emit(OpCodes.Call, writeObject);
            }

            il.Emit(OpCodes.Ldstr, elementName);
            il.Emit(OpCodes.Ldarg_1);
            il.Emit(OpCodes.Call, writeEnd);

            il.Emit(OpCodes.Ret);

            var serializer = (XmlObjectSerializer)method.CreateDelegate(typeof(XmlObjectSerializer));
            return serializer;
        }
    }

    public static class XmlSerializationReader
    {   
        private static object ReadValue(XmlReader reader, Type objectType, string name)
        {
            object value = null;
            var hasValue = false;
            if (name != null && reader.HasAttributes)
            {
                var attrValue = reader.GetAttribute(name);
                hasValue = !string.IsNullOrEmpty(attrValue);
                attrValue.TryParse(objectType, out value);
            }

            if (!hasValue)
            {
                value = reader.ReadElementContentAs(objectType, null);
            }
            return value;
        }

        public static object ReadObject(XmlReader reader, Type objectType, out bool isArray)
        {
            var states = new LinkedList<SerializationReaderState>();
            object result = null;
            isArray = false;

            IBusinessObject item = null;
            IList list = null;
            ITypeUnion union = null;
            Type elementType = null;
            IDictionary<string, ReflectedProperty> propertyMap = null;
            var isSimple = false;
            string name = null;

            if (reader.IsStartElement())
            {
                name = reader.Name;
                isArray = name.StartsWith("ArrayOf");
                var reflectedType = Reflector.GetReflectedType(objectType);
                if (isArray || reflectedType.IsList)
                {
                    elementType = reflectedType.IsList ? reflectedType.ElementType : objectType;
                    isSimple = reflectedType.IsList ? reflectedType.IsSimpleList : reflectedType.IsSimpleType;
                    propertyMap = Reflector.GetPropertyNameMap(elementType);
                    if (!objectType.IsInterface)
                    {
                        list = (IList)Nemo.Reflection.Activator.New(objectType);
                    }
                    else
                    {
                        list = List.Create(elementType);
                    }
                    result = list;
                }
                else if (reflectedType.IsBusinessObject)
                {
                    propertyMap = Reflector.GetPropertyNameMap(objectType);
                    item = (IBusinessObject)ObjectFactory.Create(objectType);
                    result = item;
                    // Handle attributes
                    if (reader.HasAttributes)
                    {
                        for (int i = 0; i < reader.AttributeCount; i++)
                        {
                            reader.MoveToAttribute(i);
                            ReflectedProperty property;
                            var propertyName = reader.Name;
                            if (propertyMap.TryGetValue(propertyName, out property))
                            {
                                object value = ReadValue(reader, property.PropertyType, propertyName);
                                if (value != null)
                                {
                                    item.Property(propertyName, value);
                                }
                            }
                        }
                    }
                }
                else if (reflectedType.IsSimpleType)
                {
                    result = ReadValue(reader, objectType, null);
                    isSimple = true;
                }
                reader.Read();
            }

            if (isSimple && list == null)
            {
                return result;
            }

            states.AddLast(new SerializationReaderState { Name = name, Item = item, List = list, Union = union, ElementType = elementType, PropertyMap = propertyMap, IsSimple = isSimple });

            while (reader.IsStartElement())
            {
                name = reader.Name;
                var lastState = states.Last;
                var currentValue = lastState.Value;
                var currentMap = currentValue.PropertyMap;
                ReflectedProperty property;
                if (currentMap != null && currentMap.TryGetValue(name, out property))
                {
                    if (property.IsBusinessObject)
                    {
                        propertyMap = Reflector.GetPropertyNameMap(property.PropertyType);
                        item = (IBusinessObject)ObjectFactory.Create(property.PropertyType);
                        states.AddLast(new SerializationReaderState { Name = name, Item = item, PropertyMap = propertyMap });

                        if (currentValue.Item != null)
                        {
                            currentValue.Item.Property(name, item);
                        }
                        else if (currentValue.Union != null)
                        {
                            currentValue.Union = TypeUnion.Create(currentValue.Union.AllTypes, item);
                        }
                        else
                        {
                            currentValue.List.Add(item);
                        }
                    }
                    else if (property.IsBusinessObjectList)
                    {
                        elementType = property.ElementType;
                        propertyMap = Reflector.GetPropertyNameMap(elementType);
                        if (!property.IsListInterface)
                        {
                            list = (IList)Nemo.Reflection.Activator.New(property.PropertyType);
                        }
                        else
                        {
                            list = List.Create(elementType, property.Distinct, property.Sorted);
                        }
                        states.AddLast(new SerializationReaderState { Name = name, List = list, ElementType = elementType, PropertyMap = propertyMap });

                        if (currentValue.Item != null)
                        {
                            currentValue.Item.Property(name, list);
                        }
                    }
                    else if (property.IsTypeUnion)
                    {
                        var allTypes = property.PropertyType.GetGenericArguments();
                        var unionTypeName = reader.GetAttribute("__type");
                        var unionType = allTypes.FirstOrDefault(t => t.FullName == unionTypeName);
                        if (unionType != null)
                        {
                            union = TypeUnion.Create(allTypes, unionType.GetDefault());
                            states.AddLast(new SerializationReaderState { Name = name, Union = union, ElementType = unionType });
                            if (currentValue.Item != null)
                            {
                                currentValue.Item.Property(name, union);
                            }
                        }
                    }
                    else
                    {
                        object value = ReadValue(reader, currentValue.ElementType ?? property.PropertyType, name);
                        if (value != null)
                        {
                            if (!currentValue.IsSimple)
                            {
                                currentValue.Item.Property(property.PropertyName, value);
                            }
                            else if (currentValue.List != null)
                            {
                                currentValue.List.Add(value);
                            }
                            else
                            {
                                currentValue.Value = value;
                            }
                        }
                        if (reader.NodeType != XmlNodeType.EndElement)
                        {
                            continue;
                        }
                    }
                }
                else if (currentValue.Union != null)
                {
                    var previousState = lastState.Previous;
                    if (previousState.Value.Item != null)
                    {
                        var currentUnion = currentValue.Union;
                        var unionType = currentUnion.UnionType;
                        for (int i = 0; i < currentUnion.AllTypes.Length; i++)
                        {
                            reader.ReadStartElement();
                            if (unionType == currentUnion.AllTypes[i])
                            {
                                var reflectedType = Reflector.GetReflectedType(unionType);
                                var subtree = reader.ReadSubtree();
                                var isArraySybtree = false;
                                var value = ReadObject(subtree, unionType, out isArraySybtree);
                                currentUnion = TypeUnion.Create(currentUnion.AllTypes, value);
                                previousState.Value.Item.Property(currentValue.Name, currentUnion);
                                reader.Read();
                                reader.ReadEndElement();
                            }
                        }
                    }
                }
                else if (currentValue.ElementType != null)
                {
                    item = (IBusinessObject)ObjectFactory.Create(currentValue.ElementType);
                    currentValue.List.Add(item);
                    states.AddLast(new SerializationReaderState { Name = name, Item = item, PropertyMap = currentValue.PropertyMap });
                }

                if (reader.NodeType == XmlNodeType.EndElement)
                {
                    while (reader.NodeType == XmlNodeType.EndElement)
                    {
                        if (states.Count > 0)
                        {
                            states.RemoveLast();
                        }
                        reader.Read();
                    }
                }
                else
                {
                    reader.Read();
                }
            }
            return result;
        }
    }
}
