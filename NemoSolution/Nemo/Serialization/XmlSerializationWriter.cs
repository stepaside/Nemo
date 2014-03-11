using Nemo.Attributes;
using Nemo.Collections.Extensions;
using Nemo.Fn;
using Nemo.Reflection;
using Nemo.Utilities;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection.Emit;
using System.Security;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Serialization;

namespace Nemo.Serialization
{
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
                    WriteObject(items[i], Xml.GetElementNameFromType(items[i].GetType()), output, false);
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
                            else if (value is IDataEntity)
                            {
                                var serializer = CreateDelegate(objectType);
                                serializer(value, output, addSchemaDeclaration);
                            }
                            else if (value is IList)
                            {
                                WriteStartElement(name, output, false, null);
                                var items = ((IList)value).Cast<object>().ToArray();
                                if (items.Length > 0 && items[0] is IDataEntity)
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
            if (Reflector.IsEmitted(objectType))
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
}
