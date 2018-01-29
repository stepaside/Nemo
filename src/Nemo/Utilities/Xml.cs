using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Xml.Schema;
using System.Xml.Serialization;
using Nemo.Collections.Extensions;
using Nemo.Reflection;

namespace Nemo.Utilities
{
    public static class Xml
    {
        internal static IEnumerable<XmlSchema> InferXmlSchema(Type type)
        {
            var reflectedType = Reflector.GetReflectedType(type);
            var isDataEntity = reflectedType.IsDataEntity;
            if (isDataEntity || reflectedType.IsList)
            {
                var innerTypes = new HashSet<Type>();
                var isArray = !isDataEntity;

                var elementName = reflectedType.XmlElementName;
                var schemaXml = new StringBuilder();
                schemaXml.Append("<?xml version=\"1.0\" encoding=\"utf-8\"?><xs:schema elementFormDefault=\"qualified\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:msdata=\"urn:schemas-microsoft-com:xml-msdata\">");
                schemaXml.AppendFormat("<xs:element name=\"{0}\" nillable=\"true\" type=\"{1}\" />", (isArray ? "ArrayOf" : "") + elementName, Reflector.ClrToXmlType(type));
                WriteTypeSchema(type, schemaXml, innerTypes);
                schemaXml.Append("</xs:schema>");

                var schema = XmlSchema.Read(new StringReader(schemaXml.ToString()), null);
                return schema.Return();
            }
            else
            {
                var importer = new XmlReflectionImporter();
                var schemas = new XmlSchemas();
                var exporter = new XmlSchemaExporter(schemas);

                var xmlTypeMapping = importer.ImportTypeMapping(type);
                exporter.ExportTypeMapping(xmlTypeMapping);

                schemas.Compile(null, false);
                return schemas;
            }
        }

        private static void WriteTypeSchema(Type objectType, StringBuilder schemaXml, HashSet<Type> allInnerTypes)
        {
            if (allInnerTypes.Contains(objectType)) return;
            allInnerTypes.Add(objectType);

            schemaXml.AppendFormat("<xs:complexType name=\"{0}\">", Reflector.ClrToXmlType(objectType));
            schemaXml.Append("<xs:sequence>");

            var reflectedType = Reflector.GetReflectedType(objectType);
            if (reflectedType.IsList && !reflectedType.IsSimpleList)
            {
                var elementType = reflectedType.ElementType;
                if (Reflector.IsEmitted(elementType))
                {
                    elementType = Reflector.GetInterface(elementType);
                }

                schemaXml.AppendFormat("<xs:element minOccurs=\"0\" maxOccurs=\"unbounded\" name=\"{0}\" nillable=\"true\" type=\"{1}\" />", GetElementNameFromType(elementType), Reflector.ClrToXmlType(elementType));
                schemaXml.Append("</xs:sequence></xs:complexType>");
                WriteTypeSchema(elementType, schemaXml, allInnerTypes);
            }
            else
            {
                var innerTypes = new List<Type>();
                var properties = Reflector.GetPropertyMap(objectType);
                foreach (var property in properties)
                {
                    if (property.Key.GetCustomAttributes(typeof(XmlIgnoreAttribute), false).Length > 0) continue;

                    var nillable = false;
                    var simpleCollection = false;
                    //var isList = false;
                    var propertyType = property.Value.PropertyType;
                    if (property.Value.IsSimpleList)
                    {
                        nillable = true;
                        simpleCollection = true;
                    }
                    else if (property.Value.IsDataEntity)
                    {
                        innerTypes.Add(property.Value.PropertyType);
                        nillable = true;
                    }
                    else if (property.Value.IsList)
                    {
                        //propertyType = property.Value.ElementType;
                        innerTypes.Add(propertyType);
                        //isList = true;
                        nillable = true;
                    }
                    else if (property.Value.IsNullableType)
                    {
                        nillable = true;
                    }
                    schemaXml.AppendFormat("<xs:element minOccurs=\"0\" maxOccurs=\"{4}\" name=\"{0}\"{2}{3}type=\"{1}\" />", property.Value.PropertyName, Reflector.ClrToXmlType(propertyType), nillable ? " nillable=\"true\" " : " ", simpleCollection ? string.Format(" msdata:DataType=\"{0}\" ", property.Key.PropertyType.AssemblyQualifiedName) : string.Empty, "1");
                }

                schemaXml.Append("</xs:sequence></xs:complexType>");

                foreach (var type in innerTypes)
                {
                    WriteTypeSchema(type, schemaXml, allInnerTypes);
                }
            }
        }

        #region Helper Methods

        internal delegate string GetElementDelegate(Type objectType);

        private static readonly ConcurrentDictionary<Type, RuntimeMethodHandle> GetElementNameMethods = new ConcurrentDictionary<Type, RuntimeMethodHandle>();

        public static string GetElementNameFromType<T>()
        {
            var attr = Reflector.GetAttribute<T, XmlRootAttribute>();
            if (attr != null && !string.IsNullOrEmpty(attr.ElementName)) return attr.ElementName;
            var objectType = Reflector.TypeCache<T>.Type;
            var name = objectType.TypeName;
            if (objectType.IsSimpleType) return Reflector.SimpleClrToXmlType(typeof(T));
            if (objectType.IsGenericType)
            {
                var pos = name.IndexOf('`');
                if (pos > -1)
                {
                    return name.Substring(0, pos).TrimStart('I');
                }
                pos = name.IndexOf('<');
                return name.Substring(0, pos).TrimStart('I');
            }
            return objectType.IsInterface ? name.TrimStart('I') : name;
        }

        internal static string GetElementNameFromType(Type objectType)
        {
            var handle = GetElementNameMethods.GetOrAdd(objectType, type =>
            {
                var method = typeof(Xml).GetMethods(BindingFlags.Public | BindingFlags.Static).First(m => m.Name == "GetElementNameFromType" && m.IsGenericMethod);
                var genericMethod = method.MakeGenericMethod(objectType);
                return genericMethod.MethodHandle;
            });
            var func = Reflector.Method.CreateDelegate(handle);
            return (string)func(null, null);
        }

        internal static string GetElementName(PropertyInfo property, bool isSimpleList)
        {
            var xmlElemAttr = property.GetCustomAttributes(typeof(XmlElementAttribute), false).Cast<XmlElementAttribute>().FirstOrDefault();
            if (xmlElemAttr == null || string.IsNullOrEmpty(xmlElemAttr.ElementName))
            {
                var arrayPrefix = isSimpleList ? "ArrayOf" : string.Empty;
                return arrayPrefix + property.Name;
            }
            else
            {
                return xmlElemAttr.ElementName;
            }
        }

        #endregion
    }
}
