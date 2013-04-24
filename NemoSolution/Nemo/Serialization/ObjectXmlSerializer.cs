﻿using System;
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

        public static IEnumerable<T> FromXml<T>(this string xml)
            where T : class, IBusinessObject
        {
            return FromXml<T>(new StringReader(xml));
        }

        public static IEnumerable<T> FromXml<T>(this Stream stream)
            where T : class, IBusinessObject
        {
            using (var reader = new StreamReader(stream))
            {
                return FromXml<T>(reader);
            }
        }

        public static IEnumerable<T> FromXml<T>(this TextReader textReader)
           where T : class, IBusinessObject
        {
            using (var reader = XmlReader.Create(textReader))
            {
                return FromXml<T>(reader);
            }
        }

        public static IEnumerable<T> FromXml<T>(this XmlReader reader)
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
}
