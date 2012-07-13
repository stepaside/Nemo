using System;
using System.Xml;

namespace Nemo.Attributes.Converters
{
    /// <summary>
    /// Represents a type converter converting strings to nullable strings.  If the object is
    /// <see cref="DBNull"/>, the nullable type will be <c>null</c> string.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class XmlTypeConverter : ITypeConverter<object, XmlDocument>
    {
        #region ITypeConverter<object,XDocument> Members
        XmlDocument ITypeConverter<object, XmlDocument>.ConvertForward(object from)
        {
            if (from == null)
            {
                return null;
            }
            else
            {
                XmlDocument doc = new XmlDocument();
                try
                {
                    doc.LoadXml(Convert.ToString(from));
                }
                catch { }
                return doc;
            }
        }

        object ITypeConverter<object, XmlDocument>.ConvertBackward(XmlDocument to)
        {
            if (to != null)
            {
                return to.OuterXml;
            }
            else
            {
                return null;
            }
        }
        #endregion
    }
}