using System;
using System.Xml;
using System.IO;

namespace Nemo.Attributes.Converters
{
    public class XmlReaderTypeConverter : ITypeConverter<object, XmlReader>
    {
        #region ITypeConverter<object,XmlReader> Members
 
        XmlReader ITypeConverter<object, XmlReader>.ConvertForward(object from)
        {
            if (from == null)
            {
                return null;
            }
            else
            {
                XmlReader reader = XmlReader.Create(new StringReader(Convert.ToString(from)));
                return reader;
            }
        }

        object ITypeConverter<object, XmlReader>.ConvertBackward(XmlReader to)
        {
            if (to != null)
            {
                return to.ReadOuterXml();
            }
            else
            {
                return null;
            }
        }
        
        #endregion
    }
}