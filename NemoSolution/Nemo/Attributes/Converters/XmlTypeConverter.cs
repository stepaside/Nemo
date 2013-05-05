using System;
using System.Xml;
using System.Xml.Linq;

namespace Nemo.Attributes.Converters
{
    public class XmlTypeConverter : ITypeConverter<object, XDocument>
    {
        #region ITypeConverter<object,XDocument> Members

        XDocument ITypeConverter<object, XDocument>.ConvertForward(object from)
        {
            if (from == null)
            {
                return null;
            }
            else
            {
                try
                {
                    return XDocument.Parse(Convert.ToString(from));
                }
                catch { }
                return null;
            }
        }

        object ITypeConverter<object, XDocument>.ConvertBackward(XDocument to)
        {
            if (to != null)
            {
                return to.ToString();
            }
            else
            {
                return null;
            }
        }
        
        #endregion
    }
}