using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml;
using System.Xml.Linq;
using System.Xml.Schema;

namespace Nemo.Validation
{
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false)]
    public class XmlSchemaAttribute : ValidationAttributeBase
    {
        private const string DEFAULT_ERROR_MESSAGE = "The field {0} contains invalid XML data.";

        public XmlSchemaAttribute()
            : base()
        {
            this.InitializeDefaultErrorMessage();
        }

        public string SchemaProperty { get; set; }

        public string SchemaLocation { get; set; }

        public string InlineSchema { get; set; }

        protected override void InitializeDefaultErrorMessage()
        {
            if (string.IsNullOrEmpty(this.DefaultErrorMessage))
            {
                this.DefaultErrorMessage = DEFAULT_ERROR_MESSAGE;
            }
        }

        public override bool IsValid(object value)
        {
            if (!string.IsNullOrEmpty(this.SchemaProperty) && value != null && value.GetType().IsArray)
            {
                object[] values = (object[])value;
                if (values.Length > 1)
                {
                    object v1 = values[0];
                    object v2 = values[1];

                    if (v2 != null && v2.GetType() == typeof(string))
                    {
                        this.InlineSchema = Convert.ToString(v2);
                    }
                    value = v1;
                }
            }

            XDocument doc = null;
            if (value is string)
            {
                doc = XDocument.Parse((string)value);
            }
            else if (value is XmlReader)
            {
                doc = XDocument.Load((XmlReader)value);
            }
            else if (value is XmlDocument)
            {
                doc = XDocument.Parse(((XmlDocument)value).OuterXml);
            }
            else
            {
                return false;
            }

            XmlSchemaSet schemas = new XmlSchemaSet();
            if (!string.IsNullOrEmpty(this.InlineSchema))
            {
                schemas.Add("", XmlReader.Create(new StringReader(this.InlineSchema)));
            }
            else if (!string.IsNullOrEmpty(this.SchemaLocation))
            {
                schemas.Add("", XmlReader.Create(this.SchemaLocation));
            }
            else
            {
                return false;
            }

            bool errors = false;
            doc.Validate(schemas, (o, e) => errors = true);
            return !errors;
        }

        public bool IsValid(string value, string schema)
        {
            return IsValid(new object[] { value, schema });
        }

        public bool IsValid(XmlReader value, string schema)
        {
            return IsValid(new object[] { value, schema });
        }

        public bool IsValid(XmlDocument value, string schema)
        {
            return IsValid(new object[] { value, schema });
        }
    }
}
