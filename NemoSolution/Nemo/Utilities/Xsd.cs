using System.IO;
using System.Linq;
using System.Xml.Schema;

namespace Nemo.Utilities
{
    public class Xsd<T>
    {
        // ReSharper disable once StaticMemberInGenericType
        public static readonly XmlSchema Schema;
        // ReSharper disable once StaticMemberInGenericType
        public static readonly string Text;

        static Xsd()
        {
            Schema = Xml.InferXmlSchema(typeof(T)).FirstOrDefault();
            if (Schema == null) return;
            using (var stream = new MemoryStream(1024))
            {
                Schema.Write(stream);
                stream.Seek(0, SeekOrigin.Begin);
                using (var reader = new StreamReader(stream))
                {
                    Text = reader.ReadToEnd();
                }
            }
        }
    }
}
