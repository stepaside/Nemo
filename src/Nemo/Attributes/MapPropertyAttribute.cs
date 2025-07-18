using System;
using System.ComponentModel;
using System.Linq;
using System.Reflection;

namespace Nemo.Attributes
{
    [AttributeUsage(AttributeTargets.Property, Inherited = true, AllowMultiple = false)]
    public class MapPropertyAttribute : MapAttribute
    {
        public MapPropertyAttribute(string sourceName) : base(sourceName) { }

        [EditorBrowsable(EditorBrowsableState.Never)]
        public static string GetMappedPropertyName(PropertyInfo property)
        {
            if (property == null) return null;

            var mapping = property.GetCustomAttributes(typeof(MapPropertyAttribute), false).Cast<MapPropertyAttribute>().FirstOrDefault();
            if (mapping == null)
            {
                //	Default mapping
                return property.Name;
            }
            else
            {
                return mapping.SourceName;
            }
        }
    }
}
