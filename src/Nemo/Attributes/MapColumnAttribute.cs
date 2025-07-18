using System;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using Nemo.Reflection;

namespace Nemo.Attributes
{
    [AttributeUsage(AttributeTargets.Property, Inherited = true, AllowMultiple = false)]
    public class MapColumnAttribute : MapAttribute
    {
        public MapColumnAttribute(string sourceName) : base(sourceName) { }

        [EditorBrowsable(EditorBrowsableState.Never)]
        public static string GetMappedColumnName(PropertyInfo property)
        {
            if (property == null) return null;

            var mapping = property.GetCustomAttributes(typeof(MapColumnAttribute), false).Cast<MapColumnAttribute>().FirstOrDefault();
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
