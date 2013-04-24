using Nemo.Collections;
using Nemo.Extensions;
using Nemo.Fn;
using Nemo.Reflection;
using Nemo.Utilities;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Nemo.Collections.Extensions;

namespace Nemo.Serialization
{
    public static class JsonSerializationReader
    {
        public static object ReadObject(JsonValue root, Type objectType)
        {
            JsonValue current = root.FirstChild ?? root;

            object result = null;
            object listResult = null;
            Type elementType = null;
            var reflectedType = Reflector.GetReflectedType(objectType);
            var simpleElement = false;
            JsonValue listRoot = null;

            if (root.Type == JsonType.Object)
            {
                result = ObjectFactory.Create(objectType);
                elementType = objectType;
            }
            else if (root.Type == JsonType.Array)
            {
                if (reflectedType.IsBusinessObject)
                {
                    result = List.Create(objectType);
                    elementType = objectType;
                }
                else if (reflectedType.IsBusinessObjectList)
                {
                    elementType = reflectedType.ElementType;
                    if (!objectType.IsInterface)
                    {
                        listResult = (IList)Nemo.Reflection.Activator.New(objectType);
                    }
                    else
                    {
                        listResult = List.Create(elementType);
                    }
                    result = ObjectFactory.Create(elementType);
                    listRoot = current;
                }
                else if (reflectedType.IsSimpleList)
                {
                    elementType = reflectedType.ElementType;
                    result = List.Create(elementType);
                    simpleElement = true;
                }
            }

            if (elementType == null && root.Parent == null)
            {
                return result;
            }

            IDictionary<string, ReflectedProperty> propertyMap = null;
            if (elementType != null && !simpleElement)
            {
                propertyMap = Reflector.GetPropertyNameMap(elementType);
            }

            if (listRoot != null)
            {
                current = listRoot.FirstChild;
            }

            while (current != null)
            {
                ReflectedProperty property = null;
                if (propertyMap == null || (current.Name != null && propertyMap.TryGetValue(current.Name, out property)))
                {
                    switch (current.Type)
                    {
                        case JsonType.Boolean:
                            if (property != null && (root.Type == JsonType.Object || (listRoot != null && listRoot.Type == JsonType.Object)))
                            {
                                if (property.IsTypeUnion)
                                {
                                    var unionType = property.PropertyType.GenericTypeArguments.FirstOrDefault(t => t == typeof(bool));
                                    if (unionType != null)
                                    {
                                        ((IBusinessObject)result).Property(property.PropertyName, TypeUnion.Create(property.PropertyType.GenericTypeArguments, current.Value.As<bool>()));
                                    }
                                }
                                else
                                {
                                    ((IBusinessObject)result).Property(property.PropertyName, current.Value.As<bool>());
                                }
                            }
                            if (result is IList)
                            {
                                ((IList)result).Add(current.Value.As<bool>());
                            }
                            else
                            {
                                result = current.Value.As<bool>();
                            }
                            break;
                        case JsonType.Decimal:
                            if (property != null && (root.Type == JsonType.Object || (listRoot != null && listRoot.Type == JsonType.Object)))
                            {
                                var value = current.Value.As<decimal>();
                                var typeCode = Type.GetTypeCode(property.PropertyType);
                                checked
                                {
                                    switch (typeCode)
                                    {
                                        case TypeCode.Double:
                                            ((IBusinessObject)result).Property(property.PropertyName, (double)value);
                                            break;
                                        case TypeCode.Single:
                                            ((IBusinessObject)result).Property(property.PropertyName, (float)value);
                                            break;
                                        case TypeCode.Decimal:
                                            ((IBusinessObject)result).Property(property.PropertyName, value);
                                            break;
                                        case TypeCode.Object:
                                            {
                                                if (property.IsTypeUnion)
                                                {
                                                    var unionType = property.PropertyType.GenericTypeArguments.FirstOrDefault(t =>
                                                                            (t == typeof(decimal) && value >= decimal.MinValue &&value <= decimal.MaxValue)
                                                                            || (t == typeof(double) && (double)value >= double.MinValue && (double)value <= double.MaxValue)
                                                                            || (t == typeof(float) && (float)value >= float.MinValue && (float)value <= float.MaxValue));
                                                    if (unionType != null)
                                                    {
                                                        ((IBusinessObject)result).Property(property.PropertyName, TypeUnion.Create(property.PropertyType.GenericTypeArguments, Reflector.ChangeType(value, unionType)));
                                                    }
                                                }
                                            }
                                            break;
                                    }
                                }
                            }
                            else
                            {
                                var value = current.Value.As<decimal>();
                                var typeCode = Type.GetTypeCode(elementType ?? objectType);
                                checked
                                {
                                    switch (typeCode)
                                    {
                                        case TypeCode.Double:
                                            if (result is IList)
                                            {
                                                ((IList)result).Add((double)value);
                                            }
                                            else
                                            {
                                                result = (double)value;
                                            }
                                            break;
                                        case TypeCode.Single:
                                            if (result is IList)
                                            {
                                                ((IList)result).Add((float)value);
                                            }
                                            else
                                            {
                                                result = (float)value;
                                            }
                                            break;
                                        case TypeCode.Decimal:
                                            if (result is IList)
                                            {
                                                ((IList)result).Add(value);
                                            }
                                            else
                                            {
                                                result = value;
                                            }
                                            break;
                                    }
                                }
                            }
                            break;
                        case JsonType.Integer:
                            if (property != null && (root.Type == JsonType.Object || (listRoot != null && listRoot.Type == JsonType.Object)))
                            {
                                var value = current.Value.As<long>();
                                var typeCode = Type.GetTypeCode(property.PropertyType);
                                checked
                                {
                                    switch (typeCode)
                                    {
                                        case TypeCode.Byte:
                                            ((IBusinessObject)result).Property(property.PropertyName, (byte)value);
                                            break;
                                        case TypeCode.SByte:
                                            ((IBusinessObject)result).Property(property.PropertyName, (sbyte)value);
                                            break;
                                        case TypeCode.Int16:
                                            ((IBusinessObject)result).Property(property.PropertyName, (short)value);
                                            break;
                                        case TypeCode.Int32:
                                            ((IBusinessObject)result).Property(property.PropertyName, (int)value);
                                            break;
                                        case TypeCode.Int64:
                                            ((IBusinessObject)result).Property(property.PropertyName, value);
                                            break;
                                        case TypeCode.UInt16:
                                            ((IBusinessObject)result).Property(property.PropertyName, (ushort)value);
                                            break;
                                        case TypeCode.UInt32:
                                            ((IBusinessObject)result).Property(property.PropertyName, (uint)value);
                                            break;
                                        case TypeCode.UInt64:
                                            ((IBusinessObject)result).Property(property.PropertyName, (ulong)value);
                                            break;
                                        case TypeCode.Object:
                                            {
                                                if (property.IsTypeUnion)
                                                {
                                                    var unionType = property.PropertyType.GenericTypeArguments.FirstOrDefault(t =>
                                                                            (t == typeof(ulong) && (ulong)value >= ulong.MinValue && (ulong)value <= ulong.MaxValue)
                                                                            || (t == typeof(long) && value >= long.MinValue && value <= long.MaxValue)
                                                                            || (t == typeof(uint) && (uint)value >= uint.MinValue && (uint)value <= uint.MaxValue)
                                                                            || (t == typeof(int) && (int)value >= int.MinValue && (int)value <= int.MaxValue)
                                                                            || (t == typeof(ushort) && (ushort)value >= ushort.MinValue && (ushort)value <= ushort.MaxValue)
                                                                            || (t == typeof(short) && (short)value >= short.MinValue && (short)value <= short.MaxValue)
                                                                            || (t == typeof(byte) && (byte)value >= byte.MinValue && (byte)value <= byte.MaxValue)
                                                                            || (t == typeof(sbyte) && (sbyte)value >= sbyte.MinValue && (sbyte)value <= sbyte.MaxValue));
                                                    if (unionType != null)
                                                    {
                                                        ((IBusinessObject)result).Property(property.PropertyName, TypeUnion.Create(property.PropertyType.GenericTypeArguments, Reflector.ChangeType(value, unionType)));
                                                    }
                                                }
                                            }
                                            break;
                                    }
                                }
                            }
                            else
                            {
                                var value = current.Value.As<long>();
                                var typeCode = Type.GetTypeCode(elementType ?? objectType);
                                checked
                                {
                                    switch (typeCode)
                                    {
                                        case TypeCode.Byte:
                                            if (result is IList)
                                            {
                                                ((IList)result).Add((byte)value);
                                            }
                                            else
                                            {
                                                result = (byte)value;
                                            }
                                            break;
                                        case TypeCode.SByte:
                                            if (result is IList)
                                            {
                                                ((IList)result).Add((sbyte)value);
                                            }
                                            else
                                            {
                                                result = (sbyte)value;
                                            }
                                            break;
                                        case TypeCode.Int16:
                                            if (result is IList)
                                            {
                                                ((IList)result).Add((short)value);
                                            }
                                            else
                                            {
                                                result = (short)value;
                                            }
                                            break;
                                        case TypeCode.Int32:
                                            if (result is IList)
                                            {
                                                ((IList)result).Add((int)value);
                                            }
                                            else
                                            {
                                                result = (int)value;
                                            } break;
                                        case TypeCode.Int64:
                                            if (result is IList)
                                            {
                                                ((IList)result).Add(value);
                                            }
                                            else
                                            {
                                                result = value;
                                            }
                                            break;
                                        case TypeCode.UInt16:
                                            if (result is IList)
                                            {
                                                ((IList)result).Add((ushort)value);
                                            }
                                            else
                                            {
                                                result = (ushort)value;
                                            }
                                            break;
                                        case TypeCode.UInt32:
                                            if (result is IList)
                                            {
                                                ((IList)result).Add((uint)value);
                                            }
                                            else
                                            {
                                                result = (uint)value;
                                            }
                                            break;
                                        case TypeCode.UInt64:
                                            if (result is IList)
                                            {
                                                ((IList)result).Add((ulong)value);
                                            }
                                            else
                                            {
                                                result = (ulong)value;
                                            }
                                            break;
                                    }
                                }
                            }
                            break;
                        case JsonType.String:
                            if (property != null && (root.Type == JsonType.Object || (listRoot != null && listRoot.Type == JsonType.Object)))
                            {
                                var value = current.Value.As<string>();
                                if (property.IsTypeUnion)
                                {
                                    DateTime date = default(DateTime);
                                    DateTimeOffset dateOffset = default(DateTimeOffset);
                                    TimeSpan time = default(TimeSpan);
                                    Guid guid = Guid.Empty;

                                    var unionType = property.PropertyType.GenericTypeArguments.FirstOrDefault(t =>
                                                            (t == typeof(DateTime) && DateTime.TryParse(value, out date))
                                                            || (t == typeof(TimeSpan) && TimeSpan.TryParse(value, out time))
                                                            || (t == typeof(DateTimeOffset) && DateTimeOffset.TryParse(value, out dateOffset))
                                                            || (t == typeof(Guid) && Guid.TryParse(value, out guid))
                                                            || (t == typeof(char) && value.Length == 1)
                                                            || t == typeof(string));
                                    if (unionType != null)
                                    {
                                        if (unionType == typeof(string))
                                        {
                                            ((IBusinessObject)result).Property(property.PropertyName, TypeUnion.Create(property.PropertyType.GenericTypeArguments, value));
                                        }
                                        else if (unionType == typeof(char))
                                        {
                                            ((IBusinessObject)result).Property(property.PropertyName, TypeUnion.Create(property.PropertyType.GenericTypeArguments, value[0]));
                                        }
                                        else if (unionType == typeof(DateTime))
                                        {
                                            ((IBusinessObject)result).Property(property.PropertyName, TypeUnion.Create(property.PropertyType.GenericTypeArguments, date));
                                        }
                                        else if (unionType == typeof(DateTimeOffset))
                                        {
                                            ((IBusinessObject)result).Property(property.PropertyName, TypeUnion.Create(property.PropertyType.GenericTypeArguments, dateOffset));
                                        }
                                        else if (unionType == typeof(TimeSpan))
                                        {
                                            ((IBusinessObject)result).Property(property.PropertyName, TypeUnion.Create(property.PropertyType.GenericTypeArguments, time));
                                        }
                                        else if (unionType == typeof(Guid))
                                        {
                                            ((IBusinessObject)result).Property(property.PropertyName, TypeUnion.Create(property.PropertyType.GenericTypeArguments, guid));
                                        }
                                    }
                                }
                                else if (property.PropertyType == typeof(string))
                                {
                                    ((IBusinessObject)result).Property(property.PropertyName, value);
                                }
                                else if (property.PropertyType == typeof(DateTime))
                                {
                                    DateTime date;
                                    if (DateTime.TryParse(value, out date))
                                    {
                                        ((IBusinessObject)result).Property(property.PropertyName, date);
                                    }
                                }
                                else if (property.PropertyType == typeof(TimeSpan))
                                {
                                    TimeSpan time;
                                    if (TimeSpan.TryParse(value, out time))
                                    {
                                        ((IBusinessObject)result).Property(property.PropertyName, time);
                                    }
                                }
                                else if (property.PropertyType == typeof(DateTimeOffset))
                                {
                                    DateTimeOffset date;
                                    if (DateTimeOffset.TryParse(value, out date))
                                    {
                                        ((IBusinessObject)result).Property(property.PropertyName, date);
                                    }
                                }
                                else if (property.PropertyType == typeof(Guid))
                                {
                                    Guid guid;
                                    if (Guid.TryParse(value, out guid))
                                    {
                                        ((IBusinessObject)result).Property(property.PropertyName, guid);
                                    }
                                }
                                else if (property.PropertyType == typeof(char) && !string.IsNullOrEmpty(value))
                                {
                                    ((IBusinessObject)result).Property(property.PropertyName, value[0]);
                                }
                            }
                            else
                            {
                                var value = current.Value.As<string>();
                                var type = elementType ?? objectType;
                                if (type == typeof(string))
                                {
                                    if (result is IList)
                                    {
                                        ((IList)result).Add(value);
                                    }
                                    else
                                    {
                                        result = value;
                                    }
                                }
                                else if (type == typeof(DateTime))
                                {
                                    DateTime date;
                                    if (DateTime.TryParse(value, out date))
                                    {
                                        if (result is IList)
                                        {
                                            ((IList)result).Add(date);
                                        }
                                        else
                                        {
                                            result = date;
                                        }
                                    }
                                }
                                else if (type == typeof(TimeSpan))
                                {
                                    TimeSpan time;
                                    if (TimeSpan.TryParse(value, out time))
                                    {
                                        if (result is IList)
                                        {
                                            ((IList)result).Add(time);
                                        }
                                        else
                                        {
                                            result = time;
                                        }
                                    }
                                }
                                else if (type == typeof(DateTimeOffset))
                                {
                                    DateTimeOffset date;
                                    if (DateTimeOffset.TryParse(value, out date))
                                    {
                                        if (result is IList)
                                        {
                                            ((IList)result).Add(date);
                                        }
                                        else
                                        {
                                            result = date;
                                        }
                                    }
                                }
                                else if (type == typeof(Guid))
                                {
                                    Guid guid;
                                    if (Guid.TryParse(value, out guid))
                                    {
                                        if (result is IList)
                                        {
                                            ((IList)result).Add(guid);
                                        }
                                        else
                                        {
                                            result = guid;
                                        }
                                    }
                                }
                                else if (type == typeof(char) && !string.IsNullOrEmpty(value))
                                {
                                    if (result is IList)
                                    {
                                        ((IList)result).Add(value[0]);
                                    }
                                    else
                                    {
                                        result = value[0];
                                    }
                                }
                            }
                            break;
                        case JsonType.Object:
                            {
                                var propertyType = property.PropertyType;
                                if (property.IsTypeUnion)
                                {
                                    var match = property.PropertyType.GenericTypeArguments
                                                .Where(t => !Reflector.IsSimpleType(t) && !Reflector.IsList(t))
                                                .Select(t => new { Type = t, Rank = ComputeTypeMatchRank(current, t) })
                                                .MaxElement(t => t.Rank);
                                    if (match != null)
                                    {
                                        propertyType = match.Type;
                                    }
                                }

                                var item = ReadObject(current, propertyType);
                                
                                if (property.IsTypeUnion)
                                {
                                    item = TypeUnion.Create(property.PropertyType.GenericTypeArguments, item);
                                }

                                if (root.Type == JsonType.Object)
                                {
                                    ((IBusinessObject)result).Property(property.PropertyName, item);
                                }
                                else if (root.Type == JsonType.Array)
                                {
                                    ((IList)result).Add(item);
                                }
                            }
                            break;
                        case JsonType.Array:
                            {
                                if (root.Type == JsonType.Object)
                                {
                                    if (property.IsTypeUnion)
                                    {
                                        var allTypes = property.PropertyType.GetGenericArguments();
                                        var child = current.FirstChild;
                                        var unionType = allTypes.FirstOrDefault(t => 
                                                            (child.Type == JsonType.Boolean && typeof(IList<bool>).IsAssignableFrom(t))
                                                            || (child.Type == JsonType.Decimal && typeof(IList<decimal>).IsAssignableFrom(t))
                                                            || (child.Type == JsonType.Decimal && typeof(IList<double>).IsAssignableFrom(t))
                                                            || (child.Type == JsonType.Decimal && typeof(IList<float>).IsAssignableFrom(t))
                                                            || (child.Type == JsonType.Integer && typeof(IList<ulong>).IsAssignableFrom(t))
                                                            || (child.Type == JsonType.Integer && typeof(IList<long>).IsAssignableFrom(t))
                                                            || (child.Type == JsonType.Integer && typeof(IList<uint>).IsAssignableFrom(t))
                                                            || (child.Type == JsonType.Integer && typeof(IList<int>).IsAssignableFrom(t))
                                                            || (child.Type == JsonType.Integer && typeof(IList<ushort>).IsAssignableFrom(t))
                                                            || (child.Type == JsonType.Integer && typeof(IList<short>).IsAssignableFrom(t))
                                                            || (child.Type == JsonType.Integer && typeof(IList<byte>).IsAssignableFrom(t))
                                                            || (child.Type == JsonType.Integer && typeof(IList<sbyte>).IsAssignableFrom(t))
                                                            || (child.Type == JsonType.String && typeof(IList<string>).IsAssignableFrom(t))
                                                            || (child.Type == JsonType.String && typeof(IList<char>).IsAssignableFrom(t))
                                                            || (child.Type == JsonType.String && typeof(IList<DateTime>).IsAssignableFrom(t))
                                                            || (child.Type == JsonType.String && typeof(IList<DateTimeOffset>).IsAssignableFrom(t))
                                                            || (child.Type == JsonType.String && typeof(IList<TimeSpan>).IsAssignableFrom(t))
                                                            || (child.Type == JsonType.String && typeof(IList<Guid>).IsAssignableFrom(t))
                                                            || (child.Type == JsonType.Object && typeof(IList).IsAssignableFrom(t) && typeof(IEnumerable<IBusinessObject>).IsAssignableFrom(t)));
                                        if (unionType != null)
                                        {
                                            var item = ReadObject(current, unionType);
                                            var typeUnion = TypeUnion.Create(allTypes, item);
                                            ((IBusinessObject)result).Property(property.PropertyName, typeUnion);
                                        }
                                    }
                                    else
                                    {
                                        IList list;
                                        if (!property.IsListInterface)
                                        {
                                            list = (IList)Nemo.Reflection.Activator.New(property.PropertyType);
                                        }
                                        else
                                        {
                                            list = List.Create(property.ElementType, property.Distinct, property.Sorted);
                                        }
                                        var child = current.FirstChild;
                                        while (child != null)
                                        {
                                            var item = (IBusinessObject)ReadObject(child, property.ElementType);
                                            list.Add(item);
                                            child = child.NexSibling;
                                        }
                                        ((IBusinessObject)result).Property(property.PropertyName, list);
                                    }
                                }
                                break;
                            }
                    }
                }

                current = current.NexSibling;
                if (listRoot != null && current == null)
                {
                    ((IList)listResult).Add(result);
                    result = ObjectFactory.Create(elementType);
                    listRoot = listRoot.NexSibling;
                    if (listRoot != null)
                    {
                        current = listRoot.FirstChild;
                    }
                }
            }
            return listResult ?? result;
        }

        private static int ComputeTypeMatchRank(JsonValue jsonObject, Type objectType)
        {
            var propertyNames = Reflector.GetAllProperties(objectType).Select(p => p.Name);
            var jsonNames = new HashSet<string>();
            var child = jsonObject.FirstChild;
            while (child != null)
            {
                if (child.Name != null)
                {
                    jsonNames.Add(child.Name);
                }
                child = child.NexSibling;
            }
            jsonNames.UnionWith(propertyNames);
            return jsonNames.Count;
        }
    }
}
