﻿using Nemo.Collections;
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
            var dictionary = false;
            JsonValue listRoot = null;

            if (root.Type == JsonType.Object)
            {
                if (reflectedType.IsDataEntity)
                {
                    result = ObjectFactory.Create(objectType);
                    elementType = objectType;
                }
                else if (reflectedType.IsDictionary)
                {
                    var types = objectType.GetGenericArguments();
                    result = Dictionary.Create(types[0], types[1]);
                    elementType = types[1];
                    dictionary = true;
                }
            }
            else if (root.Type == JsonType.Array)
            {
                if (reflectedType.IsDataEntity)
                {
                    result = List.Create(objectType);
                    elementType = objectType;
                }
                else if (reflectedType.IsDataEntityList)
                {
                    elementType = reflectedType.ElementType;
                    listResult = !reflectedType.IsInterface ? objectType.New() : List.Create(elementType);
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
            if (elementType != null && !simpleElement && !dictionary)
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
                        {
                            if (property != null && (root.Type == JsonType.Object || (listRoot != null && listRoot.Type == JsonType.Object)))
                            {
                                ((IDataEntity)result).Property(property.PropertyName, current.Value.As<bool>());
                            }
                            var list = result as IList;
                            if (list != null)
                            {
                                list.Add(current.Value.As<bool>());
                            }
                            else
                            {
                                var map = result as IDictionary;
                                if (map != null)
                                {
                                    map.Add(current.Name, current.Value.As<bool>());
                                }
                                else
                                {
                                    result = current.Value.As<bool>();
                                }
                            }
                            break;
                        }
                        case JsonType.Decimal:
                        {
                            if (property != null && (root.Type == JsonType.Object || (listRoot != null && listRoot.Type == JsonType.Object)))
                            {
                                var value = current.Value.As<decimal>();
                                var typeCode = Type.GetTypeCode(property.PropertyType);
                                checked
                                {
                                    switch (typeCode)
                                    {
                                        case TypeCode.Double:
                                            ((IDataEntity)result).Property(property.PropertyName, (double)value);
                                            break;
                                        case TypeCode.Single:
                                            ((IDataEntity)result).Property(property.PropertyName, (float)value);
                                            break;
                                        case TypeCode.Decimal:
                                            ((IDataEntity)result).Property(property.PropertyName, value);
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
                                        {
                                            var list = result as IList;
                                            if (list != null)
                                            {
                                                list.Add((double)value);
                                            }
                                            else
                                            {
                                                var map = result as IDictionary;
                                                if (map != null)
                                                {
                                                    map.Add(current.Name, (double)value);
                                                }
                                                else
                                                {
                                                    result = (double)value;
                                                }
                                            }
                                            break;
                                        }
                                        case TypeCode.Single:
                                        {
                                            var list = result as IList;
                                            if (list != null)
                                            {
                                                list.Add((float)value);
                                            }
                                            else
                                            {
                                                var map = result as IDictionary;
                                                if (map != null)
                                                {
                                                    map.Add(current.Name, (float)value);
                                                }
                                                else
                                                {
                                                    result = (float)value;
                                                }
                                            }
                                            break;
                                        }
                                        case TypeCode.Decimal:
                                        {
                                            var list = result as IList;
                                            if (list != null)
                                            {
                                                list.Add(value);
                                            }
                                            else
                                            {
                                                var map = result as IDictionary;
                                                if (map != null)
                                                {
                                                    map.Add(current.Name, value);
                                                }
                                                else
                                                {
                                                    result = value;
                                                }
                                            }
                                            break;
                                        }
                                    }
                                }
                            }
                            break;
                        }
                        case JsonType.Integer:
                        {
                            if (property != null && (root.Type == JsonType.Object || (listRoot != null && listRoot.Type == JsonType.Object)))
                            {
                                var value = current.Value.As<long>();
                                var typeCode = Type.GetTypeCode(property.PropertyType);
                                checked
                                {
                                    switch (typeCode)
                                    {
                                        case TypeCode.Byte:
                                            ((IDataEntity)result).Property(property.PropertyName, (byte)value);
                                            break;
                                        case TypeCode.SByte:
                                            ((IDataEntity)result).Property(property.PropertyName, (sbyte)value);
                                            break;
                                        case TypeCode.Int16:
                                            ((IDataEntity)result).Property(property.PropertyName, (short)value);
                                            break;
                                        case TypeCode.Int32:
                                            ((IDataEntity)result).Property(property.PropertyName, (int)value);
                                            break;
                                        case TypeCode.Int64:
                                            ((IDataEntity)result).Property(property.PropertyName, value);
                                            break;
                                        case TypeCode.UInt16:
                                            ((IDataEntity)result).Property(property.PropertyName, (ushort)value);
                                            break;
                                        case TypeCode.UInt32:
                                            ((IDataEntity)result).Property(property.PropertyName, (uint)value);
                                            break;
                                        case TypeCode.UInt64:
                                            ((IDataEntity)result).Property(property.PropertyName, (ulong)value);
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
                                        {
                                            var list = result as IList;
                                            if (list != null)
                                            {
                                                list.Add((byte)value);
                                            }
                                            else
                                            {
                                                var map = result as IDictionary;
                                                if (map != null)
                                                {
                                                    map.Add(current.Name, (byte)value);
                                                }
                                                else
                                                {
                                                    result = (byte)value;
                                                }
                                            }
                                            break;
                                        }
                                        case TypeCode.SByte:
                                        {
                                            var list = result as IList;
                                            if (list != null)
                                            {
                                                list.Add((sbyte)value);
                                            }
                                            else
                                            {
                                                var map = result as IDictionary;
                                                if (map != null)
                                                {
                                                    map.Add(current.Name, (sbyte)value);
                                                }
                                                else
                                                {
                                                    result = (sbyte)value;
                                                }
                                            }
                                            break;
                                        }
                                        case TypeCode.Int16:
                                        {
                                            var list = result as IList;
                                            if (list != null)
                                            {
                                                list.Add((short)value);
                                            }
                                            else
                                            {
                                                var map = result as IDictionary;
                                                if (map != null)
                                                {
                                                    map.Add(current.Name, (short)value);
                                                }
                                                else
                                                {
                                                    result = (short)value;
                                                }
                                            }
                                            break;
                                        }
                                        case TypeCode.Int32:
                                        {
                                            var list = result as IList;
                                            if (list != null)
                                            {
                                                list.Add((int)value);
                                            }
                                            else
                                            {
                                                var map = result as IDictionary;
                                                if (map != null)
                                                {
                                                    map.Add(current.Name, (int)value);
                                                }
                                                else
                                                {
                                                    result = (int)value;
                                                }
                                            }
                                            break;
                                        }
                                        case TypeCode.Int64:
                                        {
                                            var list = result as IList;
                                            if (list != null)
                                            {
                                                list.Add(value);
                                            }
                                            else
                                            {
                                                var map = result as IDictionary;
                                                if (map != null)
                                                {
                                                    map.Add(current.Name, value);
                                                }
                                                else
                                                {
                                                    result = value;
                                                }
                                            }
                                            break;
                                        }
                                        case TypeCode.UInt16:
                                        {
                                            var list = result as IList;
                                            if (list != null)
                                            {
                                                list.Add((ushort)value);
                                            }
                                            else
                                            {
                                                var map = result as IDictionary;
                                                if (map != null)
                                                {
                                                    map.Add(current.Name, (ushort)value);
                                                }
                                                else
                                                {
                                                    result = (ushort)value;
                                                }
                                            }
                                            break;
                                        }
                                        case TypeCode.UInt32:
                                        {
                                            var list = result as IList;
                                            if (list != null)
                                            {
                                                list.Add((uint)value);
                                            }
                                            else
                                            {
                                                var map = result as IDictionary;
                                                if (map != null)
                                                {
                                                    map.Add(current.Name, (uint)value);
                                                }
                                                else
                                                {
                                                    result = (uint)value;
                                                }
                                            }
                                            break;
                                        }
                                        case TypeCode.UInt64:
                                        {
                                            var list = result as IList;
                                            if (list != null)
                                            {
                                                list.Add((ulong)value);
                                            }
                                            else
                                            {
                                                var map = result as IDictionary;
                                                if (map != null)
                                                {
                                                    map.Add(current.Name, (ulong)value);
                                                }
                                                else
                                                {
                                                    result = (ulong)value;
                                                }
                                            }
                                            break;
                                        }
                                    }
                                }
                            }
                            break;
                        }
                        case JsonType.String:
                        {
                            if (property != null && (root.Type == JsonType.Object || (listRoot != null && listRoot.Type == JsonType.Object)))
                            {
                                var value = current.Value.As<string>();
                                if (property.PropertyType == typeof(string))
                                {
                                    ((IDataEntity)result).Property(property.PropertyName, value);
                                }
                                else if (property.PropertyType == typeof(DateTime))
                                {
                                    DateTime date;
                                    if (DateTime.TryParse(value, out date))
                                    {
                                        ((IDataEntity)result).Property(property.PropertyName, date);
                                    }
                                }
                                else if (property.PropertyType == typeof(TimeSpan))
                                {
                                    TimeSpan time;
                                    if (TimeSpan.TryParse(value, out time))
                                    {
                                        ((IDataEntity)result).Property(property.PropertyName, time);
                                    }
                                }
                                else if (property.PropertyType == typeof(DateTimeOffset))
                                {
                                    DateTimeOffset date;
                                    if (DateTimeOffset.TryParse(value, out date))
                                    {
                                        ((IDataEntity)result).Property(property.PropertyName, date);
                                    }
                                }
                                else if (property.PropertyType == typeof(Guid))
                                {
                                    Guid guid;
                                    if (Guid.TryParse(value, out guid))
                                    {
                                        ((IDataEntity)result).Property(property.PropertyName, guid);
                                    }
                                }
                                else if (property.PropertyType == typeof(char) && !string.IsNullOrEmpty(value))
                                {
                                    ((IDataEntity)result).Property(property.PropertyName, value[0]);
                                }
                            }
                            else
                            {
                                var value = current.Value.As<string>();
                                var type = elementType ?? objectType;
                                if (type == typeof(string))
                                {
                                    var list = result as IList;
                                    if (list != null)
                                    {
                                        list.Add(value);
                                    }
                                    else
                                    {
                                        var map = result as IDictionary;
                                        if (map != null)
                                        {
                                            map.Add(current.Name, value);
                                        }
                                        else
                                        {
                                            result = value;
                                        }
                                    }
                                }
                                else if (type == typeof(DateTime))
                                {
                                    DateTime date;
                                    if (DateTime.TryParse(value, out date))
                                    {
                                        var list = result as IList;
                                        if (list != null)
                                        {
                                            list.Add(date);
                                        }
                                        else
                                        {
                                            var map = result as IDictionary;
                                            if (map != null)
                                            {
                                                map.Add(current.Name, date);
                                            }
                                            else
                                            {
                                                result = date;
                                            }
                                        }
                                    }
                                }
                                else if (type == typeof(TimeSpan))
                                {
                                    TimeSpan time;
                                    if (TimeSpan.TryParse(value, out time))
                                    {
                                        var list = result as IList;
                                        if (list != null)
                                        {
                                            list.Add(time);
                                        }
                                        else
                                        {
                                            var map = result as IDictionary;
                                            if (map != null)
                                            {
                                                map.Add(current.Name, time);
                                            }
                                            else
                                            {
                                                result = time;
                                            }
                                        }
                                    }
                                }
                                else if (type == typeof(DateTimeOffset))
                                {
                                    DateTimeOffset date;
                                    if (DateTimeOffset.TryParse(value, out date))
                                    {
                                        var list = result as IList;
                                        if (list != null)
                                        {
                                            list.Add(date);
                                        }
                                        else
                                        {
                                            var map = result as IDictionary;
                                            if (map != null)
                                            {
                                                map.Add(current.Name, date);
                                            }
                                            else
                                            {
                                                result = date;
                                            }
                                        }
                                    }
                                }
                                else if (type == typeof(Guid))
                                {
                                    Guid guid;
                                    if (Guid.TryParse(value, out guid))
                                    {
                                        var list = result as IList;
                                        if (list != null)
                                        {
                                            list.Add(guid);
                                        }
                                        else
                                        {
                                            var map = result as IDictionary;
                                            if (map != null)
                                            {
                                                map.Add(current.Name, guid);
                                            }
                                            else
                                            {
                                                result = guid;
                                            }
                                        }
                                    }
                                }
                                else if (type == typeof(char) && !string.IsNullOrEmpty(value))
                                {
                                    var list = result as IList;
                                    if (list != null)
                                    {
                                        list.Add(value[0]);
                                    }
                                    else
                                    {
                                        var map = result as IDictionary;
                                        if (map != null)
                                        {
                                            map.Add(current.Name, value[0]);
                                        }
                                        else
                                        {
                                            result = value[0];
                                        }
                                    }
                                }
                            }
                            break;
                        }
                        case JsonType.Object:
                        {
                            var propertyType = property.PropertyType;
                            var item = ReadObject(current, propertyType);

                            switch (root.Type)
                            {
                                case JsonType.Object:
                                    ((IDataEntity)result).Property(property.PropertyName, item);
                                    break;
                                case JsonType.Array:
                                    ((IList)result).Add(item);
                                    break;
                            }
                            break;
                        }
                        case JsonType.Array:
                        {
                            if (root.Type == JsonType.Object)
                            {
                                if (property != null)
                                {
                                    IList list;
                                    if (!property.IsListInterface)
                                    {
                                        list = (IList)property.PropertyType.New();
                                    }
                                    else
                                    {
                                        list = List.Create(property.ElementType, property.Distinct, property.Sorted);
                                    }
                                    var child = current.FirstChild;
                                    while (child != null)
                                    {
                                        var item = (IDataEntity)ReadObject(child, property.ElementType);
                                        list.Add(item);
                                        child = child.NexSibling;
                                    }
                                    ((IDataEntity)result).Property(property.PropertyName, list);
                                }
                                else
                                {
                                    var map = result as IDictionary;
                                    if (map != null)
                                    {
                                        var listType = Reflector.GetReflectedType(elementType);
                                        if (elementType.IsArray)
                                        {
                                            var list = (IList)ReadObject(current, typeof(List<>).MakeGenericType(listType.ElementType));
                                            map.Add(current.Name, List.CreateArray(listType.ElementType, list));
                                        }
                                        else
                                        {
                                            var list = (IList)ReadObject(current, elementType);
                                            map.Add(current.Name, list);
                                        }
                                    }
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
