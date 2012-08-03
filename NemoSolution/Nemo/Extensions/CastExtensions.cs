using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Nemo.Reflection;

namespace Nemo.Extensions
{
    public static class CastExtensions
    {
        public static T SafeCast<T>(this object source)
        {
            var destinationType = typeof(T);

            //if (null == source && destinationType.IsPrimitive)
            //{
            //    throw new InvalidCastException();
            //}

            if (null == source || !(source is IConvertible))
            {
                return (T)source;
            }

            return (T)Reflector.ChangeType(source, destinationType);
        }

        public static T SafeCast<T>(this string source)
        {
            return ((object)source).SafeCast<T>();
        }

        public static IEnumerable<T> SafeCast<T>(this IEnumerable source)
        {
            return source.Cast<object>().Select(i => i.SafeCast<T>());
        }

        public static IEnumerable<TResult> SafeCast<TSource, TResult>(this IEnumerable<TSource> source)
        {
            return source.Select(i => i.SafeCast<TResult>());
        }

        public delegate bool ParseDelegate<T>(string source, out T result);

        public static bool TryParse<T>(this string source, ParseDelegate<T> parse, out T result) 
        {
            return parse(source, out result);
        }

        public static bool TryParse(this string source, Type structType, out object result)
        {
            bool success = false;
            if (structType != null)
            {
                var typeCode = Type.GetTypeCode(structType);
                switch (typeCode)
                {
                    case TypeCode.Boolean:
                        bool temp1;
                        success = bool.TryParse(source, out temp1);
                        result = temp1;
                        break;
                    case TypeCode.Byte:
                        byte temp2;
                        success = byte.TryParse(source, out temp2);
                        result = temp2;
                        break;
                    case TypeCode.Char:
                        char temp3;
                        success = char.TryParse(source, out temp3);
                        result = temp3;
                        break;
                    case TypeCode.DateTime:
                        DateTime temp4;
                        success = DateTime.TryParse(source, out temp4);
                        result = temp4;
                        break;
                    case TypeCode.DBNull:
                        success = source == DBNull.Value.ToString();
                        result = DBNull.Value;
                        break;
                    case TypeCode.Decimal:
                        decimal temp5;
                        success = decimal.TryParse(source, out temp5);
                        result = temp5;
                        break;
                    case TypeCode.Double:
                        double temp6;
                        success = double.TryParse(source, out temp6);
                        result = temp6;
                        break;
                    case TypeCode.Int16:
                        short temp7;
                        success = short.TryParse(source, out temp7);
                        result = temp7;
                        break;
                    case TypeCode.Int32:
                        int temp8;
                        success = int.TryParse(source, out temp8);
                        result = temp8;
                        break;
                    case TypeCode.Int64:
                        long temp9;
                        success = long.TryParse(source, out temp9);
                        result = temp9;
                        break;
                    case TypeCode.SByte:
                        sbyte temp10;
                        success = sbyte.TryParse(source, out temp10);
                        result = temp10;
                        break;
                    case TypeCode.Single:
                        float temp11;
                        success = float.TryParse(source, out temp11);
                        result = temp11;
                        break;
                    case TypeCode.UInt16:
                        ushort temp12;
                        success = ushort.TryParse(source, out temp12);
                        result = temp12;
                        break;
                    case TypeCode.UInt32:
                        uint temp13;
                        success = uint.TryParse(source, out temp13);
                        result = temp13;
                        break;
                    case TypeCode.UInt64:
                        ulong temp14;
                        success = ulong.TryParse(source, out temp14);
                        result = temp14;
                        break;
                    case TypeCode.String:
                        result = source;
                        success = true;
                        break;
                    default:
                        if (structType == typeof(DateTimeOffset))
                        {
                            DateTimeOffset temp;
                            success = DateTimeOffset.TryParse(source, out temp);
                            result = temp;
                        }
                        else if (structType == typeof(Guid))
                        {
                            Guid temp;
                            success = Guid.TryParse(source, out temp);
                            result = temp;
                        }
                        else if (structType == typeof(TimeSpan))
                        {
                            TimeSpan temp;
                            success = TimeSpan.TryParse(source, out temp);
                            result = temp;
                        }
                        else if (structType == typeof(Version))
                        {
                            Version temp;
                            success = Version.TryParse(source, out temp);
                            result = temp;
                        }
                        else
                        {
                            result = structType.GetDefault();
                        }
                        break;
                }
            }
            else
            {
                result = structType.GetDefault();
            }

            return success;
        }

        public static string NullIfEmpty(this string source)
        {
            return string.IsNullOrEmpty(source) ? null : source;
        }
    }
}
