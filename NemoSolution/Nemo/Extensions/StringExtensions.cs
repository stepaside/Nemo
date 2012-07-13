using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace Nemo.Extensions
{
    public static class StringExtensions
    {
       public static string ToDelimitedString<T>(this IEnumerable<T> source)
        {
            return ToDelimitedString(source, null);
        }

        public static string ToDelimitedString<T>(this IEnumerable<T> source, string delimiter)
        {
            source.ThrowIfNull("source");

            delimiter = delimiter ?? CultureInfo.CurrentCulture.TextInfo.ListSeparator;

            var sb = new StringBuilder();
            foreach (var value in source)
            {
                if (sb.Length > 0)
                    sb.Append(delimiter);
                sb.Append(value);
            }
            return sb.ToString();
        }

        public static byte[] ToByteArray(this string value, Encoding encoding = null)
        {
            if (encoding == null)
            {
                return Encoding.Default.GetBytes(value);
            }
            else
            {
                return encoding.GetBytes(value);
            }
        }

        private static Regex _lowerUpperLowerMatcher = new Regex("(?<lower>[a-z])(?<upper_lower>[A-Z][a-z]?)", RegexOptions.Compiled);
        private static Regex _underscoreMatcher = new Regex("(?<first_char>(?<=^)\\w)|(?<underscore>_)(?<char>\\w)", RegexOptions.Compiled);

        public static string ToCamelCase(this string value)
        {
            return _underscoreMatcher.Replace(value, m => m.Groups["first_char"].Success ? m.Groups["first_char"].Value.ToLower() : m.Groups["char"].Value.ToUpper());
        }

        public static string ToPascalCase(this string value)
        {
            return _underscoreMatcher.Replace(value, m => m.Groups["first_char"].Success ? m.Groups["first_char"].Value.ToUpper() : m.Groups["char"].Value.ToUpper());
        }

        public static string ToUnderscoreDelimitedLowerCase(this string value)
        {
            return _lowerUpperLowerMatcher.Replace(value, m => m.Groups["lower"].Value + '_' + m.Groups["upper_lower"].Value).ToLower();
        }

        private static Dictionary<char, int> _phoneMap = new Dictionary<char, int>
        {
            {'a',2},{'b',2},{'c',2},{'d',3},{'e',3},{'f',3},{'g',4},{'h',4},{'i',4},{'j',5},{'k',5},{'l',5},{'m',6},
            {'n',6},{'o',6},{'p',7},{'q',7},{'r',7},{'s',7},{'t',8},{'u',8},{'v',8},{'w',9},{'x',9},{'y',9},{'z',9}
        };

        public static string ToNumericPhoneNumber(this string phonenumber)
        {
            int n;
            return phonenumber.Select(c => _phoneMap.TryGetValue(Char.ToLower(c), out n) ? n.ToString() : c.ToString()).ToDelimitedString(string.Empty);
        }

        public static string Slice(this string source, int start, int end)
        {
            if (end < 0) // Keep this for negative end support
            {
                end = source.Length + end;
            }
            int len = end - start;               // Calculate length
            return source.Substring(start, len); // Return Substring of length
        }

        public static string Truncate(this string source, int length)
        {
            if (source.Length > length)
            {
                source = source.Substring(0, length);
            }
            return source;
        }

        public static string Left(this string source, int length)
        {
            return source.Substring(0, length);
        }

        public static string Right(this string source, int length)
        {
            return source.Substring(source.Length - length);
        }
    }
}
