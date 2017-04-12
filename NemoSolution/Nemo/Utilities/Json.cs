/**
 * This is a C# port of the vjson parser for C++.
 * The original vjson code can be found here http://code.google.com/p/vjson/
 **/

using System;
using System.Collections.Concurrent;
using Nemo.Fn;

namespace Nemo.Utilities
{
    public enum JsonType
    {
        Null,
        Object,
        Array,
        String,
        Integer,
        Decimal,
        Boolean
    };

    public class JsonValue
    {
        public JsonType Type { get; internal set; }
        public string Name { get; internal set; }
        public TypeUnion<string, long, decimal, bool> Value { get; internal set; }

        public JsonValue Parent { get; internal set; }
        public JsonValue NexSibling { get; internal set; }
        public JsonValue FirstChild { get; internal set; }
        public JsonValue LastChild { get; internal set; }

        private readonly ConcurrentDictionary<string, JsonValue> _properties = new ConcurrentDictionary<string, JsonValue>();

        internal void Append(JsonValue value)
        {
            //if (value.Name != null)
            //{
            //    _properties.Add(value.Name, value);
            //}
            value.Parent = this;
            if (LastChild != null)
            {
                LastChild = LastChild.NexSibling = value;
            }
            else
            {
                FirstChild = LastChild = value;
            }
        }
        
        public JsonValue this[string name]
        {
            get
            {
                //JsonValue value;
                //_properties.TryGetValue(name, out value);
                //return value;

                return _properties.GetOrAdd(name, key =>
                {
                    var child = FirstChild;
                    while (child != null)
                    {
                        if (child.Name == key)
                        {
                            _properties[key] = child;
                            return child;
                        }
                        child = child.NexSibling;
                    }
                    return null;
                });
            }
        }
    }

    public class JsonParserException : Exception
    {
        public JsonParserException() { }
        public JsonParserException(string message) : base(message) { }
        public JsonParserException(int charPosition, string message) : base(message) 
        {
            Position = charPosition;
        }

        public int Position
        {
            get;
            private set;
        }
    }

    public static class Json
    {
        private static void CheckTop(JsonValue top, int charPosition)
        {
            if (top == null)
            {
                throw new JsonParserException(charPosition, "Unexpected character");
            }
        }

        #region Conversion Methods

        internal static void IntegerToHex(int value, char[] hex)
        {
            for (var i = 0; i < 4; i++)
            {
                var num = value % 16;

                if (num < 10)
                    hex[3 - i] = (char)('0' + num);
                else
                    hex[3 - i] = (char)('A' + (num - 10));

                value >>= 4;
            }
        }

        internal static int HexToInteger(string source, out uint output)
        {
            var result = 0u;
            var i = 0;
            for (; i < source.Length; i++)
            {
                int digit;
                if (char.IsDigit(source[i]))
                {
                    digit = source[i] - '0';
                }
                else if (source[i] >= 'a' && source[i] <= 'f')
                {
                    digit = source[i] - 'a' + 10;
                }
                else if (source[i] >= 'A' && source[i] <= 'F')
                {
                    digit = source[i] - 'A' + 10;
                }
                else
                {
                    break;
                }
                result = (uint)(16 * result + digit);
            }
            output = result;

            return i;
        }

        internal static int TextToInteger(string source, out long output)
        {
            var sign = 1L;
            var i = 0;
            if (source.Length > 0)
            {
                switch (source[0])
                {
                    case '-':
                        sign = -1;
                        i++;
                        break;
                    case '+':
                        i++;
                        break;
                }
            }

            long result = 0;
            for (; i < source.Length && char.IsDigit(source[i]); i++)
            {
                result = 10 * result + (source[i] - '0');
            }
            output = result * sign;

            return i;
        }

        internal static int TextToDecimal(string source, out decimal output)
        {
            // sign
            var sign = 1m;
            var i = 0;
            if (source.Length > 0)
            {
                switch (source[0])
                {
                    case '-':
                        sign = -1;
                        i++;
                        break;
                    case '+':
                        i++;
                        break;
                }
            }

            // integer part
            decimal result = 0;
            for (; i < source.Length && char.IsDigit(source[i]); i++)
            {
                result = 10 * result + (source[i] - '0');
            }

            // fraction part
            if (i < source.Length && source[i] == '.')
            {
                i++;

                var invBase = 0.1m;
                for (; i < source.Length && char.IsDigit(source[i]); i++)
                {
                    result += (source[i] - '0') * invBase;
                    invBase *= 0.1m;
                }
            }

            // result w\o exponent
            result *= sign;

            // exponent
            var exponentNegative = false;
            var exponent = 0;
            if (i < source.Length && (source[i] == 'e' || source[i] == 'E'))
            {
                i++;

                switch (source[i])
                {
                    case '-':
                        exponentNegative = true;
                        i++;
                        break;
                    case '+':
                        i++;
                        break;
                }

                for (; i < source.Length && char.IsDigit(source[i]); i++)
                {
                    exponent = 10 * exponent + (source[i] - '0');
                }
            }

            if (exponent > 0)
            {
                var powerOfTen = 10m;
                for (; exponent > 1; exponent--)
                {
                    powerOfTen *= 10;
                }

                if (exponentNegative)
                {
                    result /= powerOfTen;
                }
                else
                {
                    result *= powerOfTen;
                }
            }

            output = result;

            return i;
        }

        #endregion

        public static JsonValue Parse(string json)
        {
            JsonValue root = null;
            JsonValue top = null;
            string name = null;

            var escapedNewlines = 0;

            char[] jsonArray = null;
            var i = 0;
            for (; i < json.Length; i++)
            {
                var ch = json[i];
                
                // skip white space
                if (ch == '\x20' || ch == '\x9' || ch == '\xD' || ch == '\xA')
                {
                    continue;
                }

                switch (ch)
                {
                    case '{':
                    case '[':
                        {
                            // create new value
                            var value = new JsonValue { Name = name };

                            // name
                            name = null;

                            // type
                            value.Type = ch == '{' ? JsonType.Object : JsonType.Array;

                            // set top and root
                            if (top != null)
                            {
                                top.Append(value);
                            }
                            else if (root == null)
                            {
                                root = value;
                            }
                            else
                            {
                                throw new JsonParserException(i, "Second root. Only one root allowed");
                            }
                            top = value;
                        }
                        break;

                    case '}':
                    case ']':
                        {
                            if (top == null || top.Type != ((ch == '}') ? JsonType.Object : JsonType.Array))
                            {
                                throw new JsonParserException(i, "Mismatch closing brace/bracket");
                            }

                            // set top
                            top = top.Parent;
                        }
                        break;

                    case ':':
                        if (top == null || top.Type != JsonType.Object)
                        {
                            throw new JsonParserException(i, "Unexpected character");
                        }
                        break;

                    case ',':
                        CheckTop(top, i);
                        break;

                    case '"':
                        {
                            CheckTop(top, i);

                            // skip '"' character
                            i++;
                            ch = json[i];

                            var first = i;
                            var last = i;
                            var chLast = ch;
                            while (i < json.Length)
                            {
                                if (ch < '\x20')
                                {
                                    throw new JsonParserException(first, "Control characters not allowed in strings");
                                }
                                if (ch == '\\')
                                {
                                    switch (json[i + 1])
                                    {
                                        case '"':
                                            chLast = '"';
                                            break;
                                        case '\\':
                                            chLast = '\\';
                                            break;
                                        case '/':
                                            chLast = '/';
                                            break;
                                        case 'b':
                                            chLast = '\b';
                                            break;
                                        case 'f':
                                            chLast = '\f';
                                            break;
                                        case 'n':
                                            chLast = '\n';
                                            ++escapedNewlines;
                                            break;
                                        case 'r':
                                            chLast = '\r';
                                            break;
                                        case 't':
                                            chLast = '\t';
                                            break;
                                        case 'u':
                                        {
                                            if (jsonArray == null)
                                            {
                                                jsonArray = json.ToCharArray();
                                            }

                                            uint codepoint;
                                            if (HexToInteger(json.Substring(i + 2, 4), out codepoint) != 4)
                                            {
                                                throw new JsonParserException(i, "Bad unicode codepoint");
                                            }

                                            if (codepoint <= 0x7F)
                                            {
                                                chLast = (char)codepoint;
                                            }
                                            else if (codepoint <= 0x7FF)
                                            {
                                                last++;
                                                jsonArray[last] = (char)(0xC0 | (codepoint >> 6));
                                                last++;
                                                jsonArray[last] = (char)(0x80 | (codepoint & 0x3F));
                                            }
                                            else if (codepoint <= 0xFFFF)
                                            {
                                                last++;
                                                jsonArray[last] = (char)(0xE0 | (codepoint >> 12));
                                                last++;
                                                jsonArray[last] = (char)(0x80 | ((codepoint >> 6) & 0x3F));
                                                last++;
                                                jsonArray[last] = (char)(0x80 | (codepoint & 0x3F));
                                            }
                                        }
                                            i += 4;
                                            break;
                                        default:
                                            throw new JsonParserException(first, "Unrecognized escape sequence");
                                    }

                                    last++;
                                    i++;
                                }
                                else if (ch == '"')
                                {
                                    break;
                                }
                                else
                                {
                                    last++;
                                    i++;
                                    ch = json[i];
                                }
                            }

                            if (top != null && (name == null && top.Type == JsonType.Object))
                            {
                                // field name in object
                                name = json.Substring(first, i - first);
                            }
                            else
                            {
                                // new string value
                                var value = new JsonValue { Name = name };

                                name = null;

                                value.Type = JsonType.String;

                                string s;
                                if (jsonArray != null)
                                {
                                    var slice = new char[i - first];
                                    Array.Copy(jsonArray, first, slice, 0, i - first);
                                    s = new string(slice);
                                }
                                else
                                {
                                    s = json.Substring(first, i - first);
                                }
                                value.Value = new TypeUnion<string, long, decimal, bool>(s);

                                if (top != null)
                                {
                                    top.Append(value);
                                }
                            }
                        }
                        break;

                    case 'n':
                    case 't':
                    case 'f':
                        {
                            CheckTop(top, i);

                            // new null/bool value
                            var value = new JsonValue { Name = name };

                            name = null;

                            // null
                            if (ch == 'n' && json[i + 1] == 'u' && json[i + 2] == 'l' && json[i + 3] == 'l')
                            {
                                value.Type = JsonType.Null;
                                i += 3;
                            }
                            // true
                            else if (ch == 't' && json[i + 1] == 'r' && json[i + 2] == 'u' && json[i + 3] == 'e')
                            {
                                value.Type = JsonType.Boolean;
                                value.Value = new TypeUnion<string, long, decimal, bool>(true);
                                i += 3;
                            }
                            // false
                            else if (jsonArray != null && (ch == 'f' && json[i + 1] == 'a' && json[i + 2] == 'l' && json[i + 3] == 's' && jsonArray[i + 4] == 'e'))
                            {
                                value.Type = JsonType.Boolean;
                                value.Value = new TypeUnion<string, long, decimal, bool>(false);
                                i += 4;
                            }
                            else
                            {
                                throw new JsonParserException(i, "Unknown identifier");
                            }

                            if (top != null)
                            {
                                top.Append(value);
                            }
                        }
                        break;

                    case '-':
                    case '0':
                    case '1':
                    case '2':
                    case '3':
                    case '4':
                    case '5':
                    case '6':
                    case '7':
                    case '8':
                    case '9':
                        {
                            CheckTop(top, i);

                            // new number value
                            var value = new JsonValue { Name = name };

                            name = null;

                            value.Type = JsonType.Integer;

                            int first = i;
                            while (ch != '\x20' && ch != '\x9' && ch != '\xD' && ch != '\xA' && ch != ',' && ch != ']' && ch != '}')
                            {
                                if (ch == '.' || ch == 'e' || ch == 'E')
                                {
                                    value.Type = JsonType.Decimal;
                                }
                                i++;
                                ch = json[i];
                            }
                        
                            if (value.Type == JsonType.Integer)
                            {
                                long n;
                                if (TextToInteger(json.Substring(first, i - first), out n) != i - first)
                                {
                                    throw new JsonParserException(first, "Bad integer number");
                                }
                                else
                                {
                                    value.Value = new TypeUnion<string, long, decimal, bool>(n);
                                }
                                i--;
                            }

                            if (value.Type == JsonType.Decimal)
                            {
                                decimal d;
                                if (TextToDecimal(json.Substring(first, i - first), out d) != i - first)
                                {
                                    throw new JsonParserException(first, "Bad decimal number");
                                }
                                value.Value = new TypeUnion<string, long, decimal, bool>(d);
                                i--;
                            }

                            if (top != null)
                            {
                                top.Append(value);
                            }
                        }
                        break;

                    default:
                        throw new JsonParserException(i, "Unexpected character");
                }
            }

            if (top != null)
            {
                throw new JsonParserException(i, "Not all objects/arrays have been properly closed");
            }

            return root;
        }
    }
}
