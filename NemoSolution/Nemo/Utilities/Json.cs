/**
 * This is a C# port of the vjson parser for C++.
 * The original vjson code can be found here http://code.google.com/p/vjson/
 **/

using System;
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
        Boolean,
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

        internal void Append(JsonValue value)
        {
            value.Parent = this;
            if (this.LastChild != null)
            {
                this.LastChild = this.LastChild.NexSibling = value;
            }
            else
            {
                this.FirstChild = this.LastChild = value;
            }
        }
    }

    public class JsonParserException : Exception
    {
        public JsonParserException() : base() { }
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
            uint result = 0;
            int i = 0;
            for (; i < source.Length; i++)
            {
                int digit;
                if (Char.IsDigit(source[i]))
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
            long sign = 1;
            int i = 0;
            if (source.Length > 0)
            {
                if (source[0] == '-')
                {
                    sign = -1;
                    i++;
                }
                else if (source[0] == '+')
                {
                    i++;
                }
            }

            long result = 0;
            for (; i < source.Length && Char.IsDigit(source[i]); i++)
            {
                result = 10 * result + (source[i] - '0');
            }
            output = result * sign;

            return i;
        }

        internal static int TextToDecimal(string source, out decimal output)
        {
            // sign
            decimal sign = 1m;
            int i = 0;
            if (source.Length > 0)
            {
                if (source[0] == '-')
                {
                    sign = -1;
                    i++;
                }
                else if (source[0] == '+')
                {
                    i++;
                }
            }

            // integer part
            decimal result = 0;
            for (; i < source.Length && Char.IsDigit(source[i]); i++)
            {
                result = 10 * result + (source[i] - '0');
            }

            // fraction part
            if (i < source.Length && source[i] == '.')
            {
                i++;

                decimal inv_base = 0.1m;
                for (; i < source.Length && Char.IsDigit(source[i]); i++)
                {
                    result += (source[i] - '0') * inv_base;
                    inv_base *= 0.1m;
                }
            }

            // result w\o exponent
            result *= sign;

            // exponent
            bool exponent_negative = false;
            int exponent = 0;
            if (i < source.Length && (source[i] == 'e' || source[i] == 'E'))
            {
                i++;

                if (source[i] == '-')
                {
                    exponent_negative = true;
                    i++;
                }
                else if (source[i] == '+')
                {
                    i++;
                }

                for (; i < source.Length && Char.IsDigit(source[i]); i++)
                {
                    exponent = 10 * exponent + (source[i] - '0');
                }
            }

            if (exponent > 0)
            {
                decimal power_of_ten = 10;
                for (; exponent > 1; exponent--)
                {
                    power_of_ten *= 10;
                }

                if (exponent_negative)
                {
                    result /= power_of_ten;
                }
                else
                {
                    result *= power_of_ten;
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

            int escaped_newlines = 0;

            char[] jsonArray = null;
            int i = 0;
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
                            var value = new JsonValue();

                            // name
                            value.Name = name;
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

                            int first = i;
                            int last = i;
                            char ch_last = ch;
                            while (i < json.Length)
                            {
                                if (ch < '\x20')
                                {
                                    throw new JsonParserException(first, "Control characters not allowed in strings");
                                }
                                else if (ch == '\\')
                                {
                                    switch (json[i + 1])
                                    {
                                        case '"':
                                            ch_last = '"';
                                            break;
                                        case '\\':
                                            ch_last = '\\';
                                            break;
                                        case '/':
                                            ch_last = '/';
                                            break;
                                        case 'b':
                                            ch_last = '\b';
                                            break;
                                        case 'f':
                                            ch_last = '\f';
                                            break;
                                        case 'n':
                                            ch_last = '\n';
                                            ++escaped_newlines;
                                            break;
                                        case 'r':
                                            ch_last = '\r';
                                            break;
                                        case 't':
                                            ch_last = '\t';
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
                                                    ch_last = (char)codepoint;
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

                            if (name == null && top.Type == JsonType.Object)
                            {
                                // field name in object
                                name = json.Substring(first, i - first);
                            }
                            else
                            {
                                // new string value
                                var value = new JsonValue();

                                value.Name = name;
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

                                top.Append(value);
                            }
                        }
                        break;

                    case 'n':
                    case 't':
                    case 'f':
                        {
                            CheckTop(top, i);

                            // new null/bool value
                            JsonValue value = new JsonValue();

                            value.Name = name;
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
                            else if (ch == 'f' && json[i + 1] == 'a' && json[i + 2] == 'l' && json[i + 3] == 's' && jsonArray[i + 4] == 'e')
                            {
                                value.Type = JsonType.Boolean;
                                value.Value = new TypeUnion<string, long, decimal, bool>(false);
                                i += 4;
                            }
                            else
                            {
                                throw new JsonParserException(i, "Unknown identifier");
                            }

                            top.Append(value);
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
                            var value = new JsonValue();

                            value.Name = name;
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
                                else
                                {
                                    value.Value = new TypeUnion<string, long, decimal, bool>(d);
                                }
                                i--;
                            }

                            top.Append(value);
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
