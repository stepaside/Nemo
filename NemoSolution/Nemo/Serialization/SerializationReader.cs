using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using Nemo.Attributes;
using Nemo.Collections;
using Nemo.Collections.Extensions;
using Nemo.Fn;
using Nemo.Reflection;

/*
 * 1. DateTime/TimeSpan serialization are taken from protobuf-net
 * 2. Integer serialiation/deserialization are taken from NetSerializer
 * 
 */

namespace Nemo.Serialization
{
    public class SerializationReader : IDisposable
    {
        private int _objectTypeHash;
        private readonly SerializationMode _mode;
        private readonly bool _serializeAll;
        private readonly bool _includePropertyNames;
        private byte? _objectByte;
        private Stream _stream;
        private Encoding _encoding;
        private static ConcurrentDictionary<string, Type> _types = new ConcurrentDictionary<string, Type>();
        
        public delegate object[] ObjectDeserializer(SerializationReader reader, int count);
        private static ConcurrentDictionary<Type, ObjectDeserializer> _deserializers = new ConcurrentDictionary<Type, ObjectDeserializer>();
        private static ConcurrentDictionary<Type, ObjectDeserializer> _deserializersNoHeader = new ConcurrentDictionary<Type, ObjectDeserializer>();
        private static ConcurrentDictionary<Type, ObjectDeserializer> _deserializersWithAllProperties = new ConcurrentDictionary<Type, ObjectDeserializer>();
        private static ConcurrentDictionary<Type, ObjectDeserializer> _deserializersWithAllPropertiesNoHeader = new ConcurrentDictionary<Type, ObjectDeserializer>();

        private SerializationReader(Stream stream, Encoding encoding)
        {
            _stream = stream;
            _encoding = encoding ?? new UTF8Encoding();
            _mode = (SerializationMode)ReadByte();
            _serializeAll = (_mode | SerializationMode.SerializeAll) == SerializationMode.SerializeAll;
            _includePropertyNames = (_mode | SerializationMode.IncludePropertyNames) == SerializationMode.IncludePropertyNames;
            if (_mode != SerializationMode.Manual)
            {
                _objectByte = ReadByte();
                if (_objectByte.Value == (byte)ObjectTypeCode.BusinessObject || _objectByte.Value == (byte)ObjectTypeCode.BusinessObjectList)
                {
                    _objectTypeHash = ReadInt32();
                }
            }
        }

        private static int DecodeZigZag32(uint n)
        {
            return (int)(n >> 1) ^ -(int)(n & 1);
        }

        private static long DecodeZigZag64(ulong n)
        {
            return (long)(n >> 1) ^ -(long)(n & 1);
        }

        private uint ReadFixed32()
        {
            var buffer = new byte[4];
            _stream.Read(buffer, 0, 4);
            var v = (uint)((int)buffer[0] | (int)buffer[1] << 8 | (int)buffer[2] << 16 | (int)buffer[3] << 24);
            return v;
        }

        private ulong ReadFixed64()
        {
            var buffer = new byte[8];
            _stream.Read(buffer, 0, 8);
            var v1 = (uint)((int)buffer[0] | (int)buffer[1] << 8 | (int)buffer[2] << 16 | (int)buffer[3] << 24);
            var v2 = (uint)((int)buffer[4] | (int)buffer[5] << 8 | (int)buffer[6] << 16 | (int)buffer[7] << 24);
            var v = (ulong)v2 << 32 | (ulong)v1;
            return v;
        }

        public static SerializationReader CreateReader(SerializationInfo info)
        {
            byte[] buffer = (byte[])info.GetValue("X", typeof(byte[]));
            return SerializationReader.CreateReader(buffer);
        }

        public static SerializationReader CreateReader(byte[] buffer)
        {
            return new SerializationReader(new MemoryStream(buffer), null);
        }

        public static int GetObjectTypeHash(byte[] buffer)
        {
            return CreateReader(buffer).ObjectTypeHash;
        }

        public int ObjectTypeHash
        {
            get
            {
                return _objectTypeHash;
            }
        }

        public byte ReadByte()
        {
            return (byte)_stream.ReadByte();
        }

        public sbyte ReadSByte()
        {
            return (sbyte)_stream.ReadByte();
        }

        public bool ReadBoolean()
        {
            var b = _stream.ReadByte();
            return b != 0;
        }

        public char ReadChar()
        {
            return (char)ReadUInt32();
        }

        public short ReadInt16()
        {
            return (short)DecodeZigZag32(ReadUInt32());
        }

        public int ReadInt32()
        {
            return DecodeZigZag32(ReadUInt32());
        }

        public long ReadInt64()
        {
            return DecodeZigZag64(ReadUInt64());
        }

        public ushort ReadUInt16()
        {
            return (ushort)ReadUInt32();
        }

        public uint ReadUInt32()
        {
            int result = 0;
            int offset = 0;

            for (; offset < 32; offset += 7)
            {
                var b = _stream.ReadByte();
                if (b == -1)
                {
                    throw new EndOfStreamException();
                }

                result |= (b & 0x7f) << offset;

                if ((b & 0x80) == 0)
                {
                    return (uint)result;
                }
            }

            throw new InvalidDataException();
        }

        public ulong ReadUInt64()
        {
            long result = 0;
            int offset = 0;

            for (; offset < 64; offset += 7)
            {
                var b = _stream.ReadByte();
                if (b == -1)
                {
                    throw new EndOfStreamException();
                }

                result |= ((long)(b & 0x7f)) << offset;

                if ((b & 0x80) == 0)
                {
                    return (ulong)result;
                }
            }

            throw new InvalidDataException();
        }

        public unsafe float ReadSingle()
        {
            var v = ReadFixed32();
            return *(float*)(&v);
        }

        public unsafe double ReadDouble()
        {
            var v = ReadFixed64();
            return *(double*)(&v);
        }

        public decimal ReadDecimal()
        {
            var bits = new int[4];
            bits[0] = ReadInt32();
            bits[1] = ReadInt32();
            bits[2] = ReadInt32();
            bits[3] = ReadInt32();
            return new decimal(bits);
        }

        public byte[] ReadBytes()
        {
            var length = ReadUInt32();
            if (length == 0)
            {
                return null;
            }
            else if (length == 1)
            {
                return new byte[0];
            }
            else
            {
                length -= 1;
                var buffer = new byte[length];
                int l = 0;
                while (l < length)
                {
                    int r = _stream.Read(buffer, l, (int)length - l);
                    if (r == 0)
                    {
                        throw new EndOfStreamException();
                    }
                    l += r;
                }
                return buffer;
            }
        }

        public char[] ReadChars()
        {
            var length = ReadUInt32();
            if (length == 0)
            {
                return null;
            }
            else if (length == 1)
            {
                return new char[0];
            }
            else
            {
                length -= 1;
                var buffer = new char[length];
                for (int i = 0; i < length; i++)
                {
                    buffer[i] = ReadChar();
                }
                return buffer;
            }
        }

        public string ReadString()
        {
            var length = ReadUInt32();
            if (length == 0)
            {
                return null;
            }
            else if (length == 1)
            {
                return string.Empty;
            }
            else
            {
                length -= 1;
                var buffer = new byte[length];
                int l = 0;
                while (l < length)
                {
                    int r = _stream.Read(buffer, l, (int)length - l);
                    if (r == 0)
                    {
                        throw new EndOfStreamException();
                    }
                    l += r;
                }
                return _encoding.GetString(buffer);
            }
        }

        public DateTime ReadDateTime()
        {
            var ticks = ReadTicks();
            if (ticks == long.MinValue) return DateTime.MinValue;
            if (ticks == long.MaxValue) return DateTime.MaxValue;
            return UnixDateTime.Epoch.AddTicks(ticks);
        }
        
        public TimeSpan ReadTimeSpan()
        {
            var ticks = ReadTicks();
            if (ticks == long.MinValue) return TimeSpan.MinValue;
            if (ticks == long.MaxValue) return TimeSpan.MaxValue;
            return TimeSpan.FromTicks(ticks);
        }

        private long ReadTicks()
        {
            var scale = (TemporalScale)ReadByte();
            var ticks = ReadInt64();

            switch (scale)
            {
                case TemporalScale.Days:
                    ticks *= TimeSpan.TicksPerDay;
                    break;
                case TemporalScale.Hours:
                    ticks *= TimeSpan.TicksPerHour;
                    break;
                case TemporalScale.Minutes:
                    ticks *= TimeSpan.TicksPerMinute;
                    break;
                case TemporalScale.Seconds:
                    ticks *= TimeSpan.TicksPerSecond;
                    break;
                case TemporalScale.Milliseconds:
                    ticks *= TimeSpan.TicksPerMillisecond;
                    break;
                case TemporalScale.Ticks:
                    break;
                case TemporalScale.MinMax:
                    switch (ticks)
                    {
                        case 1: return long.MaxValue;
                        case -1: return long.MinValue;
                    }
                    break;
            }
            return ticks;
        }

        public DateTimeOffset ReadDateTimeOffset()
        {
            return new DateTimeOffset(ReadDateTime(), ReadTimeSpan());
        }

        public Guid ReadGuid()
        {
            return new Guid(ReadBytes());
        }

        public Version ReadVersion()
        {
            var major = (int)ReadUInt32();
            var minor = (int)ReadUInt32();
            var build = (int)ReadUInt32();
            var revision = (int)ReadUInt32();
            return new Version(major, minor, build, revision);
        }

        public Uri ReadUri()
        {
            var absoluteUri = ReadString();
            return new Uri(absoluteUri);
        }

        public IList<T> ReadList<T>()
        {
            var count = ReadUInt32();
            if (count == 0)
            {
                return null;
            }
            else if (count == 1)
            {
                return new List<T>();
            }
            else
            {
                IList<T> list = new List<T>();
                for (int i = 0; i < count - 1; i++)
                {
                    list.Add((T)ReadObject(typeof(T)));
                }
                return list;
            }
        }

        public void ReadList(IList list, Type elementType)
        {
            var count = ReadUInt32();
            if (count == 0 || count == 1)
            {
                return;
            }
            else
            {
                for (int i = 0; i < count - 1; i++)
                {
                    list.Add(ReadObject(elementType));
                }
            }
        }

        public IDictionary<TKey, TValue> ReadDictionary<TKey, TValue>()
        {
            var count = ReadUInt32();
            if (count == 0)
            {
                return null;
            }
            else if (count == 1)
            {
                return new Dictionary<TKey, TValue>();
            }
            else
            {
                var map = new Dictionary<TKey, TValue>();
                for (int i = 0; i < count - 1; i++)
                {
                    map.Add((TKey)ReadObject(typeof(TKey)), (TValue)ReadObject(typeof(TValue)));
                }
                return map;
            }
        }

        public void ReadDictionary(IDictionary map, Type keyType, Type valueType)
        {
            var count = ReadUInt32();
            if (count == 0 || count == 1)
            {
                return;
            }
            for (int i = 0; i < count - 1; i++)
            {
                map.Add(ReadObject(keyType), ReadObject(valueType));
            }
        }

        private static Type GetType(string typeName)
        {
            if (string.IsNullOrEmpty(typeName))
            {
                return null;
            }
            Type type = _types.GetOrAdd(typeName, k => typeName.IndexOf(',') > -1 ? Type.GetType(typeName) : AppDomain.CurrentDomain.GetAssemblies().SelectMany(a => a.GetTypes()).FirstOrDefault(t => t.FullName == typeName));
            return type;
        }

        private void Skip(int count)
        {
            var buffer = new byte[count];
            _stream.Read(buffer, 0, count);
        }
        
        public object ReadObject(Type objectType)
        {
            return ReadObject(objectType, _objectByte.HasValue ? (ObjectTypeCode)_objectByte.Value : Reflector.GetObjectTypeCode(objectType), typeof(IConvertible).IsAssignableFrom(objectType));
        }

        public object ReadObject(Type objectType, ObjectTypeCode expectedTypeCode, bool isConvertible)
        {
            ObjectTypeCode typeCode;
            if (_objectByte.HasValue)
            {
                typeCode = (ObjectTypeCode)_objectByte.Value;
                _objectByte = null;
            }
            else
            {
                typeCode = (ObjectTypeCode)ReadByte();
            }

            switch (typeCode)
            {
                case ObjectTypeCode.Boolean:
                    return ConvertIfNecessary(ReadBoolean(), objectType, typeCode, expectedTypeCode, isConvertible);
                case ObjectTypeCode.Byte:
                    return ConvertIfNecessary(ReadByte(), objectType, typeCode, expectedTypeCode, isConvertible);
                case ObjectTypeCode.UInt16:
                    return ConvertIfNecessary(ReadUInt16(), objectType, typeCode, expectedTypeCode, isConvertible);
                case ObjectTypeCode.UInt32:
                    return ConvertIfNecessary(ReadUInt32(), objectType, typeCode, expectedTypeCode, isConvertible);
                case ObjectTypeCode.UInt64:
                    return ConvertIfNecessary(ReadUInt64(), objectType, typeCode, expectedTypeCode, isConvertible);
                case ObjectTypeCode.SByte:
                    return ConvertIfNecessary(ReadSByte(), objectType, typeCode, expectedTypeCode, isConvertible);
                case ObjectTypeCode.Int16:
                    return ConvertIfNecessary(ReadInt16(), objectType, typeCode, expectedTypeCode, isConvertible);
                case ObjectTypeCode.Int32:
                    return ConvertIfNecessary(ReadInt32(), objectType, typeCode, expectedTypeCode, isConvertible);
                case ObjectTypeCode.Int64:
                    return ConvertIfNecessary(ReadInt64(), objectType, typeCode, expectedTypeCode, isConvertible);
                case ObjectTypeCode.Char:
                    return ConvertIfNecessary(ReadChar(), objectType, typeCode, expectedTypeCode, isConvertible);
                case ObjectTypeCode.String:
                    return ConvertIfNecessary(ReadString(), objectType, typeCode, expectedTypeCode, isConvertible);
                case ObjectTypeCode.Single:
                    return ConvertIfNecessary(ReadSingle(), objectType, typeCode, expectedTypeCode, isConvertible);
                case ObjectTypeCode.Double:
                    return ConvertIfNecessary(ReadDouble(), objectType, typeCode, expectedTypeCode, isConvertible);
                case ObjectTypeCode.Decimal:
                    return ConvertIfNecessary(ReadDecimal(), objectType, typeCode, expectedTypeCode, isConvertible);
                case ObjectTypeCode.DateTime:
                    return ConvertIfNecessary(ReadDateTime(), objectType, typeCode, expectedTypeCode, isConvertible);
                case ObjectTypeCode.ByteArray:
                    return ReadBytes();
                case ObjectTypeCode.CharArray:
                    return ReadChars();
                case ObjectTypeCode.TimeSpan:
                    return ReadTimeSpan();
                case ObjectTypeCode.DateTimeOffset:
                    return ReadDateTimeOffset();
                case ObjectTypeCode.Guid:
                    return ReadGuid();
                case ObjectTypeCode.Version:
                    return ReadVersion();
                case ObjectTypeCode.Uri:
                    return ReadUri();
                case ObjectTypeCode.DBNull:
                    return DBNull.Value;
                case ObjectTypeCode.Object:
                    return new BinaryFormatter().Deserialize(_stream);
                case ObjectTypeCode.ObjectList:
                    {
                        var objectList = (IList)Reflection.Activator.New(objectType);
                        ReadList(objectList, objectType.GetGenericArguments()[0]);
                        return objectList;
                    }
                case ObjectTypeCode.ObjectMap:
                    {
                        var objectMap = (IDictionary)Reflection.Activator.New(objectType);
                        var genericArgs = objectType.GetGenericArguments();
                        ReadDictionary(objectMap, genericArgs[0], genericArgs[1]);
                        return objectMap;
                    }
                case ObjectTypeCode.BusinessObject:
                    {
                        var deserializer = CreateDelegate(objectType);
                        var businessObjects = deserializer(this, 1);
                        if (businessObjects != null && businessObjects.Length > 0)
                        {
                            return businessObjects[0];
                        }
                        else
                        {
                            return null;
                        }
                    }
                case ObjectTypeCode.BusinessObjectList:
                    {
                        var itemCount = (int)ReadUInt32();

                        var listAspectType = (ListAspectType)ReadByte();
                        DistinctAttribute distinctAttribute = null;
                        SortedAttribute sortedAttribute = null;
                        Type comparerType = null;
                        if (listAspectType != ListAspectType.None)
                        {
                            comparerType = GetType(ReadString());
                            if (listAspectType == ListAspectType.Distinct)
                            {
                                distinctAttribute = new DistinctAttribute { EqualityComparerType = comparerType };
                            }
                            else if (listAspectType == ListAspectType.Sorted || listAspectType == (ListAspectType.Sorted | ListAspectType.Distinct))
                            {
                                sortedAttribute = new SortedAttribute { ComparerType = comparerType };
                                if (listAspectType != ListAspectType.Sorted)
                                {
                                    distinctAttribute = new DistinctAttribute();
                                } 
                            }
                        }

                        var list = List.Create(objectType, distinctAttribute, sortedAttribute);
                        var deserializer = CreateDelegate(objectType);
                        var businessObjects = deserializer(this, itemCount);
                        for (int i = 0; i < businessObjects.Length; i++ )
                        {
                            list.Add(businessObjects[i]);
                        }
                        return list;
                    }
                case ObjectTypeCode.TypeUnion:
                    var unionTypeIndex = ReadInt32();
                    var unionTypes = objectType.GetGenericArguments();
                    var unionValue = ReadObject(unionTypes[unionTypeIndex]);
                    var union = TypeUnion.Create(unionTypes, unionValue);
                    return union;

                default:
                    return null;
            }
        }

        /// <summary>
        /// Reads data from a stream until the end is reached. The
        /// data is returned as a byte array. An IOException is
        /// thrown if any of the underlying IO calls fail.
        /// </summary>
        /// <param name="stream">The stream to read data from</param>
        /// <param name="initialLength">The initial buffer length</param>
        public static byte[] ReadStream(Stream stream, int initialLength)
        {
            // If we've been passed an unhelpful initial length, just
            // use 32K.
            if (initialLength < 1)
            {
                initialLength = 32768;
            }

            var buffer = new byte[initialLength];
            int read = 0;

            int chunk;
            while ((chunk = stream.Read(buffer, read, buffer.Length - read)) > 0)
            {
                read += chunk;

                // If we've reached the end of our buffer, check to see if there's
                // any more information
                if (read == buffer.Length)
                {
                    int nextByte = stream.ReadByte();

                    // End of stream? If so, we're done
                    if (nextByte == -1)
                    {
                        return buffer;
                    }

                    // Nope. Resize the buffer, put in the byte we've just
                    // read, and continue
                    var newBuffer = new byte[buffer.Length * 2];
                    Buffer.BlockCopy(buffer, 0, newBuffer, 0, buffer.Length);
                    newBuffer[read] = (byte)nextByte;
                    buffer = newBuffer;
                    read++;
                }
            }
            // Buffer is now too big. Shrink it.
            var result = new byte[read];
            Buffer.BlockCopy(buffer, 0, result, 0, read);
            return result;
        }

        public static byte[] ReadStream(Stream stream)
        {
            return ReadStream(stream, -1);
        }

        public static object ReadObjectWithType(byte[] buffer)
        {
            var typeLength = BitConverter.ToInt32(buffer.Slice(0, 4), 0);
            var typeName = Encoding.UTF8.GetString(buffer.Slice(4, typeLength));
            var reader = SerializationReader.CreateReader(buffer.Slice(4 + typeLength, buffer.Length - 1));
            var result = reader.ReadObject(SerializationReader.GetType(typeName));
            return result;
        }

        private static object ConvertIfNecessary(object value, Type objectType, ObjectTypeCode typeCode, ObjectTypeCode expectedTypeCode, bool isConvertible)
        {
            if (expectedTypeCode != typeCode && isConvertible)
            {
                return Reflector.ChangeType(value, objectType);
            }
            else
            {
                return value;
            }
        }

        private ObjectDeserializer CreateDelegate(Type objectType)
        {
            var exists = true;
            var propertyCount = -1; 
            var propertyLength = -1;
            if (_includePropertyNames)
            {
                propertyCount = (int)ReadUInt32();
                propertyLength = (int)ReadUInt32();
            }
            ObjectDeserializer deserializer;
            if (!_serializeAll)
            {
                if (_includePropertyNames)
                {
                    deserializer = _deserializers.GetOrAdd(objectType, t => CreateDeserializer(t, propertyCount, ref exists));
                }
                else
                {
                    deserializer = _deserializersNoHeader.GetOrAdd(objectType, t => CreateDeserializer(t, propertyCount, ref exists));
                }
            }
            else
            {
                if (_includePropertyNames)
                {
                    deserializer = _deserializersWithAllProperties.GetOrAdd(objectType, t => CreateDeserializer(t, propertyCount, ref exists));
                }
                else
                {
                    deserializer = _deserializersWithAllPropertiesNoHeader.GetOrAdd(objectType, t => CreateDeserializer(t, propertyCount, ref exists));
                }
            }
            if (exists && _includePropertyNames)
            {
                Skip(propertyLength);
            }
            return deserializer;
        }

        private ObjectDeserializer CreateDeserializer(Type objectType, int propertyCount, ref bool exists)
        {
            var businessObject = ObjectFactory.Create(objectType);
            var propertyNames = new List<string>();
            if (_includePropertyNames)
            {
                for (int i = 0; i < propertyCount; i++)
                {
                    propertyNames.Add(this.ReadString());
                }
            }
            exists = false;
            return GenerateDelegate(businessObject.GetType(), propertyNames);
        }

        private ObjectDeserializer GenerateDelegate(Type objectType, List<string> propertyNames)
        {
            var method = new DynamicMethod("Deserialize_" + objectType.Name, typeof(object[]), new[] { typeof(SerializationReader), typeof(int) }, typeof(SerializationReader).Module);
            var il = method.GetILGenerator();

            var readObject = this.GetType().GetMethod("ReadObject", new[] { typeof(Type), typeof(ObjectTypeCode), typeof(bool) });
            var getTypeFromHandle = typeof(Type).GetMethod("GetTypeFromHandle");

            var interfaceType = objectType;
            if (Reflector.IsEmitted(objectType))
            {
                interfaceType = Reflector.ExtractInterface(objectType);
                if (interfaceType == null)
                {
                    interfaceType = objectType;
                }
            }

            var properties = Reflector.GetAllProperties(interfaceType);
            PropertyInfo[] orderedProperties;
            if(_includePropertyNames)
            {
                orderedProperties = properties.Where(p => propertyNames.Contains(p.Name)).Arrange(propertyNames, p => p.Name).ToArray();
            }
            else
            {
                var propertyPositions = Reflector.GetAllPropertyPositions(interfaceType).OrderBy(p => p.Value).Select(p => p.Key);
                orderedProperties = properties.Where(p => p.CanRead && p.CanWrite && p.Name != "Indexer" && (_serializeAll || !p.GetCustomAttributes(typeof(DoNotSerializeAttribute), false).Any())).Arrange(propertyPositions, p => p.Name).ToArray();
            }

            var enterLoop = il.DefineLabel();
            var exitLoop = il.DefineLabel();
            var result = il.DeclareLocal(typeof(object[]));
            var index = il.DeclareLocal(typeof(int));
            var value = il.DeclareLocal(interfaceType);

            // Instanciate result array
            il.Emit(OpCodes.Ldarg_1);
            il.Emit(OpCodes.Newarr, typeof(object));
            il.Emit(OpCodes.Stloc_0);

            // Set-up loop
            il.Emit(OpCodes.Ldc_I4_0);
            il.Emit(OpCodes.Stloc_1);
            il.Emit(OpCodes.Br, exitLoop);
            il.MarkLabel(enterLoop);

            // Create instance of a given type
            il.Emit(OpCodes.Newobj, objectType.GetConstructor(Type.EmptyTypes));
            il.Emit(OpCodes.Stloc_2);

            foreach (var property in orderedProperties)
            {
                // Read property value
                il.Emit(OpCodes.Ldloc_2);
                il.Emit(OpCodes.Ldarg_0);
                Type propertyType;
                if (!Reflector.IsBusinessObjectList(property.PropertyType, out propertyType))
                {
                    propertyType = property.PropertyType;
                }
                il.Emit(OpCodes.Ldtoken, propertyType);
                il.Emit(OpCodes.Call, getTypeFromHandle);
                il.Emit(OpCodes.Ldc_I4, (int)Reflector.GetObjectTypeCode(propertyType));
                if (typeof(IConvertible).IsAssignableFrom(propertyType))
                {
                    il.Emit(OpCodes.Ldc_I4_1);
                }
                else
                {
                    il.Emit(OpCodes.Ldc_I4_0);
                }
                il.Emit(OpCodes.Callvirt, readObject);
                il.EmitCastToReference(property.PropertyType);
                il.EmitCall(OpCodes.Callvirt, property.GetSetMethod(), null);
            }

            // Set array's element
            il.Emit(OpCodes.Ldloc_0);
            il.Emit(OpCodes.Ldloc_1);
            il.Emit(OpCodes.Ldloc_2);
            il.Emit(OpCodes.Stelem_Ref);

            il.Emit(OpCodes.Ldloc_1);
            il.Emit(OpCodes.Ldc_I4_1);
            il.Emit(OpCodes.Add);
            il.Emit(OpCodes.Stloc_1);
            il.MarkLabel(exitLoop);
            il.Emit(OpCodes.Ldloc_1);
            il.Emit(OpCodes.Ldarg_1);
            il.Emit(OpCodes.Blt, enterLoop);

            il.Emit(OpCodes.Ldloc_0);
            il.Emit(OpCodes.Ret);

            var deserializer = (ObjectDeserializer)method.CreateDelegate(typeof(ObjectDeserializer));
            return deserializer;
        }

        #region IDisposable Members

        public void Dispose()
        {
            _stream.Dispose();
        }

        #endregion
    }
}
