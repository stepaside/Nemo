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
    public enum ObjectTypeCode : byte
    {
        Empty = TypeCode.Empty,
        Object = TypeCode.Object,
        DBNull = TypeCode.DBNull,
        Boolean = TypeCode.Boolean,
        Char = TypeCode.Char,
        SByte = TypeCode.SByte,
        Byte = TypeCode.Byte,
        Int16 = TypeCode.Int16,
        UInt16 = TypeCode.UInt16,
        Int32 = TypeCode.Int32,
        UInt32 = TypeCode.UInt32,
        Int64 = TypeCode.Int64,
        UInt64 = TypeCode.UInt64,
        Single = TypeCode.Single,
        Double = TypeCode.Double,
        Decimal = TypeCode.Decimal,
        DateTime = TypeCode.DateTime,
        String = TypeCode.String,
        TimeSpan = 32,
        DateTimeOffset = 33,
        Guid = 34,
        Version = 35,
        Uri = 36,
        ByteArray = 64,
        CharArray = 65,
        ObjectList = 66,
        ObjectMap = 67,
        TypeUnion = 128,
        BusinessObject = 129,
        BusinessObjectList = 130,
        ProtocolBuffer = 255
    }

    public enum ListAspectType : byte
    {
        None = 1,
        Distinct = 2,
        Sorted = 4
    }

    public enum SerializationMode : byte
    {
        Compact = 1,
        SerializeAll = 2,
        IncludePropertyNames = 4
    }

    public enum TemporalScale : byte
    {
        Days = 0,
        Hours = 1,
        Minutes = 2,
        Seconds = 3,
        Milliseconds = 4,
        Ticks = 5,
        MinMax = 15
    }
    
    public static class UnixDateTime
    {
        public static readonly DateTime Epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    }

    public class SerializationWriter : IDisposable
    {
        public delegate void ObjectSerializer(SerializationWriter writer, IList values, int count);

        private static ConcurrentDictionary<Type, ObjectSerializer> _serializers = new ConcurrentDictionary<Type, ObjectSerializer>();
        private static ConcurrentDictionary<Type, ObjectSerializer> _serializersNoHeader = new ConcurrentDictionary<Type, ObjectSerializer>();
        private static ConcurrentDictionary<Type, ObjectSerializer> _serializersWithAllProperties = new ConcurrentDictionary<Type, ObjectSerializer>();
        private static ConcurrentDictionary<Type, ObjectSerializer> _serializersWithAllPropertiesNoHeader = new ConcurrentDictionary<Type, ObjectSerializer>();

        private readonly SerializationMode _mode;
        private readonly bool _serializeAll;
        private readonly bool _includePropertyNames;
        private bool _objectTypeWritten;
        private Stream _stream;
        private Encoding _encoding;

        private SerializationWriter(Stream stream, SerializationMode mode, Encoding encoding)
        {
            _stream = stream ?? new MemoryStream(512);
            _encoding = encoding ?? new UTF8Encoding();
            _mode = mode;
            _serializeAll = (mode | SerializationMode.SerializeAll) == SerializationMode.SerializeAll;
            _includePropertyNames = (mode | SerializationMode.IncludePropertyNames) == SerializationMode.IncludePropertyNames;
            Write((byte)_mode);
        }

        private static uint EncodeZigZag32(int n)
        {
            return (uint)((n << 1) ^ (n >> 31));
        }

        private static ulong EncodeZigZag64(long n)
        {
            return (ulong)((n << 1) ^ (n >> 63));
        }

        private void WriteFixed32(uint value)
        {
            var buffer = new byte[4];
            buffer[0] = (byte)value;
            buffer[1] = (byte)(value >> 8);
            buffer[2] = (byte)(value >> 16);
            buffer[3] = (byte)(value >> 24);
            _stream.Write(buffer, 0, 4);
        }

        private void WriteFixed64(ulong value)
        {
            var buffer = new byte[8];
            buffer[0] = (byte)value;
            buffer[1] = (byte)(value >> 8);
            buffer[2] = (byte)(value >> 16);
            buffer[3] = (byte)(value >> 24);
            buffer[4] = (byte)(value >> 32);
            buffer[5] = (byte)(value >> 40);
            buffer[6] = (byte)(value >> 48);
            buffer[7] = (byte)(value >> 56);
            _stream.Write(buffer, 0, 8);
        }
        
        public static SerializationWriter CreateWriter(SerializationMode mode)
        {
            return new SerializationWriter(null, mode, null);
        }

        public byte[] GetBytes()
        {
            if (_stream is MemoryStream)
            {
                var data = ((MemoryStream)_stream).ToArray();
                return data;
            }
            else
            {
                return new byte[0];
            }
        }

        public void Write(byte value)
        {
            _stream.WriteByte(value);
        }

        public void Write(sbyte value)
        {
            _stream.WriteByte((byte)value);
        }

        public void Write(bool value)
        {
            _stream.WriteByte(value ? (byte)1 : (byte)0);
        }

        public void Write(short value)
        {
            Write((int)value);
        }

        public void Write(int value)
        {
            Write(EncodeZigZag32(value));
        }

        public void Write(long value)
        {
            Write(EncodeZigZag64(value));
        }

        public void Write(ushort value)
        {
            Write((uint)value);
        }

        public void Write(uint value)
        {
            for (; value >= 0x80u; value >>= 7)
            {
                _stream.WriteByte((byte)(value | 0x80u));
            }
            _stream.WriteByte((byte)value);
        }

        public void Write(ulong value)
        {
            for (; value >= 0x80u; value >>= 7)
            {
                _stream.WriteByte((byte)(value | 0x80u));
            }
            _stream.WriteByte((byte)value);
        }

        public unsafe void Write(float value)
        {
            var v = *(uint*)(&value);
            WriteFixed32(v);
        }

        public unsafe void Write(double value)
        {
            var v = *(ulong*)(&value);
            WriteFixed64(v);
        }

        public void Write(decimal value)
        {
            var bits = decimal.GetBits(value);
            Write(bits[0]);
            Write(bits[1]);
            Write(bits[2]);
            Write(bits[3]);
        }

        public void Write(char value)
        {
            Write((uint)value);
        }

        public void Write(string value)
        {
            if (value == null)
            {
                Write(0u);
            }
            else
            {
                var chars = value.ToCharArray();
                var length = _encoding.GetByteCount(chars);
                Write((uint)length + 1);
                if (length > 0)
                {
                    var buffer = new byte[length];
                    _encoding.GetBytes(chars, 0, length, buffer, 0);
                    _stream.Write(buffer, 0, length);
                }
            }
        }

        public void Write(byte[] value)
        {
            if (value == null)
            {
                Write(0u);
            }
            else
            {
                var length = value.Length;
                Write((uint)length + 1);
                if (length > 0)
                {
                    _stream.Write(value, 0, length);
                }
            }
        }

        public void Write(char[] value)
        {
            if (value == null)
            {
                Write(0u);
            }
            else
            {
                var length = value.Length;
                Write((uint)length + 1);
                for (int i = 0; i < length; i++)
                {
                    Write(value[i]);
                }
            }
        }
        
        public void Write(DateTime value)
        {
            if (value == DateTime.MaxValue)
            {
                Write(TimeSpan.MaxValue);
            }
            else if (value == DateTime.MinValue)
            {
                Write(TimeSpan.MinValue);
            }
            else
            {
                Write(value - UnixDateTime.Epoch);
            }
        }
        
        public void Write(TimeSpan value)
        {
            var ticks = value.Ticks;
            var scale = TemporalScale.Ticks;

            if (value == TimeSpan.MaxValue)
            {
                scale = TemporalScale.MinMax;
                ticks = 1;
            }
            else if (value == TimeSpan.MinValue)
            {
                scale = TemporalScale.MinMax;
                ticks = -1;
            }
            else if (ticks % TimeSpan.TicksPerDay == 0)
            {
                scale = TemporalScale.Days;
                ticks /= TimeSpan.TicksPerDay;
            }
            else if (ticks % TimeSpan.TicksPerHour == 0)
            {
                scale = TemporalScale.Hours;
                ticks /= TimeSpan.TicksPerHour;
            }
            else if (ticks % TimeSpan.TicksPerMinute == 0)
            {
                scale = TemporalScale.Minutes;
                ticks /= TimeSpan.TicksPerMinute;
            }
            else if (ticks % TimeSpan.TicksPerSecond == 0)
            {
                scale = TemporalScale.Seconds;
                ticks /= TimeSpan.TicksPerSecond;
            }
            else if (ticks % TimeSpan.TicksPerMillisecond == 0)
            {
                scale = TemporalScale.Milliseconds;
                ticks /= TimeSpan.TicksPerMillisecond;
            }

            Write((byte)scale);
            Write(ticks);
        }

        public void Write(DateTimeOffset value)
        {
            Write(value.DateTime);
            Write(value.Offset);
        }

        public void WriteList(IList items)
        {
            if (items == null)
            {
                Write(0u);
            }
            else
            {
                Write((uint)items.Count + 1);
                for (int i = 0; i < items.Count; i++)
                {
                    WriteObject(items[i]);
                }
            }
        }

        public void WriteList<T>(IList<T> items)
        {
            WriteList((IList)items);
        }

        public void WriteDictionary<T, U>(IDictionary<T, U> map)
        {
            if (map == null)
            {
                Write(-1);
            }
            else
            {
                Write((uint)map.Count + 1);
                foreach (var pair in map)
                {
                    WriteObject(pair.Key);
                    WriteObject(pair.Value);
                }
            }
        }

        public void WriteDictionary(IDictionary map)
        {
            if (map == null)
            {
                Write(0u);
            }
            else
            {
                Write((uint)map.Count + 1);
                var iterator = map.GetEnumerator();
                while(iterator.MoveNext())
                {
                    WriteObject(iterator.Key);
                    WriteObject(iterator.Value);
                }
            }
        }

        public void WriteObjectType(int typeHash)
        {
            if (!_objectTypeWritten)
            {
                Write(typeHash);
                _objectTypeWritten = true;
            }
        }

        public void WriteObject(object value)
        {
            WriteObject(value, value != null ? Reflector.GetObjectTypeCode(value.GetType()) : ObjectTypeCode.Empty);
        }

        public void WriteObject(object value, ObjectTypeCode typeCode)
        {
            if (value == null)
            {
                Write((byte)TypeCode.Empty);
            }
            else
            {
                Write((byte)typeCode);
                switch (typeCode)
                {
                    case ObjectTypeCode.Boolean:
                        Write((bool)value);
                        break;

                    case ObjectTypeCode.Byte:
                        Write((byte)value);
                        break;

                    case ObjectTypeCode.SByte:
                        Write((sbyte)value);
                        break;

                    case ObjectTypeCode.UInt16:
                        Write((ushort)value);
                        break;

                    case ObjectTypeCode.UInt32:
                        Write((uint)value);
                        break;

                    case ObjectTypeCode.UInt64:
                        Write((ulong)value);
                        break;

                    case ObjectTypeCode.Int16:
                        Write((short)value);
                        break;

                    case ObjectTypeCode.Int32:
                        Write((int)value);
                        break;

                    case ObjectTypeCode.Int64:
                        Write((long)value);
                        break;

                    case ObjectTypeCode.Char:
                        Write((char)value);
                        break;

                    case ObjectTypeCode.String:
                        Write((string)value);
                        break;

                    case ObjectTypeCode.Single:
                        Write((float)value);
                        break;

                    case ObjectTypeCode.Double:
                        Write((double)value);
                        break;

                    case ObjectTypeCode.Decimal:
                        Write((decimal)value);
                        break;

                    case ObjectTypeCode.DateTime:
                        Write((DateTime)value);
                        break;

                    case ObjectTypeCode.DBNull:
                        break;

                    case ObjectTypeCode.BusinessObject:
                        {
                            var serializer = CreateDelegate(value.GetType());
                            serializer(this, new object[] { value }, 1);
                        }
                        break;

                    case ObjectTypeCode.BusinessObjectList:
                        {
                            var items = (IList)value;

                            Write((uint)items.Count);
                            if (value is ISortedList)
                            {
                                if (((ISortedList)value).Distinct)
                                {
                                    Write((byte)(ListAspectType.Sorted | ListAspectType.Distinct));
                                }
                                else
                                {
                                    Write((byte)ListAspectType.Sorted);
                                }
                                Write(((ISortedList)value).Comparer.FullName);
                            }
                            else if (value is ISet)
                            {
                                Write((byte)ListAspectType.Distinct);
                                Write(((ISet)value).Comparer.FullName);
                            }
                            else
                            {
                                Write((byte)ListAspectType.None);
                            }

                            var serializer = CreateDelegate(value.GetType());
                            serializer(this, items, items.Count);
                        }
                        break;

                    case ObjectTypeCode.ObjectList:
                        {
                            var items = (IList)value;
                            WriteList(items);
                        }
                        break;

                    case ObjectTypeCode.TypeUnion:
                        var typeUnion = (ITypeUnion)value;
                        Write(typeUnion.AllTypes.FindIndex(t => t == typeUnion.UnionType));
                        WriteObject(typeUnion.GetObject(), Reflector.GetObjectTypeCode(typeUnion.UnionType));
                        break;

                    case ObjectTypeCode.ByteArray:
                        Write((byte[])value);
                        break;

                    case ObjectTypeCode.CharArray:
                        Write((char[])value);
                        break;

                    case ObjectTypeCode.TimeSpan:
                        Write((TimeSpan)value);
                        break;

                    case ObjectTypeCode.Guid:
                        Write(((Guid)value).ToByteArray());
                        break;

                    case ObjectTypeCode.DateTimeOffset:
                        Write((DateTimeOffset)value);
                        break;

                    case ObjectTypeCode.Version:
                        {
                            var version = (Version)value;
                            Write((uint)version.Major);
                            Write((uint)version.Minor);
                            Write((uint)version.Build);
                            Write((uint)version.Revision);
                            break;
                        }

                    case ObjectTypeCode.Uri:
                        Write(((Uri)value).AbsoluteUri);
                        break;

                    case ObjectTypeCode.ObjectMap:
                        var map = (IDictionary)value;
                        WriteDictionary(map);
                        break;

                    default:
                        new BinaryFormatter().Serialize(_stream, value);
                        break;
                }
            }
        }

        public static byte[] WriteObjectWithType(object value)
        {
            var writer = SerializationWriter.CreateWriter(SerializationMode.Compact);
            writer.WriteObject(value);
            var fullName = value.GetType().FullName;
            var tdata = Encoding.UTF8.GetBytes(fullName);
            var data = writer.GetBytes();
            var buffer = new byte[4 + tdata.Length + data.Length];
            Buffer.BlockCopy(BitConverter.GetBytes(tdata.Length), 0, buffer, 0, 4);
            Buffer.BlockCopy(tdata, 0, buffer, 4, tdata.Length);
            Buffer.BlockCopy(data, 0, buffer, 4 + tdata.Length, data.Length);
            return buffer;
        }

        public void AddToSerializationInfo(SerializationInfo info)
        {
            byte[] buffer = this.GetBytes();
            info.AddValue("X", buffer, typeof(byte[]));
        }
        
        private ObjectSerializer CreateDelegate(Type objectType)
        {
            //var reflectedType = Reflector.GetReflectedType(objectType);
            //return _serializers.GetOrAdd(reflectedType.InterfaceTypeName ?? reflectedType.FullTypeName, k => GenerateDelegate(k, objectType));
            if (!_serializeAll)
            {
                if (_includePropertyNames)
                {
                    return _serializers.GetOrAdd(objectType, t => GenerateDelegate(t));
                }
                else
                {
                    return _serializersNoHeader.GetOrAdd(objectType, t => GenerateDelegate(t));
                }
            }
            else
            {
                if (_includePropertyNames)
                {
                    return _serializersWithAllProperties.GetOrAdd(objectType, t => GenerateDelegate(t));
                }
                else
                {
                    return _serializersWithAllPropertiesNoHeader.GetOrAdd(objectType, t => GenerateDelegate(t));
                }
            }
        }

        private ObjectSerializer GenerateDelegate(Type objectType)
        {
            var method = new DynamicMethod("Serialize_" + objectType.Name, null, new[] { typeof(SerializationWriter), typeof(IList), typeof(int) }, typeof(SerializationWriter).Module);
            var il = method.GetILGenerator();

            var writeObjectType = this.GetType().GetMethod("WriteObjectType");            
            var writeObject = this.GetType().GetMethod("WriteObject", new[] { typeof(object), typeof(ObjectTypeCode) });
            var writeLength = this.GetType().GetMethod("Write", new[] { typeof(uint) });
            var writeName = this.GetType().GetMethod("Write", new[] { typeof(string) });
            var getItem = typeof(IList).GetMethod("get_Item");

            Type containerType = null;
            Type elementType = null;
            Type interfaceType = null;
            var isEmitted = false;

            if (Reflector.IsList(objectType))
            {
                containerType = objectType;
                elementType = Reflector.ExtractCollectionElementType(objectType);
                interfaceType = elementType;
                isEmitted = Reflector.IsEmitted(elementType);
            }
            else
            {
                interfaceType = objectType;
                isEmitted = Reflector.IsEmitted(objectType);
            }

            if (isEmitted && !interfaceType.IsInterface)
            {
                if (elementType != null)
                {
                    interfaceType = Reflector.ExtractInterface(elementType);
                }
                else
                {
                    interfaceType = Reflector.ExtractInterface(objectType);
                }

                if (interfaceType == null)
                {
                    if (elementType != null)
                    {
                        interfaceType = elementType;
                    }
                    else
                    {
                        interfaceType = objectType;
                    }
                }
            }

            var properties = Reflector.GetAllProperties(interfaceType).Where(p => p.CanRead && p.CanWrite && p.Name != "Indexer" && (_serializeAll || !p.GetCustomAttributes(typeof(DoNotSerializeAttribute), false).Any()));
            var propertyPositions = Reflector.GetAllPropertyPositions(interfaceType).OrderBy(p => p.Value).Select(p => p.Key);
            var orderedProperties = _includePropertyNames ? properties.ToArray() : properties.Arrange(propertyPositions, p => p.Name).ToArray();

            if (Reflector.IsBusinessObject(interfaceType) || Reflector.IsBusinessObjectList(interfaceType, out elementType))
            {
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldc_I4, interfaceType.FullName.GetHashCode());
                il.Emit(OpCodes.Callvirt, writeObjectType);
            }

            if (_includePropertyNames)
            {
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldc_I4, orderedProperties.Length);
                il.Emit(OpCodes.Conv_U4);
                il.Emit(OpCodes.Callvirt, writeLength);

                var orderedPropertiesWithLength = orderedProperties.Select(p => Tuple.Create(p, Encoding.UTF8.GetByteCount(p.Name))).ToList();
                var overhead = orderedPropertiesWithLength.Select(t => t.Item2 <= byte.MaxValue ? 1 : (t.Item2 <= ushort.MaxValue ? 2 : (t.Item2 <= uint.MaxValue ? 4 : 8))).Sum();

                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldc_I4, orderedPropertiesWithLength.Sum(p => p.Item2) + overhead);
                il.Emit(OpCodes.Conv_U4);
                il.Emit(OpCodes.Callvirt, writeLength);

                foreach (var property in properties)
                {
                    //Write property name
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldstr, property.Name);
                    il.Emit(OpCodes.Callvirt, writeName);
                }
            }

            var enterLoop = il.DefineLabel();
            var exitLoop = il.DefineLabel();
            var local = il.DeclareLocal(typeof(int));

            il.Emit(OpCodes.Ldc_I4_0);
            il.Emit(OpCodes.Stloc_0);
            il.Emit(OpCodes.Br, exitLoop);
            il.MarkLabel(enterLoop);

            foreach (var property in orderedProperties)
            {
                // Write property value
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldarg_1);
                il.Emit(OpCodes.Ldloc_0);
                il.Emit(OpCodes.Callvirt, getItem);
                il.EmitCastToReference(interfaceType);
                il.EmitCall(OpCodes.Callvirt, property.GetGetMethod(), null);
                il.BoxIfNeeded(property.PropertyType);
                il.Emit(OpCodes.Ldc_I4, (int)Reflector.GetObjectTypeCode(property.PropertyType));
                il.Emit(OpCodes.Callvirt, writeObject);
            }

            il.Emit(OpCodes.Ldloc_0);
            il.Emit(OpCodes.Ldc_I4_1);
            il.Emit(OpCodes.Add);
            il.Emit(OpCodes.Stloc_0);
            il.MarkLabel(exitLoop);
            il.Emit(OpCodes.Ldloc_0);
            il.Emit(OpCodes.Ldarg_2);
            il.Emit(OpCodes.Blt, enterLoop);

            il.Emit(OpCodes.Ret);

            var serializer = (ObjectSerializer)method.CreateDelegate(typeof(ObjectSerializer));
            return serializer;
        }

        #region IDisposable Members

        public void Dispose()
        {
            _stream.Dispose();
        }

        #endregion
    }

    public class SerializationReader : IDisposable
    {
        private int _objectTypeHash;
        private readonly SerializationMode _mode;
        private readonly bool _serializeAll;
        private readonly bool _includePropertyNames;
        private byte? _objectByte;
        private int _itemCount;
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
            _objectByte = ReadByte();
            if (_objectByte.Value == (byte)ObjectTypeCode.BusinessObject)
            {
                _objectTypeHash = ReadInt32();
            }
            else if (_objectByte.Value == (byte)ObjectTypeCode.BusinessObjectList)
            {
                _itemCount = (int)ReadUInt32();
                _objectTypeHash = ReadInt32();
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
            return ReadObject(objectType, _objectByte.HasValue ? (ObjectTypeCode)_objectByte.Value : Reflector.GetObjectTypeCode(objectType));
        }

        public object ReadObject(Type objectType, ObjectTypeCode expectedTypeCode)
        {
            var skip = false;
            ObjectTypeCode typeCode;
            if (_objectByte.HasValue)
            {
                typeCode = (ObjectTypeCode)_objectByte.Value;
                _objectByte = null;
                skip = true;
            }
            else
            {
                typeCode = (ObjectTypeCode)ReadByte();
            }

            switch (typeCode)
            {
                case ObjectTypeCode.Boolean:
                    return ReadBoolean();
                case ObjectTypeCode.Byte:
                    return ReadByte();
                case ObjectTypeCode.UInt16:
                    return ReadUInt16();
                case ObjectTypeCode.UInt32:
                    return ReadUInt32();
                case ObjectTypeCode.UInt64:
                    return ReadUInt64();
                case ObjectTypeCode.SByte:
                    return ReadSByte();
                case ObjectTypeCode.Int16:
                    return ReadInt16();
                case ObjectTypeCode.Int32:
                    return ReadInt32();
                case ObjectTypeCode.Int64:
                    return ReadInt64();
                case ObjectTypeCode.Char:
                    return ReadChar();
                case ObjectTypeCode.String:
                    return ReadString();
                case ObjectTypeCode.Single:
                    return ReadSingle();
                case ObjectTypeCode.Double:
                    return ReadDouble();
                case ObjectTypeCode.Decimal:
                    return ReadDecimal();
                case ObjectTypeCode.DateTime:
                    return ReadDateTime();
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
                        var activator = Reflection.Activator.CreateDelegate(objectType);
                        var objectList = (IList)activator();
                        ReadList(objectList, objectType.GetGenericArguments()[0]);
                        return objectList;
                    }
                case ObjectTypeCode.ObjectMap:
                    {
                        var activator = Reflection.Activator.CreateDelegate(objectType);
                        var objectMap = (IDictionary)activator();
                        var genericArgs = objectType.GetGenericArguments();
                        ReadDictionary(objectMap, genericArgs[0], genericArgs[1]);
                        return objectMap;
                    }
                case ObjectTypeCode.BusinessObject:
                    {
                        var deserializer = CreateDelegate(objectType, skip);
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
                        var itemCount = skip ? _itemCount : (int)ReadUInt32();

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
                        var deserializer = CreateDelegate(objectType, skip);
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

        private ObjectDeserializer CreateDelegate(Type objectType, bool skip)
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

            var readObject = this.GetType().GetMethod("ReadObject", new[] { typeof(Type), typeof(ObjectTypeCode) });
            var getTypeFromHandle = typeof(Type).GetMethod("GetTypeFromHandle");

            var interfaceType = objectType;
            if (Reflector.IsEmitted(objectType) && !interfaceType.IsInterface)
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
