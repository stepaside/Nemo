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
using Nemo.Utilities;

/*
 * 1. DateTime/TimeSpan serialization are taken from protobuf-net
 * 2. Integer serialiation/deserialization are taken from NetSerializer
 * 
 */

namespace Nemo.Serialization
{
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
                    _encoding.GetBytes(chars, 0, chars.Length, buffer, 0);
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

        public void WriteListAspectType(IList value)
        {
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

                    case ObjectTypeCode.DataEntity:
                        {
                            var serializer = CreateDelegate(value.GetType());
                            serializer(this, new object[] { value }, 1);
                        }
                        break;

                    case ObjectTypeCode.DataEntityList:
                        {
                            var items = (IList)value;
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
            var writeListAspectType = this.GetType().GetMethod("WriteListAspectType");                        
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

            if (isEmitted)
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
            var objectTypeCode = Reflector.GetObjectTypeCode(objectType);

            if (objectTypeCode == ObjectTypeCode.DataEntity || objectTypeCode == ObjectTypeCode.DataEntityList)
            {
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldc_I4, interfaceType.FullName.GetHashCode());
                il.Emit(OpCodes.Callvirt, writeObjectType);

                if (objectTypeCode == ObjectTypeCode.DataEntityList)
                {
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldarg_2);
                    il.Emit(OpCodes.Conv_U4);
                    il.Emit(OpCodes.Callvirt, writeLength);

                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldarg_1);
                    il.Emit(OpCodes.Callvirt, writeListAspectType);
                }
            }

            if (_includePropertyNames)
            {
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldc_I4, orderedProperties.Length);
                il.Emit(OpCodes.Conv_U4);
                il.Emit(OpCodes.Callvirt, writeLength);

                var orderedPropertiesWithLength = orderedProperties.Select(p => Tuple.Create(p, Encoding.UTF8.GetByteCount(p.Name))).ToList();
                var overhead = orderedPropertiesWithLength.Select(t => t.Item2 <= byte.MaxValue ? 1 : (t.Item2 <= ushort.MaxValue ? 2 : ((uint)t.Item2 <= uint.MaxValue ? 4 : 8))).Sum();

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
}
