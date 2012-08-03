using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection.Emit;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using Nemo.Attributes;
using Nemo.Collections;
using Nemo.Collections.Extensions;
using Nemo.Fn;
using Nemo.Reflection;

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

    /// <summary> SerializationWriter.  Extends BinaryWriter to add additional data types,
    /// handle null strings and simplify use with ISerializable. </summary>
    public class SerializationWriter : BinaryWriter
    {
        public delegate void ObjectSerializer(SerializationWriter writer, IList values, int count);

        private static ConcurrentDictionary<Type, ObjectSerializer> _serializers = new ConcurrentDictionary<Type, ObjectSerializer>();
        private static ConcurrentDictionary<Type, ObjectSerializer> _serializersWithAllProperties = new ConcurrentDictionary<Type, ObjectSerializer>();

        private bool _serializeAll;

        private SerializationWriter(Stream s, bool serializeAll)
            : base(s)
        {
            _serializeAll = serializeAll;
            Write(_serializeAll);
        }

        /// <summary> Static method to initialise the writer with a suitable MemoryStream. </summary>
        public static SerializationWriter CreateWriter(bool serializeAll)
        {
            Stream ms = new MemoryStream(1024);
            return new SerializationWriter(ms, serializeAll);
        }

        public byte[] GetBytes()
        {
            var ms = (MemoryStream)this.BaseStream;
            var data = ms.ToArray();
            return data;
        }

        /// <summary> Writes a string to the buffer.  Overrides the base implementation so it can cope with nulls </summary>
        public override void Write(string value)
        {
            if (value == null)
            {
                Write((byte)ObjectTypeCode.Empty);
            }
            else
            {
                base.Write(value);
            }
        }

        /// <summary> Writes a byte array to the buffer.  Overrides the base implementation to
        /// send the length of the array which is needed when it is retrieved </summary>
        public override void Write(byte[] value)
        {
            if (value == null)
            {
                Write(-1);
            }
            else
            {
                int length = value.Length;
                Write(length);
                if (length > 0)
                {
                    base.Write(value);
                }
            }
        }

        /// <summary> Writes a char array to the buffer.  Overrides the base implementation to
        /// sends the length of the array which is needed when it is read. </summary>
        public override void Write(char[] value)
        {
            if (value == null)
            {
                Write(-1);
            }
            else
            {
                int length = value.Length;
                Write(length);
                if (length > 0)
                {
                    base.Write(value);
                }
            }
        }

        /// <summary> Writes a DateTime to the buffer. <summary>
        public void Write(DateTime value)
        {
            Write(value.Ticks);
        }

        /// <summary> Writes a TimeSpan to the buffer. <summary>
        public void Write(TimeSpan value)
        {
            Write(value.Ticks);
        }

        /// <summary> Writes a DateTimeOffset to the buffer. <summary>
        public void Write(DateTimeOffset value)
        {
            Write(value.DateTime.Ticks);
            Write(value.Offset.Ticks);
        }

        /// <summary> Writes a generic ICollection (such as an IList<T>) to the buffer. </summary>
        public void WriteList(IList items)
        {
            if (items == null)
            {
                Write(-1);
            }
            else
            {
                Write(items.Count);
                for (int i = 0; i < items.Count; i++)
                {
                    WriteObject(items[i]);
                }
            }
        }

        /// <summary> Writes a generic ICollection (such as an IList<T>) to the buffer. </summary>
        public void WriteList<T>(IList<T> items)
        {
            WriteList((IList)items);
        }

        /// <summary> Writes a generic IDictionary to the buffer. </summary>
        public void WriteDictionary<T, U>(IDictionary<T, U> map)
        {
            if (map == null)
            {
                Write(-1);
            }
            else
            {
                Write(map.Count);
                foreach (var pair in map)
                {
                    WriteObject(pair.Key);
                    WriteObject(pair.Value);
                }
            }
        }

        /// <summary> Writes a generic IDictionary to the buffer. </summary>
        public void WriteDictionary(IDictionary map)
        {
            if (map == null)
            {
                Write(-1);
            }
            else
            {
                Write(map.Count);
                var iterator = map.GetEnumerator();
                while(iterator.MoveNext())
                {
                    WriteObject(iterator.Key);
                    WriteObject(iterator.Value);
                }
            }
        }

        /// <summary> Writes an arbitrary object to the buffer.  Useful where we have something of type "object"
        /// and don't know how to treat it.  This works out the best method to use to write to the buffer. </summary>
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

                    case ObjectTypeCode.UInt16:
                        Write((ushort)value);
                        break;

                    case ObjectTypeCode.UInt32:
                        Write((uint)value);
                        break;

                    case ObjectTypeCode.UInt64:
                        Write((ulong)value);
                        break;

                    case ObjectTypeCode.SByte:
                        Write((sbyte)value);
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
                        base.Write((char)value);
                        break;

                    case ObjectTypeCode.String:
                        base.Write((string)value);
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

                            Write(items.Count);
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
                                Write(((ISortedList)value).Comparer.AssemblyQualifiedName);
                            }
                            else if (value is ISet)
                            {
                                Write((byte)ListAspectType.Distinct);
                                Write(((ISet)value).Comparer.AssemblyQualifiedName);
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
                            Write(value.GetType().AssemblyQualifiedName);
                            WriteList(items);
                        }
                        break;
                    
                    case ObjectTypeCode.TypeUnion:
                        var typeUnion = (ITypeUnion)value;
                        WriteList<string>(typeUnion.AllTypes.Select(t => t.AssemblyQualifiedName).ToList());
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
                        Write(((Version)value).Major);
                        Write(((Version)value).Minor);
                        Write(((Version)value).Build);
                        Write(((Version)value).Revision);
                        break;
                    
                    case ObjectTypeCode.ObjectMap:
                        var map = (IDictionary)value;
                        Write(value.GetType().AssemblyQualifiedName);
                        WriteDictionary(map);
                        break;

                    default:
                        new BinaryFormatter().Serialize(BaseStream, value);
                        break;
                }
            }
        }

        /// <summary> Adds the SerializationWriter buffer to the SerializationInfo at the end of GetObjectData(). </summary>
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
                return _serializers.GetOrAdd(objectType, t => GenerateDelegate(t, false));
            }
            else
            {
                return _serializersWithAllProperties.GetOrAdd(objectType, t => GenerateDelegate(t, true));
            }
        }

        private ObjectSerializer GenerateDelegate(Type objectType, bool serializeAll)
        {
            var method = new DynamicMethod("Serialize_" + objectType.Name, null, new[] { typeof(SerializationWriter), typeof(IList), typeof(int) }, typeof(SerializationWriter).Module);
            var il = method.GetILGenerator();

            var writeObject = this.GetType().GetMethod("WriteObject", new[] { typeof(object), typeof(ObjectTypeCode) });
            var writeFlag = this.GetType().GetMethod("Write", new[] { typeof(bool) });
            var writeLength = this.GetType().GetMethod("Write", new[] { typeof(int) });
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

            var properties = Reflector.GetAllProperties(interfaceType).Where(p => p.CanRead && p.CanWrite && p.Name != "Indexer" && (serializeAll || !p.GetCustomAttributes(typeof(DoNotSerializeAttribute), false).Any())).ToArray();

            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldstr, interfaceType.FullName);
            il.Emit(OpCodes.Callvirt, writeName);
                        
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldc_I4, properties.Length);
            il.Emit(OpCodes.Callvirt, writeLength);

            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldc_I4, properties.Sum(p => p.Name.Length));
            il.Emit(OpCodes.Callvirt, writeLength);
            
            foreach (var property in properties)
            {
                //Write property name
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldstr, property.Name);
                il.Emit(OpCodes.Callvirt, writeName);
            }

            var enterLoop = il.DefineLabel();
            var exitLoop = il.DefineLabel();
            var local = il.DeclareLocal(typeof(int));

            il.Emit(OpCodes.Ldc_I4_0);
            il.Emit(OpCodes.Stloc_0);
            il.Emit(OpCodes.Br, exitLoop);
            il.MarkLabel(enterLoop);

            foreach (var property in properties)
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
    }

    /// <summary> SerializationReader.  Extends BinaryReader to add additional data types,
    /// handle null strings and simplify use with ISerializable. </summary>
    public class SerializationReader : BinaryReader
    {
        private string _objectTypeName;
        private bool _serializeAll;
        private byte? _objectByte;
        private int _itemCount;
        private static ConcurrentDictionary<string, Type> _types = new ConcurrentDictionary<string, Type>();
        
        public delegate object[] ObjectDeserializer(SerializationReader reader, int count);
        private static ConcurrentDictionary<Type, ObjectDeserializer> _deserializers = new ConcurrentDictionary<Type, ObjectDeserializer>();
        private static ConcurrentDictionary<Type, ObjectDeserializer> _deserializersWithAllProperties = new ConcurrentDictionary<Type, ObjectDeserializer>();

        private SerializationReader(Stream s)
            : base(s)
        {
            _serializeAll = ReadBoolean();
            _objectByte = ReadByte();
            if (_objectByte.Value == (byte)ObjectTypeCode.BusinessObject)
            {
                _objectTypeName = ReadString();
            }
            else if (_objectByte.Value == (byte)ObjectTypeCode.BusinessObjectList)
            {
                _itemCount = ReadInt32();
                _objectTypeName = ReadString();
            }
        }

        /// <summary> Static method to take a SerializationInfo object (an input to an ISerializable constructor)
        /// and produce a SerializationReader from which serialized objects can be read </summary>.
        public static SerializationReader CreateReader(SerializationInfo info)
        {
            byte[] buffer = (byte[])info.GetValue("X", typeof(byte[]));
            return SerializationReader.CreateReader(buffer);
        }

        /// <summary> Static method to take a SerializationInfo object (an input to an ISerializable constructor)
        /// and produce a SerializationReader from which serialized objects can be read </summary>.
        public static SerializationReader CreateReader(byte[] buffer)
        {
            return new SerializationReader(new MemoryStream(buffer));
        }

        /// <summary> Static method to determine the object type </summary>.
        public static string GetObjectType(byte[] buffer)
        {
            return CreateReader(buffer).ObjectTypeName;
        }

        public string ObjectTypeName
        {
            get
            {
                return _objectTypeName;
            }
        }

        /// <summary> Reads a byte array from the buffer, handling nulls and the array length. </summary>
        public byte[] ReadByteArray()
        {
            int length = ReadInt32();
            if (length > 0)
            {
                return ReadBytes(length);
            }
            if (length < 0)
            {
                return null;
            }
            return new byte[0];
        }

        /// <summary> Reads a char array from the buffer, handling nulls and the array length. </summary>
        public char[] ReadCharArray()
        {
            int length = ReadInt32();
            if (length > 0)
            {
                return ReadChars(length);
            }
            if (length < 0)
            {
                return null;
            }
            return new char[0];
        }

        /// <summary> Reads a DateTime from the buffer. </summary>
        public DateTime ReadDateTime()
        {
            return new DateTime(ReadInt64());
        }

        /// <summary> Reads a TimeSpan from the buffer. </summary>
        public TimeSpan ReadTimeSpan()
        {
            return new TimeSpan(ReadInt64());
        }

        /// <summary> Writes a DateTimeOffset to the buffer. <summary>
        public DateTimeOffset ReadDateTimeOffset()
        {
            return new DateTimeOffset(ReadInt64(), new TimeSpan(ReadInt64()));
        }

        /// <summary> Reads a Guid from the buffer. </summary>
        public Guid ReadGuid()
        {
            return new Guid(ReadByteArray());
        }

        /// <summary> Reads a Guid from the buffer. </summary>
        public Version ReadVersion()
        {
            var major = ReadInt32();
            var minor = ReadInt32();
            var build = ReadInt32();
            var revision = ReadInt32();
            return new Version(major, minor, build, revision);
        }

        /// <summary> Reads a generic list from the buffer. </summary>
        public IList<T> ReadList<T>()
        {
            int count = ReadInt32();
            if (count < 0)
            {
                return null;
            }
            IList<T> list = new List<T>();
            for (int i = 0; i < count; i++)
            {
                list.Add((T)ReadObject());
            }
            return list;
        }

        public void ReadList(IList list)
        {
            int count = ReadInt32();
            if (count < 0)
            {
                return;
            }
            for (int i = 0; i < count; i++)
            {
                list.Add(ReadObject());
            }
        }

        /// <summary> Reads a generic Dictionary from the buffer. </summary>
        public IDictionary<T, U> ReadDictionary<T, U>()
        {
            int count = ReadInt32();
            if (count < 0)
            {
                return null;
            }
            IDictionary<T, U> map = new Dictionary<T, U>();
            for (int i = 0; i < count; i++)
            {
                map.Add((T)ReadObject(), (U)ReadObject());
            }
            return map;
        }

        public void ReadDictionary(IDictionary map)
        {
            int count = ReadInt32();
            if (count < 0)
            {
                return;
            }
            for (int i = 0; i < count; i++)
            {
                map.Add(ReadObject(), ReadObject());
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
            base.Read(buffer, 0, count);
        }

        /// <summary> Reads an object which was added to the buffer by WriteObject. </summary>
        public object ReadObject()
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
                    return ReadByteArray();
                case ObjectTypeCode.CharArray:
                    return ReadCharArray();
                case ObjectTypeCode.TimeSpan:
                    return ReadTimeSpan();
                case ObjectTypeCode.DateTimeOffset:
                    return ReadDateTimeOffset();
                case ObjectTypeCode.Guid:
                    return ReadGuid();
                case ObjectTypeCode.Version:
                    return ReadVersion();
                case ObjectTypeCode.DBNull:
                    return DBNull.Value;
                case ObjectTypeCode.Object:
                    return new BinaryFormatter().Deserialize(BaseStream);
                case ObjectTypeCode.ObjectList:
                    {
                        var simpleCollectionType = GetType(ReadString());
                        var activator = Reflection.Activator.CreateDelegate(simpleCollectionType);
                        var objectList = (IList)activator();
                        ReadList(objectList);
                        return objectList;
                    }
                case ObjectTypeCode.ObjectMap:
                    {
                        var dictionaryType = GetType(ReadString());
                        var activator = Reflection.Activator.CreateDelegate(dictionaryType);
                        var objectMap = (IDictionary)activator();
                        ReadDictionary(objectMap);
                        return objectMap;
                    }
                case ObjectTypeCode.BusinessObject:
                    {
                        var businessObjectType = GetType(skip ? _objectTypeName : ReadString());
                        var deserializer = CreateDelegate(businessObjectType);
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
                        var itemCount = skip ? _itemCount : ReadInt32();
                        
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

                        var businessObjectType = GetType(skip ? _objectTypeName : ReadString());
                        var deserializer = CreateDelegate(businessObjectType);
                        var businessObjects = deserializer(this, itemCount);
                        var list = List.Create(businessObjectType, distinctAttribute, sortedAttribute);
                        for (int i = 0; i < businessObjects.Length; i++ )
                        {
                            list.Add(businessObjects[i]);
                        }
                        return list;
                    }
                case ObjectTypeCode.TypeUnion:
                    var unionTypes = ReadList<string>().Select(t => GetType(t)).ToArray();
                    var unionValue = ReadObject();
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

            byte[] buffer = new byte[initialLength];
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
                    byte[] newBuffer = new byte[buffer.Length * 2];
                    Array.Copy(buffer, newBuffer, buffer.Length);
                    newBuffer[read] = (byte)nextByte;
                    buffer = newBuffer;
                    read++;
                }
            }
            // Buffer is now too big. Shrink it.
            byte[] ret = new byte[read];
            Array.Copy(buffer, ret, read);
            return ret;
        }

        public static byte[] ReadStream(Stream stream)
        {
            return ReadStream(stream, -1);
        }

        private ObjectDeserializer CreateDelegate(Type objectType)
        {
            var exists = true;
            var propertyCount = ReadInt32();
            var propertyLength = ReadInt32();
            ObjectDeserializer deserializer;
            if (!_serializeAll)
            {
                deserializer = _deserializers.GetOrAdd(objectType, t => CreateDeserializer(t, propertyCount, ref exists));
            }
            else
            {
                deserializer = _deserializersWithAllProperties.GetOrAdd(objectType, t => CreateDeserializer(t, propertyCount, ref exists));
            }
            if (exists)
            {
                Skip(propertyCount + propertyLength);
            }
            return deserializer;
        }

        private ObjectDeserializer CreateDeserializer(Type objectType, int propertyCount, ref bool exists)
        {
            var businessObject = ObjectFactory.Create(objectType);
            var propertyNames = new List<string>();
            for (int i = 0; i < propertyCount; i++)
            {
                propertyNames.Add(this.ReadString());
            }
            exists = false;
            return GenerateDelegate(businessObject.GetType(), propertyNames);
        }

        private ObjectDeserializer GenerateDelegate(Type objectType, List<string> propertyNames)
        {
            var method = new DynamicMethod("Deserialize_" + objectType.Name, typeof(object[]), new[] { typeof(SerializationReader), typeof(int) }, typeof(SerializationReader).Module);
            var il = method.GetILGenerator();

            var readObject = this.GetType().GetMethod("ReadObject");

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
            var orderedProperties = properties.Where(p => propertyNames.Contains(p.Name)).Arrange(propertyNames, p => p.Name).ToArray();

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
    }
}
