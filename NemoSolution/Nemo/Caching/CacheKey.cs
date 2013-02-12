using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Nemo.Collections.Extensions;
using Nemo.Extensions;
using Nemo.Reflection;
using Nemo.Utilities;

namespace Nemo.Caching
{
    public class CacheKey
    {
        private HashAlgorithmName _hashAlgorithm = ObjectFactory.Configuration.DefaultHashAlgorithm;

        public CacheKey() { }

        public CacheKey(HashAlgorithmName hashAlgorithm, string value)
        {
            _hashAlgorithm = hashAlgorithm == HashAlgorithmName.Default ? ObjectFactory.Configuration.DefaultHashAlgorithm : hashAlgorithm;
            _value = value;
        }

        public CacheKey(HashAlgorithmName hashAlgorithm, byte[] data)
        {
            _hashAlgorithm = hashAlgorithm == HashAlgorithmName.Default ? ObjectFactory.Configuration.DefaultHashAlgorithm : hashAlgorithm;
            _data = data;
        }

        private string _value;
        private byte[] _data;
        private Tuple<string, byte[]> _hash;

        public CacheKey(IBusinessObject businessObject, string operation, OperationReturnType returnType, HashAlgorithmName hashAlgorithm = HashAlgorithmName.Default)
            : this(businessObject.GetPrimaryKey(true), businessObject.GetType(), operation, returnType, true, hashAlgorithm)
        { }

        public CacheKey(IBusinessObject businessObject, HashAlgorithmName hashAlgorithm = HashAlgorithmName.Default)
            : this(businessObject.GetPrimaryKey(true), businessObject.GetType(), hashAlgorithm)
        { }

        public CacheKey(IDictionary<string, object> key, HashAlgorithmName hashAlgorithm = HashAlgorithmName.Default)
            : this(key, typeof(IBusinessObject), null, OperationReturnType.Guess,  hashAlgorithm)
        { }

        public CacheKey(IDictionary<string, object> key, Type type, HashAlgorithmName hashAlgorithm = HashAlgorithmName.Default)
            : this(key, type, null, OperationReturnType.Guess, hashAlgorithm)
        { }

        internal CacheKey(IEnumerable<Param> parameters, Type type, string operation, OperationReturnType returnType, HashAlgorithmName hashAlgorithm = HashAlgorithmName.Default)
            : this(new SortedDictionary<string, object>(parameters.ToDictionary(p => p.Name, p => p.Value)), type, operation, returnType, hashAlgorithm)
        { }

        internal CacheKey(IEnumerable<Param> parameters, Type type, HashAlgorithmName hashAlgorithm = HashAlgorithmName.Default)
            : this(parameters, type, null, OperationReturnType.Guess, hashAlgorithm)
        { }

        internal CacheKey(IDictionary<string, object> key, Type type, string operation, OperationReturnType returnType, HashAlgorithmName hashAlgorithm = HashAlgorithmName.Default)
            : this(key, type, operation, returnType, key is SortedDictionary<string, object>, hashAlgorithm)
        { }

        protected CacheKey(IDictionary<string, object> key, Type type, string operation, OperationReturnType returnType, bool sorted, HashAlgorithmName hashAlgorithm = HashAlgorithmName.Default)
        {
            var reflectedType = Reflector.GetReflectedType(type);
            var typeName = reflectedType.IsBusinessObject && reflectedType.InterfaceTypeName != null ? reflectedType.InterfaceTypeName : reflectedType.FullTypeName;
            
            _hashAlgorithm = hashAlgorithm == HashAlgorithmName.Default ? ObjectFactory.Configuration.DefaultHashAlgorithm : hashAlgorithm;
            if (_hashAlgorithm == HashAlgorithmName.Native || _hashAlgorithm == HashAlgorithmName.None)
            {
                var keyValue = (sorted ? key.Select(k => string.Format("{0}={1}", k.Key, Uri.EscapeDataString(Convert.ToString(k.Value)))) : key.OrderBy(k => k.Key).Select(k => string.Format("{0}={1}", k.Key, Uri.EscapeDataString(Convert.ToString(k.Value))))).ToDelimitedString("&");
                if (!string.IsNullOrEmpty(operation))
                {
                    _value = string.Concat(typeName, "->", operation, "[", returnType, "]", "::", keyValue);
                }
                else
                {
                    _value = string.Concat(typeName, "::", keyValue);
                }
                _data = _value.ToByteArray();
            }
            else
            {
                Func<KeyValuePair<string, object>, IEnumerable<byte>> func = k => BitConverter.GetBytes(k.Key.GetHashCode()).Append((byte)'=').Concat(BitConverter.GetBytes(k.Value.GetHashCode())).Append((byte)'&');
                var keyValue = (sorted ? key.Select(func) : key.OrderBy(k => k.Key).Select(func)).Flatten().ToArray();
                if (!string.IsNullOrEmpty(operation))
                {
                    _data = new byte[4 + 4 + 1 + keyValue.Length];
                    Buffer.BlockCopy(BitConverter.GetBytes(typeName.GetHashCode()), 0, _data, 0, 4);
                    Buffer.BlockCopy(BitConverter.GetBytes(operation.GetHashCode()), 0, _data, 4, 4);
                    Buffer.BlockCopy(new[] { (byte)returnType }, 0, _data, 8, 1);
                    Buffer.BlockCopy(keyValue, 0, _data, 9, keyValue.Length);
                }
                else
                {
                    _data = new byte[4 + keyValue.Length];
                    Buffer.BlockCopy(BitConverter.GetBytes(typeName.GetHashCode()), 0, _data, 0 , 4);
                    Buffer.BlockCopy(keyValue, 0, _data, 4 , keyValue.Length);
                }
            }
        }

        public override bool Equals(object obj)
        {
            if (obj != null && obj is CacheKey)
            {
                return this.GetHashCode() == ((CacheKey)obj).GetHashCode();
            }
            return false;
        }

        public override int GetHashCode()
        {
            return _value != null ? _value.GetHashCode() : (_hash != null ? _hash.Item1.GetHashCode() : base.GetHashCode());
        }

        public override string ToString()
        {
            return _value ?? Value;
        }

        public string Value
        {
            get
            {
                return this.Compute().Item1;
            }
        }

        public HashAlgorithmName Algorithm
        {
            get
            {
                return _hashAlgorithm;
            }
        }

        private byte[] ComputeHash(HashAlgorithm algorithm)
        {
            return algorithm.ComputeHash(_data);
        }
        
        public Tuple<string, byte[]> Compute(int maxSize = 250)
        {
            if (_hash == null)
            {
                byte[] data = null;
                string value = null;
                switch (Algorithm)
                {
                    case HashAlgorithmName.MD5:
                        data = this.ComputeHash(MD5.Create());
                        break;
                    case HashAlgorithmName.SHA1:
                        data = this.ComputeHash(SHA1.Create());
                        break;
                    case HashAlgorithmName.SHA2:
                        data = this.ComputeHash(SHA256.Create());
                        break;
                    case HashAlgorithmName.HMAC_SHA1:
                        data = this.ComputeHash(new HMACSHA1(ObjectFactory.Configuration.SecretKey.ToByteArray()));
                        break;
                    case HashAlgorithmName.Default:
                    case HashAlgorithmName.JenkinsHash:
                        {
                            var h = Hash.Jenkins.Compute(_data);
                            data = BitConverter.GetBytes(h);
                            break;
                        }
                    case HashAlgorithmName.SBox:
                        {
                            var h = Hash.SBox.Compute(_data);
                            data = BitConverter.GetBytes(h);
                            break;
                        }
                    case HashAlgorithmName.SuperFastHash:
                        {
                            var h = Hash.SuperFastHash.Compute(_data);
                            data = BitConverter.GetBytes(h);
                            break;
                        }
                    case HashAlgorithmName.MurmurHash:
                        {
                            var h = Hash.MurmurHash2.Compute(_data);
                            data = BitConverter.GetBytes(h);
                            break;
                        }
                    case HashAlgorithmName.None:
                        value = maxSize >= _value.Length ? _value : _value.Substring(0, maxSize);
                        data = Encoding.UTF8.GetBytes(value);
                        break;
                    case HashAlgorithmName.Native:
                        {
                            var h = this.GetHashCode();
                            data = BitConverter.GetBytes(h);
                            break;
                        }
                }
                _hash = Tuple.Create(value ?? Bytes.ToHex(data), data);
            }
            return _hash;
        }

        private static string GetTypeName(Type type)
        {
            if (!type.IsInterface)
            {
                return Reflector.ExtractInterface(type).FullName;
            }
            else
            {
                return type.FullName;
            }
        }
    }
}
