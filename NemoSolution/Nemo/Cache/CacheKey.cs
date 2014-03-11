using Nemo.Collections.Extensions;
using Nemo.Configuration;
using Nemo.Extensions;
using Nemo.Reflection;
using Nemo.Security.Cryptography;
using Nemo.Utilities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace Nemo.Cache
{
    public class CacheKey
    {
        private HashAlgorithmName _hashAlgorithm = ConfigurationFactory.Configuration.DefaultHashAlgorithm;
        
        private string _value;
        private byte[] _data;
        private Tuple<string, byte[]> _hash;

        public CacheKey(IDataEntity entity)
            : this(entity.GetPrimaryKey(true), entity.GetType())
        { }

        public CacheKey(IDictionary<string, object> key, Type type, string operation = null, OperationReturnType returnType = OperationReturnType.Guess)
        {
            var reflectedType = Reflector.GetReflectedType(type);
            var typeName = reflectedType.IsDataEntity && reflectedType.InterfaceTypeName != null ? reflectedType.InterfaceTypeName : reflectedType.FullTypeName;
            var sorted = key is SortedDictionary<string, object>;

            _hashAlgorithm = ConfigurationFactory.Configuration.DefaultHashAlgorithm;
            if (_hashAlgorithm == HashAlgorithmName.Native || _hashAlgorithm == HashAlgorithmName.None)
            {
                IEnumerable<KeyValuePair<string, object>> values;
                if (sorted)
                {
                    values = key;
                }
                else
                {
                    values = key.OrderBy(k => k.Key);
                }

                var keyValue = values.Select(k => string.Format("{0}:={1}", k.Key.ToUpper(), k.Value)).ToDelimitedString(",");
                if (!string.IsNullOrEmpty(operation))
                {
                    _value = string.Format("{0}->{1}:{2}::{3}", typeName, operation, returnType, keyValue);
                }
                else
                {
                    _value = string.Format("{0}::{2}", typeName, keyValue);
                }
                _data = _value.ToByteArray();
            }
            else
            {
                Func<KeyValuePair<string, object>, IEnumerable<byte>> func = k => BitConverter.GetBytes(k.Key.ToUpper().GetHashCode()).Append((byte)':').Concat(BitConverter.GetBytes(k.Value.GetHashCode())).Append((byte)',');
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
                    Buffer.BlockCopy(BitConverter.GetBytes(typeName.GetHashCode()), 0, _data, 0, 4);
                    Buffer.BlockCopy(keyValue, 0, _data, 4, keyValue.Length);
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
                    case HashAlgorithmName.SHA256:
                        data = this.ComputeHash(SHA256.Create());
                        break;
                    case HashAlgorithmName.HMAC_SHA1:
                        data = this.ComputeHash(new HMACSHA1(ConfigurationFactory.Configuration.SecretKey.ToByteArray()));
                        break;
                    case HashAlgorithmName.HMAC_SHA256:
                        data = this.ComputeHash(new HMACSHA256(ConfigurationFactory.Configuration.SecretKey.ToByteArray()));
                        break;
                    case HashAlgorithmName.Default:
                    case HashAlgorithmName.JenkinsHash:
                        {
                            var h = Jenkins96Hash.Compute(_data);
                            data = BitConverter.GetBytes(h);
                            break;
                        }
                    case HashAlgorithmName.SBox:
                        {
                            var h = SBoxHash.Compute(_data);
                            data = BitConverter.GetBytes(h);
                            break;
                        }
                    case HashAlgorithmName.SuperFastHash:
                        {
                            var h = SuperFastHash.Compute(_data);
                            data = BitConverter.GetBytes(h);
                            break;
                        }
                    case HashAlgorithmName.MurmurHash:
                        {
                            var h = MurmurHash2.Compute(_data);
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
    }
}
