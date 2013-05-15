using Nemo.Extensions;
using Nemo.Reflection;
using Nemo.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Nemo.Caching
{
    public class CacheItem : IEquatable<CacheItem>
    {
        private byte[] _data;
        private string _key;
        private IBusinessObject _dataObject;
        private string[] _index;

        internal CacheItem(IBusinessObject dataObject)
        {
            _dataObject = dataObject;
        }

        internal CacheItem(string key, IBusinessObject dataObject)
            : this(dataObject)
        {
            _key = key;
        }

        internal CacheItem(string key, string[] index)
        {
            _key = key;
            _index = index;
        }

        internal CacheItem(byte[] data)
        {
            _data = data;
        }

        internal CacheItem(string key, byte[] data)
            : this(data)
        {
            _key = key;
        }

        public byte[] Data
        {
            get
            {
                if (_data == null)
                {
                    byte[] data = null;
               
                    if (_dataObject != null)
                    {
                        data = _dataObject.Serialize();
                    }
                    else if (_index != null)
                    {
                        using (var writer = SerializationWriter.CreateWriter(SerializationMode.Manual))
                        {
                            writer.WriteList<string>(_index);
                            data = writer.GetBytes();
                        }
                    }

                    if (data != null)
                    {
                        _data = new byte[1 + data.Length];
                        _data[0] = (byte)(_dataObject != null ? 1 : 0);
                        Buffer.BlockCopy(data, 0, _data, 1, data.Length);
                    }
                }
                return _data;
            }
        }

        public string Key
        {
            get
            {
                if (_key == null && _dataObject != null)
                {
                    _key = _dataObject.ComputeHash();
                }
                return _key;
            }
        }

        public string[] ToIndex()
        {
            if (_index == null && _data != null && _data[0] == 0)
            {
                var data = new byte[_data.Length - 1];
                Buffer.BlockCopy(_data, 1, data, 0, data.Length);
                using (var reader = SerializationReader.CreateReader(data))
                {
                    _index = reader.ReadList<string>().ToArray();
                }
            }

            if (_index != null)
            {
                return _index;
            }
            return null;
        }

        public string ComputeKey<T>()
            where T : class, IBusinessObject
        {
            if (string.IsNullOrEmpty(_key))
            {
                var dataObject = this.ToObject<T>();
                if (dataObject != null)
                {
                    _key = dataObject.ComputeHash();
                }
            }
            return _key;
        }

        public T ToObject<T>()
            where T : class, IBusinessObject
        {
            if (_dataObject == null && _data != null && _data[0] == 1)
            {
                var data = new byte[_data.Length - 1];
                Buffer.BlockCopy(_data, 1, data, 0, data.Length);
                _dataObject = data.Deserialize<T>();
            }

            if (_dataObject != null)
            {
                return (T)_dataObject;
            }
            return default(T);
        }
                
        public bool IsValid<T>()
            where T : class, IBusinessObject
        {
            var isValid = true;
            if (_dataObject == null && _data != null)
            {
                isValid = ObjectSerializer.CheckType<T>(_data);
            }
            else if (_dataObject != null)
            {
                isValid = _dataObject is T;
            }
            return isValid;
        }

        #region IEquatable<CacheItem> Members

        public bool Equals(CacheItem other)
        {
            return object.Equals(this, other);
        }

        #endregion
    }
}
