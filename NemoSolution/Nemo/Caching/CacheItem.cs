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
    public class CacheItem
    {
        private Lazy<CacheValue> _lazy;
        private string _key;
        private IBusinessObject _dataObject;
        private string[] _index;

        internal CacheItem(string key, IBusinessObject dataObject)
        {
            _key = key;
            _dataObject = dataObject;
            _lazy = new Lazy<CacheValue>(() => CacheValueFactory());
        }

        internal CacheItem(string key, string[] index)
        {
            _key = key;
            _index = index;
            _lazy = new Lazy<CacheValue>(() => CacheValueFactory());
        }

        internal CacheItem(string key, CacheValue data)
        {
            _key = key;
            _lazy = new Lazy<CacheValue>(() => data);
        }

        internal CacheItem(string key, byte[] data)
            : this(key, CacheValue.FromBytes(data))
        { }

        public CacheValue Value
        {
            get
            {
                return _lazy.Value;
            }
        }

        public byte[] Data
        {
            get
            {
                return _lazy.Value.Buffer;
            }
        }

        private CacheValue CacheValueFactory()
        {
            var cacheValue = new CacheValue();

            if (_dataObject != null)
            {
                cacheValue.Buffer = _dataObject.Serialize();
            }
            else if (_index != null)
            {
                using (var writer = SerializationWriter.CreateWriter(SerializationMode.Manual))
                {
                    writer.WriteList<string>(_index);
                    cacheValue.Buffer = writer.GetBytes();
                    cacheValue.QueryKey = true;
                }
            }
            else
            {
                cacheValue = null;
            }

            return cacheValue;
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
            if (_index == null && Value != null && Value.QueryKey && Value.Buffer != null)
            {
                using (var reader = SerializationReader.CreateReader(Value.Buffer))
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
            if (_dataObject == null && Value != null && !Value.QueryKey && Value.Buffer != null)
            {
                _dataObject = Value.Buffer.Deserialize<T>();
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
            if (_dataObject == null && Value != null && !Value.QueryKey && Value.Buffer != null)
            {
                isValid = ObjectSerializer.CheckType<T>(Value.Buffer);
            }
            else if (_dataObject != null)
            {
                isValid = _dataObject is T;
            }
            return isValid;
        }
    }
}
