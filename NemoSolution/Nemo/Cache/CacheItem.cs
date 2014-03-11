using Nemo.Extensions;
using Nemo.Reflection;
using Nemo.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Nemo.Cache
{
    public abstract class CacheItem
    {
        private Lazy<CacheValue> _lazy;
        protected string _key;

        protected CacheItem(string key, Func<CacheValue> valueFactory)
        {
            _key = key;
            _lazy = new Lazy<CacheValue>(valueFactory);
        }

        protected CacheItem(string key, byte[] data)
            : this(key, () => CacheValue.FromBytes(data))
        { }

        protected CacheItem(string key)
        {
            _key = key;
            _lazy = new Lazy<CacheValue>(ComputeCacheValue);
        }

        protected abstract CacheValue ComputeCacheValue();

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

        public virtual string Key
        {
            get
            {
                return _key;
            }
        }
    }

    public class CacheIndex : CacheItem
    {
        private string[] _index;

        public CacheIndex(string key, CacheValue value)
            : base(key, () => value)
        { }

        public CacheIndex(string key, string[] index)
            : base(key)
        {
            _index = index;
        }

        protected override CacheValue ComputeCacheValue()
        {
 	        var cacheValue = new CacheValue();

            if (_index != null)
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
    }

    public class CacheDataObject : CacheItem
    {
        private IDataEntity _dataObject;

        public CacheDataObject(string key, CacheValue value)
            : base(key, () => value)
        { }

        public CacheDataObject(string key, IDataEntity dataObject)
            : base(key)
        {
            _dataObject = dataObject;
        }

        public override string Key
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

        public string ComputeKey<T>()
            where T : class, IDataEntity
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
            where T : class, IDataEntity
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
            where T : class, IDataEntity
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

        protected override CacheValue ComputeCacheValue()
        {
            var cacheValue = new CacheValue();

            if (_dataObject != null)
            {
                cacheValue.Buffer = _dataObject.Serialize();
            }
            else
            {
                cacheValue = null;
            }

            return cacheValue;
        }
    }
}
