using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nemo.Reflection;
using Nemo.Serialization;
using System.Runtime.Serialization;

namespace Nemo.Caching
{
    [Serializable]
    public class CacheItem : IEquatable<CacheItem>, ISerializable
    {
        private byte[] _data;
        private string _key;
        [NonSerialized]
        private IBusinessObject _dataObject;

        protected CacheItem(SerializationInfo info, StreamingContext context)
        {
            _data = (byte[])info.GetValue("_data", typeof(byte[]));
            _key = info.GetString("_key");
        }

        internal CacheItem(IBusinessObject dataObject)
        {
            _dataObject = dataObject;
        }

        internal CacheItem(string key, IBusinessObject dataObject)
        {
            _dataObject = dataObject;
            _key = key;
        }

        internal CacheItem(byte[] data)
        {
            _data = data;
        }

        internal CacheItem(string key, byte[] data)
        {
            _data = data;
            _key = key;
        }

        public byte[] Data
        {
            get
            {
                if (_data == null && _dataObject != null)
                {
                    _data = _dataObject.Serialize();
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
                    _key = new CacheKey(_dataObject).Value;
                }
                return _key;
            }
        }

        public string GetKey<T>()
            where T : class, IBusinessObject
        {
            if (string.IsNullOrEmpty(_key))
            {
                var dataObject = this.GetDataObject<T>();
                if (dataObject != null)
                {
                    _key = new CacheKey(dataObject).Value;
                }
            }
            return _key;
        }

        public T GetDataObject<T>()
            where T : class, IBusinessObject
        {
            if (_dataObject == null && _data != null)
            {
                _dataObject = _data.Deserialize<T>();
            }

            if (_dataObject != null)
            {
                return (T)_dataObject;
            }
            return default(T);
        }

        public bool SetDataObject<T>(T dataObject)
            where T : class, IBusinessObject
        {
            if (_dataObject != null && dataObject != null && Reflector.ExtractInterface(_dataObject.GetType()) == typeof(T))
            {
                if (ObjectCache.Modify<T>(dataObject))
                {
                    _dataObject = dataObject;
                    _data = null;
                    return true;
                }
            }
            return false;
        }

        public bool IsValidDataObject<T>()
            where T : class, IBusinessObject
        {
            var isValid = true;
            if (_dataObject == null && _data != null)
            {
                isValid = ObjectSerializer.CheckType<T>(_data);
            }
            return isValid;
        }

        #region IEquatable<CacheItem> Members

        public bool Equals(CacheItem other)
        {
            return object.Equals(this, other);
        }

        #endregion

        #region ISerializable Members

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("_data", this.Data);
            info.AddValue("_key", this.Key);
        }

        #endregion
    }
}
