using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nemo.Reflection;
using Nemo.Serialization;

namespace Nemo.Caching
{
    [Serializable]
    public class CacheItem : IEquatable<CacheItem>
    {
        private byte[] _data;
        private string _key;
        [NonSerialized]
        private IBusinessObject _dataObject;

        public CacheItem() { }

        public CacheItem(string key) 
        {
            _key = key;
        }

        internal CacheItem(IBusinessObject dataObject)
        {
            _dataObject = dataObject;
        }

        internal CacheItem(IBusinessObject dataObject, string key)
        {
            _dataObject = dataObject;
            _key = key;
        }

        public byte[] Data
        {
            get
            {
                return _data;
            }
            internal set
            {
                _data = value;
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
            if (_dataObject == null)
            {
                if (this.Data != null)
                {
                    _dataObject = SerializationExtensions.Deserialize<T>(this.Data);
                }
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
                    return true;
                }
            }
            return false;
        }

        public bool IsValidDataObject<T>()
            where T : class, IBusinessObject
        {
            var isValid = true;
            if (_dataObject == null)
            {
                if (this.Data != null)
                {
                    isValid = SerializationExtensions.CheckType<T>(this.Data);
                }
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
