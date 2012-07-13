using System;
using Nemo.Attributes;
using Nemo.Caching;
using Nemo.Extensions;
using Nemo.Fn;
using Nemo.Reflection;
using Nemo.Serialization;

namespace Nemo.BusinessObjects
{
    class CachedObjectSettings
    {
        public const string OBJECT_CACHE_NAMESPACE = "__ObjectCache";
        public const int OBJECT_CACHE_DEFAULT_LIFESPAN = 60;
    }

    public enum ObjectCacheStatus { Unknown, Cached, Stale }

    public abstract class CachedBusinessObject<T> : BusinessObject<T>, ICachedObjectProvider
        where T : class, IBusinessObject
    {
        //private readonly object _cacheLock = new object();
        private readonly CacheProvider _cache = null;
        private ObjectCacheStatus _status = ObjectCacheStatus.Unknown;
        private Func<T> _retrieve = null;
        private T _item = null;

        protected CachedBusinessObject(Func<string> getKey, Func<T> retrieve)
        {
            getKey.ThrowIfNull("getKey");
            retrieve.ThrowIfNull("retrieve");
            _retrieve = retrieve;
            CacheKey = getKey();
            var reflectedType = Nemo.Reflection.Reflector.TypeCache<T>.Type;
            if (reflectedType.IsCacheableBusinessObject)
            {
                var attr = Reflector.GetAttribute<T, CacheAttribute>(false);
                var options = attr.Options;
                options.Namespace = CachedObjectSettings.OBJECT_CACHE_NAMESPACE;
                _cache = CacheFactory.GetProvider(attr.Type, options);
                if (!options.LifeSpan.HasValue && !string.IsNullOrEmpty(options.TimeOfDay))
                {
                    _cache.TimeOfDay = options.TimeOfDay;
                }
            }
            else
            {
                _cache = CacheFactory.GetProvider(CacheType.Local, new CacheOptions { Namespace = CachedObjectSettings.OBJECT_CACHE_NAMESPACE, LifeSpan = TimeSpan.FromSeconds(CachedObjectSettings.OBJECT_CACHE_DEFAULT_LIFESPAN), UserContext = true });
            }
        }

        /// <summary>
        /// Invalidates cached object
        /// </summary>
        /// <returns></returns>
        public Maybe<bool> Invalidate()
        {
            var success = Maybe<bool>.Empty;
            if (CanBeCached && Status == ObjectCacheStatus.Cached)
            {
                success = _cache.Clear(CacheKey);
                if (success.Value)
                {
                    _status = ObjectCacheStatus.Stale;
                }
            }
            return success;
        }

        public Maybe<bool> Touch()
        {
            return false;
        }

        /// <summary>
        /// Updates cached object in the cache storage
        /// </summary>
        /// <returns></returns>
        public Maybe<bool> Sync()
        {
            var success = Maybe<bool>.Empty;
            if (CanBeCached && Status == ObjectCacheStatus.Cached)
            {
                success = Save();
                if (!success.Value)
                {
                    _status = ObjectCacheStatus.Stale;
                }
            }
            return success;
        }

        /// <summary>
        /// Tranforms and caches stale object
        /// </summary>
        /// <returns></returns>
        public Maybe<bool> Refresh()
        {
            var success = Maybe<bool>.Empty;
            if (CanBeCached && (Status == ObjectCacheStatus.Unknown || Status == ObjectCacheStatus.Stale))
            {
                success = LoadAndSave();
            }
            return success;
        }

        /// <summary>
        /// Invalidates and refreshes cached object
        /// </summary>
        /// <returns></returns>
        public Maybe<bool> Reload()
        {
            var success = Invalidate();
            success = Refresh();
            return success;
        }

        public string CacheKey
        {
            get;
            private set;
        }

        public bool CanBeCached
        {
            get
            {
                return !string.IsNullOrEmpty(CacheKey);
            }
        }

        public ObjectCacheStatus Status
        {
            get
            {
                return _status;
            }
        }

        public override T DataObject
        {
            get
            {
                if (_item == null)
                {
                    if (CanBeCached && (Status == ObjectCacheStatus.Unknown || Status == ObjectCacheStatus.Cached))
                    {
                        _item = Retrieve();
                    }

                    if (_item == null)
                    {
                        LoadAndSave();
                    }
                    else
                    {
                        _status = ObjectCacheStatus.Cached;
                    }
                }
                return _item;
            }
        }

        private T Retrieve()
        {
            T result = null;
            var value = _cache.Retrieve(CacheKey);
            if (value != null)
            {
                if (_cache.IsOutOfProcess)
                {
                    result = SerializationExtensions.Deserialize<T>((byte[])value);
                }
                else
                {
                    result = (T)value;
                }
            }
            return result;
        }
        
        private bool Save()
        {
            return _cache.Save(CacheKey, _cache.IsOutOfProcess ? (object)_item.Serialize() : (object)_item);
        }

        private Maybe<bool> LoadAndSave()
        {
            var success = Maybe<bool>.Empty;
            _item = _retrieve();
            if (CanBeCached)
            {
                success = Save();
                if (success.Value)
                {
                    _status = ObjectCacheStatus.Cached;
                }
            }
            return success;
        }
    }
}
