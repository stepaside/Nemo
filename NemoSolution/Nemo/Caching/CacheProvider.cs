using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Nemo.Extensions;
using Nemo.Fn;
using System.Threading;

namespace Nemo.Caching
{
    public abstract class CacheProvider
    {
        private readonly CacheType _cacheType;
        private readonly bool _userContext;
        protected readonly string _cacheNamespace;
        protected ulong? _namespaceVersion = null;

        private bool _slidingExpiration;
        private CacheExpirationType _expirationType = CacheExpirationType.Never;
        private Maybe<TimeSpan> _lifeSpan;
        private Maybe<DateTimeOffset> _expiresAt;
        private string _timeOfDay;
        
        protected CacheProvider(CacheType cacheType, CacheOptions options)
        {
            _cacheType = cacheType;
            if (options != null)
            {
                _cacheNamespace = options.Namespace;
                _userContext = options.UserContext;
                _slidingExpiration = options.SlidingExpiration;
                if (options.LifeSpan.HasValue)
                {
                    LifeSpan = options.LifeSpan;
                }
                else if (options.ExpiresAt.HasValue)
                {
                    ExpiresAt = options.ExpiresAt;
                    if (_slidingExpiration)
                    {
                        LifeSpan = ExpiresAt.Subtract(DateTimeOffset.Now);
                    }
                }
                else if (options.TimeOfDay.NullIfEmpty() != null)
                {
                    TimeOfDay = options.TimeOfDay;
                    if (_slidingExpiration && ExpiresAtSpecificTime.HasValue)
                    {
                        LifeSpan = ExpiresAtSpecificTime.Value.Subtract(DateTimeOffset.Now);
                    }
                }
            }
        }

        public abstract void RemoveAll();
        public virtual void RemoveByNamespace()
        {
            throw new NotSupportedException();
        }
        public abstract object Remove(string key);
        public abstract bool Clear(string key);
        public abstract bool AddNew(string key, object val);
        public abstract bool Save(string key, object val);
        public abstract bool Save(IDictionary<string, object> items);
        public abstract object Retrieve(string key);
        public abstract IDictionary<string, object> Retrieve(IEnumerable<string> keys);
        
        public bool IsDistributed
        {
            get
            {
                return this is IDistributedCacheProvider;
            }
        }

        public virtual bool IsOutOfProcess
        {
            get
            {
                return false;
            }
        }

        public CacheType Type
        {
            get
            {
                return _cacheType;
            }
        }

        public string Namespace
        {
            get
            {
                if (!string.IsNullOrEmpty(_cacheNamespace))
                {
                    var ns = _cacheNamespace + "::";
                    if (_namespaceVersion.HasValue)
                    {
                        ns += _namespaceVersion.Value + "::";
                    }
                    return ns;
                }
                return string.Empty;
            }
        }

        public bool IsUserContext
        {
            get
            {
                return _userContext;
            }
        }

        public bool SlidingExpiration
        {
            get
            {
                return _slidingExpiration;
            }
        }

        public CacheExpirationType ExpirationType
        {
            get
            {
                return _expirationType;
            }
        }

        public TimeSpan LifeSpan
        {
            get
            {
                return _lifeSpan.HasValue ? _lifeSpan.Value : TimeSpan.Zero;
            }
            protected internal set
            {
                _lifeSpan = value;
                _expiresAt = Maybe<DateTimeOffset>.Empty;
                _timeOfDay = null;
                _expirationType = CacheExpirationType.TimeSpan;
            }
        }

        public DateTimeOffset ExpiresAt
        {
            get
            {
                return _expiresAt.HasValue ? _expiresAt.Value : (IsDistributed ? DateTimeOffset.Now.AddDays(30) : DateTimeOffset.MaxValue);
            }
            protected internal set
            {
                _expiresAt = value;
                _lifeSpan = Maybe<TimeSpan>.Empty;
                _timeOfDay = null;
                _expirationType = CacheExpirationType.DateTime;
            }
        }

        public string TimeOfDay
        {
            get
            {
                return _timeOfDay;
            }
            protected internal set
            {
                var dateValue = ParseTimeOfDay(value, false);
                if (dateValue.HasValue)
                {
                    _timeOfDay = value;
                    _lifeSpan = Maybe<TimeSpan>.Empty;
                    _expiresAt = Maybe<DateTimeOffset>.Empty;
                    _expirationType = CacheExpirationType.TimeOfDay;
                }
            }
        }

        public Maybe<DateTimeOffset> ExpiresAtSpecificTime
        {
            get
            {
                return ParseTimeOfDay(TimeOfDay, true);
            }
        }

        private  Maybe<DateTimeOffset> ParseTimeOfDay(string timeOfDay, bool adjustForSave)
        {
            if (!string.IsNullOrEmpty(timeOfDay))
            {
                DateTimeOffset t;
                if (DateTimeOffset.TryParse(timeOfDay, out t))
                {
                    if (adjustForSave && t < DateTimeOffset.Now)
                    {
                        t = t.AddDays(1.0);
                    }
                    return new Maybe<DateTimeOffset>(t);
                }
            }
            return Maybe<DateTimeOffset>.Empty;
        }

        protected IDictionary<string, string> ComputeKey(IEnumerable<string> keys)
        {
            return keys.ToDictionary(key => ComputeKey(key), key => key);
        }

        protected string ComputeKey(string key)
        {
            string result = null;
            if (IsUserContext)
            {
                result = ComputeUserKey(Namespace + key);
            }
            else
            {
                result = Namespace + key;
            }
            return result;
        }

        protected string ComputeUserKey(string key)
        {
            return GetUserPrefix() + key;
        }

        protected string GetUserPrefix()
        {
            var userName = Thread.CurrentPrincipal.Identity.Name;
            return "__U_" + userName.Length + "_" + userName + "_";
        }

        protected string RemoveUserPrefix(string key)
        {
            if (!string.IsNullOrEmpty(key) && key.StartsWith("__U_"))
            {
                var pos = key.IndexOf('_', 4);
                var lengthValue = key.Substring(4, pos - 4);
                int length;
                if (int.TryParse(lengthValue, out length))
                {
                    // prefix (__U_) + length of the length value + underscore + length of the user id + underscore
                    key = key.Substring(4 + lengthValue.Length + 1 + length + 1);
                }
            }
            return key;
        }
    }
}

