using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Nemo.Extensions;
using Nemo.Fn;
using System.Threading;
using System.Collections.Concurrent;
using Nemo.Configuration;

namespace Nemo.Cache
{
    public abstract class CacheProvider
    {
        private readonly bool _userContext;
        protected readonly string _cacheNamespace;

        private readonly bool _slidingExpiration;
        private CacheExpirationType _expirationType = CacheExpirationType.Never;
        private TimeSpan? _lifeSpan;
        private DateTimeOffset? _expiresAt;
        private string _timeOfDay;
        
        protected CacheProvider(CacheOptions options)
        {
            if (options != null)
            {
                _cacheNamespace = options.Namespace;
                _userContext = options.UserContext;
                _slidingExpiration = options.SlidingExpiration;
                
                if (options.LifeSpan.HasValue)
                {
                    SetLifeSpan(options.LifeSpan);
                }
                else if (options.ExpiresAt.HasValue)
                {
                    SetExpiresAt(options.ExpiresAt);
                    if (_slidingExpiration)
                    {
                        SetLifeSpan(ExpiresAt.Subtract(DateTimeOffset.Now));
                    }
                }
                else if (options.TimeOfDay.NullIfEmpty() != null)
                {
                    SetTimeOfDay(options.TimeOfDay);
                    if (_slidingExpiration)
                    {
                        var expiresAtSpecificTime = ExpiresAtSpecificTime;
                        if (expiresAtSpecificTime.HasValue)
                        {
                            SetLifeSpan(expiresAtSpecificTime.Value.Subtract(DateTimeOffset.Now));
                        }
                    }
                }
            }
        }

        public abstract void Clear();
        public abstract object Pop(string key);
        public abstract bool Remove(string key);
        public abstract bool Add(string key, object val);
        public abstract bool Set(string key, object val);
        public abstract bool Set(IDictionary<string, object> items);
        public abstract object Get(string key);
        public abstract IDictionary<string, object> Get(IEnumerable<string> keys);
        public abstract bool Touch(string key, TimeSpan lifeSpan);

        public bool IsDistributed
        {
            get
            {
                return this is DistributedCacheProvider;
            }
        }

        public virtual bool IsOutOfProcess
        {
            get
            {
                return false;
            }
        }

        public string Namespace
        {
            get
            {
                if (!string.IsNullOrEmpty(_cacheNamespace))
                {
                    return _cacheNamespace + "::";
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

        public string UserPrefix
        {
            get
            {
                if (IsUserContext)
                {
                    return Thread.CurrentPrincipal.Identity.Name + "::";
                }
                return string.Empty;
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

        public virtual TimeSpan LifeSpan
        {
            get
            {
                return _lifeSpan.HasValue ? _lifeSpan.Value : TimeSpan.Zero;
            }
        }

        public virtual DateTimeOffset ExpiresAt
        {
            get
            {
                return _expiresAt.HasValue ? _expiresAt.Value : (IsDistributed ? DateTimeOffset.Now.AddDays(30) : DateTimeOffset.MaxValue);
            }
        }

        public string TimeOfDay
        {
            get
            {
                return _timeOfDay;
            }
        }
        
        public virtual DateTimeOffset? ExpiresAtSpecificTime
        {
            get
            {
                return ParseTimeOfDay(TimeOfDay, true);
            }
        }

        protected void SetLifeSpan(TimeSpan? value)
        {
            if (value.HasValue)
            {
                _lifeSpan = value.Value;
                _expiresAt = null;
                _timeOfDay = null;
                _expirationType = CacheExpirationType.Sliding;
            }
        }

        protected void SetExpiresAt(DateTimeOffset? value)
        {
            if (value.HasValue)
            {
                _expiresAt = value.Value;
                _lifeSpan = null;
                _timeOfDay = null;
                _expirationType = CacheExpirationType.Absolute;
            }
        }

        protected void SetTimeOfDay(string value)
        {
            var dateValue = ParseTimeOfDay(value, false);
            if (dateValue.HasValue)
            {
                _timeOfDay = value;
                _lifeSpan = null;
                _expiresAt = null;
                _expirationType = CacheExpirationType.TimeOfDay;
            }
        }

        protected DateTimeOffset? ParseTimeOfDay(string timeOfDay, bool adjustForSave)
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
                    return t;
                }
            }
            return null;
        }

        public IDictionary<string, string> ComputeKey(IEnumerable<string> keys)
        {
            return keys.ToDictionary(key => ComputeKey(key, null), key => key);
        }

        public IDictionary<string, string> ComputeKey(IDictionary<string, ulong?> keysWithRevision)
        {
            return keysWithRevision.ToDictionary(p => ComputeKey(p.Key, p.Value), p => p.Key);
        }

        public string ComputeKey(string key, ulong? revision = null)
        {
            string result = null;

            result = UserPrefix + Namespace + key;

            if (revision != null)
            {
                result = result + ";" + revision.Value;
            }

            return result;
        }

        public string CleanKey(string computedKey)
        {
            var pos = computedKey.LastIndexOf(';');
            if (pos > -1)
            {
                computedKey = computedKey.Substring(0, pos);
            }

            if (IsUserContext)
            {
                pos = computedKey.IndexOf("::");
                if (pos > -1)
                {
                    computedKey = computedKey.Substring(pos + 2);
                }
            }

            computedKey = computedKey.Substring(Namespace.Length);

            return computedKey;
        }
    }
}

