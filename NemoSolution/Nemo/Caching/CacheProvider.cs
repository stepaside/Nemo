using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Nemo.Extensions;
using Nemo.Fn;
using System.Threading;
using System.Collections.Concurrent;
using Nemo.Configuration;

namespace Nemo.Caching
{
    public abstract class CacheProvider
    {
        private readonly bool _userContext;
        protected readonly string _cacheNamespace;

        private bool _slidingExpiration;
        private CacheExpirationType _expirationType = CacheExpirationType.Never;
        private Maybe<TimeSpan> _lifeSpan;
        private Maybe<DateTimeOffset> _expiresAt;
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
        public abstract object Remove(string key);
        public abstract bool Clear(string key);
        public abstract bool AddNew(string key, object val);
        public abstract bool Save(string key, object val);
        public abstract bool Save(IDictionary<string, object> items);
        public abstract object Retrieve(string key);
        public abstract IDictionary<string, object> Retrieve(IEnumerable<string> keys);
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
            protected internal set
            {
                _lifeSpan = value;
                _expiresAt = Maybe<DateTimeOffset>.Empty;
                _timeOfDay = null;
                _expirationType = CacheExpirationType.TimeSpan;
            }
        }

        public virtual DateTimeOffset ExpiresAt
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

        public virtual Maybe<DateTimeOffset> ExpiresAtSpecificTime
        {
            get
            {
                return ParseTimeOfDay(TimeOfDay, true);
            }
        }

        protected Maybe<DateTimeOffset> ParseTimeOfDay(string timeOfDay, bool adjustForSave)
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

