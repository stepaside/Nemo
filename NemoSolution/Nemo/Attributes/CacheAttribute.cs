using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nemo.Cache;
using Nemo.Extensions;

namespace Nemo.Attributes
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Interface, AllowMultiple = false, Inherited = false)]
    public sealed class CacheAttribute : Attribute
    {
        private CacheOptions _options;

        public CacheAttribute()
        {
            TrackKeys = true;
        }

        public Type Type { get; set; }

        public string ConfigurationKey { get; set; }

        public bool TrackKeys { get; set; }

        public CacheOptions Options
        {
            get
            {
                if (_options == null && ConfigurationKey.NullIfEmpty() != null)
                {
                    _options = new CacheOptions(ConfigurationKey);
                }
                return _options;
            }
            set
            {
                _options = value;
            }
        }
    }
}
