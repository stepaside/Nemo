using Nemo.Cache;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nemo.Configuration.Mapping
{
    public class CacheMap : ICacheMap
    {
        public CacheMap()
        {
            QueryDependencies = new List<QueryDependency>();
        }

        public Type CacheProvider
        {
            get;
            private set;
        }

        public string ConfigurationKey
        {
            get;
            private set;
        }

        public bool TrackKeys
        {
            get;
            private set;
        }

        public CacheOptions CacheOptions
        {
            get;
            private set;
        }

        public IList<QueryDependency> QueryDependencies
        {
            get;
            private set;
        }

        public CacheMap Type<T>()
            where T : CacheProvider
        {
            CacheProvider = typeof(T);
            return this;
        }

        public CacheMap Configuration(string value)
        {
            ConfigurationKey = value;
            return this;
        }

        public CacheMap Track()
        {
            TrackKeys = true;
            return this;
        }

        public CacheMap DoNotTrack()
        {
            TrackKeys = false;
            return this;
        }

        public CacheMap Options(CacheOptions options)
        {
            CacheOptions = options;
            return this;
        }

        public CacheMap QueryDependency(QueryDependency dependency)
        {
            QueryDependencies.Add(dependency);
            return this;
        }
    }
}
