using Nemo.Cache;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nemo.Configuration.Mapping
{
    public interface ICacheMap
    {
        Type CacheProvider { get; }
        string ConfigurationKey { get; }
        bool TrackKeys { get; }
        CacheOptions CacheOptions { get; }
        IList<QueryDependency> QueryDependencies { get; } 
    }
}
