using System;
using System.Web;
using Nemo.Extensions;
using Nemo.Fn;
using Nemo.Collections.Extensions;
using Nemo.Utilities;

namespace Nemo.Caching
{
    public class CacheOptions
    {
        public CacheOptions() { }

        public CacheOptions(string configKey)
        {
            var configValue = Config.AppSettings(configKey);
            if (!string.IsNullOrEmpty(configValue))
            {
                var nvp = Http.ParseQueryString(configValue);
                Namespace = nvp["namespace"];
                // Revision = nvp["revision"].ToMaybe().Select(s => s.SafeCast<ulong>()).Let(m => m.HasValue ? m.Value : 0ul);
                UserContext = nvp["usercontext"].ToMaybe().Select(s => s.SafeCast<bool>()).Let(m => m.HasValue ? m.Value : false);
                LifeSpan = nvp["lifespan"].ToMaybe().Select(s => s.SafeCast<TimeSpan>());
                ExpiresAt = nvp["expiresat"].ToMaybe().Select(s => s.SafeCast<DateTimeOffset>());
                TimeOfDay = nvp["timeofday"];
                SlidingExpiration = nvp["slidingexpiration"].ToMaybe().Select(s => s.SafeCast<bool>()).Let(m => m.HasValue ? m.Value : false);
                ClusterName = nvp["clustername"];
                ClusterPassword = nvp["clusterpwd"];
                FilePath = nvp["filepath"];
                HostName = nvp["hostname"];
                Database = nvp["database"].ToMaybe().Select(s => s.SafeCast<int>()).Let(m => m.HasValue ? m.Value : default(int)); ;
            }
        }

        public string Namespace { get; set; }
        public bool UserContext { get; set; }
        public Maybe<TimeSpan> LifeSpan { get; set; }
        public Maybe<DateTimeOffset> ExpiresAt { get; set; }
        public string TimeOfDay { get; set; }
        public bool SlidingExpiration { get; set; }
        public string ClusterName { get; set; }
        public string ClusterPassword { get; set; }
        public string FilePath { get; set; }
        public string HostName { get; set; }
        public int Database { get; set; }
    }
}
