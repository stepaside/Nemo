using System;
using System.Web;
using Nemo.Extensions;
using Nemo.Fn;
using Nemo.Collections.Extensions;
using Nemo.Utilities;

namespace Nemo.Cache
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
                
                var userContext = nvp["usercontext"].SafeCast<bool?>();
                UserContext = userContext.HasValue && userContext.Value;

                LifeSpan = nvp["lifespan"].SafeCast<TimeSpan?>();
                ExpiresAt = nvp["expiresat"].SafeCast<DateTimeOffset?>();
                TimeOfDay = nvp["timeofday"];
                
                var slidingExpiration = nvp["slidingexpiration"].SafeCast<bool?>();
                SlidingExpiration = slidingExpiration.HasValue && slidingExpiration.Value;

                ClusterName = nvp["clustername"];
                ClusterPassword = nvp["clusterpwd"];
                FilePath = nvp["filepath"];
                HostName = nvp["hostname"];

                var database = nvp["database"].SafeCast<int?>();
                Database = database.HasValue ? database.Value : default(int);
            }
        }

        public string Namespace { get; set; }
        public bool UserContext { get; set; }
        public TimeSpan? LifeSpan { get; set; }
        public DateTimeOffset? ExpiresAt { get; set; }
        public string TimeOfDay { get; set; }
        public bool SlidingExpiration { get; set; }
        public string ClusterName { get; set; }
        public string ClusterPassword { get; set; }
        public string FilePath { get; set; }
        public string HostName { get; set; }
        public int Database { get; set; }
    }
}
