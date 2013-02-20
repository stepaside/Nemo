using System;
using System.Web;
using Nemo.Extensions;
using Nemo.Fn;
using Nemo.Utilities;

namespace Nemo.Caching
{
    public class CacheOptions
    {
        public CacheOptions() { }

        public CacheOptions(string configKey)
        {
            var configValue = Config.AppSettings(configKey);
            if (configValue.NullIfEmpty() != null)
            {
                var nvp = Http.ParseQueryString(configValue);
                foreach (var key in nvp.AllKeys)
                {
                    switch (key.ToLower())
                    {
                        case "namespace":
                            Namespace = nvp[key];
                            break;
                        case "usercontext":
                            UserContext = nvp[key].SafeCast<bool>();
                            break;
                        case "lifespan":
                            LifeSpan = nvp[key].SafeCast<TimeSpan>();
                            break;
                        case "expiresat":
                            ExpiresAt = nvp[key].SafeCast<DateTimeOffset>();
                            break;
                        case "timeofday":
                            TimeOfDay = nvp[key];
                            break;
                        case "slidingexpiration":
                            SlidingExpiration = nvp[key].SafeCast<bool>();
                            break;
                        case "clustername":
                            ClusterName = nvp[key];
                            break;
                        case "clusterpwd":
                            ClusterPassword = nvp[key];
                            break;
                        case "filepath":
                            FilePath = nvp[key];
                            break;
                        case "hashalgorithm":
                            HashAlgorithmName value;
                            if (Enum.TryParse<HashAlgorithmName>(nvp[key], true, out value))
                            {
                                HashAlgorithm = value;
                            }
                            break;
                        case "hostname":
                            HostName = nvp[key];
                            break;
                        case "database":
                            Database = nvp[key].SafeCast<int>();
                            break;
                    }
                }
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
        public Maybe<HashAlgorithmName> HashAlgorithm { get; set; }
        public string HostName { get; set; }
        public int Database { get; set; }
    }
}
