using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections.Specialized;

namespace Nemo.Utilities
{
    public static class Http
    {
        public static NameValueCollection ParseQueryString(string queryString)
        {
            var queryParameters = new NameValueCollection(StringComparer.OrdinalIgnoreCase);
            var querySegments = queryString.Split('&');
            foreach (var segment in querySegments)
            {
                var parts = segment.Split('=');
                if (parts.Length <= 0) continue;

                var key = parts[0].Trim('?', ' ');
                var val = Uri.UnescapeDataString(parts[1].Trim());

                queryParameters.Add(key, val);
            }
            return queryParameters;
        }
    }
}
