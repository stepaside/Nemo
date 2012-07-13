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
            var queryParameters = new NameValueCollection();
            string[] querySegments = queryString.Split('&');
            foreach (string segment in querySegments)
            {
                string[] parts = segment.Split('=');
                if (parts.Length > 0)
                {
                    string key = parts[0].Trim(new char[] { '?', ' ' });
                    string val = Uri.UnescapeDataString(parts[1].Trim());

                    queryParameters.Add(key, val);
                }
            }
            return queryParameters;
        }
    }
}
