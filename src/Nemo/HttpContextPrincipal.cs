using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Security.Principal;

namespace Nemo
{
    public class HttpContextPrincipal : IPrincipal
    {
        private readonly IPrincipal _principal;

        public HttpContextPrincipal(IPrincipal principal)
        {
            _principal = principal;
            Items = new Dictionary<string, object>();
        }

        public IDictionary<string, object> Items { get; }

        #region IPrincipal Members

        public IIdentity Identity => _principal.Identity;

        public bool IsInRole(string role)
        {
            return _principal.IsInRole(role);
        }

        #endregion
    }
}
