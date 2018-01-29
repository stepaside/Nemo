using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Security.Principal;

namespace Nemo
{
    /*
     * This class was created specifically to be used as an execution context for ASP.NET
     * The idea was taken from here:
     * http://piers7.blogspot.com/2005/11/threadstatic-callcontext-and_02.html
     * Basically for each request at the PostAuthenticateRequest stage, 
     * replace the existing principal with the ThreadedPrincipal like so:
     * 
     * Thread.CurrentPrincipal = new ThreadedPrincipal(Thread.CurrentPrincipal) 
     * HttpContext.Current.User = new ThreadedPrincipal(HttpContext.Current.User) 
     * 
     * Then you can use ExecutionContext class anywhere in your application.
     *
     * NOTE: this step is required if you plan to use context cache in you ASP.NET application
     */
    public class ThreadedPrincipal : IPrincipal
    {
        private readonly IPrincipal _principal;
        private readonly IDictionary<string, object> _items;

        public ThreadedPrincipal(IPrincipal principal)
        {
            _principal = principal;
            _items = new Dictionary<string, object>();
        }

        public IDictionary<string, object> Items
        {
            get
            {
                return _items;
            }
        }

        #region IPrincipal Members

        public IIdentity Identity
        {
            get { return _principal.Identity; }
        }

        public bool IsInRole(string role)
        {
            return _principal.IsInRole(role);
        }

        #endregion
    }
}
