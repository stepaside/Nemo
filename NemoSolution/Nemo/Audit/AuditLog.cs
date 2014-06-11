using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using System.Security.Principal;
using System.Text;
using System.Threading.Tasks;
using Nemo.Fn;

namespace Nemo.Audit
{
    public class AuditLog<T>
    {
        public AuditLog(string action, T oldValue, T newValue)
        {
            Id = Guid.NewGuid();
            Action = action;
            OldValue = oldValue;
            NewValue = newValue;
            DateCreated = DateTime.UtcNow;
        }

        public Guid Id
        {
            get;
            private set;
        }

        public DateTime DateCreated
        {
            get;
            private set;
        }

        public string CreatedBy
        {
            get
            {
                var createdBy = ClaimsPrincipal.Current.ToMaybe().Select(m => m.Identity).Select(m => m.Name);
                return createdBy.HasValue ? createdBy.Value : null;
            }
        }

        public string Action
        {
            get;
            private set;
        }

        public T OldValue
        {
            get;
            private set;
        }

        public T NewValue
        {
            get;
            private set;
        }

        public string Notes
        {
            get;
            set;
        }
    }
}
