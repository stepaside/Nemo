using System;
using System.Threading;
using Nemo.Fn;

namespace Nemo.Logging
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

        public string CreatedBy => Thread.CurrentPrincipal?.Identity?.Name;

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
