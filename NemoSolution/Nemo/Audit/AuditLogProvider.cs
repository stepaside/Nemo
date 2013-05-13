using Nemo.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nemo.Audit
{
    public abstract class AuditLogProvider
    {
        public abstract void Write<T>(AuditLog<T> auditTrail);

        public static AuditLogProvider Current
        {
            get
            {
                var logProvider = ConfigurationFactory.Configuration.AuditLogProvider;
                if (logProvider != null)
                {
                    return (AuditLogProvider)Nemo.Reflection.Activator.New(logProvider);
                }
                return null;
            }
        }
    }
}
