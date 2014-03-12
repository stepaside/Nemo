using Nemo.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nemo.Audit
{
    public interface IAuditLogProvider
    {
        void Write<T>(AuditLog<T> auditTrail);
    }
}
