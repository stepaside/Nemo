namespace Nemo.Logging
{
    public interface IAuditLogProvider
    {
        void Write<T>(AuditLog<T> auditTrail);
    }
}
