namespace Nemo
{
    public interface IAggregatePropertyMapper<TAggregateRoot, T>
         where TAggregateRoot : class
         where T : class
    {
        TAggregateRoot Map(TAggregateRoot aggregate, T entity);
    }
}
