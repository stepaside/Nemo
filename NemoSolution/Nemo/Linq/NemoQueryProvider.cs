using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using Nemo.Reflection;
using Activator = System.Activator;

namespace Nemo.Linq
{
    public class NemoQueryProvider : IQueryProvider
    {
        private readonly DbConnection _connection;

        public NemoQueryProvider(DbConnection connection = null)
        {
            _connection = connection;
        }

        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            return new NemoQueryable<TElement>(this, expression);
        }

        public IQueryable CreateQuery(Expression expression)
        {
            var elementType = Reflector.GetElementType(expression.Type) ?? expression.Type;
            try
            {
                return (IQueryable)Activator.CreateInstance(typeof(NemoQueryable<>).MakeGenericType(elementType), new object[] { this, expression });
            }
            catch (System.Reflection.TargetInvocationException tie)
            {
                throw tie.InnerException;
            }
        }

        public TResult Execute<TResult>(Expression expression)
        {
            return (TResult)NemoQueryContext.Execute(expression, _connection);
        }

        public object Execute(Expression expression)
        {
            return NemoQueryContext.Execute(expression, _connection);
        }
    }
}