using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Nemo.Configuration;
using Nemo.Extensions;

namespace Nemo.Linq
{
    public class NemoQueryable<T> : IOrderedQueryable<T>
    {
        private readonly NemoQueryProvider _provider;
        private readonly Expression _expression;
        
        public NemoQueryable(DbConnection connection = null, INemoConfiguration config = null)
        {
            _provider = new NemoQueryProvider(connection, config);
            _expression = Expression.Constant(this);
        }

        public NemoQueryable(NemoQueryProvider provider, Expression expression)
        {
            provider.ThrowIfNull("provider");
            expression.ThrowIfNull("expression");

            _provider = provider;
            _expression = expression;
        }

        public IEnumerator<T> GetEnumerator()
        {
            return (Provider.Execute<IEnumerable<T>>(Expression)).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return (Provider.Execute<IEnumerable>(Expression)).GetEnumerator();
        }

        public Type ElementType
        {
            get { return typeof(T); }
        }

        public Expression Expression 
        {
            get
            {
                return _expression;
            }
        }

        public IQueryProvider Provider
        {
            get
            {
                return _provider;
            }
        }
    }
}
