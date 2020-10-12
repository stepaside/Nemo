using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using Nemo.Extensions;

namespace Nemo.Linq
{
    public class NemoQueryableAsync<T> : IOrderedAsyncQueryable<T>
    {
        private readonly NemoQueryProvider _provider;
        private readonly Expression _expression;
        private readonly CancellationToken _cancellationToken;
        private readonly CancellationTokenSource _tokenSource = new CancellationTokenSource();

        public NemoQueryableAsync(DbConnection connection, CancellationToken cancellationToken)
        {
            _provider = new NemoQueryProvider(connection);
            _expression = Expression.Constant(this);
            _cancellationToken = cancellationToken == CancellationToken.None ? _tokenSource.Token : _cancellationToken;
        }

        public NemoQueryableAsync() : this(CancellationToken.None)
        {
        }

        public NemoQueryableAsync(CancellationToken cancellationToken) : this(null, cancellationToken)
        {
        }

        public NemoQueryableAsync(DbConnection connection) : this(connection, CancellationToken.None)
        {
        }

        public NemoQueryableAsync(NemoQueryProvider provider, Expression expression, CancellationToken cancellationToken)
        {
            provider.ThrowIfNull("provider");
            expression.ThrowIfNull("expression");

            _provider = provider;
            _expression = expression;
            _cancellationToken = cancellationToken == CancellationToken.None ? _tokenSource.Token : _cancellationToken;
        }

        public NemoQueryableAsync(NemoQueryProvider provider, Expression expression) : this(provider, expression, CancellationToken.None)
        {
        }

        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            return Provider.ExecuteAsync<T>(Expression, _cancellationToken).AsTask().ToAsyncEnumerable().GetAsyncEnumerator();
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

        public IAsyncQueryProvider Provider
        {
            get
            {
                return _provider;
            }
        }
    }
}