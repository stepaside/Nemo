using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Nemo.Configuration;
using Nemo.Extensions;

namespace Nemo.Linq
{
    public class NemoQueryableAsync<T> : IOrderedAsyncQueryable<T>
    {
        private readonly NemoQueryProvider _provider;
        private readonly Expression _expression;
        private readonly CancellationToken _cancellationToken;

        public NemoQueryableAsync(DbConnection connection, INemoConfiguration config, CancellationToken cancellationToken)
        {
            _provider = new NemoQueryProvider(connection, config);
            _expression = Expression.Constant(this);
            _cancellationToken = cancellationToken;
        }

        public NemoQueryableAsync() : this(CancellationToken.None)
        {
        }

        public NemoQueryableAsync(CancellationToken cancellationToken) : this((DbConnection)null, null, cancellationToken)
        {
        }

        public NemoQueryableAsync(DbConnection connection) : this(connection, null, CancellationToken.None)
        {
        }

        public NemoQueryableAsync(INemoConfiguration config) : this(null, config, CancellationToken.None)
        {
        }

        public NemoQueryableAsync(NemoQueryProvider provider, Expression expression, CancellationToken cancellationToken)
        {
            provider.ThrowIfNull("provider");
            expression.ThrowIfNull("expression");

            _provider = provider;
            _expression = expression;
            _cancellationToken = cancellationToken;
        }

        public NemoQueryableAsync(NemoQueryProvider provider, Expression expression) : this(provider, expression, CancellationToken.None)
        {
        }

        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            return Enumerate(cancellationToken).GetAsyncEnumerator();
        }

        private async IAsyncEnumerable<T> Enumerate([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            CancellationTokenSource linkedSource = null;
            var token = _cancellationToken;
            if (cancellationToken.CanBeCanceled)
            {
                if (token.CanBeCanceled)
                {
                    linkedSource = CancellationTokenSource.CreateLinkedTokenSource(token, cancellationToken);
                    token = linkedSource.Token;
                }
                else
                {
                    token = cancellationToken;
                }
            }

            try
            {
                var result = _provider.ExecuteAsyncCore(_expression, token);
                if (result is IAsyncEnumerable<T> stream)
                {
                    await foreach (var item in stream.WithCancellation(token).ConfigureAwait(false))
                    {
                        yield return item;
                    }
                }
                else
                {
                    yield return await ((Task<T>)result).ConfigureAwait(false);
                }
            }
            finally
            {
                linkedSource?.Dispose();
            }
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