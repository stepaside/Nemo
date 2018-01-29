using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Nemo.Collections;
using Nemo.Reflection;
using Activator = System.Activator;

namespace Nemo.Linq
{
    public class NemoQueryProvider : IAsyncQueryProvider, IQueryProvider
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
                throw tie.GetBaseException();
            }
        }

        public TResult Execute<TResult>(Expression expression)
        {
            var result = NemoQueryContext.Execute(expression, _connection);
            if (typeof(IEnumerable).IsAssignableFrom(typeof(TResult)))
            {
                return (TResult)result;
            }
            return ((IEnumerable)result).OfType<TResult>().FirstOrDefault();
        }

        public object Execute(Expression expression)
        {
            return NemoQueryContext.Execute(expression, _connection);
        }

        IAsyncQueryable<TElement> IAsyncQueryProvider.CreateQuery<TElement>(Expression expression)
        {
            return new NemoQueryableAsync<TElement>(this, expression);
        }

        private static readonly MethodInfo ToEnumerableAsyncMethod = typeof(ObjectFactory).GetMethod("ToEnumerableAsync");

        public async Task<TResult> ExecuteAsync<TResult>(Expression expression, CancellationToken token)
        {
            var async = NemoQueryContext.Execute(expression, _connection, true);
            if (typeof(IEnumerable).IsAssignableFrom(typeof(TResult)))
            {
                var type = Reflector.GetElementType(typeof(TResult));
                if (typeof(IList).IsAssignableFrom(typeof(TResult)))
                {
                    var task = (Task)ToEnumerableAsyncMethod.MakeGenericMethod(type).Invoke(null, new object[] { async });
                    await task;
                    var items = (IEnumerable)typeof(Task<>).MakeGenericType(typeof(IEnumerable<>).MakeGenericType(type)).GetProperty("Result").GetGetMethod().Invoke(task, null);
                    var list = List.Create(type);
                    foreach (var item in items)
                    {
                        list.Add(item);
                    }

                    if (typeof(TResult).IsArray)
                    {
                        return (TResult)(object)List.CreateArray(type, list);
                    }
                    return (TResult)list;
                }
                else
                {
                    var task = (Task<TResult>)ToEnumerableAsyncMethod.MakeGenericMethod(type).Invoke(null, new object[] { async });
                    return await task;
                }
            }
            else
            {
                var task = (Task<IEnumerable<TResult>>)ToEnumerableAsyncMethod.MakeGenericMethod(typeof(TResult)).Invoke(null, new object[] { async });
                return (await task).FirstOrDefault();
            }
        }
    }
}