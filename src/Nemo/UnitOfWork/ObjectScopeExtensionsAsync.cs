using System;
using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Nemo.Data;
using Nemo.Reflection;

namespace Nemo.UnitOfWork
{
    public static partial class ObjectScopeExtensions
    {
        private static readonly ConcurrentDictionary<Type, RuntimeMethodHandle> CommitAsyncMethods = new ConcurrentDictionary<Type, RuntimeMethodHandle>();

        public static async Task<bool> CommitAsync<T>(this T dataEntity, CancellationToken cancellationToken = default)
            where T : class
        {
            var success = true;
            var context = ObjectScope.Current;

            if (context != null)
            {
                if (context.ChangeTracking == ChangeTrackingMode.Automatic)
                {
                    success = await ApplyChangesAsync(dataEntity, context, true, cancellationToken).ConfigureAwait(false);
                }
                else if (context.ChangeTracking == ChangeTrackingMode.Debug)
                {
                    success = await ApplyChangesAsync(dataEntity, context, false, cancellationToken).ConfigureAwait(false);
                }

                if (context.IsNested)
                {
                    success = context.UpdateOuterSnapshot(dataEntity);
                }

                if (success)
                {
                    context.Cleanup();

                    context.Transaction?.Complete();
                }
            }

            return success;
        }

        internal static Task<bool> CommitAsync(this object dataEntity, Type objectType, CancellationToken cancellationToken = default)
        {
            var methodHandle = CommitAsyncMethods.GetOrAdd(objectType, type =>
            {
                var commitMethod = typeof(ObjectScopeExtensions).GetMethods(BindingFlags.Static | BindingFlags.Public).FirstOrDefault(m => m.Name == nameof(CommitAsync) && m.IsGenericMethodDefinition);
                var genericCommitMethod = commitMethod.MakeGenericMethod(type);
                return genericCommitMethod.MethodHandle;
            });
            var invoker = Reflector.Method.CreateDelegate(methodHandle);
            return (Task<bool>)invoker(null, new[] { dataEntity, (object)cancellationToken });
        }

        private static async Task<bool> ApplyChangesAsync<T>(T dataEntity, ObjectScope context, bool execute, CancellationToken cancellationToken)
            where T : class
        {
            var success = true;
            var connection = context.Connection ?? DbFactory.CreateConnection(null, typeof(T), context.Configuration);
            var externalConnection = context.Connection != null;
            var openConnectionRequired = !externalConnection || context.Connection.State != ConnectionState.Open;
            DbTransaction transaction = null;
            try
            {
                if (openConnectionRequired) await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                if (context.Transaction == null)
                {
                    transaction = await BeginTransactionAsync(connection, cancellationToken).ConfigureAwait(false);
                }

                var changes = CompareObjects(dataEntity, dataEntity.Old());
                var statement = GetCommitStatement(changes, connection);

                if (execute)
                {
                    if (!string.IsNullOrEmpty(statement.Item1))
                    {
                        var response = await ObjectFactory.ExecuteAsync<T>(new OperationRequest
                        {
                            Operation = statement.Item1,
                            OperationType = OperationType.Sql,
                            Parameters = statement.Item2,
                            Connection = connection,
                            ReturnType = OperationReturnType.SingleResult,
                            Transaction = transaction,
                            Configuration = context.Configuration
                        }, cancellationToken).ConfigureAwait(false);
                        success = response.Value != null;
                        if (success)
                        {
                            SetGeneratedPropertyValues(statement.Item3, (IDataReader)response.Value);
                        }
                    }
                }
                else
                {
                    Console.WriteLine(statement.Item1);
                }

                if (transaction != null)
                {
                    await CommitTransactionAsync(transaction, cancellationToken).ConfigureAwait(false);
                }
            }
            catch
            {
                if (transaction != null)
                {
                    try
                    {
                        await RollbackTransactionAsync(transaction, cancellationToken).ConfigureAwait(false);
                    }
                    catch
                    {
                        // Do not mask the original exception with a rollback failure
                    }
                }
                throw;
            }
            finally
            {
                if (!externalConnection)
                {
                    connection.Dispose();
                }
            }

            return success;
        }

        private static async Task<DbTransaction> BeginTransactionAsync(DbConnection connection, CancellationToken cancellationToken)
        {
#if NETSTANDARD2_1_OR_GREATER || NET5_0_OR_GREATER
            return await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
#else
            await Task.CompletedTask.ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            return connection.BeginTransaction();
#endif
        }

        private static async Task CommitTransactionAsync(DbTransaction transaction, CancellationToken cancellationToken)
        {
#if NETSTANDARD2_1_OR_GREATER || NET5_0_OR_GREATER
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
#else
            await Task.CompletedTask.ConfigureAwait(false);
            transaction.Commit();
#endif
        }

        private static async Task RollbackTransactionAsync(DbTransaction transaction, CancellationToken cancellationToken)
        {
#if NETSTANDARD2_1_OR_GREATER || NET5_0_OR_GREATER
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
#else
            await Task.CompletedTask.ConfigureAwait(false);
            transaction.Rollback();
#endif
        }
    }
}
