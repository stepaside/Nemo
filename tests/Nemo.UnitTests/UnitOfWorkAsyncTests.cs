using Microsoft.VisualStudio.TestTools.UnitTesting;
using Nemo.Attributes;
using Nemo.UnitOfWork;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Nemo.UnitTests
{
    [TestClass]
    public class UnitOfWorkAsyncTests
    {
        public class Person
        {
            [PrimaryKey]
            public int Id { get; set; }
            public string Name { get; set; }
        }

        [TestInitialize]
        public void ClearScopes()
        {
            ObjectScope.ClearScopes();
        }

        [TestMethod]
        public void Scope_CanBeDisposedAfterThreadHop()
        {
            var scope = ObjectScope.New(new Person { Id = 1, Name = "a" });

            Exception captured = null;
            var thread = new Thread(() =>
            {
                try
                {
                    scope.Dispose();
                }
                catch (Exception ex)
                {
                    captured = ex;
                }
            });
            thread.Start();
            thread.Join();

            Assert.IsNull(captured, captured?.Message);
            Assert.AreEqual(0, ObjectScope.ScopeCount);
        }

        [TestMethod]
        public async Task ExecuteSqlAsync_ForwardsCancellationTokenToNonQuery()
        {
            using var cts = new CancellationTokenSource();
            using var connection = new Microsoft.Data.Sqlite.SqliteConnection();

            await ObjectFactory.ExecuteSqlAsync("update Person set Name = 'a'", true, connection: connection, cancellationToken: cts.Token);

            Assert.AreEqual(cts.Token, connection.OpenToken);
            Assert.AreEqual(cts.Token, connection.NonQueryToken);
        }

        [TestMethod]
        public async Task ExecuteAsync_ForwardsCancellationTokenToScalar()
        {
            using var cts = new CancellationTokenSource();
            using var connection = new Microsoft.Data.Sqlite.SqliteConnection();

            await ObjectFactory.ExecuteAsync(new OperationRequest
            {
                Operation = "select count(*) from Person",
                OperationType = OperationType.Sql,
                ReturnType = OperationReturnType.Scalar,
                Connection = connection
            }, cts.Token);

            Assert.AreEqual(cts.Token, connection.ScalarToken);
        }

        [TestMethod]
        public async Task CommitAsync_ManualChangeTracking_CompletesScope()
        {
            var entity = new Person { Id = 1, Name = "a" };

            using (ObjectScope.New(entity, mode: ChangeTrackingMode.Manual))
            {
                entity.Name = "b";

                Assert.IsTrue(await entity.CommitAsync());
            }

            Assert.AreEqual("b", entity.Name);
            Assert.AreEqual(0, ObjectScope.ScopeCount);
        }

        [TestMethod]
        public async Task DisposeAsync_RemovesScope()
        {
            var scope = ObjectScope.New(new Person { Id = 1, Name = "a" });

            await scope.DisposeAsync();

            Assert.AreEqual(0, ObjectScope.ScopeCount);
            Assert.IsNull(ObjectScope.Current);
        }

        [TestMethod]
        public async Task DisposeAsync_CalledAfterDispose_RemovesScopeOnce()
        {
            var outer = ObjectScope.New(new Person { Id = 1 });
            var inner = ObjectScope.New(new Person { Id = 2 });

            inner.Dispose();
            await inner.DisposeAsync();

            Assert.AreEqual(1, ObjectScope.ScopeCount);
            Assert.AreSame(outer, ObjectScope.Current);

            await outer.DisposeAsync();
            Assert.AreEqual(0, ObjectScope.ScopeCount);
        }

        [TestMethod]
        public async Task DisposeAsync_WithAutoCommit_KeepsChanges()
        {
            var entity = new Person { Id = 1, Name = "a" };

            await using (ObjectScope.New(entity, autoCommit: true, mode: ChangeTrackingMode.Manual))
            {
                await Task.Yield();
                entity.Name = "b";
            }

            Assert.AreEqual("b", entity.Name);
            Assert.AreEqual(0, ObjectScope.ScopeCount);
        }

        [TestMethod]
        public async Task DisposeAsync_WithAutoCommitAndMarkFailed_RollsBackEntity()
        {
            var entity = new Person { Id = 1, Name = "a" };

            var scope = ObjectScope.New(entity, autoCommit: true, mode: ChangeTrackingMode.Manual);
            await using (scope)
            {
                entity.Name = "b";
                scope.MarkFailed();
            }

            Assert.AreEqual("a", entity.Name);
            Assert.AreEqual(0, ObjectScope.ScopeCount);
        }
    }
}
