using Microsoft.VisualStudio.TestTools.UnitTesting;
using Nemo.Attributes;
using Nemo.UnitOfWork;
using System;
using System.Threading.Tasks;

namespace Nemo.UnitTests
{
    [TestClass]
    public class UnitOfWorkScopeIsolationTests
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
        public async Task Scopes_OpenedInParallelFlows_DoNotSeeEachOther()
        {
            var first = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var second = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            async Task<(bool IsCurrent, int Count)> Run(Person entity, TaskCompletionSource<bool> ready, Task other)
            {
                using (var scope = ObjectScope.New(entity))
                {
                    ready.SetResult(true);
                    await other;

                    return (ReferenceEquals(ObjectScope.Current, scope), ObjectScope.ScopeCount);
                }
            }

            var a = Task.Run(() => Run(new Person { Id = 1 }, first, second.Task));
            var b = Task.Run(() => Run(new Person { Id = 2 }, second, first.Task));

            var results = await Task.WhenAll(a, b);

            foreach (var result in results)
            {
                Assert.IsTrue(result.IsCurrent, "ObjectScope.Current belonged to another execution flow");
                Assert.AreEqual(1, result.Count, "A parallel flow's scope was visible as a nested scope");
            }

            Assert.AreEqual(0, ObjectScope.ScopeCount);
            Assert.IsNull(ObjectScope.Current);
        }

        [TestMethod]
        public async Task Scope_OpenedInChildFlow_IsNotVisibleToParent()
        {
            await Task.Run(() =>
            {
                ObjectScope.New(new Person { Id = 1 });
                Assert.AreEqual(1, ObjectScope.ScopeCount);
            });

            Assert.AreEqual(0, ObjectScope.ScopeCount);
            Assert.IsNull(ObjectScope.Current);
        }

        [TestMethod]
        public async Task Scope_OpenedInParent_IsVisibleToChildFlow()
        {
            using (var scope = ObjectScope.New(new Person { Id = 1 }))
            {
                var current = await Task.Run(() => ObjectScope.Current);
                Assert.AreSame(scope, current);
            }
        }

        [TestMethod]
        public async Task Scope_SurvivesAwaitWithinSameFlow()
        {
            using (var scope = ObjectScope.New(new Person { Id = 1 }))
            {
                await Task.Yield();
                Assert.AreSame(scope, ObjectScope.Current);
            }

            Assert.AreEqual(0, ObjectScope.ScopeCount);
        }

        [TestMethod]
        public async Task Scope_DisposedInChildFlow_IsNoLongerAmbientInParent()
        {
            var scope = ObjectScope.New(new Person { Id = 1 });

            await Task.Run(() => scope.Dispose());

            Assert.AreEqual(0, ObjectScope.ScopeCount);
            Assert.IsNull(ObjectScope.Current);
        }

        [TestMethod]
        public async Task Scopes_OpenedInManyParallelFlows_DoNotCorruptState()
        {
            var tasks = new Task[64];
            for (var i = 0; i < tasks.Length; i++)
            {
                var id = i + 1;
                tasks[i] = Task.Run(async () =>
                {
                    for (var iteration = 0; iteration < 50; iteration++)
                    {
                        using (var outer = ObjectScope.New(new Person { Id = id }))
                        {
                            await Task.Yield();
                            using (var inner = ObjectScope.New(new Person { Id = id }))
                            {
                                await Task.Yield();
                                Assert.AreEqual(2, ObjectScope.ScopeCount);
                                Assert.AreSame(inner, ObjectScope.Current);
                            }
                            Assert.AreSame(outer, ObjectScope.Current);
                        }
                        Assert.AreEqual(0, ObjectScope.ScopeCount);
                    }
                });
            }

            await Task.WhenAll(tasks);
            Assert.AreEqual(0, ObjectScope.ScopeCount);
        }
    }
}
