using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Nemo;
using Nemo.Collections;

namespace Nemo.UnitTests
{
    [TestClass]
    public class AsyncStreamingTests
    {
        public class Item
        {
            public int Id { get; set; }
        }

        private static EagerLoadEnumerableAsync<Item> CreateSource(IEnumerable<Item> items, SelectOption selectOption = SelectOption.All, Func<CancellationToken, Task> onLoad = null)
        {
            return new EagerLoadEnumerableAsync<Item>(new[] { "sql" }, new[] { typeof(Item) },
                async (sql, types, token) =>
                {
                    if (onLoad != null) await onLoad(token);
                    return items;
                },
                null, null, selectOption, null, null, 0, 0, 0, null);
        }

        [TestMethod]
        public async Task AwaitForeach_YieldsAllRows()
        {
            var source = CreateSource(new[] { new Item { Id = 1 }, new Item { Id = 2 }, new Item { Id = 3 } });

            var ids = new List<int>();
            await foreach (var item in source)
            {
                ids.Add(item.Id);
            }

            CollectionAssert.AreEqual(new[] { 1, 2, 3 }, ids);
        }

        [TestMethod]
        public async Task AwaitForeach_EmptyResult_YieldsNothing()
        {
            var source = CreateSource(Enumerable.Empty<Item>());

            var count = 0;
            await foreach (var _ in source)
            {
                count++;
            }

            Assert.AreEqual(0, count);
        }

        [TestMethod]
        public async Task AwaitForeach_FirstOrDefault_EmptyResult_YieldsNothing()
        {
            var source = CreateSource(Enumerable.Empty<Item>(), SelectOption.FirstOrDefault);

            var count = 0;
            await foreach (var _ in source)
            {
                count++;
            }

            Assert.AreEqual(0, count);
        }

        [TestMethod]
        public async Task AwaitForeach_First_YieldsSingleRow()
        {
            var source = CreateSource(new[] { new Item { Id = 1 }, new Item { Id = 2 } }, SelectOption.First);

            var ids = new List<int>();
            await foreach (var item in source)
            {
                ids.Add(item.Id);
            }

            CollectionAssert.AreEqual(new[] { 1 }, ids);
        }

        [TestMethod]
        public async Task GetAsyncEnumerator_PassesCancellationTokenToLoader()
        {
            using var cts = new CancellationTokenSource();
            CancellationToken observed = default;
            var source = CreateSource(new[] { new Item { Id = 1 } }, onLoad: token =>
            {
                observed = token;
                return Task.CompletedTask;
            });

            var enumerator = source.GetAsyncEnumerator(cts.Token);
            await enumerator.MoveNextAsync();
            await enumerator.DisposeAsync();

            Assert.AreEqual(cts.Token, observed);
        }

        [TestMethod]
        public async Task MoveNextAsync_PreCancelledToken_Throws()
        {
            using var cts = new CancellationTokenSource();
            cts.Cancel();
            var source = CreateSource(new[] { new Item { Id = 1 } });

            var enumerator = source.GetAsyncEnumerator(cts.Token);
            await Assert.ThrowsExceptionAsync<OperationCanceledException>(async () => await enumerator.MoveNextAsync());
            await enumerator.DisposeAsync();
        }

        [TestMethod]
        public async Task MoveNextAsync_CancelledDuringEnumeration_Throws()
        {
            using var cts = new CancellationTokenSource();
            var source = CreateSource(new[] { new Item { Id = 1 }, new Item { Id = 2 } });

            var enumerator = source.GetAsyncEnumerator(cts.Token);
            Assert.IsTrue(await enumerator.MoveNextAsync());
            cts.Cancel();
            await Assert.ThrowsExceptionAsync<OperationCanceledException>(async () => await enumerator.MoveNextAsync());
            await enumerator.DisposeAsync();
        }

        [TestMethod]
        public async Task DisposeAsync_WithoutEnumeration_DoesNotThrow()
        {
            var source = CreateSource(new[] { new Item { Id = 1 } });

            var enumerator = source.GetAsyncEnumerator();
            await enumerator.DisposeAsync();
        }

        [TestMethod]
        public async Task MoveNextAsync_AfterExhaustion_ReturnsFalse()
        {
            var source = CreateSource(new[] { new Item { Id = 1 } });

            var enumerator = source.GetAsyncEnumerator();
            Assert.IsTrue(await enumerator.MoveNextAsync());
            Assert.IsFalse(await enumerator.MoveNextAsync());
            Assert.IsFalse(await enumerator.MoveNextAsync());
            await enumerator.DisposeAsync();
        }

        [TestMethod]
        public async Task ToEnumerableAsync_PassesCancellationTokenToLoader()
        {
            using var cts = new CancellationTokenSource();
            CancellationToken observed = default;
            var source = CreateSource(new[] { new Item { Id = 1 } }, onLoad: token =>
            {
                observed = token;
                return Task.CompletedTask;
            });

            var result = await ObjectFactory.ToEnumerableAsync(source, cts.Token);

            Assert.AreEqual(cts.Token, observed);
            Assert.AreEqual(1, result.First().Id);
        }
    }
}
