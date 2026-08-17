using System;
using System.Collections.Generic;
using System.Linq;
using Nemo.Collections;

namespace Nemo.UnitTests
{
    [TestClass]
    public class MemoizedEnumerableTests
    {
        private static IEnumerable<int> Counting(Action onIterate, int count)
        {
            for (var i = 0; i < count; i++)
            {
                onIterate();
                yield return i;
            }
        }

        [TestMethod]
        public void Memoize_EnumeratesSourceOnlyOnce()
        {
            var iterations = 0;
            var buffer = Counting(() => iterations++, 5).Memoize();

            var first = buffer.ToList();
            var second = buffer.ToList();

            CollectionAssert.AreEqual(first, second);
            Assert.AreEqual(5, iterations);
        }

        [TestMethod]
        public void Memoize_SupportsPartialThenFullEnumeration()
        {
            var iterations = 0;
            var buffer = Counting(() => iterations++, 5).Memoize();

            var head = buffer.Take(2).ToList();
            var all = buffer.ToList();

            CollectionAssert.AreEqual(new List<int> { 0, 1 }, head);
            CollectionAssert.AreEqual(new List<int> { 0, 1, 2, 3, 4 }, all);
            Assert.AreEqual(5, iterations);
        }

        [TestMethod]
        public void Memoize_ReturnsIBuffer()
        {
            Assert.IsInstanceOfType(new[] { 1, 2, 3 }.Memoize(), typeof(IBuffer<int>));
        }

        [TestMethod]
        public void Memoize_ThrowsAfterDispose()
        {
            var buffer = new[] { 1, 2, 3 }.Memoize();
            buffer.Dispose();
            Assert.ThrowsException<ObjectDisposedException>(() => buffer.ToList());
        }

        [TestMethod]
        public void Memoize_NullSource_Throws()
        {
            Assert.ThrowsException<ArgumentNullException>(() => ((IEnumerable<int>)null).Memoize());
        }
    }
}
