using Nemo.Extensions;
using Nemo.Fn;

namespace Nemo.Utilities
{
    public static class Numbers
    {
        public static Stream<ulong> Hamming()
        {
            Stream<ulong> hamming = null;
            hamming = new Stream<ulong>(1, () => hamming.Map(x => x * 2).Merge(hamming.Map(x => x * 3).Merge(hamming.Map(x => x * 5))));
            return hamming;
        }

        //does not overflow upto 998 (inclusively)
        public static Stream<ulong> Fibonacci()
        {
            Stream<ulong> fibs = null;
            fibs = new Stream<ulong>(0).Merge(new Stream<ulong>(1, () => fibs.ZipWith(fibs.Tail, (a, b) => a + b)));
            return fibs;
        }

        public static Stream<ulong> Primes()
        {
            var naturals = Naturals();
            var naturalsFromTwo = naturals.Tail.Tail;
            return PrimesImplementation(naturalsFromTwo);
        }

        private static Stream<ulong> PrimesImplementation(Stream<ulong> stream)
        {
            return new Stream<ulong>(stream.Head, () => PrimesImplementation(stream.Tail.Filter(x => x % stream.Head != 0)));
        }

        public static Stream<ulong> Naturals()
        {
            var ones = Ones();
            Stream<ulong> naturals = null;
            naturals = new Stream<ulong>(0, () => ones.ZipWith(naturals, (x, y) => x + y));
            return naturals;
        }

        public static Stream<ulong> Ones()
        {
            var ones = LinqExtensions.Generate((ulong)1, x => (ulong)1).AsStream();
            return ones;
        }
    }
}
