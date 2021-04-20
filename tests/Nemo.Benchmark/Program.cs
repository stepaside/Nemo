using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;
using System;

namespace Nemo.Benchmark
{
    class Program
    {
        static void Main(string[] args)
        {
#if DEBUG
            var summary = BenchmarkRunner.Run<OrmBenchmark>(new DebugInProcessConfig());
#else
            var summary = BenchmarkRunner.Run<OrmBenchmark>();
#endif
            Console.WriteLine(summary.Reports.Length);
            Console.ReadLine();
        }
    }
}
