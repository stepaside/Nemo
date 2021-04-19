using BenchmarkDotNet.Running;
using System;

namespace Nemo.Benchmark
{
    class Program
    {
        static void Main(string[] args)
        {
            var summary = BenchmarkRunner.Run<OrmBenchmark>();
            Console.WriteLine(summary.Reports.Length);
            Console.ReadLine();
        }
    }
}
