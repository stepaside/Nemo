using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;
using System;

namespace Nemo.Benchmark
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                DatabaseSetup.CreateNorthwindDatabase();
            }
            catch (System.Exception ex)
            {
                System.Console.WriteLine($"Database setup failed: {ex.Message}");
                return;
            }

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
