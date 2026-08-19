using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;

namespace Nemo.Benchmark
{
    class Program
    {
        // Supports standard BenchmarkDotNet arguments, e.g.:
        //   --anyCategories SelectAll     run only the select-all benchmarks
        //   --anyCategories SelectById    run only the by-id benchmarks
        //   --filter *Dapper*             run benchmarks matching a name pattern
        static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                args = new[] { "--filter", "*" };
            }
#if DEBUG
            BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, new DebugInProcessConfig());
#else
            BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
#endif
        }
    }
}
