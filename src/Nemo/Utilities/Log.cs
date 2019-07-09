using System.Diagnostics;
using Nemo.Configuration;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Web;

namespace Nemo.Utilities
{
    public static class Log
    {
        private const string LogContextName = "__LogContext";
        
        public static bool IsEnabled => ConfigurationFactory.DefaultConfiguration.Logging && LogProvider != null;

        private static ILogProvider LogProvider => ConfigurationFactory.DefaultConfiguration.LogProvider;

        public static void Configure()
        {
            if (IsEnabled)
            {
                LogProvider.Configure();
            }
        }

        public static void Configure(string configFile)
        {
            if (!IsEnabled) return;

            if (File.Exists(configFile))
            {
                LogProvider.Configure(configFile);
            }
        }

        public static void Capture(Func<string> computeMessage)
        {
            if (!IsEnabled) return;

            var context = Context;
            var message = computeMessage();
            if (context.Item1 != Guid.Empty && context.Item2 != null)
            {
                message = $"{context.Item1}: {message}";
            }
            LogProvider.Write(message);
        }

        public static void Capture(Exception error)
        {
            if (!IsEnabled) return;

            var context = Context;
            LogProvider.Write(error, context.Item1 != Guid.Empty ? context.Item1.ToString() : null);
        }

        public static void Capture(string message)
        {
            Capture(() => message);
        }
        
        public static bool CaptureBegin(Func<string> computeMessage)
        {
            if (!IsEnabled) return false;

            CreateContext();
            var context = Context;
            Capture(computeMessage);
            context.Item2.Start();
            return true;
        }

        public static bool CaptureBegin(string message)
        {
            return CaptureBegin(() => message);
        }
        
        public static void CaptureEnd()
        {
            if (!IsEnabled) return;
            
            var context = Context;
            if (context.Item2 == null) return;
            
            context.Item2.Stop();
            var message = Convert.ToString(context.Item2.Elapsed.TotalMilliseconds, CultureInfo.InvariantCulture);
            Capture(message);
            ClearContext();
        }

        private static Tuple<Guid, Stopwatch> Context
        {
            get
            {
                if (ConfigurationFactory.DefaultConfiguration.ExecutionContext.TryGet(LogContextName, out var context))
                {
                    var logContext = (Stack<Tuple<Guid, Stopwatch>>)context;
                    if (logContext != null && logContext.Count > 0)
                    {
                        return logContext.Peek();
                    }
                }
                return new Tuple<Guid, Stopwatch>(Guid.Empty, null);
            }
        }

        private static void ClearContext()
        {
            if (ConfigurationFactory.DefaultConfiguration.ExecutionContext.TryGet(LogContextName, out var context))
            {
                var logContext = (Stack<Tuple<Guid, Stopwatch>>)context;
                if (logContext != null && logContext.Count > 0)
                {
                    logContext.Pop();
                }
            }
        }

        private static void CreateContext()
        {
            var executionContext = ConfigurationFactory.DefaultConfiguration.ExecutionContext;

            if (!executionContext.TryGet(LogContextName, out var logContext))
            {
                logContext = new Stack<Tuple<Guid, Stopwatch>>();
                executionContext.Set(LogContextName, logContext);
            }

            ((Stack<Tuple<Guid, Stopwatch>>)logContext)?.Push(Tuple.Create(Guid.NewGuid(), new Stopwatch()));
        }
    }
}
