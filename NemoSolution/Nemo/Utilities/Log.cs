using System.Diagnostics;
using log4net;
using log4net.Config;
using Nemo.Configuration;
using System;
using System.Collections.Generic;
using System.IO;
using System.Web;

namespace Nemo.Utilities
{
    public static class Log
    {
        private const string LogContextName = "__LogContext";
        private const string LoggerName = "Nemo";

        public static bool IsEnabled
        {
            get
            {
                return ConfigurationFactory.Default.Logging;
            }
        }

        public static void Configure()
        {
            if (IsEnabled)
            {
                XmlConfigurator.Configure();
            }
        }

        public static void Configure(string configFile)
        {
            if (!IsEnabled) return;

            if (File.Exists(configFile))
            {
                XmlConfigurator.Configure(new FileInfo(configFile));
            }
        }

        public static void Capture(Func<string> computeMessage)
        {
            if (!IsEnabled) return;

            var logger = LogManager.GetLogger(LoggerName);
            if (logger == null) return;
            
            var context = Context;
            var message = computeMessage();
            if (context.Item1 != Guid.Empty && context.Item2 != null)
            {
                message = string.Format("{0}-{1}", context.Item1, message);
            }
            logger.Info(message);
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
            var message = Convert.ToString(context.Item2.Elapsed.TotalMilliseconds);
            Capture(message);
            ClearContext();
        }

        private static Tuple<Guid, Stopwatch> Context
        {
            get
            {
                object context;
                if (ConfigurationFactory.Default.ExecutionContext.TryGet(LogContextName, out context))
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
            object context;
            if (ConfigurationFactory.Default.ExecutionContext.TryGet(LogContextName, out context))
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
            var executionContext = ConfigurationFactory.Default.ExecutionContext;

            object logContext;
            if (!executionContext.TryGet(LogContextName, out logContext))
            {
                logContext = new Stack<Tuple<Guid, Stopwatch>>();
                executionContext.Set(LogContextName, logContext);
            }

            if (logContext != null)
            {
                ((Stack<Tuple<Guid, Stopwatch>>)logContext).Push(Tuple.Create(Guid.NewGuid(), new Stopwatch()));
            }
        }
    }
}
