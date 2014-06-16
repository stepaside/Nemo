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
        public const string LogFile = "File";
        public const string LogRollingFile = "RollingFile";
        public const string LogEmail = "Email";
        public const string LogDatabase = "Database";
        public const string LogEventViewer = "EventViewer";

        private const string LogContextName = "LogContext";

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

        public static void Capture(string loggerName, Func<string> computeMessage)
        {
            if (!IsEnabled) return;
            
            var logger = LogManager.GetLogger(loggerName);
            if (logger == null) return;
            
            var context = Context;
            var message = computeMessage();
            if (context.Item1 != Guid.Empty && context.Item2 != null)
            {
                message = string.Format("{0}-{1}", context.Item1, message);
            }
            logger.Info(message);
        }

        public static void Capture(Func<string> computeMessage)
        {
            Capture(LogRollingFile, computeMessage);
        }

        public static void Capture(string loggerName, string message)
        {
            Capture(loggerName, () => message);
        }

        public static void Capture(string message)
        {
            Capture(LogRollingFile, message);
        }
        
        public static bool CaptureBegin(string loggerName, Func<string> computeMessage)
        {
            if (IsEnabled)
            {
                CreateContext();
                var context = Context;
                Capture(loggerName, computeMessage);
                context.Item2.Start();
                return true;
            }
            return false;
        }

        public static bool CaptureBegin(Func<string> computeMessage)
        {
            return CaptureBegin(LogRollingFile, computeMessage);
        }

        public static bool CaptureBegin(string loggerName, string message)
        {
            return CaptureBegin(loggerName, () => message);
        }

        public static bool CaptureBegin(string message)
        {
            return CaptureBegin(LogRollingFile, message);
        }

        public static void CaptureEnd(string loggerName)
        {
            if (!IsEnabled) return;
            
            var context = Context;
            if (context.Item2 == null) return;
            
            context.Item2.Stop();
            var message = Convert.ToString(context.Item2.GetElapsedTimeInMicroseconds() / 1000.0);
            Capture(loggerName, message);
            ClearContext();
        }

        public static void CaptureEnd()
        {
            CaptureEnd(LogRollingFile);
        }

        private static Tuple<Guid, HiPerfTimer> Context
        {
            get
            {
                object context;
                if (ConfigurationFactory.Default.ExecutionContext.TryGet(LogContextName, out context))
                {
                    var logContext = (Stack<Tuple<Guid, HiPerfTimer>>)context;
                    if (logContext != null && logContext.Count > 0)
                    {
                        return logContext.Peek();
                    }
                }
                return new Tuple<Guid, HiPerfTimer>(Guid.Empty, null);
            }
        }

        private static void ClearContext()
        {
            object context;
            if (ConfigurationFactory.Default.ExecutionContext.TryGet(LogContextName, out context))
            {
                var logContext = (Stack<Tuple<Guid, HiPerfTimer>>)context;
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
                logContext = new Stack<Tuple<Guid, HiPerfTimer>>();
                executionContext.Set(LogContextName, logContext);
            }

            if (logContext != null)
            {
                ((Stack<Tuple<Guid, HiPerfTimer>>)logContext).Push(Tuple.Create(Guid.NewGuid(), new HiPerfTimer()));
            }
        }
    }
}
