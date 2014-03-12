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
        public const string LOG_FILE = "File";
        public const string LOG_ROLLING_FILE = "RollingFile";
        public const string LOG_EMAIL = "Email";
        public const string LOG_DATABASE = "Database";
        public const string LOG_EVENT_VIEWER = "EventViewer";

        private const string LOG_CONTEXT_NAME = "LogContext";

        public static bool IsEnabled
        {
            get
            {
                return ConfigurationFactory.Configuration.Logging;
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
            if (IsEnabled)
            {
                if (File.Exists(configFile))
                {
                    XmlConfigurator.Configure(new FileInfo(configFile));
                }
            }
        }

        public static void Capture(string loggerName, Func<string> computeMessage)
        {
            if (IsEnabled)
            {
                ILog logger = LogManager.GetLogger(loggerName);
                if (logger != null)
                {
                    var context = Log.Context;
                    var message = computeMessage();
                    if (context.Item1 != Guid.Empty && context.Item2 != null)
                    {
                        message = string.Format("{0}-{1}", context.Item1, message);
                    }
                    logger.Info(message);
                }
            }
        }

        public static void Capture(Func<string> computeMessage)
        {
            Capture(LOG_ROLLING_FILE, computeMessage);
        }

        public static void Capture(string loggerName, string message)
        {
            Capture(loggerName, () => message);
        }

        public static void Capture(string message)
        {
            Capture(LOG_ROLLING_FILE, message);
        }
        
        public static bool CaptureBegin(string loggerName, Func<string> computeMessage)
        {
            if (IsEnabled)
            {
                CreateContext();
                var context = Log.Context;
                Capture(loggerName, computeMessage);
                context.Item2.Start();
                return true;
            }
            return false;
        }

        public static bool CaptureBegin(Func<string> computeMessage)
        {
            return CaptureBegin(LOG_ROLLING_FILE, computeMessage);
        }

        public static bool CaptureBegin(string loggerName, string message)
        {
            return CaptureBegin(loggerName, () => message);
        }

        public static bool CaptureBegin(string message)
        {
            return CaptureBegin(LOG_ROLLING_FILE, message);
        }

        public static void CaptureEnd(string loggerName)
        {
            if (IsEnabled)
            {
                var context = Log.Context;
                if (context.Item2 != null)
                {
                    context.Item2.Stop();
                    var message = Convert.ToString(context.Item2.GetElapsedTimeInMicroseconds() / 1000.0);
                    Capture(loggerName, message);
                    ClearContext();
                }
            }
        }

        public static void CaptureEnd()
        {
            CaptureEnd(LOG_ROLLING_FILE);
        }

        private static Tuple<Guid, HiPerfTimer> Context
        {
            get
            {
                object context;
                if (ConfigurationFactory.Configuration.ExecutionContext.TryGet(LOG_CONTEXT_NAME, out context))
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
            if (ConfigurationFactory.Configuration.ExecutionContext.TryGet(LOG_CONTEXT_NAME, out context))
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
            object logContext;
            if (!ConfigurationFactory.Configuration.ExecutionContext.TryGet(LOG_CONTEXT_NAME, out logContext))
            {
                logContext = new Stack<Tuple<Guid, HiPerfTimer>>();
                ConfigurationFactory.Configuration.ExecutionContext.Set(LOG_CONTEXT_NAME, logContext);
            }

            if (logContext != null)
            {
                ((Stack<Tuple<Guid, HiPerfTimer>>)logContext).Push(Tuple.Create(Guid.NewGuid(), new HiPerfTimer()));
            }
        }
    }
}
