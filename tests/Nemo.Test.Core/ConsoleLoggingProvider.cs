using Nemo.Configuration;
using System;

namespace NemoTest
{
    internal class ConsoleLoggingProvider : ILogProvider
    {
        public void Configure()
        {
            
        }

        public void Configure(string configFile)
        {
            
        }

        public void Write(string message)
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine(message);
            Console.ResetColor();
        }

        public void Write(Exception exception, string id)
        {
            if (exception == null) return;

            Console.ForegroundColor = ConsoleColor.Red;
            var message = exception.ToString();
            if (string.Equals(message, exception.GetType().FullName))
            {
                message = exception.Message;
            }
            Console.WriteLine(message);
            Console.ResetColor();
        }
    }
}