using System;

namespace Nemo.Configuration
{
    public interface ILogProvider
    {
        void Configure();
        void Configure(string configFile);
        void Write(string message);
        void Write(Exception exception, string id);
    }
}