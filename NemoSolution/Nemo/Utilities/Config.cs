using System;
using System.Configuration;
using System.Collections.Generic;
using System.Web;

namespace Nemo.Utilities
{
	public static class Config
	{
		public static string AppSettings(string name)
		{
			return ConfigurationManager.AppSettings[name];
		}

		public static string AppSettings(string name, string defaultValue)
		{
			return AppSettings(name) ?? defaultValue;
		}

		public static int AppSettings(string name, int defaultValue)
		{
			var value = AppSettings(name);
			if (string.IsNullOrEmpty(value))
			{
				return defaultValue;
			}

			int result;
			if (!int.TryParse(value, out result))
			{
				result = defaultValue;
			}
			return result;
		}

		public static bool AppSettings(string name, bool defaultValue)
		{
			var value = AppSettings(name);
			if (string.IsNullOrEmpty(value))
			{
				return defaultValue;
			}

			bool result;
			if (!bool.TryParse(value, out result))
			{
				result = defaultValue;
			}
			return result;
		}

        public static double AppSettings(string name, double defaultValue)
        {
            var value = AppSettings(name);
            if (string.IsNullOrEmpty(value))
            {
                return defaultValue;
            }

            double result;
            if (!double.TryParse(value, out result))
            {
                result = defaultValue;
            }
            return result;
        }

        public static TEnum AppSettings<TEnum>(string name, TEnum defaultValue)
            where TEnum : struct
        {
            if (!typeof(TEnum).IsEnum)
            {
                return defaultValue;
            }

            var value = AppSettings(name);
            if (string.IsNullOrEmpty(value))
            {
                return defaultValue;
            }

            TEnum result;
            if (!Enum.TryParse(value, true, out result))
            {
                result = defaultValue;
            }
            return result;
        }

		public static string ConnectionString(string keyName)
		{
			ConnectionStringSettings connSettings = ConnectionStringSetting(keyName);

			return (connSettings != null)? connSettings.ConnectionString : null;
		}

		public static ConnectionStringSettings ConnectionStringSetting(string keyName)
		{
			return ConfigurationManager.ConnectionStrings[keyName];
		}

		public static T ConfigSection<T>(string name)
			where T : ConfigurationSection
		{
			return (T)ConfigurationManager.GetSection(name);
		}
	}
}
