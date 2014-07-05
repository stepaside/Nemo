using System;
using System.Runtime.InteropServices;
using System.Diagnostics;

namespace Nemo.Utilities
{
    public class HiPerfTimer
    {
     
        [DllImport("kernel32.dll")]
		extern static short QueryPerformanceCounter(ref long x);
		[DllImport("kernel32.dll")]
		extern static short QueryPerformanceFrequency(ref long x);

        private static readonly bool _usePerformanceCounter = IsWindows && !Stopwatch.IsHighResolution;

		private long _startTime;
		private long _stopTime;
		private long _clockFrequency;
		private long _calibrationTime;
        private bool _isStopped;

        private Stopwatch _timer;
        private readonly double _microSecPerTick = 1000000D / Stopwatch.Frequency;

        public HiPerfTimer() : this(true) { }

        public HiPerfTimer(bool calibrate)
        {
            _startTime = 0;
            _stopTime = 0;

            if (!_usePerformanceCounter)
            {
                _timer = new Stopwatch();
            }
			
            _clockFrequency = 0;
            if (!calibrate && _usePerformanceCounter)
            {
                QueryPerformanceFrequency(ref _clockFrequency);
            }

            _calibrationTime = 0;
            if (calibrate)
            {
                Calibrate();
            }
        }

        public void Calibrate()
        {
            if (_usePerformanceCounter)
            {
                QueryPerformanceFrequency(ref _clockFrequency);
            }

            for (int i = 0; i < 1000; i++)
            {
                Start();
                Stop();
                _calibrationTime += _stopTime - _startTime;
            }

            _calibrationTime /= 1000;
        }


		public void Reset()
		{
            if (_usePerformanceCounter)
            {
                _startTime = 0;
                _stopTime = 0;
            }
            else
            {
                _timer.Reset();
            }
            _isStopped = false;
		}

		public void Start()
		{
            if (_usePerformanceCounter)
            {
                QueryPerformanceCounter(ref _startTime);
            }
            else
            {
                _timer.Start();
            }
            _isStopped = false;
		}

		public void Stop()
		{
            if (_usePerformanceCounter)
            {
                QueryPerformanceCounter(ref _stopTime);
            }
            else
            {
                _timer.Stop();
            }
            _isStopped = true;
		}

		public TimeSpan GetElapsedTimeSpan()
		{
            return TimeSpan.FromMilliseconds(GetElapsedTime());
		}

        public double GetElapsedTime()
        {
            return GetElapsedTimeInMicroseconds() / 1000.0; 
        }

		public double GetElapsedTimeInMicroseconds()
        {
            if (!_isStopped)
            {
                return double.NaN;
            }

            if (_usePerformanceCounter)
            {
                return (((_stopTime - _startTime - _calibrationTime) * 1000000.0 / _clockFrequency));
            }
            else
            {
                return _timer.ElapsedTicks * _microSecPerTick;
            }
        }

        public TimeSpan GetSplitTimeSpan()
        {
            return TimeSpan.FromMilliseconds(GetSplitTime());
        }

        public double GetSplitTime()
		{
            return GetSplitTimeInMicroseconds() / 1000.0;
		}

        public double GetSplitTimeInMicroseconds()
        {
            if (_usePerformanceCounter)
            {
                long current_count = 0;
                QueryPerformanceCounter(ref current_count);
                return (((current_count - _startTime - _calibrationTime) * 1000000.0 / _clockFrequency));
            }
            else
            {
                return _timer.ElapsedTicks * _microSecPerTick;
            }
        }

        private static bool IsLinux
        {
            get
            {
                var platform = (int)Environment.OSVersion.Platform;
                return platform == 4 || platform == 6 || platform == 128;
            }
        }

        private static bool IsWindows
        {
            get
            {
                var platform = Environment.OSVersion.Platform;
                return platform == PlatformID.Win32NT || platform == PlatformID.Win32S || platform == PlatformID.Win32Windows || platform == PlatformID.WinCE;
            }
        }
    }
}
