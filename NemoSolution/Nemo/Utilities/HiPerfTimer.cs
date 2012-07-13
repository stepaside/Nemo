using System;
using System.Runtime.InteropServices;

namespace Nemo.Utilities
{
    public class HiPerfTimer
    {
        [DllImport("kernel32.dll")]
		extern static short QueryPerformanceCounter(ref long x);
		[DllImport("kernel32.dll")]
		extern static short QueryPerformanceFrequency(ref long x);

		private long _startTime;
		private long _stopTime;
		private long _clockFrequency;
		private long _calibrationTime;

        public HiPerfTimer() : this(true) { }

        public HiPerfTimer(bool calibrate)
		{
			_startTime       = 0;
			_stopTime        = 0;
			
            _clockFrequency  = 0;
            if (!calibrate)
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
            QueryPerformanceFrequency(ref _clockFrequency);

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
			_startTime = 0;
			_stopTime  = 0;
		}

		public void Start()
		{
			QueryPerformanceCounter(ref _startTime);
		}

		public void Stop()
		{
			QueryPerformanceCounter(ref _stopTime);
		}

		public TimeSpan GetElapsedTimeSpan()
		{
			return TimeSpan.FromMilliseconds(_GetElapsedTime_ms());
		}

		public TimeSpan GetSplitTimeSpan()
		{
			return TimeSpan.FromMilliseconds(_GetSplitTime_ms());
		}

		public double GetElapsedTimeInMicroseconds()
		{
			return (((_stopTime - _startTime - _calibrationTime) * 1000000.0 / _clockFrequency));
		}

		public double GetSplitTimeInMicroseconds()
		{
			long current_count = 0;
			QueryPerformanceCounter(ref current_count);
			return (((current_count - _startTime - _calibrationTime) * 1000000.0 / _clockFrequency));
		}

		private double _GetSplitTime_ms()
		{
			long current_count = 0;
			QueryPerformanceCounter(ref current_count);
			return (((current_count - _startTime - _calibrationTime) * 1000000.0 / _clockFrequency) / 1000.0);
		}

		private double _GetElapsedTime_ms()
		{
			return (((_stopTime - _startTime - _calibrationTime) * 1000000.0 / _clockFrequency) / 1000.0);
		}
    }
}
