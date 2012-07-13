using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Nemo.Extensions;

namespace Nemo.Utilities
{
    public class StringStream : Stream
    {
        private readonly string _source;
        private readonly long _byteLength;
        private int _position;

        public StringStream(string source)
        {
            source.ThrowIfNull("source");
            _source = source;
            _byteLength = _source.Length * 2;
            _position = 0;
        }

        public override bool CanRead { get { return true; } }
        public override bool CanSeek { get { return true; } }
        public override bool CanWrite { get { return false; } }

        public override long Length { get { return _byteLength; } }

        public override long Position
        {
            get { return _position; }
            set
            {
                if (value < 0 || value > int.MaxValue)
                    throw new ArgumentOutOfRangeException("Position");
                _position = (int)value;
            }
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            switch (origin)
            {
                case SeekOrigin.Begin: Position = offset; break;
                case SeekOrigin.End: Position = _byteLength + offset; break;
                case SeekOrigin.Current: Position = Position + offset; break;
            }
            return Position;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (_position < 0) throw new InvalidOperationException();

            int bytesRead = 0;
            while (bytesRead < count)
            {
                if (_position >= _byteLength) return bytesRead;

                char c = _source[_position / 2];
                buffer[offset + bytesRead] = (byte)((_position % 2 == 0) ?
                    c & 0xFF : (c >> 8) & 0xFF);
                Position++;
                bytesRead++;
            }
            return bytesRead;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        public override void Flush()
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }
    }
}
