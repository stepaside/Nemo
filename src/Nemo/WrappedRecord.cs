using System;
using System.Collections.Generic;
using System.Data;

namespace Nemo
{
    internal class WrappedRecord : IDataRecord
    {
        private readonly IDataRecord _record;
        private readonly ISet<string> _columns;

        public WrappedRecord(IDataRecord record, ISet<string> columns)
        {
            _record = record ?? throw new ArgumentNullException(nameof(record));
            _columns = columns ?? record.GetColumns();
        }

        public object this[int i] => _record[i];

        public object this[string name]
        {
            get
            {
                if (_columns.Contains(name))
                {
                    return _record[name];
                }
                return default;
            }
        }

        public int FieldCount => _record.FieldCount;

        public bool GetBoolean(int i)
        {
            return _record.GetBoolean(i);
        }

        public byte GetByte(int i)
        {
            return _record.GetByte(i);
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            return _record.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
        }

        public char GetChar(int i)
        {
            return _record.GetChar(i);
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            return _record.GetChars(i, fieldoffset, buffer, bufferoffset, length);
        }

        public IDataReader GetData(int i)
        {
            return _record.GetData(i);
        }

        public string GetDataTypeName(int i)
        {
            return _record.GetDataTypeName(i);
        }

        public DateTime GetDateTime(int i)
        {
            return _record.GetDateTime(i);
        }

        public decimal GetDecimal(int i)
        {
            return _record.GetDecimal(i);
        }

        public double GetDouble(int i)
        {
            return _record.GetDouble(i);
        }

        public Type GetFieldType(int i)
        {
            return _record.GetFieldType(i);
        }

        public float GetFloat(int i)
        {
            return _record.GetFloat(i);
        }

        public Guid GetGuid(int i)
        {
            return _record.GetGuid(i);
        }

        public short GetInt16(int i)
        {
            return _record.GetInt16(i);
        }

        public int GetInt32(int i)
        {
            return _record.GetInt32(i);
        }

        public long GetInt64(int i)
        {
            return _record.GetInt64(i);
        }

        public string GetName(int i)
        {
            return _record.GetName(i);
        }

        public int GetOrdinal(string name)
        {
            return _record.GetOrdinal(name);
        }

        public string GetString(int i)
        {
            return _record.GetString(i);
        }

        public object GetValue(int i)
        {
            return _record.GetValue(i);
        }

        public int GetValues(object[] values)
        {
            return _record.GetValues(values);
        }

        public bool IsDBNull(int i)
        {
            return _record.IsDBNull(i);
        }
    }

}
