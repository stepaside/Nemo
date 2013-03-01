using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace Nemo.Security.Cryptography
{
    public class Hash
    {
        static RandomNumberGenerator rng = new RNGCryptoServiceProvider();
        static byte[] b = new byte[4];

        private static int GetRandomLength()
        {
            rng.GetBytes(b);
            uint s = BitConverter.ToUInt32(b, 0);
            double x = 1 - s / (uint.MaxValue + 1.0);
            return (int)Math.Floor(Math.Sqrt(-800.0 * Math.Log(x)));
        }

        public static byte[] GetUniformKey()
        {
            // with 8 bits/byte we need at least two octets 
            int length = GetRandomLength() + 2;
            byte[] key = new byte[length];
            rng.GetBytes(key);
            return key;
        }

        public static byte[] GetTextKey()
        {
            // with 4.34 bits/byte we need at least 4 octets 
            int length = GetRandomLength() + 4;
            byte[] key = new byte[length];
            rng.GetBytes(key);
            for (int i = 0; i < length; i++)
                key[i] = (byte)(65 + (key[i] * key[i] * 26) / 65026);
            return key;
        }

        public static byte[] GetSparseKey()
        {
            // with 3 bits/byte we need at least 6 octets 
            int length = GetRandomLength() + 6;
            byte[] key = new byte[length];
            rng.GetBytes(key);
            for (int i = 0; i < length; i++)
                key[i] = (byte)(1 << (key[i] & 7));
            return key;
        }
    }
}
