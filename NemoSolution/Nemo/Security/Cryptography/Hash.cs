using System;
using System.Security.Cryptography;

namespace Nemo.Security.Cryptography
{
    public class Hash
    {
        private static readonly RandomNumberGenerator rng = new RNGCryptoServiceProvider();
        private static readonly byte[] b = new byte[4];

        private static int GetRandomLength()
        {
            rng.GetBytes(b);
            uint s = BitConverter.ToUInt32(b, 0);
            double x = 1 - s/(uint.MaxValue + 1.0);
            return (int)Math.Floor(Math.Sqrt(-800.0*Math.Log(x)));
        }

        public static byte[] GetUniformKey()
        {
            // with 8 bits/byte we need at least two octets 
            var length = GetRandomLength() + 2;
            var key = new byte[length];
            rng.GetBytes(key);
            return key;
        }

        public static byte[] GetTextKey()
        {
            // with 4.34 bits/byte we need at least 4 octets 
            var length = GetRandomLength() + 4;
            var key = new byte[length];
            rng.GetBytes(key);
            for (var i = 0; i < length; i++)
            {
                key[i] = (byte)(65 + (key[i]*key[i]*26)/65026);
            }
            return key;
        }

        public static byte[] GetSparseKey()
        {
            // with 3 bits/byte we need at least 6 octets 
            var length = GetRandomLength() + 6;
            var key = new byte[length];
            rng.GetBytes(key);
            for (var i = 0; i < length; i++)
            {
                key[i] = (byte)(1 << (key[i] & 7));
            }
            return key;
        }
    }
}
