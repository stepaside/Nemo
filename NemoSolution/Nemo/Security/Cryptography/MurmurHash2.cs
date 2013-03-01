using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Security.Cryptography
{
    public class MurmurHash2
    {
        const uint m = 0x5bd1e995;
        const int r = 24;

        public static uint Compute(byte[] data)
        {
            return Compute(data, 0xc58f1a7b);
        }

        public unsafe static uint Compute(byte[] data, uint seed)
        {
            var length = data.Length;
            if (length == 0)
                return 0;
            uint h = seed ^ (uint)length;
            var remainingBytes = length & 3; // mod 4
            var numberOfLoops = length >> 2; // div 4
            fixed (byte* firstByte = &(data[0]))
            {
                uint* realData = (uint*)firstByte;
                while (numberOfLoops != 0)
                {
                    var k = *realData;
                    k *= m;
                    k ^= k >> r;
                    k *= m;

                    h *= m;
                    h ^= k;
                    numberOfLoops--;
                    realData++;
                }
                switch (remainingBytes)
                {
                    case 3:
                        h ^= (ushort)(*realData);
                        h ^= ((uint)(*(((byte*)(realData)) + 2))) << 16;
                        h *= m;
                        break;
                    case 2:
                        h ^= (ushort)(*realData);
                        h *= m;
                        break;
                    case 1:
                        h ^= *((byte*)realData);
                        h *= m;
                        break;
                    default:
                        break;
                }
            }

            // Do a few final mixes of the hash to ensure the last few
            // bytes are well-incorporated.

            h ^= h >> 13;
            h *= m;
            h ^= h >> 15;

            return h;
        }
    }
}
