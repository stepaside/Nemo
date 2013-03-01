using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Security.Cryptography
{
    public class SuperFastHash
    {
        public unsafe static uint Compute(byte[] data)
        {
            var dataLength = data.Length;
            if (dataLength == 0)
                return 0;
            var hash = (uint)dataLength;
            var remainingBytes = dataLength & 3; // mod 4
            var numberOfLoops = dataLength >> 2; // div 4

            fixed (byte* firstByte = &(data[0]))
            {
                /* Main loop */
                ushort* readlData = (ushort*)firstByte;
                for (; numberOfLoops > 0; numberOfLoops--)
                {
                    hash += *readlData;
                    var tmp = (uint)(*(readlData + 1) << 11) ^ hash;
                    hash = (hash << 16) ^ tmp;
                    readlData += 2;
                    hash += hash >> 11;
                }
                switch (remainingBytes)
                {
                    case 3: hash += *readlData;
                        hash ^= hash << 16;
                        hash ^= ((uint)(*(((byte*)(readlData)) + 2))) << 18;
                        hash += hash >> 11;
                        break;
                    case 2: hash += *readlData;
                        hash ^= hash << 11;
                        hash += hash >> 17;
                        break;
                    case 1:
                        hash += *((byte*)readlData);
                        hash ^= hash << 10;
                        hash += hash >> 1;
                        break;
                    default:
                        break;
                }
            }

            /* Force "avalanching" of final 127 bits */
            hash ^= hash << 3;
            hash += hash >> 5;
            hash ^= hash << 4;
            hash += hash >> 17;
            hash ^= hash << 25;
            hash += hash >> 6;

            return hash;
        }
    }
}
