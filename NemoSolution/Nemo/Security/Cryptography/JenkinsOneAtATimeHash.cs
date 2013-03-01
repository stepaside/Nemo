using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Security.Cryptography
{
    public class JenkinsOneAtATimeHash
    {
        public static uint Compute(byte[] data)
        {
            uint hash = 0;
            foreach (byte b in data)
            {
                hash += b;
                hash += (hash << 10);	// 10
                hash ^= (hash >> 6);	// 6
            }
            hash += (hash << 3);	// 3
            hash ^= (hash >> 11);	// 11
            hash += (hash << 15);	// 15
            return hash;
        }
    }
}
