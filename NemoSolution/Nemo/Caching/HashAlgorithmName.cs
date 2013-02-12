using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Caching
{
    public enum HashAlgorithmName
    {
        Default, 
        None, 
        Native, 
        MD5, 
        SHA1, 
        SHA2, 
        JenkinsHash, 
        SBox, 
        HMAC_SHA1,
        SuperFastHash,
        MurmurHash
    }
}
