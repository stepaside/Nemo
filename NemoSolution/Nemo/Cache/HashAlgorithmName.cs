using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Cache
{
    public enum HashAlgorithmName
    {
        Default, 
        None, 
        Native, 
        MD5, 
        SHA1, 
        SHA256, 
        JenkinsHash, 
        SBox, 
        HMAC_SHA1,
        HMAC_SHA256,
        SuperFastHash,
        MurmurHash
    }
}
