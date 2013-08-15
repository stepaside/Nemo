using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Caching
{
    public interface IRevisionProvider
    {
        ulong GetRevision(string key);
        IDictionary<string, ulong> GetRevisions(IEnumerable<string> keys);
        IDictionary<string, ulong> GetAllRevisions();
        ulong IncrementRevision(string key, ulong delta = 1);
        ulong GenerateRevision();
    }
}
