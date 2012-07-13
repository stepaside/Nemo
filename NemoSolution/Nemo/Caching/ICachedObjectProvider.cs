using Nemo.Fn;

namespace Nemo.Caching
{
    public interface ICachedObjectProvider
    {
        Maybe<bool> Invalidate();
        Maybe<bool> Touch();
        Maybe<bool> Sync();
        Maybe<bool> Refresh();
        Maybe<bool> Reload();
        string CacheKey { get; }
    }
}
