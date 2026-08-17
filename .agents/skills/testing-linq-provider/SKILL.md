---
name: testing-nemo-linq-provider
description: How to end-to-end test Nemo's LINQ provider (NemoQueryable and its async execution extensions) against a real SQLite database on Linux
---

# Testing Nemo's LINQ provider against SQLite

## Environment
- .NET 10 SDK at `$HOME/.dotnet` (add to PATH; also set `DOTNET_ROOT=$HOME/.dotnet` if running the built apphost binary directly instead of `dotnet run`).
- Full `Nemo.sln` build fails on Linux (tests/Nemo.Test is net472-only). Build `src/Nemo/Nemo.csproj` and `tests/Nemo.UnitTests` only.

## Harness pattern
- Create a console app referencing `src/Nemo/Nemo.csproj`; model usage on `tests/Nemo.Test.Core/Features/DbMappingLinq.cs` (POCO + `EntityMap<T>` with `TableName`/`Property(...).Column(...)`, `ConfigurationFactory.CloneCurrentConfiguration()` + `ConfigurationFactory.Set<T>(config)`).
- Query via `new NemoQueryable<T>(connection, config)`; execute asynchronously with the `Nemo.Linq.NemoQueryableExtensions` methods on `IQueryable<T>` (`AsAsyncEnumerable`, `ToListAsync`, `ToArrayAsync`, `FirstAsync`, `FirstOrDefaultAsync`, `CountAsync`, `LongCountAsync`, `MaxAsync`, `MinAsync`, `SumAsync`, `AverageAsync`).
- Capture generated SQL by implementing `Nemo.Configuration.ILogProvider` and enabling `config.SetLogging(true).SetLogProvider(...)` — SQL statements are emitted through `Log.Capture` in `ObjectFactory.RetrieveItems`.
- Set `config.SetDefaultCacheRepresentation(CacheRepresentation.List)` so results materialize eagerly.

## SQLite gotchas
- Use **System.Data.SQLite.Core** (`SQLiteConnection`), NOT Microsoft.Data.Sqlite: `ObjectFactory.Execute` disposes the DbCommand before the deferred reader is enumerated, and Microsoft.Data.Sqlite closes readers on command dispose ("Invalid attempt to call FieldCount when reader is closed"). This is pre-existing library behavior.
- Skip-only queries (`SqlBuilder.SqlSelectSkipFormat` = `SELECT ... OFFSET n` without LIMIT) are invalid SQLite/MySQL syntax and fail at runtime; may be fixed later — verify before assuming broken.

## SQL assertion notes
- Nemo logs generated SQL with lowercase null checks (`is null` / `is not null`) — use case-insensitive assertions.
- As of Phase 3 (PR #22), LINQ predicate values appear as `@p__N` parameters in the logged SQL, except bool constants which still inline as `(1=1)`/`(1=0)`.
- DateTime/bool columns work on SQLite when the schema uses `DATETIME`/`BOOLEAN` column types and the config sets `SetAutoTypeCoercion(true)`.

## Async LINQ notes
- As of v3.0.0 (Phase 5), `NemoQueryableAsync<T>`/Ix.NET `IAsyncQueryProvider` are removed; async execution is via Nemo's own extension methods on `IQueryable<T>` and returns `Task<...>`/`IAsyncEnumerable<T>`. All of them accept a `CancellationToken`.
- On pre-3.0 branches use `NemoQueryableAsync<T>` with Ix.NET operators instead; `ToListAsync`/`ToArrayAsync` there require PR #20, and `AverageAsync` can be ambiguous when both System.Linq.Async and System.Interactive.Async are referenced — call `System.Linq.AsyncQueryable.AverageAsync(...)` explicitly.

## Devin Secrets Needed
None — SQLite is file-based, no credentials.
