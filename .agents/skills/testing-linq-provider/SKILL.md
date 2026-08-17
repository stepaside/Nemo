---
name: testing-nemo-linq-provider
description: How to end-to-end test Nemo's LINQ provider (NemoQueryable/NemoQueryableAsync) against a real SQLite database on Linux
---

# Testing Nemo's LINQ provider against SQLite

## Environment
- .NET 10 SDK at `$HOME/.dotnet` (add to PATH; also set `DOTNET_ROOT=$HOME/.dotnet` if running the built apphost binary directly instead of `dotnet run`).
- Full `Nemo.sln` build fails on Linux (tests/Nemo.Test is net472-only). Build `src/Nemo/Nemo.csproj` and `tests/Nemo.UnitTests` only.

## Harness pattern
- Create a console app referencing `src/Nemo/Nemo.csproj`; model usage on `tests/Nemo.Test.Core/Features/DbMappingLinq.cs` (POCO + `EntityMap<T>` with `TableName`/`Property(...).Column(...)`, `ConfigurationFactory.CloneCurrentConfiguration()` + `ConfigurationFactory.Set<T>(config)`).
- Query via `new NemoQueryable<T>(connection, config)` and `new NemoQueryableAsync<T>(connection, config, CancellationToken.None)`.
- Capture generated SQL by implementing `Nemo.Configuration.ILogProvider` and enabling `config.SetLogging(true).SetLogProvider(...)` — SQL statements are emitted through `Log.Capture` in `ObjectFactory.RetrieveItems`.
- Set `config.SetDefaultCacheRepresentation(CacheRepresentation.List)` so results materialize eagerly.

## SQLite gotchas
- Use **System.Data.SQLite.Core** (`SQLiteConnection`), NOT Microsoft.Data.Sqlite: `ObjectFactory.Execute` disposes the DbCommand before the deferred reader is enumerated, and Microsoft.Data.Sqlite closes readers on command dispose ("Invalid attempt to call FieldCount when reader is closed"). This is pre-existing library behavior.
- Skip-only queries (`SqlBuilder.SqlSelectSkipFormat` = `SELECT ... OFFSET n` without LIMIT) are invalid SQLite/MySQL syntax and fail at runtime; may be fixed later — verify before assuming broken.

## Devin Secrets Needed
None — SQLite is file-based, no credentials.
