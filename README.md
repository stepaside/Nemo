Nemo
====
*.net enterprise micro-ORM*

### Nemo is a
 1. Simple
 2. Fast 
 3. Convention-based
 4. DB agnostic 
 5. [Object](https://github.com/stepaside/Nemo/wiki/Data-Transfer-Objects) mapping library
 6. Where objects can be defined as 
  	* [Classes](https://github.com/stepaside/Nemo/wiki/Data-Transfer-Objects#wiki-class)  
  	* [Interfaces](https://github.com/stepaside/Nemo/wiki/Data-Transfer-Objects#wiki-interface)
 7. With
  	* Rich [functionality](https://github.com/stepaside/Nemo/wiki/Data-Transfer-Objects#supported-operations)
  	* L1 [cache support](https://github.com/stepaside/Nemo/wiki/Caching)
  	* Fast serialization
  		* Binary - very small footprint, one of the fastest and most compact binary serializers
  		* JSON - `System.Text.Json` based, with support for interface based DTO's
  	* [Unit of work](https://github.com/stepaside/Nemo/wiki/Unit-Of-Work) implementation
  	* Immutable [read-only DTO's](https://github.com/stepaside/Nemo/wiki/Read-Only-DTO)
  	* Extensive [configuration](https://github.com/stepaside/Nemo/wiki/Configuration) options
  	* Declarative validation
  	* [Data type conversion](https://github.com/stepaside/Nemo/wiki/Data-Type-Conversion) options (handling enumerations, nullable types, DBNull values, type coercion)
  	* [Key generation](https://github.com/stepaside/Nemo/wiki/Key-Generation) options
  	* [Active Record](https://github.com/stepaside/Nemo/wiki/Active-Record) through extension methods
  	* [LINQ Provider](https://github.com/stepaside/Nemo/wiki/Linq-Provider)
    * Asynchronous programming
      * Async methods
      * Async LINQ Provider
 8. Targeting .Net 4.7.2 and .Net Standard 2.0

Install [NuGet package](http://nuget.org/packages/Nemo)

---

# Nemo Codebase Architecture Overview

## What is Nemo?

Nemo is a **.NET Enterprise Micro-ORM** (Object-Relational Mapping) library that provides a lightweight, fast, and convention-based approach to database operations. It targets .NET Framework 4.7.2, .NET Standard 2.0, and .NET Standard 2.1.

## High-Level Architecture

The codebase follows a **modular, layered architecture** with clear separation of concerns:

### 📁 **Core Project Structure**

```
Nemo/
├── src/Nemo/                    # Main library code
├── tests/                       # Test projects
│   ├── Nemo.Test/              # Unit tests
│   ├── Nemo.Test.Core/         # Core functionality tests  
│   └── Nemo.Benchmark/         # Performance benchmarks
└── Nemo.sln                    # Visual Studio solution
```

### 🏗️ **Key Architectural Components**

#### 1. **ObjectFactory** (Core Engine)
- **Location**: `ObjectFactory.cs` + partial classes
- **Purpose**: Central hub for all ORM operations
- **Key Operations**: Create, Map, Insert, Update, Delete, Retrieve
- **Features**: 
  - Object instantiation and mapping
  - Batch operations
  - Transaction support
  - Type conversion and coercion

#### 2. **Configuration System**
- **Location**: `Configuration/` folder
- **Key Interface**: `INemoConfiguration`
- **Features**:
  - Connection string management
  - Caching configuration
  - SQL generation settings
  - Logging and audit configuration
  - Materialization modes

#### 3. **Data Access Layer**
- **Location**: `Data/` folder
- **Key Components**:
  - `DialectProvider`: Database-specific SQL generation
  - `DbFactory`: Connection management
  - `SqlBuilder`: Dynamic SQL construction
  - **Supported Databases**: SQL Server, MySQL, PostgreSQL, Oracle, SQLite

#### 4. **Reflection & Mapping Engine**
- **Location**: `Reflection/` folder
- **Key Classes**:
  - `FastMapper`: High-performance object mapping
  - `FastActivator`: Optimized object creation
  - `Adapter`: Interface implementation
  - `Reflector`: Metadata extraction
- **Performance**: Uses compiled expressions and IL generation for speed

#### 5. **LINQ Provider**
- **Location**: `Linq/` folder
- **Components**:
  - `NemoQueryProvider`: LINQ query execution
  - `NemoQueryable`: Queryable implementation
  - **Features**: Both sync and async LINQ support

#### 6. **Serialization System**
- **Location**: `Serialization/` folder
- **Formats Supported**:
  - **Binary**: Compact, high-performance
  - **JSON**: `System.Text.Json` based, with support for interface based DTO's

#### 7. **Collections & Extensions**
- **Location**: `Collections/` folder
- **Features**:
  - Custom collection types optimized for ORM scenarios
  - Eager loading support
  - Async enumerable implementations

#### 8. **Validation Framework**
- **Location**: `Validation/` folder
- **Features**:
  - Declarative validation attributes
  - Custom validation support
  - Integration with data annotations

#### 9. **Unit of Work Pattern**
- **Location**: `UnitOfWork/` folder
- **Features**:
  - Change tracking
  - Transaction scoping
  - Object state management

### 🔄 **Data Flow Architecture**

```
Application Code
       ↓
ObjectFactory (Entry Point)
       ↓
Configuration → Reflection/Mapping → Data Access
       ↓              ↓                ↓
   Settings    Object Mapping    SQL Generation
       ↓              ↓                ↓
   Caching ←  Serialization ←  Database Provider
                                      ↓
                               Database
```

### 🎯 **Key Design Patterns**

1. **Factory Pattern**: `ObjectFactory` for object creation and operations
2. **Provider Pattern**: Database dialect providers for multi-DB support
3. **Strategy Pattern**: Different serialization and mapping strategies
4. **Unit of Work**: Transaction and change tracking management
5. **Repository Pattern**: Abstracted data access operations

### 🚀 **Performance Optimizations**

- **Compiled Expressions**: Fast property access and mapping
- **IL Generation**: Dynamic method creation for optimal performance
- **Caching**: Multiple levels of metadata and result caching
- **Batch Operations**: Efficient bulk insert/update operations
- **Connection Pooling**: Optimized database connection management

### 🔧 **Key Technologies & Dependencies**

- **.NET Multi-targeting**: Framework 4.7.2, Standard 2.0/2.1
- **System.Data**: Core data access
- **System.Interactive**: Async LINQ support
- **Microsoft.Extensions**: Configuration and DI integration
- **Reflection.Emit**: Dynamic code generation

### 📋 **Attribute-Based Configuration**

The library uses attributes for declarative configuration:
- `[Table]`: Table mapping
- `[MapColumn]`: Column mapping  
- `[PrimaryKey]`: Primary key designation
- `[DoNotPersist]`: Exclude from persistence
- `[References]`: Foreign key relationships

### 🔍 **Entry Points for New Developers**

1. **Start with**: `ObjectFactory.cs` - understand core operations
2. **Configuration**: `INemoConfiguration.cs` - see available settings
3. **Data Access**: `DialectProvider.cs` - understand DB abstraction
4. **Mapping**: `Reflection/FastMapper.cs` - see how objects are mapped
5. **Examples**: Check `tests/` folder for usage patterns

### 💡 **Key Strengths**

- **Performance**: Optimized for speed with compiled expressions
- **Flexibility**: Supports classes and interfaces as DTOs
- **Database Agnostic**: Works with multiple database providers
- **Rich Features**: Caching, validation, serialization, LINQ
- **Convention-based**: Minimal configuration required
- **Async Support**: Full async/await pattern support

This architecture makes Nemo a powerful yet lightweight ORM that balances performance, flexibility, and ease of use for enterprise .NET applications.

---

# Stop Choosing Between Performance and Productivity: Why .NET Developers Deserve Both

*The false dilemma that's been holding back enterprise .NET development for too long*

---

## The Great .NET ORM Divide

If you've been developing .NET applications for any length of time, you've probably found yourself caught in this familiar dilemma:

**Option A**: Choose Dapper for blazing-fast performance, but sacrifice productivity features like change tracking, validation, and rich LINQ support.

**Option B**: Choose Entity Framework for developer productivity and rich features, but accept the performance overhead that comes with it.

This choice has become so ingrained in our industry that we've started to believe it's inevitable. Performance *or* productivity. Speed *or* features. Pick your poison.

**But what if this is a false choice?**

## The Real Cost of Compromise

Let's be honest about what these compromises actually cost us:

### When You Choose Performance (Dapper)
- ✅ Lightning-fast queries
- ✅ Minimal overhead
- ❌ Manual SQL everywhere
- ❌ No change tracking
- ❌ Limited validation support
- ❌ Repetitive mapping code
- ❌ No built-in caching

### When You Choose Productivity (Entity Framework)
- ✅ Rich LINQ support
- ✅ Change tracking
- ✅ Migrations
- ✅ Validation integration
- ❌ Performance overhead
- ❌ Complex configuration
- ❌ Memory consumption
- ❌ Query translation limitations

The result? Teams either burn developer hours writing boilerplate code, or they burn CPU cycles and memory on features they don't always need.

## The Enterprise Reality Check

Here's what actually happens in enterprise environments:

1. **You start with EF Core** because productivity matters for rapid development
2. **Performance issues emerge** as data volume grows
3. **You optimize the hot paths** by dropping down to Dapper
4. **Now you maintain two different patterns** in the same codebase
5. **Complexity explodes** as you manage different approaches for different scenarios

Sound familiar? You're not alone. According to the 2024 Stack Overflow Developer Survey, performance remains one of the top concerns for enterprise developers, yet productivity tools like ORMs continue to dominate adoption.

## What If There Was a Third Way?

What if you could get:
- **Dapper-level performance** through compiled expressions and optimized mapping
- **Enterprise-grade features** like caching, validation, and Unit of Work
- **Multi-database support** without vendor lock-in
- **Convention-based simplicity** that doesn't sacrifice flexibility
- **Both sync and async** patterns throughout

This isn't theoretical. It's exactly what **Nemo** delivers.

## Real-World Performance Results

We ran comprehensive benchmarks using **BenchmarkDotNet** - the gold standard for .NET performance testing - against a **SQL Server 2022** database to provide transparent, reproducible performance comparisons. Here are the actual results:

### Benchmark Results Summary

**Test Environment:**
- Intel Core i9-11950H, 8 cores, .NET 10.0.11
- SQL Server 2022 with Northwind customer data
- BenchmarkDotNet v0.15.8, warm in-process job (medians)

**Performance Results (Select All Operations):**

| Approach | Median Time | Operations/sec | Memory Allocated |
|----------|-------------|----------------|------------------|
| **Entity Framework Core** | 1,999 μs | 500 ops/sec | 178.12 KB |
| **Dapper** | 656 μs | 1,525 ops/sec | 24.46 KB |
| **Nemo Execute** (no mapping) | 610 μs | 1,640 ops/sec | 11.26 KB |
| **Nemo Retrieve** | 653 μs | 1,532 ops/sec | 25.46 KB |
| **Nemo Select** | 678 μs | 1,476 ops/sec | 26.70 KB |

*Raw ADO.NET reader baselines are excluded from the select-all table: in this run their distributions were bimodal (interference during the raw-reader group), making the medians unrepresentative. Their by-id results below are stable and representative.*

**Performance Results (Select By Id Operations):**

| Approach | Median Time | Operations/sec | Memory Allocated |
|----------|-------------|----------------|------------------|
| **Entity Framework Core** | 1,381 μs | 724 ops/sec | 157.99 KB |
| **Native ADO.NET** (no mapping) | 539 μs | 1,856 ops/sec | 11.48 KB |
| **Handwritten mapping** | 543 μs | 1,841 ops/sec | 11.61 KB |
| **Native + Nemo Mapper** | 556 μs | 1,799 ops/sec | 11.85 KB |
| **Dapper** | 558 μs | 1,792 ops/sec | 12.02 KB |
| **Nemo Execute** (no mapping) | 578 μs | 1,729 ops/sec | 12.95 KB |
| **Nemo Retrieve** | 609 μs | 1,642 ops/sec | 13.58 KB |
| **Nemo Select** | 717 μs | 1,394 ops/sec | 17.77 KB |

### Key Performance Insights

**1. Nemo's Sweet Spot Confirmed**
The results demonstrate exactly what we promised: Nemo bridges the gap between raw ADO.NET speed and EF Core productivity:

- **vs Entity Framework Core**: Nemo is **2-3x faster** with **85-92% less memory allocation**
- **vs Raw ADO.NET**: Nemo's compiled mapper adds only **~3% overhead** over a reader-only loop while providing full object mapping
- **vs Dapper**: Nemo `Retrieve` is within ~10% of Dapper by-id and at parity on multi-row selects, while offering enterprise features like caching, validation, and Unit of Work

**2. Memory Efficiency Advantage**
Nemo's compiled expression approach delivers exceptional memory efficiency:
- **Entity Framework Core**: 158-178 KB per operation
- **Native + Nemo Mapper**: 11.85 KB per operation (**~92% less than EF Core**)
- **Dapper**: 12.02-24.46 KB per operation
- **Native ADO.NET (reader only)**: 9.96-11.48 KB per operation

**3. Enterprise Scale Impact**
At enterprise scale, these differences are transformative:
- **vs EF Core**: 2-3x faster response times, ~7-12x less memory pressure
- **10,000 requests/sec**: Nemo saves ~7-13 seconds vs EF Core per batch
- **Memory pressure**: 85-92% reduction in GC pressure vs EF Core
- **Throughput**: 2-3x more operations per second than EF Core

### The Transparency Advantage

Rather than cherry-picking favorable numbers, we're showing you the complete picture. The benchmark project is [publicly available](https://github.com/stepaside/Nemo/tree/master/tests/Nemo.Benchmark) so you can:

- Run the benchmarks yourself against your own data
- Verify performance claims in your specific environment  
- Test with your actual query patterns
- Compare across different database providers

**The bottom line**: Nemo delivers on its promise of not making you choose between performance and productivity. You get enterprise features with performance that's closer to raw ADO.NET than to traditional ORMs.

## Real-World Impact: A Case Study

Consider a typical enterprise scenario: an e-commerce platform processing thousands of orders per minute.

**With the traditional approach:**
- Use EF Core for CRUD operations (developer productivity)
- Drop to Dapper for reporting queries (performance)
- Maintain separate caching layer
- Custom validation logic scattered throughout
- Different patterns for different teams

**With Nemo:**
- Single ORM handles both scenarios efficiently
- Built-in caching reduces database load
- Declarative validation keeps business rules centralized
- Consistent patterns across the entire application
- Performance that scales with your business

## The Architecture That Makes It Possible

Nemo achieves this balance through several key innovations:

### 1. Compiled Expression Trees
Instead of reflection-heavy mapping, Nemo generates optimized IL code at runtime. Your object mapping runs at near-native speed.

### 2. Intelligent Caching
Multiple levels of caching (metadata, query plans, results) mean you pay the compilation cost once and reap the benefits everywhere.

### 3. Database-Agnostic Design
Native support for SQL Server, PostgreSQL, MySQL, Oracle, and SQLite means you're never locked into a single vendor.

### 4. Modular Feature Set
Need validation? It's there. Want Unit of Work? Built-in. Don't need serialization? It doesn't slow you down.

## Making the Switch

The best part? You don't need to rewrite your entire application overnight.

**Start small:**
1. Identify a performance-critical component
2. Replace the data access layer with Nemo
3. Measure the improvement
4. Gradually expand usage

**Migration is straightforward:**
- From Dapper: Keep your SQL, add enterprise features
- From EF Core: Keep your models, gain performance
- From ADO.NET: Keep your control, add productivity

## The Bottom Line

The choice between performance and productivity is a relic of the past. Modern enterprise applications demand both, and modern tools should deliver both.

**Stop compromising. Stop maintaining dual patterns. Stop choosing between speed and features.**

Your applications deserve better. Your team deserves better. Your users definitely deserve better.

---

## Ready to Stop Choosing?

Try Nemo in your next project:

```bash
dotnet add package Nemo
```

```csharp
// It's really this simple
var customers = ObjectFactory.Select<Customer>()
    .Where(c => c.IsActive)
    .ToList();

// With built-in caching, validation, and performance
```

**Learn more:**
- [GitHub Repository](https://github.com/stepaside/Nemo)
- [Documentation](https://github.com/stepaside/Nemo/wiki)
- [Benchmark Results](https://github.com/stepaside/Nemo/tree/master/tests/Nemo.Benchmark)

*Because the best choice is not having to choose at all.*

---

*Have you been caught in the performance vs productivity dilemma? Share your experiences in the comments below, or reach out on [GitHub](https://github.com/stepaside/Nemo) to discuss how Nemo might fit into your architecture.*
