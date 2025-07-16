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
  		* XML - fast XML serializer supporting both element and attribute based serialization
  		* JSON - one of the fastest JSON serializers
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
  - **JSON**: Fast JSON serialization
  - **XML**: Element and attribute-based

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
