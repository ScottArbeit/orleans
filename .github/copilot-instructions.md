# Orleans - Distributed Application Framework

Orleans is a cross-platform framework for building robust, scalable distributed applications using the Virtual Actor Model. It extends familiar .NET concepts like objects, interfaces, async/await to multi-server environments.

**ALWAYS follow these instructions first. Only fallback to additional search or bash commands when the information here is incomplete or found to be in error.**

## Essential Setup & Building

### SDK Installation Requirements
- **CRITICAL**: Install .NET 9.0.303 SDK (exact version required per global.json)
- **CRITICAL**: Install .NET 8.0 runtime for running tests
- Use the following validated installation commands:

```bash
# Install .NET 9.0.303 SDK
curl -L https://dot.net/v1/dotnet-install.sh -o dotnet-install.sh
chmod +x dotnet-install.sh
./dotnet-install.sh --jsonfile global.json

# Install .NET 8.0 runtime for tests
./dotnet-install.sh --runtime dotnet --version 8.0.7

# Set PATH (add to shell profile for persistence)
export PATH="$HOME/.dotnet:$PATH"
```

### Building Orleans
- **NEVER CANCEL**: Build takes 2-3 minutes. Set timeout to 300+ seconds.
- **PRIMARY BUILD COMMAND**: `dotnet build` (cross-platform)
- **ALTERNATIVE**: `Build.cmd` (Windows) or `./build.ps1` (PowerShell)

```bash
# Validated build command - NEVER CANCEL, takes ~2-3 minutes
dotnet build
```

**Build Success Indicators:**
- Completes in ~130-180 seconds
- Outputs: "Build succeeded in XXXs"
- No build errors (warnings are acceptable)

## Testing

### Test Categories & Timing Expectations
- **BVT** (Build Verification Tests): 5-10 seconds per project, dozens of projects
- **SlowBVT**: 10+ minutes total - NEVER CANCEL, set timeout to 1800+ seconds  
- **Functional**: 15+ minutes total - NEVER CANCEL, set timeout to 2400+ seconds

### Validated Test Commands
```bash
# Quick validation - single test project (~5 seconds)
dotnet test test/Analyzers.Tests/Analyzers.Tests.csproj --framework net8.0 --no-build

# Basic tests - NEVER CANCEL, takes 2-5 minutes
dotnet test --filter "Category=BVT" --framework net8.0 --no-build --logger "console;verbosity=minimal" -- -parallel none -noshadow

# Comprehensive tests - NEVER CANCEL, takes 15+ minutes
dotnet test --filter "Category=SlowBVT|Category=Functional" --framework net8.0 --no-build --logger "console;verbosity=minimal" -- -parallel none -noshadow
```

**CRITICAL Testing Notes:**
- **NEVER CANCEL** long-running test commands - they can take 15-30 minutes
- Must build first before running `--no-build` tests
- Tests require both .NET 9.0 SDK and .NET 8.0 runtime
- Use `-- -parallel none -noshadow` parameters for Orleans tests

## Repository Structure & Key Areas

### Core Components
- **src/Orleans.Core**: Core Orleans runtime and abstractions
- **src/Orleans.Runtime**: Main runtime implementation  
- **src/Orleans.Client**: Client-side Orleans functionality
- **src/Orleans.Server**: Server-side hosting
- **src/Orleans.CodeGenerator**: Code generation for grains
- **src/Orleans.Serialization**: Serialization infrastructure

### Extensions & Providers
- **src/Extensions/**: Database providers (Azure, AWS, Redis, etc.)
- **test/**: Comprehensive test suite organized by functionality
- **playground/**: Working examples demonstrating Orleans features

### Important Files
- **Orleans.slnx**: Main solution file (XML format)
- **global.json**: Specifies exact .NET SDK version (9.0.303)
- **.editorconfig**: Code formatting and style rules
- **Directory.Build.props**: Shared MSBuild properties

## Working with Orleans Applications

### Validation Scenarios
After making changes, ALWAYS test Orleans functionality with these scenarios:

1. **Grain Interface & Implementation**: 
   ```bash
   # Create simple grain, build, test in playground/
   dotnet build
   dotnet run --project playground/DashboardToy/DashboardToy.AppHost
   ```

2. **Serialization Changes**:
   ```bash
   # Test serialization generators
   dotnet test test/Orleans.Serialization.Tests/ --framework net8.0 --no-build
   ```

3. **Runtime Changes**:
   ```bash
   # Test core runtime functionality
   dotnet test test/TesterInternal/ --framework net8.0 --no-build --filter "Category=BVT"
   ```

### Key Development Patterns
- **Grains**: Entities with identity, behavior, and state (core Orleans concept)
- **Interfaces**: Strongly-typed contracts for grain communication
- **Persistence**: State management using `IPersistentState<T>`
- **Streaming**: Real-time data processing with Orleans Streams
- **Clustering**: Automatic grain placement and failover

## Pre-Commit Validation
Always run these before committing changes:

```bash
# 1. Build check - NEVER CANCEL (2-3 minutes)
dotnet build

# 2. Basic test validation - NEVER CANCEL (5-10 minutes)  
dotnet test --filter "Category=BVT" --framework net8.0 --no-build --logger "console;verbosity=minimal" -- -parallel none -noshadow

# 3. Code format check (uses .editorconfig)
# Orleans uses built-in .NET formatting - no additional tools needed
```

## External Dependencies & Samples

### Sample Applications
- **MOVED**: Official samples relocated to [dotnet/samples repository](https://github.com/dotnet/samples/tree/main/orleans)
- **PLAYGROUND**: Use `playground/` directory for local testing and examples
- **EXAMPLES**: DashboardToy, ActivationRebalancing show typical Orleans apps

### Provider Testing
Orleans supports many providers (tested in CI):
- **Redis**: Clustering and persistence
- **Azure Storage**: Tables, blobs, queues
- **SQL Databases**: PostgreSQL, MySQL, SQL Server, Cassandra
- **Message Queues**: Azure Event Hubs, NATS
- **Cloud Services**: DynamoDB, Cosmos DB

## Troubleshooting Common Issues

### Build Issues
- **"SDK not found"**: Install exact .NET 9.0.303 version per global.json
- **"Framework not found"**: Install .NET 8.0 runtime for test execution
- **Long build times**: Normal - Orleans is a large codebase, expect 2-3 minutes

### Test Issues  
- **"Test host exited"**: Missing .NET 8.0 runtime, install with dotnet-install script
- **Timeout errors**: Increase timeout to 1800+ seconds for comprehensive tests
- **Provider test failures**: Often due to missing external services (Redis, SQL, etc.)

### Performance Expectations
- **Initial build**: 2-3 minutes (NEVER CANCEL)
- **Incremental builds**: 30-60 seconds  
- **BVT tests**: 5-10 minutes total
- **Full test suite**: 30+ minutes (includes provider tests)

## Coding Standards

### General
- Make only high confidence suggestions when reviewing code changes
- Always use the latest version C#, currently C# 13 features
- **Never change global.json unless explicitly asked to**

### Formatting
- Apply code-formatting style defined in `.editorconfig`
- Prefer file-scoped namespace declarations and single-line using directives
- Insert a newline before the opening curly brace of any code block (e.g., after `if`, `for`, `while`, `foreach`, `using`, `try`, etc.)
- Ensure that the final return statement of a method is on its own line
- Use pattern matching and switch expressions wherever possible
- Use `nameof` instead of string literals when referring to member names
- Ensure that XML doc comments are created for any public APIs. When applicable, include `<example>` and `<code>` documentation in the comments

### Nullable Reference Types
- Declare variables non-nullable, and check for `null` at entry points
- Always use `is null` or `is not null` instead of `== null` or `!= null`
- Trust the C# null annotations and don't add null checks when the type system says a value cannot be null

### Testing
- We use xUnit SDK v3 for tests
- Do not emit "Act", "Arrange" or "Assert" comments
- Use NSubstitute for mocking in tests
- Copy existing style in nearby files for test method names and capitalization

## Contributing Guidelines
- Follow existing code style defined in .editorconfig
- All public APIs require XML documentation
- Test changes with appropriate grain scenarios
- Ensure compatibility with .NET Standard 2.0+
- Review [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines
