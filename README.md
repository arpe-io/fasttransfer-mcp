# FastTransfer MCP Server

A [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server that exposes [FastTransfer](https://aetperf.github.io/FastTransfer-Documentation/) functionality for efficient data transfer between various database systems.

## Overview

FastTransfer is a high-performance CLI tool for transferring data between databases. This MCP server wraps FastTransfer functionality and provides:

- **Safety-first approach**: Preview commands before execution with user confirmation required
- **Password masking**: Credentials are never displayed in logs or output
- **Intelligent validation**: Parameter validation with database-specific compatibility checks
- **Smart suggestions**: Automatic parallelism method recommendations
- **Comprehensive logging**: Full execution logs with timestamps and results

## Supported Databases

### Source Databases
ClickHouse, DuckDB, MySQL, Netezza, Oracle, PostgreSQL, SQL Server, SAP HANA, Teradata

### Target Databases
ClickHouse, DuckDB, MySQL, Netezza, Oracle, PostgreSQL, SQL Server, SAP HANA, Teradata

## Features

### Five MCP Tools

1. **preview_transfer_command** - Build and preview commands WITHOUT executing
2. **execute_transfer** - Execute previewed commands with confirmation
3. **validate_connection** - Validate connection parameters
4. **list_supported_combinations** - Show supported database pairs
5. **suggest_parallelism_method** - Get parallelism recommendations

### Parallelism Methods

- **Ctid** - PostgreSQL-specific (optimal for PostgreSQL sources)
- **Rowid** - Oracle-specific (optimal for Oracle sources)
- **NZDataSlice** - Netezza-specific (optimal for Netezza sources)
- **RangeId** - Numeric range distribution (requires numeric key)
- **Random** - Modulo-based distribution (requires numeric key)
- **DataDriven** - Distinct value distribution (works with any column)
- **Ntile** - Even distribution (works with numeric/date/string columns)
- **None** - Single-threaded (best for small tables)

## Installation

### Prerequisites

- Python 3.8 or higher
- FastTransfer binary (obtain from [Arpe.io](https://arpe.io))
- Claude Code or another MCP client

### Setup

1. **Clone or download this repository**:
   ```bash
   cd /path/to/fasttransfer-mcp
   ```

2. **Install Python dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your FastTransfer path
   ```

4. **Add to Claude Code configuration** (~/.claude.json):
   ```json
   {
     "mcpServers": {
       "fasttransfer": {
         "type": "stdio",
         "command": "python",
         "args": ["/absolute/path/to/fasttransfer-mcp/src/server.py"],
         "env": {
           "FASTTRANSFER_PATH": "/absolute/path/to/fasttransfer/FastTransfer"
         }
       }
     }
   }
   ```

5. **Restart Claude Code** to load the MCP server

6. **Verify installation**:
   ```bash
   # In Claude Code, run:
   /mcp
   # You should see "fasttransfer: connected"
   ```

## Configuration

### Environment Variables

Edit .env to configure:

```bash
# Path to FastTransfer binary (required)
FASTTRANSFER_PATH=./fasttransfer/FastTransfer

# Execution timeout in seconds (default: 1800 = 30 minutes)
FASTTRANSFER_TIMEOUT=1800

# Log directory (default: ./logs)
FASTTRANSFER_LOG_DIR=./logs

# Log level (default: INFO)
LOG_LEVEL=INFO
```

## Usage Examples

### Example 1: PostgreSQL to SQL Server Transfer

```
User: "I need to copy the 'orders' table from my PostgreSQL database
      (localhost:5432, database: sales_db, schema: public) to SQL Server
      (localhost:1433, database: warehouse, schema: dbo). Use parallel
      transfer and truncate the target table first."

Claude Code will:
1. Call preview_transfer_command with your parameters
2. Show you the command with masked passwords
3. Explain what will happen
4. Ask for confirmation
5. Execute with execute_transfer when you approve
```

For more usage examples, see the full documentation in README.

## Two-Step Safety Process

**IMPORTANT**: This server implements a mandatory two-step process:

1. **Preview** - Always use preview_transfer_command first
2. **Execute** - Use execute_transfer with confirmation required

**You cannot execute without previewing first and confirming!**

## Security Best Practices

- Never log passwords in plain text
- Use environment variables for sensitive config
- Review commands carefully before executing
- Limit access to the MCP server
- Use minimum required database permissions

## Testing

Run the test suite:

```bash
# Run all tests
pytest tests/

# Run with coverage
pytest tests/ --cov=src --cov-report=html
```

Expected coverage: >80%

## License

This MCP server wrapper is provided as-is. FastTransfer itself is a separate product from Arpe.io.

## Related Links

- [FastTransfer Documentation](https://aetperf.github.io/FastTransfer-Documentation/)
- [Model Context Protocol](https://modelcontextprotocol.io/)
- [Claude Code](https://claude.com/claude-code)
