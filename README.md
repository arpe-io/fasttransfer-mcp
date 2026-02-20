# FastTransfer MCP Server

<!-- mcp-name: io.github.aetperf/fasttransfer-mcp -->

A [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server that exposes [FastTransfer](https://aetperf.github.io/FastTransfer-Documentation/) functionality for efficient data transfer between various database systems.

## Overview

FastTransfer is a high-performance CLI tool for transferring data between databases. This MCP server wraps FastTransfer functionality and provides:

- **Safety-first approach**: Preview commands before execution with user confirmation required
- **Password masking**: Credentials and connection strings are never displayed in logs or output
- **Intelligent validation**: Parameter validation with database-specific compatibility checks
- **Smart suggestions**: Automatic parallelism method recommendations
- **Version detection**: Automatic binary version detection with capability registry
- **Comprehensive logging**: Full execution logs with timestamps and results

## MCP Tools

### 1. `preview_transfer_command`
Build and preview a FastTransfer command WITHOUT executing it. Shows the exact command with passwords masked. Always use this first.

### 2. `execute_transfer`
Execute a previously previewed command. Requires `confirmation: true` as a safety mechanism.

### 3. `validate_connection`
Validate database connection parameters (parameter check only, does not test actual connectivity).

### 4. `list_supported_combinations`
List all supported source-to-target database combinations.

### 5. `suggest_parallelism_method`
Recommend the optimal parallelism method based on source database type and table characteristics.

### 6. `get_version`
Report the detected FastTransfer binary version, supported types, and feature flags.

## Installation

### Prerequisites

- Python 3.10 or higher
- FastTransfer binary v0.16+ (obtain from [Arpe.io](https://arpe.io))
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

4. **Add to Claude Code configuration** (`~/.claude.json`):
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

5. **Restart Claude Code** to load the MCP server.

6. **Verify installation**:
   ```
   # In Claude Code, run:
   /mcp
   # You should see "fasttransfer: connected"
   ```

## Configuration

### Environment Variables

Edit `.env` to configure:

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

## Connection Options

The server supports multiple ways to authenticate and connect:

| Parameter | Description |
|-----------|-------------|
| `server` | Host:port or host\instance (optional with `connect_string` or `dsn`) |
| `user` / `password` | Standard credentials |
| `trusted_auth` | Windows trusted authentication |
| `connect_string` | Full connection string (excludes server/user/password/dsn) |
| `dsn` | ODBC DSN name (excludes server/provider) |
| `provider` | OleDB provider name |
| `file_input` | File path for data input (source only, excludes query) |

## Transfer Options

| Option | CLI Flag | Description |
|--------|----------|-------------|
| `method` | `--method` | Parallelism method |
| `distribute_key_column` | `--distributeKeyColumn` | Column for data distribution |
| `degree` | `--degree` | Parallelism degree (0=auto, >0=fixed, <0=CPU adaptive) |
| `load_mode` | `--loadmode` | Append or Truncate |
| `batch_size` | `--batchsize` | Batch size for bulk operations |
| `map_method` | `--mapmethod` | Column mapping: Position or Name |
| `run_id` | `--runid` | Run ID for logging |
| `data_driven_query` | `--datadrivenquery` | Custom SQL for DataDriven method |
| `use_work_tables` | `--useworktables` | Intermediate work tables for CCI |
| `settings_file` | `--settingsfile` | Custom settings JSON file |
| `log_level` | `--loglevel` | Override log level (error/warning/information/debug/fatal) |
| `no_banner` | `--nobanner` | Suppress banner output |
| `license_path` | `--license` | License file path or URL |

## Usage Examples

### PostgreSQL to SQL Server Transfer

```
User: "Copy the 'orders' table from PostgreSQL (localhost:5432, database: sales_db,
       schema: public) to SQL Server (localhost:1433, database: warehouse, schema: dbo).
       Use parallel transfer and truncate the target first."

Claude Code will:
1. Call suggest_parallelism_method to recommend Ctid for PostgreSQL
2. Call preview_transfer_command with your parameters
3. Show the command with masked passwords
4. Explain what will happen
5. Ask for confirmation
6. Execute with execute_transfer when you approve
```

### File Import via DuckDB Stream

```
User: "Import /data/export.parquet into the SQL Server 'staging' table
       using DuckDB stream."

Claude Code will use duckdbstream source type with file_input parameter.
```

### Check Version and Capabilities

```
User: "What version of FastTransfer is installed?"

Claude Code will call get_version and display the detected version,
supported source/target types, and available features.
```

## Two-Step Safety Process

This server implements a mandatory two-step process:

1. **Preview** - Always use `preview_transfer_command` first
2. **Execute** - Use `execute_transfer` with `confirmation: true`

You cannot execute without previewing first and confirming.

## Security

- Passwords and connection strings are masked in all output and logs
- Sensitive flags masked: `--sourcepassword`, `--targetpassword`, `--sourceconnectstring`, `--targetconnectstring`, `-x`, `-X`, `-g`, `-G`
- Use environment variables for sensitive configuration
- Review commands carefully before executing
- Use minimum required database permissions

## Testing

Run the test suite:

```bash
# Run all tests
python -m pytest tests/ -v

# Run with coverage
python -m pytest tests/ --cov=src --cov-report=html
```

## Project Structure

```
fasttransfer-mcp/
  src/
    __init__.py
    server.py          # MCP server (tool definitions, handlers)
    fasttransfer.py    # Command builder, executor, suggestions
    validators.py      # Pydantic models, enums, validation
    version.py         # Version detection and capabilities registry
  tests/
    __init__.py
    test_command_builder.py
    test_validators.py
    test_version.py
  .env.example
  requirements.txt
  CHANGELOG.md
  README.md
```

## License

This MCP server wrapper is provided as-is. FastTransfer itself is a separate product from Arpe.io.

## Related Links

- [FastTransfer Documentation](https://aetperf.github.io/FastTransfer-Documentation/)
- [Model Context Protocol](https://modelcontextprotocol.io/)
