# Changelog

All notable changes to the FastTransfer MCP Server will be documented in this file.

## [0.1.5] - 2026-02-27

### Added
- Version compatibility check infrastructure in `preview_transfer_command` output

## [0.1.4] - 2026-02-24

### Added
- PyPI, License, and MCP Registry badges in README
- GitHub Actions workflow for automated PyPI publishing on release
- Missing environment variables (`FASTTRANSFER_LOG_DIR`, `LOG_LEVEL`) in server.json
- GitHub repository topics for MCP Registry discoverability

### Fixed
- Documentation URL in pyproject.toml

## [0.1.2] - 2026-02-23

### Added

- `server.json` MCP Registry configuration file with package metadata, transport settings, and environment variable definitions

### Changed

- GitHub repository URL updated from `aetperf/fasttransfer-mcp` to `arpe-io/fasttransfer-mcp` in `pyproject.toml`

## [0.1.1] - 2026-02-20

### Added

- `pyproject.toml` with hatchling build backend, PyPI metadata, and `fasttransfer-mcp` console script entry point
- MIT LICENSE file (copyright Arpe.io)
- MCP Registry marker in README (`<!-- mcp-name: io.github.aetperf/fasttransfer-mcp -->`)

### Changed

- README rewritten: focused on MCP tools and configuration, removed database-specific reference tables
- Entry point renamed from `cli()` to `main()` for console script compatibility
- Code formatted with Black and Ruff

### Removed

- `verify_installation.sh` (superseded by `get_version` tool)
- `example_config.json` (duplicated by README installation section)

## [0.1.0] - 2026-02-20

### Added

- MCP server with six tools: `preview_transfer_command`, `execute_transfer`, `validate_connection`, `list_supported_combinations`, `suggest_parallelism_method`, `get_version`
- Version detection module (`src/version.py`) with static capability registry for FastTransfer v0.16.0.0
  - `FastTransferVersion` dataclass with comparison operators
  - `VersionDetector` with subprocess-based detection, caching, and graceful fallback
  - `VersionCapabilities` registry mapping versions to supported types and feature flags
- 16 source connection types: `clickhouse`, `duckdb`, `duckdbstream`, `hana`, `mssql`, `msoledbsql`, `mysql`, `nzoledb`, `nzsql`, `nzbulk`, `odbc`, `oledb`, `oraodp`, `pgcopy`, `pgsql`, `teradata`
- 11 target connection types: `clickhousebulk`, `duckdb`, `hanabulk`, `msbulk`, `mysqlbulk`, `nzbulk`, `orabulk`, `oradirect`, `pgcopy`, `pgsql`, `teradata`
- 9 parallelism methods: `Ctid`, `DataDriven`, `Ntile`, `NZDataSlice`, `None`, `Physloc`, `Random`, `RangeId`, `Rowid`
- Flexible connection configuration: `server` (optional), `connect_string`, `dsn`, `provider`, `file_input`, `trusted_auth`
- Mutual exclusivity validation for connection parameters
- Transfer options: `data_driven_query`, `use_work_tables`, `settings_file`, `log_level`, `no_banner`, `license_path`
- `LogLevel` enum: `error`, `warning`, `information`, `debug`, `fatal`
- Physloc method validation (SQL Server sources only)
- Password and connection string masking in command output
- DuckDB Stream file import as a source category
- Parallelism suggestion engine with Physloc recommendation for SQL Server without numeric key
- 122 tests across three test modules
