#!/usr/bin/env python3
"""
FastTransfer MCP Server

A Model Context Protocol (MCP) server that exposes FastTransfer functionality
for efficient data transfer between database systems.

This server provides six tools:
1. preview_transfer_command - Build and preview command without executing
2. execute_transfer - Execute a previously previewed command with confirmation
3. validate_connection - Test database connectivity
4. list_supported_combinations - Show supported database pairs
5. suggest_parallelism_method - Recommend parallelism method
6. get_version - Report FastTransfer version and capabilities
"""

import os
import sys
import logging
import asyncio
from pathlib import Path
from typing import Any, Dict

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from dotenv import load_dotenv
    from mcp.server import Server
    from mcp.types import Tool, TextContent
    from pydantic import ValidationError
except ImportError as e:
    print(f"Error: Required package not found: {e}", file=sys.stderr)
    print("Please run: pip install -r requirements.txt", file=sys.stderr)
    sys.exit(1)

from src.validators import (
    TransferRequest,
    ConnectionValidationRequest,
    ParallelismSuggestionRequest,
    SourceConnectionType,
    TargetConnectionType,
    ParallelismMethod,
    LoadMode,
    MapMethod,
    LogLevel,
)
from src.fasttransfer import (
    CommandBuilder,
    FastTransferError,
    get_supported_combinations,
    suggest_parallelism_method,
)


# Load environment variables
load_dotenv()

# Configure logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
)
logger = logging.getLogger(__name__)

# Configuration
FASTTRANSFER_PATH = os.getenv("FASTTRANSFER_PATH", "./fasttransfer/FastTransfer")
FASTTRANSFER_TIMEOUT = int(os.getenv("FASTTRANSFER_TIMEOUT", "1800"))
FASTTRANSFER_LOG_DIR = Path(os.getenv("FASTTRANSFER_LOG_DIR", "./logs"))

# Initialize MCP server
app = Server("fasttransfer")

# Global command builder instance
try:
    command_builder = CommandBuilder(FASTTRANSFER_PATH)
    version_info = command_builder.get_version()
    logger.info(f"FastTransfer binary found at: {FASTTRANSFER_PATH}")
    if version_info["detected"]:
        logger.info(f"FastTransfer version: {version_info['version']}")
    else:
        logger.warning("FastTransfer version could not be detected")
except FastTransferError as e:
    logger.error(f"Failed to initialize CommandBuilder: {e}")
    command_builder = None


@app.list_tools()
async def list_tools() -> list[Tool]:
    """List all available MCP tools."""
    return [
        Tool(
            name="preview_transfer_command",
            description=(
                "Build and preview a FastTransfer command WITHOUT executing it. "
                "This shows the exact command that will be run, with passwords masked. "
                "Use this FIRST before executing any transfer."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "source": {
                        "type": "object",
                        "properties": {
                            "type": {
                                "type": "string",
                                "enum": [e.value for e in SourceConnectionType],
                                "description": "Source database connection type",
                            },
                            "server": {
                                "type": "string",
                                "description": "Server address (host:port or host\\instance)",
                            },
                            "database": {
                                "type": "string",
                                "description": "Database name",
                            },
                            "schema": {
                                "type": "string",
                                "description": "Schema name (optional)",
                            },
                            "table": {
                                "type": "string",
                                "description": "Table name (optional if query or file_input provided)",
                            },
                            "query": {
                                "type": "string",
                                "description": "SQL query (alternative to table)",
                            },
                            "file_input": {
                                "type": "string",
                                "description": "File path for data input (alternative to table/query)",
                            },
                            "user": {"type": "string", "description": "Username"},
                            "password": {"type": "string", "description": "Password"},
                            "trusted_auth": {
                                "type": "boolean",
                                "description": "Use trusted authentication",
                                "default": False,
                            },
                            "connect_string": {
                                "type": "string",
                                "description": "Full connection string (alternative to server/user/password)",
                            },
                            "dsn": {
                                "type": "string",
                                "description": "ODBC DSN name",
                            },
                            "provider": {
                                "type": "string",
                                "description": "OleDB provider name",
                            },
                        },
                        "required": ["type", "database"],
                    },
                    "target": {
                        "type": "object",
                        "properties": {
                            "type": {
                                "type": "string",
                                "enum": [e.value for e in TargetConnectionType],
                                "description": "Target database connection type",
                            },
                            "server": {
                                "type": "string",
                                "description": "Server address (host:port or host\\instance)",
                            },
                            "database": {
                                "type": "string",
                                "description": "Database name",
                            },
                            "schema": {"type": "string", "description": "Schema name"},
                            "table": {
                                "type": "string",
                                "description": "Table name (required)",
                            },
                            "user": {"type": "string", "description": "Username"},
                            "password": {"type": "string", "description": "Password"},
                            "trusted_auth": {
                                "type": "boolean",
                                "description": "Use trusted authentication",
                                "default": False,
                            },
                            "connect_string": {
                                "type": "string",
                                "description": "Full connection string (alternative to server/user/password)",
                            },
                        },
                        "required": ["type", "database", "table"],
                    },
                    "options": {
                        "type": "object",
                        "properties": {
                            "method": {
                                "type": "string",
                                "enum": [e.value for e in ParallelismMethod],
                                "description": "Parallelism method",
                                "default": "None",
                            },
                            "distribute_key_column": {
                                "type": "string",
                                "description": "Column for data distribution",
                            },
                            "degree": {
                                "type": "integer",
                                "description": "Parallelism degree",
                                "default": -2,
                            },
                            "load_mode": {
                                "type": "string",
                                "enum": [e.value for e in LoadMode],
                                "description": "Load mode",
                                "default": "Append",
                            },
                            "batch_size": {
                                "type": "integer",
                                "description": "Batch size for bulk operations",
                            },
                            "map_method": {
                                "type": "string",
                                "enum": [e.value for e in MapMethod],
                                "description": "Column mapping method",
                                "default": "Position",
                            },
                            "run_id": {
                                "type": "string",
                                "description": "Run ID for logging",
                            },
                            "data_driven_query": {
                                "type": "string",
                                "description": "Custom SQL query for DataDriven parallelism method",
                            },
                            "use_work_tables": {
                                "type": "boolean",
                                "description": "Use intermediate work tables for CCI",
                            },
                            "settings_file": {
                                "type": "string",
                                "description": "Path to custom settings JSON file",
                            },
                            "log_level": {
                                "type": "string",
                                "enum": [e.value for e in LogLevel],
                                "description": "Override log level",
                            },
                            "no_banner": {
                                "type": "boolean",
                                "description": "Suppress the FastTransfer banner",
                            },
                            "license_path": {
                                "type": "string",
                                "description": "Path or URL to license file",
                            },
                        },
                    },
                },
                "required": ["source", "target"],
            },
        ),
        Tool(
            name="execute_transfer",
            description=(
                "Execute a FastTransfer command that was previously previewed. "
                "IMPORTANT: You must set confirmation=true to execute. "
                "This is a safety mechanism to prevent accidental execution."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "command": {
                        "type": "string",
                        "description": "The exact command from preview_transfer_command (including actual passwords)",
                    },
                    "confirmation": {
                        "type": "boolean",
                        "description": "Must be true to execute. This confirms the user has reviewed the command.",
                    },
                },
                "required": ["command", "confirmation"],
            },
        ),
        Tool(
            name="validate_connection",
            description=(
                "Validate database connection parameters. "
                "This checks that all required parameters are provided but does NOT "
                "actually test connectivity (would require database access)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "connection": {
                        "type": "object",
                        "properties": {
                            "type": {
                                "type": "string",
                                "description": "Connection type",
                            },
                            "server": {
                                "type": "string",
                                "description": "Server address",
                            },
                            "database": {
                                "type": "string",
                                "description": "Database name",
                            },
                            "user": {"type": "string", "description": "Username"},
                            "password": {"type": "string", "description": "Password"},
                            "connect_string": {
                                "type": "string",
                                "description": "Full connection string (alternative to server/user/password)",
                            },
                            "dsn": {
                                "type": "string",
                                "description": "ODBC DSN name",
                            },
                            "provider": {
                                "type": "string",
                                "description": "OleDB provider name",
                            },
                            "trusted_auth": {
                                "type": "boolean",
                                "description": "Use trusted authentication",
                            },
                            "file_input": {
                                "type": "string",
                                "description": "File path for data input",
                            },
                        },
                        "required": ["type", "database"],
                    },
                    "side": {
                        "type": "string",
                        "enum": ["source", "target"],
                        "description": "Connection side",
                    },
                },
                "required": ["connection", "side"],
            },
        ),
        Tool(
            name="list_supported_combinations",
            description="List all supported source to target database combinations.",
            inputSchema={"type": "object", "properties": {}},
        ),
        Tool(
            name="suggest_parallelism_method",
            description=(
                "Suggest the optimal parallelism method based on source database type "
                "and table characteristics. Provides recommendations for best performance."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "source_type": {
                        "type": "string",
                        "description": "Source database type (e.g., 'pgsql', 'oraodp', 'mssql')",
                    },
                    "has_numeric_key": {
                        "type": "boolean",
                        "description": "Whether the table has a numeric key column",
                    },
                    "has_identity_column": {
                        "type": "boolean",
                        "description": "Whether the table has an identity/auto-increment column",
                        "default": False,
                    },
                    "table_size_estimate": {
                        "type": "string",
                        "enum": ["small", "medium", "large"],
                        "description": "Estimated table size",
                    },
                },
                "required": ["source_type", "has_numeric_key", "table_size_estimate"],
            },
        ),
        Tool(
            name="get_version",
            description=(
                "Get the detected FastTransfer binary version, capabilities, "
                "and supported source/target types."
            ),
            inputSchema={"type": "object", "properties": {}},
        ),
    ]


@app.call_tool()
async def call_tool(name: str, arguments: Any) -> list[TextContent]:
    """Handle tool calls."""
    try:
        if name == "preview_transfer_command":
            return await handle_preview_transfer(arguments)
        elif name == "execute_transfer":
            return await handle_execute_transfer(arguments)
        elif name == "validate_connection":
            return await handle_validate_connection(arguments)
        elif name == "list_supported_combinations":
            return await handle_list_combinations(arguments)
        elif name == "suggest_parallelism_method":
            return await handle_suggest_parallelism(arguments)
        elif name == "get_version":
            return await handle_get_version(arguments)
        else:
            return [TextContent(type="text", text=f"Error: Unknown tool '{name}'")]

    except Exception as e:
        logger.exception(f"Error handling tool '{name}': {e}")
        return [TextContent(type="text", text=f"Error: {str(e)}")]


async def handle_preview_transfer(arguments: Dict[str, Any]) -> list[TextContent]:
    """Handle preview_transfer_command tool."""
    if command_builder is None:
        return [
            TextContent(
                type="text",
                text=(
                    "Error: FastTransfer binary not found or not accessible.\n"
                    f"Expected location: {FASTTRANSFER_PATH}\n"
                    "Please set FASTTRANSFER_PATH environment variable correctly."
                ),
            )
        ]

    try:
        # Validate and parse request
        request = TransferRequest(**arguments)

        # Build command
        command = command_builder.build_command(request)

        # Format for display (with masked passwords)
        display_command = command_builder.format_command_display(command, mask=True)

        # Create explanation
        explanation = _build_transfer_explanation(request)

        # Build response
        response = [
            "# FastTransfer Command Preview",
            "",
            "## What this command will do:",
            explanation,
            "",
            "## Command (passwords masked):",
            "```bash",
            display_command,
            "```",
            "",
            "## To execute this transfer:",
            "1. Review the command carefully",
            "2. Use the `execute_transfer` tool with the FULL command (not the masked version)",
            "3. Set `confirmation: true` to proceed",
            "",
            "## Security Notice:",
            "- Passwords are masked in this preview (shown as ******)",
            "- The actual execution will use the real passwords you provided",
            "- All executions are logged (with masked passwords) to: "
            + str(FASTTRANSFER_LOG_DIR),
            "",
            "## Full command for execution:",
            "```",
            " ".join(command),
            "```",
        ]

        return [TextContent(type="text", text="\n".join(response))]

    except ValidationError as e:
        error_msg = [
            "# Validation Error",
            "",
            "The provided parameters are invalid:",
            "",
        ]
        for error in e.errors():
            field = " -> ".join(str(x) for x in error["loc"])
            error_msg.append(f"- **{field}**: {error['msg']}")
        return [TextContent(type="text", text="\n".join(error_msg))]

    except FastTransferError as e:
        return [TextContent(type="text", text=f"Error: {str(e)}")]


async def handle_execute_transfer(arguments: Dict[str, Any]) -> list[TextContent]:
    """Handle execute_transfer tool."""
    if command_builder is None:
        return [
            TextContent(
                type="text",
                text="Error: FastTransfer binary not found. Please check FASTTRANSFER_PATH.",
            )
        ]

    # Check confirmation
    if not arguments.get("confirmation", False):
        return [
            TextContent(
                type="text",
                text=(
                    "# Execution Blocked\n\n"
                    "You must set `confirmation: true` to execute a transfer.\n"
                    "This safety mechanism ensures commands are only executed with explicit approval.\n\n"
                    "Please review the command carefully and confirm by setting:\n"
                    "```json\n"
                    '{"confirmation": true}\n'
                    "```"
                ),
            )
        ]

    # Get command
    command_str = arguments.get("command", "")
    if not command_str:
        return [
            TextContent(
                type="text",
                text="Error: No command provided. Please provide the command from preview_transfer_command.",
            )
        ]

    # Parse command string into list
    # Simple split by spaces (handles quoted strings)
    import shlex

    try:
        command = shlex.split(command_str)
    except ValueError as e:
        return [TextContent(type="text", text=f"Error parsing command: {str(e)}")]

    # Execute
    try:
        logger.info("Starting FastTransfer execution...")
        return_code, stdout, stderr = command_builder.execute_command(
            command, timeout=FASTTRANSFER_TIMEOUT, log_dir=FASTTRANSFER_LOG_DIR
        )

        # Format response
        success = return_code == 0
        status_emoji = "✅" if success else "❌"

        response = [
            f"# FastTransfer Execution {status_emoji}",
            "",
            f"**Status**: {'Success' if success else 'Failed'}",
            f"**Return Code**: {return_code}",
            f"**Log Location**: {FASTTRANSFER_LOG_DIR}",
            "",
            "## Output:",
            "```",
            stdout if stdout else "(no output)",
            "```",
        ]

        if stderr:
            response.extend(["", "## Error Output:", "```", stderr, "```"])

        if not success:
            response.extend(
                [
                    "",
                    "## Troubleshooting:",
                    "- Check database credentials and connectivity",
                    "- Verify table/schema names exist",
                    "- Check FastTransfer documentation for error details",
                    "- Review the full log file for more information",
                ]
            )

        return [TextContent(type="text", text="\n".join(response))]

    except FastTransferError as e:
        return [TextContent(type="text", text=f"# Execution Failed\n\nError: {str(e)}")]


async def handle_validate_connection(arguments: Dict[str, Any]) -> list[TextContent]:
    """Handle validate_connection tool."""
    try:
        # Validate request
        request = ConnectionValidationRequest(**arguments)

        # Build validation response
        connection = request.connection
        issues = []

        # Check for required fields based on connection type
        if (
            not connection.trusted_auth
            and not connection.connect_string
            and not connection.dsn
        ):
            if not connection.user:
                issues.append(
                    "- Username is required (unless using trusted authentication, connect_string, or dsn)"
                )

        # Check server format (only if server is provided)
        if (
            connection.server
            and ":" not in connection.server
            and "\\" not in connection.server
        ):
            issues.append(
                f"- Server '{connection.server}' may need port (e.g., localhost:5432) or instance name"
            )

        if issues:
            response = [
                f"# Connection Validation - {request.side.upper()}",
                "",
                "**Issues Found:**",
                "",
                *issues,
                "",
                "Note: This is a parameter check only. Actual connectivity is tested during transfer execution.",
            ]
        else:
            auth_method = "Trusted"
            if connection.connect_string:
                auth_method = "Connection String"
            elif connection.dsn:
                auth_method = "DSN"
            elif connection.trusted_auth:
                auth_method = "Trusted"
            else:
                auth_method = "Username/Password"

            response = [
                f"# Connection Validation - {request.side.upper()}",
                "",
                "**All required parameters present**",
                "",
                f"- Connection Type: {connection.type}",
                f"- Server: {connection.server or '(not specified)'}",
                f"- Database: {connection.database}",
                f"- Authentication: {auth_method}",
                "",
                "Note: This validates parameters only. Actual connectivity will be tested during transfer.",
            ]

        return [TextContent(type="text", text="\n".join(response))]

    except ValidationError as e:
        error_msg = ["# Validation Error", ""]
        for error in e.errors():
            field = " -> ".join(str(x) for x in error["loc"])
            error_msg.append(f"- **{field}**: {error['msg']}")
        return [TextContent(type="text", text="\n".join(error_msg))]


async def handle_list_combinations(arguments: Dict[str, Any]) -> list[TextContent]:
    """Handle list_supported_combinations tool."""
    combinations = get_supported_combinations()

    response = [
        "# Supported Database Combinations",
        "",
        "FastTransfer supports transfers between the following database systems:",
        "",
    ]

    for source, targets in combinations.items():
        response.append(f"## {source}")
        response.append("")
        response.append("Can transfer to:")
        for target in targets:
            response.append(f"- {target}")
        response.append("")

    response.extend(
        [
            "## Notes:",
            "- All combinations support both Append and Truncate load modes",
            "- Parallelism method availability depends on source database type",
            "- Some database-specific features (like Ctid for PostgreSQL) only work with specific sources",
        ]
    )

    return [TextContent(type="text", text="\n".join(response))]


async def handle_suggest_parallelism(arguments: Dict[str, Any]) -> list[TextContent]:
    """Handle suggest_parallelism_method tool."""
    try:
        # Validate request
        request = ParallelismSuggestionRequest(**arguments)

        # Get suggestion
        suggestion = suggest_parallelism_method(
            request.source_type,
            request.has_numeric_key,
            request.has_identity_column,
            request.table_size_estimate,
        )

        response = [
            "# Parallelism Method Recommendation",
            "",
            f"**Recommended Method**: `{suggestion['method']}`",
            "",
            "## Explanation:",
            suggestion["explanation"],
            "",
            "## Your Table Characteristics:",
            f"- Source Database: {request.source_type}",
            f"- Has Numeric Key: {'Yes' if request.has_numeric_key else 'No'}",
            f"- Has Identity Column: {'Yes' if request.has_identity_column else 'No'}",
            f"- Table Size: {request.table_size_estimate.capitalize()}",
            "",
            "## Other Considerations:",
            "- **Ctid**: Best for PostgreSQL (no key column needed)",
            "- **Rowid**: Best for Oracle (no key column needed)",
            "- **Physloc**: Best for SQL Server without numeric key",
            "- **RangeId**: Requires numeric key with good distribution",
            "- **Random**: Requires numeric key, uses modulo distribution",
            "- **DataDriven**: Works with any data type, uses distinct values",
            "- **Ntile**: Even distribution, works with numeric/date/string columns",
            "- **None**: Single-threaded, best for small tables or troubleshooting",
        ]

        return [TextContent(type="text", text="\n".join(response))]

    except ValidationError as e:
        error_msg = ["# Validation Error", ""]
        for error in e.errors():
            field = " -> ".join(str(x) for x in error["loc"])
            error_msg.append(f"- **{field}**: {error['msg']}")
        return [TextContent(type="text", text="\n".join(error_msg))]


async def handle_get_version(arguments: Dict[str, Any]) -> list[TextContent]:
    """Handle get_version tool."""
    if command_builder is None:
        return [
            TextContent(
                type="text",
                text=(
                    "Error: FastTransfer binary not found or not accessible.\n"
                    f"Expected location: {FASTTRANSFER_PATH}\n"
                    "Please set FASTTRANSFER_PATH environment variable correctly."
                ),
            )
        ]

    version_info = command_builder.get_version()
    caps = version_info["capabilities"]

    response = [
        "# FastTransfer Version Information",
        "",
        f"**Version**: {version_info['version'] or 'Unknown'}",
        f"**Detected**: {'Yes' if version_info['detected'] else 'No'}",
        f"**Binary Path**: {version_info['binary_path']}",
        "",
        "## Supported Source Types:",
        ", ".join(f"`{t}`" for t in caps["source_types"]),
        "",
        "## Supported Target Types:",
        ", ".join(f"`{t}`" for t in caps["target_types"]),
        "",
        "## Supported Parallelism Methods:",
        ", ".join(f"`{m}`" for m in caps["parallelism_methods"]),
        "",
        "## Feature Flags:",
        f"- No Banner: {'Yes' if caps['supports_nobanner'] else 'No'}",
        f"- Version Flag: {'Yes' if caps['supports_version_flag'] else 'No'}",
        f"- File Input: {'Yes' if caps['supports_file_input'] else 'No'}",
        f"- Settings File: {'Yes' if caps['supports_settings_file'] else 'No'}",
        f"- License Path: {'Yes' if caps['supports_license_path'] else 'No'}",
    ]

    return [TextContent(type="text", text="\n".join(response))]


def _build_transfer_explanation(request: TransferRequest) -> str:
    """Build a human-readable explanation of what the transfer will do."""
    parts = []

    # Source
    if request.source.file_input:
        parts.append(
            f"Import file '{request.source.file_input}' via {request.source.type} into {request.source.database}"
        )
    elif request.source.query:
        server_info = (
            f" ({request.source.server}/{request.source.database})"
            if request.source.server
            else f" ({request.source.database})"
        )
        parts.append(f"Execute query on {request.source.type}{server_info}")
    else:
        source_table = (
            f"{request.source.schema}.{request.source.table}"
            if request.source.schema
            else request.source.table
        )
        parts.append(
            f"Read from {request.source.type} table: {request.source.database}.{source_table}"
        )

    # Target
    target_table = (
        f"{request.target.schema}.{request.target.table}"
        if request.target.schema
        else request.target.table
    )
    parts.append(
        f"Write to {request.target.type} table: {request.target.database}.{target_table}"
    )

    # Load mode
    if request.options.load_mode.value == "Truncate":
        parts.append(
            "Mode: TRUNCATE target table before loading (all existing data will be deleted)"
        )
    else:
        parts.append("Mode: APPEND to existing target table data")

    # Parallelism
    if request.options.method.value != "None":
        parallel_desc = f"Parallelism: {request.options.method.value} method"
        if request.options.distribute_key_column:
            parallel_desc += f" on column '{request.options.distribute_key_column}'"
        parallel_desc += f" with degree {request.options.degree}"
        parts.append(parallel_desc)
    else:
        parts.append("Parallelism: None (single-threaded transfer)")

    # Mapping
    parts.append(f"Column mapping: {request.options.map_method.value}")

    return "\n".join(f"{i+1}. {part}" for i, part in enumerate(parts))


async def _run():
    """Async server startup logic."""
    logger.info("Starting FastTransfer MCP Server...")
    logger.info(f"FastTransfer binary: {FASTTRANSFER_PATH}")
    logger.info(f"Execution timeout: {FASTTRANSFER_TIMEOUT}s")
    logger.info(f"Log directory: {FASTTRANSFER_LOG_DIR}")

    from mcp.server.stdio import stdio_server

    async with stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())


def main():
    """Entry point for the MCP server (console script)."""
    asyncio.run(_run())


if __name__ == "__main__":
    main()
