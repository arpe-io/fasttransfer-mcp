"""
FastTransfer command builder and executor.

This module provides functionality to build, validate, and execute
FastTransfer commands with proper security measures.
"""

import os
import shlex
import subprocess
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime

from .validators import (
    TransferRequest,
    ConnectionConfig,
    SourceConnectionType,
    TargetConnectionType,
)


logger = logging.getLogger(__name__)


class FastTransferError(Exception):
    """Base exception for FastTransfer operations."""

    pass


class CommandBuilder:
    """Builds FastTransfer commands from validated requests."""

    def __init__(self, binary_path: str):
        """
        Initialize the command builder.

        Args:
            binary_path: Path to the FastTransfer binary

        Raises:
            FastTransferError: If binary doesn't exist or isn't executable
        """
        self.binary_path = Path(binary_path)
        self._validate_binary()

    def _validate_binary(self) -> None:
        """Validate that FastTransfer binary exists and is executable."""
        if not self.binary_path.exists():
            raise FastTransferError(
                f"FastTransfer binary not found at: {self.binary_path}"
            )

        if not self.binary_path.is_file():
            raise FastTransferError(
                f"FastTransfer path is not a file: {self.binary_path}"
            )

        if not os.access(self.binary_path, os.X_OK):
            raise FastTransferError(
                f"FastTransfer binary is not executable: {self.binary_path}"
            )

    def build_command(self, request: TransferRequest) -> List[str]:
        """
        Build a FastTransfer command from a validated request.

        Args:
            request: Validated transfer request

        Returns:
            Command as list of strings (suitable for subprocess)
        """
        cmd = [str(self.binary_path)]

        # Add source connection parameters
        cmd.extend(self._build_source_params(request.source))

        # Add target connection parameters
        cmd.extend(self._build_target_params(request.target))

        # Add transfer options
        cmd.extend(self._build_option_params(request.options))

        return cmd

    def _build_source_params(self, source: ConnectionConfig) -> List[str]:
        """Build source connection parameters."""
        params = []

        # Connection type
        params.extend(["--sourceconnectiontype", source.type])

        # Connection string or individual parameters
        if source.connect_string:
            params.extend(["--sourceconnectstring", source.connect_string])
        elif source.dsn:
            params.extend(["--sourcedsn", source.dsn])
        else:
            # Standard connection parameters
            if source.server:
                params.extend(["--sourceserver", source.server])
            if source.user:
                params.extend(["--sourceuser", source.user])
            if source.password:
                params.extend(["--sourcepassword", source.password])
            if source.trusted_auth:
                params.append("--sourcetrusted")

        # Database, schema, table/query
        if source.database:
            params.extend(["--sourcedatabase", source.database])
        if source.schema:
            params.extend(["--sourceschema", source.schema])
        if source.table:
            params.extend(["--sourcetable", source.table])
        elif source.query:
            params.extend(["--query", source.query])

        # Provider (for OleDB)
        if source.provider:
            params.extend(["--sourceprovider", source.provider])

        return params

    def _build_target_params(self, target: ConnectionConfig) -> List[str]:
        """Build target connection parameters."""
        params = []

        # Connection type
        params.extend(["--targetconnectiontype", target.type])

        # Connection string or individual parameters
        if target.connect_string:
            params.extend(["--targetconnectstring", target.connect_string])
        else:
            # Standard connection parameters
            if target.server:
                params.extend(["--targetserver", target.server])
            if target.user:
                params.extend(["--targetuser", target.user])
            if target.password:
                params.extend(["--targetpassword", target.password])
            if target.trusted_auth:
                params.append("--targettrusted")

        # Database, schema, table
        if target.database:
            params.extend(["--targetdatabase", target.database])
        if target.schema:
            params.extend(["--targetschema", target.schema])
        if target.table:
            params.extend(["--targettable", target.table])

        return params

    def _build_option_params(self, options) -> List[str]:
        """Build transfer option parameters."""
        params = []

        # Parallelism method
        params.extend(["--method", options.method.value])

        # Distribute key column
        if options.distribute_key_column:
            params.extend(["--distributeKeyColumn", options.distribute_key_column])

        # Degree of parallelism
        params.extend(["--degree", str(options.degree)])

        # Load mode
        params.extend(["--loadmode", options.load_mode.value])

        # Batch size
        if options.batch_size:
            params.extend(["--batchsize", str(options.batch_size)])

        # Map method
        params.extend(["--mapmethod", options.map_method.value])

        # Run ID
        if options.run_id:
            params.extend(["--runid", options.run_id])

        return params

    def mask_password(self, command: List[str]) -> List[str]:
        """
        Create a copy of command with passwords masked.

        Args:
            command: Command list to mask

        Returns:
            Command list with passwords replaced by '******'
        """
        masked = []
        mask_next = False

        for part in command:
            if mask_next:
                masked.append("******")
                mask_next = False
            else:
                if part in ["--sourcepassword", "--targetpassword", "-x", "-X"]:
                    mask_next = True
                masked.append(part)

        return masked

    def format_command_display(self, command: List[str], mask: bool = True) -> str:
        """
        Format command for display with optional password masking.

        Args:
            command: Command list
            mask: Whether to mask passwords (default: True)

        Returns:
            Formatted command string
        """
        display_cmd = self.mask_password(command) if mask else command

        # Format for readability
        formatted_parts = [display_cmd[0]]  # Binary path

        i = 1
        while i < len(display_cmd):
            if i < len(display_cmd) - 1 and display_cmd[i].startswith("--"):
                # This is a parameter with a value
                param = display_cmd[i]
                value = display_cmd[i + 1]
                # Quote values that might contain spaces
                if " " in value:
                    formatted_parts.append(f'{param} "{value}"')
                else:
                    formatted_parts.append(f"{param} {value}")
                i += 2
            else:
                # This is a standalone flag or orphaned value
                formatted_parts.append(display_cmd[i])
                i += 1

        return " \\\n  ".join(formatted_parts)

    def execute_command(
        self, command: List[str], timeout: int = 1800, log_dir: Optional[Path] = None
    ) -> Tuple[int, str, str]:
        """
        Execute a FastTransfer command.

        Args:
            command: Command to execute
            timeout: Timeout in seconds (default: 1800 = 30 minutes)
            log_dir: Directory for execution logs

        Returns:
            Tuple of (return_code, stdout, stderr)

        Raises:
            subprocess.TimeoutExpired: If execution exceeds timeout
            FastTransferError: If execution fails
        """
        start_time = datetime.now()

        # Log command execution (with masked passwords)
        masked_cmd = self.mask_password(command)
        logger.info(f"Executing FastTransfer command: {' '.join(masked_cmd)}")

        try:
            # Execute command
            result = subprocess.run(
                command, capture_output=True, text=True, timeout=timeout, check=False
            )

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            # Log result
            logger.info(
                f"FastTransfer completed in {duration:.2f}s with return code {result.returncode}"
            )

            # Save logs if directory provided
            if log_dir:
                self._save_execution_log(
                    log_dir,
                    command,
                    result.returncode,
                    result.stdout,
                    result.stderr,
                    duration,
                )

            return result.returncode, result.stdout, result.stderr

        except subprocess.TimeoutExpired as e:
            logger.error(f"FastTransfer execution timed out after {timeout}s")
            raise FastTransferError(
                f"Execution timed out after {timeout} seconds"
            ) from e

        except Exception as e:
            logger.error(f"FastTransfer execution failed: {e}")
            raise FastTransferError(f"Execution failed: {e}") from e

    def _save_execution_log(
        self,
        log_dir: Path,
        command: List[str],
        return_code: int,
        stdout: str,
        stderr: str,
        duration: float,
    ) -> None:
        """Save execution log to file."""
        try:
            log_dir.mkdir(parents=True, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = log_dir / f"fasttransfer_{timestamp}.log"

            masked_cmd = self.mask_password(command)

            with open(log_file, "w") as f:
                f.write(f"FastTransfer Execution Log\n")
                f.write(f"{'=' * 80}\n\n")
                f.write(f"Timestamp: {datetime.now().isoformat()}\n")
                f.write(f"Duration: {duration:.2f} seconds\n")
                f.write(f"Return Code: {return_code}\n\n")
                f.write(f"Command:\n{' '.join(masked_cmd)}\n\n")
                f.write(f"{'=' * 80}\n")
                f.write(f"STDOUT:\n{stdout}\n\n")
                f.write(f"{'=' * 80}\n")
                f.write(f"STDERR:\n{stderr}\n")

            logger.info(f"Execution log saved to: {log_file}")

        except Exception as e:
            logger.warning(f"Failed to save execution log: {e}")


def get_supported_combinations() -> Dict[str, List[str]]:
    """
    Get supported source -> target database combinations.

    Returns:
        Dictionary mapping source types to list of compatible target types
    """
    return {
        "ClickHouse": [
            "ClickHouse",
            "DuckDB",
            "PostgreSQL",
            "SQL Server",
            "MySQL",
            "Oracle",
        ],
        "DuckDB": [
            "DuckDB",
            "PostgreSQL",
            "SQL Server",
            "MySQL",
            "Oracle",
            "ClickHouse",
        ],
        "MySQL": [
            "MySQL",
            "PostgreSQL",
            "SQL Server",
            "Oracle",
            "DuckDB",
            "ClickHouse",
        ],
        "Netezza": ["Netezza", "PostgreSQL", "SQL Server", "Oracle", "DuckDB"],
        "Oracle": [
            "Oracle",
            "PostgreSQL",
            "SQL Server",
            "MySQL",
            "DuckDB",
            "ClickHouse",
        ],
        "PostgreSQL": [
            "PostgreSQL",
            "SQL Server",
            "MySQL",
            "Oracle",
            "DuckDB",
            "ClickHouse",
            "Netezza",
        ],
        "SAP HANA": ["SAP HANA", "PostgreSQL", "SQL Server", "Oracle", "DuckDB"],
        "SQL Server": [
            "SQL Server",
            "PostgreSQL",
            "MySQL",
            "Oracle",
            "DuckDB",
            "ClickHouse",
        ],
        "Teradata": ["Teradata", "PostgreSQL", "SQL Server", "Oracle", "DuckDB"],
    }


def suggest_parallelism_method(
    source_type: str,
    has_numeric_key: bool,
    has_identity_column: bool,
    table_size_estimate: str,
) -> Dict[str, str]:
    """
    Suggest optimal parallelism method based on source database and table characteristics.

    Args:
        source_type: Source database type
        has_numeric_key: Whether table has a numeric key column
        has_identity_column: Whether table has an identity/auto-increment column
        table_size_estimate: Table size ('small', 'medium', 'large')

    Returns:
        Dictionary with 'method' and 'explanation' keys
    """
    source_lower = source_type.lower()

    # Small tables - no parallelism needed
    if table_size_estimate == "small":
        return {
            "method": "None",
            "explanation": "Table is small - parallelism overhead would likely reduce performance. Use None for best results.",
        }

    # PostgreSQL - Ctid is optimal
    if source_lower in ["pgsql", "pgcopy", "postgres", "postgresql"]:
        return {
            "method": "Ctid",
            "explanation": "PostgreSQL source detected. Ctid method provides efficient parallel reading using PostgreSQL's native tuple identifier.",
        }

    # Oracle - Rowid is optimal
    if source_lower in ["oracle", "oraodp"]:
        return {
            "method": "Rowid",
            "explanation": "Oracle source detected. Rowid method provides efficient parallel reading using Oracle's native row identifier.",
        }

    # Netezza - NZDataSlice is optimal
    if source_lower in ["nzoledb", "nzcopy", "nzsql", "nzbulk", "netezza"]:
        return {
            "method": "NZDataSlice",
            "explanation": "Netezza source detected. NZDataSlice method leverages Netezza's data slicing for optimal parallel performance.",
        }

    # If has numeric key - RangeId or Random are good choices
    if has_numeric_key:
        if has_identity_column or table_size_estimate == "large":
            return {
                "method": "RangeId",
                "explanation": "RangeId recommended for tables with numeric keys. It distributes data by dividing the numeric range into chunks, providing good load balancing for large tables.",
            }
        else:
            return {
                "method": "Random",
                "explanation": "Random method recommended. Uses modulo operation on numeric key for distribution. Works well when key values are evenly distributed.",
            }

    # No specific optimization available - use DataDriven or Ntile
    if table_size_estimate == "large":
        return {
            "method": "DataDriven",
            "explanation": "DataDriven method recommended. Distributes based on distinct values of a key column. Choose a column with good cardinality for best results.",
        }
    else:
        return {
            "method": "Ntile",
            "explanation": "Ntile method recommended. Evenly distributes data across parallel workers. Works with numeric, date, or string columns.",
        }
