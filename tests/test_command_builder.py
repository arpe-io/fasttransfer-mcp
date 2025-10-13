"""Tests for FastTransfer command builder."""

import os
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import subprocess

import pytest

from src.fasttransfer import (
    CommandBuilder,
    FastTransferError,
    get_supported_combinations,
    suggest_parallelism_method,
)
from src.validators import TransferRequest, ParallelismMethod


@pytest.fixture
def mock_binary(tmp_path):
    """Create a mock FastTransfer binary."""
    binary = tmp_path / "FastTransfer"
    binary.write_text("#!/bin/bash\necho 'mock binary'")
    binary.chmod(0o755)
    return str(binary)


@pytest.fixture
def command_builder(mock_binary):
    """Create a CommandBuilder with mock binary."""
    return CommandBuilder(mock_binary)


@pytest.fixture
def sample_request():
    """Create a sample transfer request."""
    return TransferRequest(
        source={
            "type": "pgsql",
            "server": "localhost:5432",
            "database": "sourcedb",
            "schema": "public",
            "table": "users",
            "user": "sourceuser",
            "password": "sourcepass",
        },
        target={
            "type": "msbulk",
            "server": "localhost",
            "database": "targetdb",
            "schema": "dbo",
            "table": "users",
            "user": "targetuser",
            "password": "targetpass",
        },
        options={
            "method": "Ctid",
            "degree": -2,
            "load_mode": "Truncate",
            "map_method": "Position",
        },
    )


class TestCommandBuilder:
    """Tests for CommandBuilder class."""

    def test_init_with_valid_binary(self, mock_binary):
        """Test initialization with valid binary."""
        builder = CommandBuilder(mock_binary)
        assert builder.binary_path == Path(mock_binary)

    def test_init_with_nonexistent_binary(self):
        """Test initialization with nonexistent binary fails."""
        with pytest.raises(FastTransferError) as exc_info:
            CommandBuilder("/nonexistent/path/FastTransfer")
        assert "not found" in str(exc_info.value)

    def test_init_with_non_executable_binary(self, tmp_path):
        """Test initialization with non-executable binary fails."""
        binary = tmp_path / "FastTransfer"
        binary.write_text("not executable")
        binary.chmod(0o644)  # No execute permission

        with pytest.raises(FastTransferError) as exc_info:
            CommandBuilder(str(binary))
        assert "not executable" in str(exc_info.value)

    def test_build_command_basic(self, command_builder, sample_request):
        """Test building a basic command."""
        command = command_builder.build_command(sample_request)

        # Check that command is a list
        assert isinstance(command, list)

        # Check binary path is first
        assert command[0] == str(command_builder.binary_path)

        # Check source parameters
        assert "--sourceconnectiontype" in command
        assert "pgsql" in command
        assert "--sourceserver" in command
        assert "localhost:5432" in command
        assert "--sourceuser" in command
        assert "sourceuser" in command
        assert "--sourcepassword" in command
        assert "sourcepass" in command
        assert "--sourcedatabase" in command
        assert "sourcedb" in command
        assert "--sourceschema" in command
        assert "public" in command
        assert "--sourcetable" in command
        assert "users" in command

        # Check target parameters
        assert "--targetconnectiontype" in command
        assert "msbulk" in command
        assert "--targetserver" in command
        assert "localhost" in command
        assert "--targetuser" in command
        assert "targetuser" in command
        assert "--targetpassword" in command
        assert "targetpass" in command
        assert "--targetdatabase" in command
        assert "targetdb" in command
        assert "--targetschema" in command
        assert "dbo" in command
        assert "--targettable" in command

        # Check options
        assert "--method" in command
        assert "Ctid" in command
        assert "--degree" in command
        assert "-2" in command
        assert "--loadmode" in command
        assert "Truncate" in command

    def test_build_command_with_query(self, command_builder):
        """Test building command with query instead of table."""
        request = TransferRequest(
            source={
                "type": "pgsql",
                "server": "localhost:5432",
                "database": "sourcedb",
                "query": "SELECT * FROM users WHERE active = true",
                "user": "sourceuser",
                "password": "sourcepass",
            },
            target={
                "type": "msbulk",
                "server": "localhost",
                "database": "targetdb",
                "table": "users",
                "user": "targetuser",
                "password": "targetpass",
            },
        )

        command = command_builder.build_command(request)

        # Check query is included
        assert "--query" in command
        query_idx = command.index("--query")
        assert "SELECT * FROM users WHERE active = true" in command[query_idx + 1]

        # Check table is NOT included
        assert "--sourcetable" not in command

    def test_build_command_with_trusted_auth(self, command_builder):
        """Test building command with trusted authentication."""
        request = TransferRequest(
            source={
                "type": "mssql",
                "server": "localhost",
                "database": "sourcedb",
                "table": "users",
                "trusted_auth": True,
            },
            target={
                "type": "msbulk",
                "server": "localhost",
                "database": "targetdb",
                "table": "users",
                "trusted_auth": True,
            },
        )

        command = command_builder.build_command(request)

        # Check trusted auth flags
        assert "--sourcetrusted" in command
        assert "--targettrusted" in command

        # Check no passwords
        assert "--sourcepassword" not in command
        assert "--targetpassword" not in command

    def test_build_command_with_optional_params(self, command_builder):
        """Test building command with all optional parameters."""
        request = TransferRequest(
            source={
                "type": "pgsql",
                "server": "localhost:5432",
                "database": "sourcedb",
                "table": "users",
                "user": "user",
                "password": "pass",
            },
            target={
                "type": "msbulk",
                "server": "localhost",
                "database": "targetdb",
                "table": "users",
                "user": "user",
                "password": "pass",
            },
            options={
                "method": "RangeId",
                "distribute_key_column": "id",
                "degree": 8,
                "load_mode": "Append",
                "batch_size": 50000,
                "map_method": "Name",
                "run_id": "test-run-001",
            },
        )

        command = command_builder.build_command(request)

        # Check all optional parameters
        assert "--distributeKeyColumn" in command
        assert "id" in command
        assert "--batchsize" in command
        assert "50000" in command
        assert "--mapmethod" in command
        assert "Name" in command
        assert "--runid" in command
        assert "test-run-001" in command

    def test_mask_password(self, command_builder):
        """Test password masking in commands."""
        command = [
            "/path/to/FastTransfer",
            "--sourceuser",
            "user1",
            "--sourcepassword",
            "secret123",
            "--targetuser",
            "user2",
            "--targetpassword",
            "pass456",
        ]

        masked = command_builder.mask_password(command)

        # Check passwords are masked
        assert "secret123" not in masked
        assert "pass456" not in masked
        assert masked.count("******") == 2

        # Check other values remain
        assert "user1" in masked
        assert "user2" in masked

    def test_format_command_display_with_mask(self, command_builder, sample_request):
        """Test formatting command for display with masked passwords."""
        command = command_builder.build_command(sample_request)
        display = command_builder.format_command_display(command, mask=True)

        # Check passwords are masked - look for password param followed by masked value
        assert "--sourcepassword ******" in display
        assert "--targetpassword ******" in display
        assert "******" in display

        # Ensure actual password values are not present
        assert " sourcepass " not in display and not display.endswith(" sourcepass")
        assert " targetpass " not in display and not display.endswith(" targetpass")

        # Check other values remain
        assert "sourceuser" in display
        assert "targetuser" in display

    def test_format_command_display_without_mask(self, command_builder, sample_request):
        """Test formatting command for display without masking."""
        command = command_builder.build_command(sample_request)
        display = command_builder.format_command_display(command, mask=False)

        # Check passwords are visible - look for password param followed by actual value
        assert "--sourcepassword sourcepass" in display
        assert "--targetpassword targetpass" in display

    @patch("subprocess.run")
    def test_execute_command_success(self, mock_run, command_builder):
        """Test successful command execution."""
        # Mock successful execution
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "Transfer completed successfully"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        command = [str(command_builder.binary_path), "--help"]
        return_code, stdout, stderr = command_builder.execute_command(
            command, timeout=10
        )

        assert return_code == 0
        assert "success" in stdout.lower()
        mock_run.assert_called_once()

    @patch("subprocess.run")
    def test_execute_command_failure(self, mock_run, command_builder):
        """Test failed command execution."""
        # Mock failed execution
        mock_result = Mock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        mock_result.stderr = "Connection failed"
        mock_run.return_value = mock_result

        command = [str(command_builder.binary_path), "--help"]
        return_code, stdout, stderr = command_builder.execute_command(
            command, timeout=10
        )

        assert return_code == 1
        assert "failed" in stderr.lower()

    @patch("subprocess.run")
    def test_execute_command_timeout(self, mock_run, command_builder):
        """Test command execution timeout."""
        # Mock timeout
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="test", timeout=1)

        command = [str(command_builder.binary_path), "--help"]
        with pytest.raises(FastTransferError) as exc_info:
            command_builder.execute_command(command, timeout=1)

        assert "timed out" in str(exc_info.value).lower()

    @patch("subprocess.run")
    def test_execute_command_with_logging(self, mock_run, command_builder, tmp_path):
        """Test command execution with log saving."""
        # Mock successful execution
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "Success"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        log_dir = tmp_path / "logs"
        command = [str(command_builder.binary_path), "--help"]
        command_builder.execute_command(command, timeout=10, log_dir=log_dir)

        # Check log file was created
        assert log_dir.exists()
        log_files = list(log_dir.glob("fasttransfer_*.log"))
        assert len(log_files) == 1

        # Check log content
        log_content = log_files[0].read_text()
        assert "FastTransfer Execution Log" in log_content
        assert "Return Code: 0" in log_content


class TestHelperFunctions:
    """Tests for helper functions."""

    def test_get_supported_combinations(self):
        """Test getting supported database combinations."""
        combinations = get_supported_combinations()

        # Check it's a dict
        assert isinstance(combinations, dict)

        # Check some known combinations
        assert "PostgreSQL" in combinations
        assert "SQL Server" in combinations
        assert "Oracle" in combinations

        # Check PostgreSQL can target multiple databases
        assert len(combinations["PostgreSQL"]) > 3

    def test_suggest_parallelism_small_table(self):
        """Test parallelism suggestion for small table."""
        suggestion = suggest_parallelism_method(
            source_type="pgsql",
            has_numeric_key=True,
            has_identity_column=False,
            table_size_estimate="small",
        )

        assert suggestion["method"] == "None"
        assert "small" in suggestion["explanation"].lower()

    def test_suggest_parallelism_postgresql(self):
        """Test parallelism suggestion for PostgreSQL source."""
        suggestion = suggest_parallelism_method(
            source_type="pgsql",
            has_numeric_key=False,
            has_identity_column=False,
            table_size_estimate="large",
        )

        assert suggestion["method"] == "Ctid"
        assert (
            "PostgreSQL" in suggestion["explanation"]
            or "Ctid" in suggestion["explanation"]
        )

    def test_suggest_parallelism_oracle(self):
        """Test parallelism suggestion for Oracle source."""
        suggestion = suggest_parallelism_method(
            source_type="oracle",
            has_numeric_key=False,
            has_identity_column=False,
            table_size_estimate="medium",
        )

        assert suggestion["method"] == "Rowid"
        assert (
            "Oracle" in suggestion["explanation"]
            or "Rowid" in suggestion["explanation"]
        )

    def test_suggest_parallelism_netezza(self):
        """Test parallelism suggestion for Netezza source."""
        suggestion = suggest_parallelism_method(
            source_type="nzcopy",
            has_numeric_key=False,
            has_identity_column=False,
            table_size_estimate="large",
        )

        assert suggestion["method"] == "NZDataSlice"
        assert (
            "Netezza" in suggestion["explanation"]
            or "NZDataSlice" in suggestion["explanation"]
        )

    def test_suggest_parallelism_with_numeric_key(self):
        """Test parallelism suggestion for table with numeric key."""
        suggestion = suggest_parallelism_method(
            source_type="mssql",
            has_numeric_key=True,
            has_identity_column=True,
            table_size_estimate="large",
        )

        assert suggestion["method"] in ["RangeId", "Random"]
        assert "numeric" in suggestion["explanation"].lower()

    def test_suggest_parallelism_generic_large_table(self):
        """Test parallelism suggestion for generic large table."""
        suggestion = suggest_parallelism_method(
            source_type="mysql",
            has_numeric_key=False,
            has_identity_column=False,
            table_size_estimate="large",
        )

        assert suggestion["method"] in ["DataDriven", "Ntile"]
