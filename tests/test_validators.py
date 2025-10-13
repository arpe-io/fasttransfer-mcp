"""Tests for validators module."""

import pytest
from pydantic import ValidationError

from src.validators import (
    SourceConnectionType,
    TargetConnectionType,
    ParallelismMethod,
    LoadMode,
    MapMethod,
    ConnectionConfig,
    TransferOptions,
    TransferRequest,
    ConnectionValidationRequest,
    ParallelismSuggestionRequest,
)


class TestConnectionConfig:
    """Tests for ConnectionConfig model."""

    def test_valid_connection_with_credentials(self):
        """Test valid connection with username and password."""
        config = ConnectionConfig(
            type="pgsql",
            server="localhost:5432",
            database="testdb",
            user="testuser",
            password="testpass",
        )
        assert config.server == "localhost:5432"
        assert config.user == "testuser"

    def test_valid_connection_with_trusted_auth(self):
        """Test valid connection with trusted authentication."""
        config = ConnectionConfig(
            type="mssql", server="localhost", database="testdb", trusted_auth=True
        )
        assert config.trusted_auth is True

    def test_valid_connection_with_connect_string(self):
        """Test valid connection with connection string."""
        config = ConnectionConfig(
            type="odbc",
            server="localhost",
            database="testdb",
            connect_string="DSN=mydsn;UID=user;PWD=pass",
        )
        assert config.connect_string is not None

    def test_invalid_connection_no_auth(self):
        """Test that connection without authentication fails."""
        with pytest.raises(ValidationError) as exc_info:
            ConnectionConfig(type="pgsql", server="localhost:5432", database="testdb")
        errors = exc_info.value.errors()
        assert any(
            "user" in str(e) or "authentication" in str(e).lower() for e in errors
        )

    def test_connection_with_schema_and_table(self):
        """Test connection with optional schema and table."""
        config = ConnectionConfig(
            type="pgsql",
            server="localhost:5432",
            database="testdb",
            schema="public",
            table="users",
            user="testuser",
            password="testpass",
        )
        assert config.schema == "public"
        assert config.table == "users"


class TestTransferOptions:
    """Tests for TransferOptions model."""

    def test_default_options(self):
        """Test default transfer options."""
        options = TransferOptions()
        assert options.method == ParallelismMethod.NONE
        assert options.degree == -2
        assert options.load_mode == LoadMode.APPEND
        assert options.map_method == MapMethod.POSITION

    def test_valid_degree_zero(self):
        """Test that degree 0 (auto) is valid."""
        options = TransferOptions(degree=0)
        assert options.degree == 0

    def test_valid_degree_positive(self):
        """Test that positive degree is valid."""
        options = TransferOptions(degree=8)
        assert options.degree == 8

    def test_valid_degree_negative(self):
        """Test that negative degree is valid."""
        options = TransferOptions(degree=-4)
        assert options.degree == -4

    def test_invalid_degree_too_large(self):
        """Test that degree >= 1024 is invalid."""
        with pytest.raises(ValidationError):
            TransferOptions(degree=1024)

    def test_data_driven_requires_distribute_key(self):
        """Test that DataDriven method requires distribute_key_column."""
        with pytest.raises(ValidationError) as exc_info:
            TransferOptions(method=ParallelismMethod.DATA_DRIVEN)
        errors = exc_info.value.errors()
        assert any("distribute_key_column" in str(e) for e in errors)

    def test_data_driven_with_distribute_key_valid(self):
        """Test that DataDriven with distribute_key_column is valid."""
        options = TransferOptions(
            method=ParallelismMethod.DATA_DRIVEN, distribute_key_column="id"
        )
        assert options.distribute_key_column == "id"

    def test_range_id_requires_distribute_key(self):
        """Test that RangeId method requires distribute_key_column."""
        with pytest.raises(ValidationError):
            TransferOptions(method=ParallelismMethod.RANGE_ID)

    def test_random_requires_distribute_key(self):
        """Test that Random method requires distribute_key_column."""
        with pytest.raises(ValidationError):
            TransferOptions(method=ParallelismMethod.RANDOM)

    def test_ctid_no_distribute_key_needed(self):
        """Test that Ctid doesn't require distribute_key_column."""
        options = TransferOptions(method=ParallelismMethod.CTID)
        assert options.method == ParallelismMethod.CTID
        assert options.distribute_key_column is None

    def test_batch_size_validation(self):
        """Test batch size must be positive."""
        with pytest.raises(ValidationError):
            TransferOptions(batch_size=0)

        options = TransferOptions(batch_size=10000)
        assert options.batch_size == 10000


class TestTransferRequest:
    """Tests for TransferRequest model."""

    def test_valid_transfer_request(self):
        """Test a valid complete transfer request."""
        request = TransferRequest(
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
            options={"method": "Ctid", "degree": -2, "load_mode": "Truncate"},
        )
        assert request.source.type == "pgsql"
        assert request.target.type == "msbulk"
        assert request.options.method == ParallelismMethod.CTID

    def test_source_requires_table_or_query(self):
        """Test that source must have either table or query."""
        with pytest.raises(ValidationError) as exc_info:
            TransferRequest(
                source={
                    "type": "pgsql",
                    "server": "localhost:5432",
                    "database": "sourcedb",
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
            )
        errors = exc_info.value.errors()
        assert any("table" in str(e) or "query" in str(e) for e in errors)

    def test_source_cannot_have_both_table_and_query(self):
        """Test that source cannot have both table and query."""
        with pytest.raises(ValidationError) as exc_info:
            TransferRequest(
                source={
                    "type": "pgsql",
                    "server": "localhost:5432",
                    "database": "sourcedb",
                    "table": "users",
                    "query": "SELECT * FROM users",
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
            )
        errors = exc_info.value.errors()
        assert any("table" in str(e) and "query" in str(e) for e in errors)

    def test_target_requires_table(self):
        """Test that target must have table."""
        with pytest.raises(ValidationError) as exc_info:
            TransferRequest(
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
                    "user": "user",
                    "password": "pass",
                },
            )
        errors = exc_info.value.errors()
        assert any("table" in str(e) for e in errors)

    def test_ctid_only_with_postgresql(self):
        """Test that Ctid method only works with PostgreSQL sources."""
        with pytest.raises(ValidationError) as exc_info:
            TransferRequest(
                source={
                    "type": "mssql",
                    "server": "localhost",
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
                options={"method": "Ctid"},
            )
        errors = exc_info.value.errors()
        assert any("Ctid" in str(e) and "PostgreSQL" in str(e) for e in errors)

    def test_ctid_valid_with_postgresql(self):
        """Test that Ctid works with PostgreSQL source."""
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
            options={"method": "Ctid"},
        )
        assert request.options.method == ParallelismMethod.CTID

    def test_rowid_only_with_oracle(self):
        """Test that Rowid method only works with Oracle sources."""
        with pytest.raises(ValidationError) as exc_info:
            TransferRequest(
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
                options={"method": "Rowid"},
            )
        errors = exc_info.value.errors()
        assert any("Rowid" in str(e) and "Oracle" in str(e) for e in errors)


class TestConnectionValidationRequest:
    """Tests for ConnectionValidationRequest model."""

    def test_valid_source_validation_request(self):
        """Test valid source validation request."""
        request = ConnectionValidationRequest(
            connection={
                "type": "pgsql",
                "server": "localhost:5432",
                "database": "testdb",
                "user": "user",
                "password": "pass",
            },
            side="source",
        )
        assert request.side == "source"

    def test_valid_target_validation_request(self):
        """Test valid target validation request."""
        request = ConnectionValidationRequest(
            connection={
                "type": "msbulk",
                "server": "localhost",
                "database": "testdb",
                "user": "user",
                "password": "pass",
            },
            side="target",
        )
        assert request.side == "target"

    def test_invalid_side(self):
        """Test that invalid side is rejected."""
        with pytest.raises(ValidationError) as exc_info:
            ConnectionValidationRequest(
                connection={
                    "type": "pgsql",
                    "server": "localhost:5432",
                    "database": "testdb",
                    "user": "user",
                    "password": "pass",
                },
                side="invalid",
            )
        errors = exc_info.value.errors()
        assert any("side" in str(e) for e in errors)


class TestParallelismSuggestionRequest:
    """Tests for ParallelismSuggestionRequest model."""

    def test_valid_suggestion_request(self):
        """Test valid parallelism suggestion request."""
        request = ParallelismSuggestionRequest(
            source_type="pgsql",
            has_numeric_key=True,
            has_identity_column=True,
            table_size_estimate="large",
        )
        assert request.source_type == "pgsql"
        assert request.has_numeric_key is True
        assert request.table_size_estimate == "large"

    def test_invalid_table_size(self):
        """Test that invalid table size is rejected."""
        with pytest.raises(ValidationError) as exc_info:
            ParallelismSuggestionRequest(
                source_type="pgsql", has_numeric_key=True, table_size_estimate="huge"
            )
        errors = exc_info.value.errors()
        assert any("table_size" in str(e) for e in errors)

    def test_default_has_identity_column(self):
        """Test default value for has_identity_column."""
        request = ParallelismSuggestionRequest(
            source_type="pgsql", has_numeric_key=True, table_size_estimate="medium"
        )
        assert request.has_identity_column is False
