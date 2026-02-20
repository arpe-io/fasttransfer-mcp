"""Tests for validators module."""

import pytest
from pydantic import ValidationError

from src.validators import (
    SourceConnectionType,
    TargetConnectionType,
    ParallelismMethod,
    LoadMode,
    MapMethod,
    LogLevel,
    ConnectionConfig,
    TransferOptions,
    TransferRequest,
    ConnectionValidationRequest,
    ParallelismSuggestionRequest,
)


class TestSourceConnectionType:
    """Tests for SourceConnectionType enum."""

    def test_oraodp_exists(self):
        """Test that oraodp source type exists."""
        assert SourceConnectionType("oraodp") == SourceConnectionType.ORACLE_ODP

    def test_msoledbsql_exists(self):
        """Test that msoledbsql source type exists."""
        assert SourceConnectionType("msoledbsql") == SourceConnectionType.MSOLEDBSQL

    def test_nzoledb_exists(self):
        """Test that nzoledb source type exists."""
        assert SourceConnectionType("nzoledb") == SourceConnectionType.NETEZZA_OLEDB

    def test_nzsql_exists(self):
        """Test that nzsql source type exists."""
        assert SourceConnectionType("nzsql") == SourceConnectionType.NETEZZA_SQL

    def test_nzbulk_source_exists(self):
        """Test that nzbulk source type exists."""
        assert SourceConnectionType("nzbulk") == SourceConnectionType.NETEZZA_BULK

    def test_oracle_removed(self):
        """Test that 'oracle' source type no longer exists."""
        with pytest.raises(ValueError):
            SourceConnectionType("oracle")

    def test_nzcopy_removed(self):
        """Test that 'nzcopy' source type no longer exists."""
        with pytest.raises(ValueError):
            SourceConnectionType("nzcopy")

    def test_all_16_source_types(self):
        """Test that there are exactly 16 source types."""
        assert len(SourceConnectionType) == 16


class TestTargetConnectionType:
    """Tests for TargetConnectionType enum."""

    def test_pgsql_target_exists(self):
        """Test that pgsql target type exists."""
        assert TargetConnectionType("pgsql") == TargetConnectionType.POSTGRES

    def test_all_11_target_types(self):
        """Test that there are exactly 11 target types."""
        assert len(TargetConnectionType) == 11


class TestLogLevel:
    """Tests for LogLevel enum."""

    def test_all_log_levels(self):
        """Test all log level values exist."""
        assert LogLevel("error") == LogLevel.ERROR
        assert LogLevel("warning") == LogLevel.WARNING
        assert LogLevel("information") == LogLevel.INFORMATION
        assert LogLevel("debug") == LogLevel.DEBUG
        assert LogLevel("fatal") == LogLevel.FATAL


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
            database="testdb",
            connect_string="DSN=mydsn;UID=user;PWD=pass",
        )
        assert config.connect_string is not None

    def test_valid_connection_with_dsn(self):
        """Test valid connection with DSN."""
        config = ConnectionConfig(
            type="odbc",
            database="testdb",
            dsn="mydsn",
        )
        assert config.dsn == "mydsn"

    def test_invalid_connection_no_auth(self):
        """Test that connection without authentication fails."""
        with pytest.raises(ValidationError) as exc_info:
            ConnectionConfig(type="pgsql", server="localhost:5432", database="testdb")
        errors = exc_info.value.errors()
        assert any(
            "user" in str(e) or "authentication" in str(e).lower() for e in errors
        )

    def test_server_is_optional(self):
        """Test that server is not required when using connect_string."""
        config = ConnectionConfig(
            type="odbc",
            database="testdb",
            connect_string="Server=myhost;Database=testdb;UID=user;PWD=pass",
        )
        assert config.server is None

    def test_connect_string_excludes_server(self):
        """Test that connect_string cannot be used with server."""
        with pytest.raises(ValidationError) as exc_info:
            ConnectionConfig(
                type="odbc",
                server="localhost",
                database="testdb",
                connect_string="DSN=mydsn;UID=user;PWD=pass",
            )
        errors = exc_info.value.errors()
        assert any("connect_string" in str(e) for e in errors)

    def test_connect_string_excludes_user(self):
        """Test that connect_string cannot be used with user."""
        with pytest.raises(ValidationError) as exc_info:
            ConnectionConfig(
                type="odbc",
                database="testdb",
                connect_string="DSN=mydsn;UID=user;PWD=pass",
                user="extra_user",
            )
        errors = exc_info.value.errors()
        assert any("connect_string" in str(e) for e in errors)

    def test_dsn_excludes_provider(self):
        """Test that dsn cannot be used with provider."""
        with pytest.raises(ValidationError) as exc_info:
            ConnectionConfig(
                type="oledb",
                database="testdb",
                dsn="mydsn",
                provider="SQLOLEDB",
            )
        errors = exc_info.value.errors()
        assert any("dsn" in str(e) for e in errors)

    def test_dsn_excludes_server(self):
        """Test that dsn cannot be used with server."""
        with pytest.raises(ValidationError) as exc_info:
            ConnectionConfig(
                type="odbc",
                server="localhost",
                database="testdb",
                dsn="mydsn",
            )
        errors = exc_info.value.errors()
        assert any("dsn" in str(e) for e in errors)

    def test_trusted_auth_excludes_user(self):
        """Test that trusted_auth cannot be used with user."""
        with pytest.raises(ValidationError) as exc_info:
            ConnectionConfig(
                type="mssql",
                server="localhost",
                database="testdb",
                trusted_auth=True,
                user="someuser",
            )
        errors = exc_info.value.errors()
        assert any("trusted_auth" in str(e) for e in errors)

    def test_trusted_auth_excludes_password(self):
        """Test that trusted_auth cannot be used with password."""
        with pytest.raises(ValidationError) as exc_info:
            ConnectionConfig(
                type="mssql",
                server="localhost",
                database="testdb",
                trusted_auth=True,
                password="somepass",
            )
        errors = exc_info.value.errors()
        assert any("trusted_auth" in str(e) for e in errors)

    def test_file_input_excludes_query(self):
        """Test that file_input cannot be used with query."""
        with pytest.raises(ValidationError) as exc_info:
            ConnectionConfig(
                type="duckdbstream",
                database="testdb",
                file_input="/data/file.csv",
                query="SELECT * FROM t",
                user="user",
                password="pass",
            )
        errors = exc_info.value.errors()
        assert any("file_input" in str(e) for e in errors)

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

    def test_data_driven_query_only_with_datadriven(self):
        """Test data_driven_query requires DataDriven method."""
        with pytest.raises(ValidationError) as exc_info:
            TransferOptions(
                method=ParallelismMethod.NONE,
                data_driven_query="SELECT DISTINCT region FROM t",
            )
        errors = exc_info.value.errors()
        assert any("data_driven_query" in str(e) for e in errors)

    def test_data_driven_query_valid_with_datadriven(self):
        """Test data_driven_query accepted with DataDriven method."""
        options = TransferOptions(
            method=ParallelismMethod.DATA_DRIVEN,
            distribute_key_column="region",
            data_driven_query="SELECT DISTINCT region FROM t",
        )
        assert options.data_driven_query == "SELECT DISTINCT region FROM t"

    def test_use_work_tables_accepted(self):
        """Test use_work_tables field is accepted."""
        options = TransferOptions(use_work_tables=True)
        assert options.use_work_tables is True

    def test_settings_file_accepted(self):
        """Test settings_file field is accepted."""
        options = TransferOptions(settings_file="/path/to/settings.json")
        assert options.settings_file == "/path/to/settings.json"

    def test_no_banner_default_false(self):
        """Test no_banner defaults to False."""
        options = TransferOptions()
        assert options.no_banner is False

    def test_no_banner_set_true(self):
        """Test no_banner can be set to True."""
        options = TransferOptions(no_banner=True)
        assert options.no_banner is True

    def test_license_path_accepted(self):
        """Test license_path field is accepted."""
        options = TransferOptions(license_path="/path/to/license.lic")
        assert options.license_path == "/path/to/license.lic"

    def test_log_level_accepted(self):
        """Test log_level field is accepted."""
        options = TransferOptions(log_level=LogLevel.DEBUG)
        assert options.log_level == LogLevel.DEBUG


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

    def test_source_requires_table_or_query_or_file_input(self):
        """Test that source must have either table, query, or file_input."""
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
        assert any(
            "table" in str(e) or "query" in str(e) or "file_input" in str(e)
            for e in errors
        )

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
        assert any("only one" in str(e).lower() for e in errors)

    def test_file_input_as_valid_source(self):
        """Test file_input as a valid data source."""
        request = TransferRequest(
            source={
                "type": "duckdbstream",
                "database": "memdb",
                "file_input": "/data/export.csv",
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
        assert request.source.file_input == "/data/export.csv"

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

    def test_rowid_only_with_oraodp(self):
        """Test that Rowid method only works with oraodp source."""
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

    def test_rowid_valid_with_oraodp(self):
        """Test that Rowid works with oraodp source."""
        request = TransferRequest(
            source={
                "type": "oraodp",
                "server": "localhost:1521",
                "database": "ORCL",
                "table": "users",
                "user": "user",
                "password": "pass",
            },
            target={
                "type": "orabulk",
                "server": "localhost:1521",
                "database": "ORCL2",
                "table": "users",
                "user": "user",
                "password": "pass",
            },
            options={"method": "Rowid"},
        )
        assert request.options.method == ParallelismMethod.ROWID

    def test_nzdataslice_with_netezza_types(self):
        """Test NZDataSlice works with all Netezza source types."""
        for nz_type in ["nzoledb", "nzsql", "nzbulk"]:
            request = TransferRequest(
                source={
                    "type": nz_type,
                    "server": "localhost",
                    "database": "nzdb",
                    "table": "data",
                    "user": "user",
                    "password": "pass",
                },
                target={
                    "type": "nzbulk",
                    "server": "localhost",
                    "database": "nzdb2",
                    "table": "data",
                    "user": "user",
                    "password": "pass",
                },
                options={"method": "NZDataSlice"},
            )
            assert request.options.method == ParallelismMethod.NZ_DATA_SLICE

    def test_physloc_only_with_sqlserver_types(self):
        """Test Physloc only works with SQL Server source types."""
        # Should fail with non-SQL-Server type
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
                options={"method": "Physloc"},
            )
        errors = exc_info.value.errors()
        assert any("Physloc" in str(e) for e in errors)

    def test_physloc_valid_with_mssql(self):
        """Test Physloc works with mssql source."""
        request = TransferRequest(
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
            options={"method": "Physloc"},
        )
        assert request.options.method == ParallelismMethod.PHYSLOC

    def test_physloc_valid_with_msoledbsql(self):
        """Test Physloc works with msoledbsql source."""
        request = TransferRequest(
            source={
                "type": "msoledbsql",
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
            options={"method": "Physloc"},
        )
        assert request.options.method == ParallelismMethod.PHYSLOC


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
