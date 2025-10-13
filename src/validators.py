"""
Input validation for FastTransfer MCP Server.

This module provides Pydantic models and enums for validating
all FastTransfer parameters and ensuring parameter compatibility.
"""

from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field, field_validator, model_validator


class SourceConnectionType(str, Enum):
    """Source database connection types supported by FastTransfer."""

    CLICKHOUSE = "clickhouse"
    DUCKDB = "duckdb"
    DUCKDB_STREAM = "duckdbstream"
    HANA = "hana"
    MSSQL = "mssql"
    MYSQL = "mysql"
    NETEZZA_COPY = "nzcopy"
    ODBC = "odbc"
    OLEDB = "oledb"
    ORACLE = "oracle"
    POSTGRES_COPY = "pgcopy"
    POSTGRES = "pgsql"
    TERADATA = "teradata"


class TargetConnectionType(str, Enum):
    """Target database connection types supported by FastTransfer."""

    CLICKHOUSE_BULK = "clickhousebulk"
    DUCKDB = "duckdb"
    HANA_BULK = "hanabulk"
    MSSQL_BULK = "msbulk"
    MYSQL_BULK = "mysqlbulk"
    NETEZZA_BULK = "nzbulk"
    ORACLE_BULK = "orabulk"
    ORACLE_DIRECT = "oradirect"
    POSTGRES_COPY = "pgcopy"
    TERADATA = "teradata"


class ParallelismMethod(str, Enum):
    """Parallelism methods for data distribution."""

    CTID = "Ctid"  # PostgreSQL-specific
    DATA_DRIVEN = "DataDriven"  # Distribute by distinct key values
    NTILE = "Ntile"  # Even distribution
    NZ_DATA_SLICE = "NZDataSlice"  # Netezza-specific
    NONE = "None"  # No parallelism
    PHYSLOC = "Physloc"  # Physical location
    RANDOM = "Random"  # Random distribution using modulo
    RANGE_ID = "RangeId"  # Numeric range distribution
    ROWID = "Rowid"  # Oracle-specific


class LoadMode(str, Enum):
    """Load mode for target table."""

    APPEND = "Append"  # Add to existing data
    TRUNCATE = "Truncate"  # Clear before loading


class MapMethod(str, Enum):
    """Column mapping method."""

    POSITION = "Position"  # Map by position
    NAME = "Name"  # Map by name (case-insensitive)


class ConnectionConfig(BaseModel):
    """Database connection configuration."""

    type: str = Field(..., description="Connection type (source or target)")
    server: str = Field(..., description="Server address (host:port or host\\instance)")
    database: str = Field(..., description="Database name")
    schema: Optional[str] = Field(None, description="Schema name")
    table: Optional[str] = Field(
        None, description="Table name (optional if query provided)"
    )
    query: Optional[str] = Field(None, description="SQL query (alternative to table)")
    user: Optional[str] = Field(None, description="Username for authentication")
    password: Optional[str] = Field(None, description="Password for authentication")
    trusted_auth: bool = Field(
        False, description="Use trusted authentication (Windows)"
    )
    connect_string: Optional[str] = Field(
        None, description="Full connection string (alternative)"
    )
    dsn: Optional[str] = Field(None, description="ODBC DSN name")
    provider: Optional[str] = Field(None, description="OleDB provider name")

    @model_validator(mode="after")
    def validate_authentication(self):
        """Ensure either credentials or trusted auth is provided."""
        if not self.trusted_auth and not self.connect_string and not self.dsn:
            if not self.user:
                raise ValueError(
                    "Either user/password, trusted_auth, connect_string, or dsn must be provided"
                )
        return self

    @model_validator(mode="after")
    def validate_table_or_query(self):
        """Ensure either table or query is provided for source connections."""
        # This will be checked in the main request validator
        return self


class TransferOptions(BaseModel):
    """Options for data transfer execution."""

    method: ParallelismMethod = Field(
        ParallelismMethod.NONE, description="Parallelism method"
    )
    distribute_key_column: Optional[str] = Field(
        None, description="Column for data distribution (required for some methods)"
    )
    degree: int = Field(
        -2,
        description="Parallelism degree: 0=auto, >0=fixed, <0=CPU adaptive (e.g., -2=half CPUs)",
    )
    load_mode: LoadMode = Field(
        LoadMode.APPEND, description="Load mode: Append or Truncate"
    )
    batch_size: Optional[int] = Field(
        None, ge=1, description="Batch size for bulk copy operations"
    )
    map_method: MapMethod = Field(
        MapMethod.POSITION, description="Column mapping method: Position or Name"
    )
    run_id: Optional[str] = Field(
        None, description="Run ID for logging and tracking purposes"
    )

    @field_validator("degree")
    @classmethod
    def validate_degree(cls, v):
        """Validate parallelism degree."""
        if v == 0 or (v > 0 and v < 1024) or v < 0:
            return v
        raise ValueError(
            "Degree must be 0 (auto), 0 < n < 1024 (fixed), or < 0 (CPU adaptive)"
        )

    @model_validator(mode="after")
    def validate_distribute_key_requirements(self):
        """Validate distribute key column requirements."""
        methods_requiring_key = {
            ParallelismMethod.DATA_DRIVEN,
            ParallelismMethod.RANDOM,
            ParallelismMethod.RANGE_ID,
            ParallelismMethod.NTILE,
        }

        if self.method in methods_requiring_key and not self.distribute_key_column:
            raise ValueError(
                f"Method '{self.method.value}' requires distribute_key_column"
            )

        return self


class TransferRequest(BaseModel):
    """Complete transfer request with source, target, and options."""

    source: ConnectionConfig = Field(..., description="Source database configuration")
    target: ConnectionConfig = Field(..., description="Target database configuration")
    options: TransferOptions = Field(
        default_factory=TransferOptions, description="Transfer execution options"
    )

    @model_validator(mode="after")
    def validate_source_table_or_query(self):
        """Ensure source has either table or query."""
        if not self.source.table and not self.source.query:
            raise ValueError("Source must specify either 'table' or 'query'")
        if self.source.table and self.source.query:
            raise ValueError("Source cannot specify both 'table' and 'query'")
        return self

    @model_validator(mode="after")
    def validate_target_requires_table(self):
        """Ensure target has table specified."""
        if not self.target.table:
            raise ValueError("Target must specify 'table'")
        return self

    @model_validator(mode="after")
    def validate_method_compatibility(self):
        """Validate parallelism method compatibility with source database."""
        method = self.options.method
        source_type = self.source.type.lower()

        # Ctid is PostgreSQL-specific
        if method == ParallelismMethod.CTID and source_type not in [
            "pgsql",
            "pgcopy",
            "postgres",
        ]:
            raise ValueError(
                f"Method 'Ctid' only works with PostgreSQL sources, not '{source_type}'"
            )

        # Rowid is Oracle-specific
        if method == ParallelismMethod.ROWID and source_type not in [
            "oracle",
            "oraodp",
        ]:
            raise ValueError(
                f"Method 'Rowid' only works with Oracle sources, not '{source_type}'"
            )

        # NZDataSlice is Netezza-specific
        if method == ParallelismMethod.NZ_DATA_SLICE and source_type not in [
            "nzoledb",
            "nzcopy",
            "nzsql",
            "nzbulk",
        ]:
            raise ValueError(
                f"Method 'NZDataSlice' only works with Netezza sources, not '{source_type}'"
            )

        return self

    @model_validator(mode="after")
    def validate_random_requires_numeric_key(self):
        """Random method requires numeric distribute key column."""
        if (
            self.options.method == ParallelismMethod.RANDOM
            and self.options.distribute_key_column
        ):
            # Note: We can't validate if column is numeric without database access
            # This is a reminder in the model
            pass
        return self


class ConnectionValidationRequest(BaseModel):
    """Request to validate a database connection."""

    connection: ConnectionConfig = Field(..., description="Connection to validate")
    side: str = Field(..., description="Connection side: 'source' or 'target'")

    @field_validator("side")
    @classmethod
    def validate_side(cls, v):
        """Ensure side is either source or target."""
        if v not in ["source", "target"]:
            raise ValueError("Side must be 'source' or 'target'")
        return v


class ParallelismSuggestionRequest(BaseModel):
    """Request for parallelism method suggestion."""

    source_type: str = Field(..., description="Source database type")
    has_numeric_key: bool = Field(
        ..., description="Whether table has a numeric key column"
    )
    has_identity_column: bool = Field(
        False, description="Whether table has an identity/auto-increment column"
    )
    table_size_estimate: str = Field(
        ..., description="Table size: 'small', 'medium', or 'large'"
    )

    @field_validator("table_size_estimate")
    @classmethod
    def validate_table_size(cls, v):
        """Validate table size estimate."""
        if v not in ["small", "medium", "large"]:
            raise ValueError(
                "Table size estimate must be 'small', 'medium', or 'large'"
            )
        return v
