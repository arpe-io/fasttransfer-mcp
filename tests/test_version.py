"""Tests for version detection and capabilities registry."""

import subprocess
from unittest.mock import patch, Mock

import pytest

from src.version import (
    FastTransferVersion,
    VersionDetector,
    VERSION_REGISTRY,
)


class TestFastTransferVersion:
    """Tests for FastTransferVersion dataclass."""

    def test_parse_full_version_string(self):
        """Test parsing a full 'FastTransfer Version X.Y.Z.W' string."""
        v = FastTransferVersion.parse("FastTransfer Version 0.16.0.0")
        assert v.major == 0
        assert v.minor == 16
        assert v.patch == 0
        assert v.build == 0

    def test_parse_numeric_only(self):
        """Test parsing a bare version number."""
        v = FastTransferVersion.parse("0.16.0.0")
        assert v == FastTransferVersion(0, 16, 0, 0)

    def test_parse_with_whitespace(self):
        """Test parsing a version string with leading/trailing whitespace."""
        v = FastTransferVersion.parse("  FastTransfer Version 1.2.3.4  ")
        assert v == FastTransferVersion(1, 2, 3, 4)

    def test_parse_invalid_string(self):
        """Test that an unparseable string raises ValueError."""
        with pytest.raises(ValueError, match="Cannot parse version"):
            FastTransferVersion.parse("no version here")

    def test_parse_incomplete_version(self):
        """Test that an incomplete version string raises ValueError."""
        with pytest.raises(ValueError, match="Cannot parse version"):
            FastTransferVersion.parse("0.16.0")

    def test_str_representation(self):
        """Test string representation."""
        v = FastTransferVersion(0, 16, 0, 0)
        assert str(v) == "0.16.0.0"

    def test_equality(self):
        """Test equality comparison."""
        a = FastTransferVersion(0, 16, 0, 0)
        b = FastTransferVersion(0, 16, 0, 0)
        assert a == b

    def test_inequality(self):
        """Test inequality comparison."""
        a = FastTransferVersion(0, 16, 0, 0)
        b = FastTransferVersion(0, 17, 0, 0)
        assert a != b

    def test_less_than(self):
        """Test less-than comparison."""
        a = FastTransferVersion(0, 15, 0, 0)
        b = FastTransferVersion(0, 16, 0, 0)
        assert a < b

    def test_greater_than(self):
        """Test greater-than comparison (via total_ordering)."""
        a = FastTransferVersion(0, 16, 0, 0)
        b = FastTransferVersion(0, 15, 9, 9)
        assert a > b

    def test_comparison_across_fields(self):
        """Test comparison across major/minor/patch/build."""
        versions = [
            FastTransferVersion(0, 15, 0, 0),
            FastTransferVersion(0, 16, 0, 0),
            FastTransferVersion(0, 16, 0, 1),
            FastTransferVersion(0, 16, 1, 0),
            FastTransferVersion(1, 0, 0, 0),
        ]
        for i in range(len(versions) - 1):
            assert versions[i] < versions[i + 1]


class TestVersionDetector:
    """Tests for VersionDetector class."""

    @patch("src.version.subprocess.run")
    def test_detect_success(self, mock_run):
        """Test successful version detection."""
        mock_result = Mock()
        mock_result.stdout = "FastTransfer Version 0.16.0.0\n"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        detector = VersionDetector("/fake/binary")
        version = detector.detect()

        assert version == FastTransferVersion(0, 16, 0, 0)
        mock_run.assert_called_once_with(
            ["/fake/binary", "--version", "--nobanner"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )

    @patch("src.version.subprocess.run")
    def test_detect_failure_no_match(self, mock_run):
        """Test detection when output doesn't match version pattern."""
        mock_result = Mock()
        mock_result.stdout = "Unknown output"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        detector = VersionDetector("/fake/binary")
        version = detector.detect()

        assert version is None

    @patch("src.version.subprocess.run")
    def test_detect_timeout(self, mock_run):
        """Test detection handles timeout gracefully."""
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="test", timeout=10)

        detector = VersionDetector("/fake/binary")
        version = detector.detect()

        assert version is None

    @patch("src.version.subprocess.run")
    def test_detect_binary_not_found(self, mock_run):
        """Test detection handles missing binary gracefully."""
        mock_run.side_effect = FileNotFoundError("No such file")

        detector = VersionDetector("/fake/binary")
        version = detector.detect()

        assert version is None

    @patch("src.version.subprocess.run")
    def test_detect_caching(self, mock_run):
        """Test that second call returns cached result without re-running subprocess."""
        mock_result = Mock()
        mock_result.stdout = "FastTransfer Version 0.16.0.0\n"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        detector = VersionDetector("/fake/binary")
        v1 = detector.detect()
        v2 = detector.detect()

        assert v1 == v2
        assert mock_run.call_count == 1

    @patch("src.version.subprocess.run")
    def test_capabilities_known_version(self, mock_run):
        """Test capabilities resolution for a known version."""
        mock_result = Mock()
        mock_result.stdout = "FastTransfer Version 0.16.0.0\n"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        detector = VersionDetector("/fake/binary")
        detector.detect()
        caps = detector.capabilities

        assert "oraodp" in caps.source_types
        assert "pgsql" in caps.target_types
        assert caps.supports_nobanner is True
        assert caps.supports_version_flag is True

    @patch("src.version.subprocess.run")
    def test_capabilities_newer_unknown_version(self, mock_run):
        """Test capabilities falls back to latest known for newer unknown version."""
        mock_result = Mock()
        mock_result.stdout = "FastTransfer Version 1.0.0.0\n"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        detector = VersionDetector("/fake/binary")
        detector.detect()
        caps = detector.capabilities

        # Should get the latest known capabilities (0.16.0.0)
        assert caps == VERSION_REGISTRY["0.16.0.0"]

    @patch("src.version.subprocess.run")
    def test_capabilities_undetected_version(self, mock_run):
        """Test capabilities falls back to latest known when detection fails."""
        mock_run.side_effect = FileNotFoundError("No such file")

        detector = VersionDetector("/fake/binary")
        detector.detect()
        caps = detector.capabilities

        # Should fall back to latest known
        assert caps == VERSION_REGISTRY["0.16.0.0"]

    def test_registry_016_source_completeness(self):
        """Test that 0.16.0.0 registry has all expected source types."""
        caps = VERSION_REGISTRY["0.16.0.0"]
        expected = {
            "clickhouse",
            "duckdb",
            "duckdbstream",
            "hana",
            "mssql",
            "msoledbsql",
            "mysql",
            "nzoledb",
            "nzsql",
            "nzbulk",
            "odbc",
            "oledb",
            "oraodp",
            "pgcopy",
            "pgsql",
            "teradata",
        }
        assert caps.source_types == expected

    def test_registry_016_target_completeness(self):
        """Test that 0.16.0.0 registry has all expected target types."""
        caps = VERSION_REGISTRY["0.16.0.0"]
        expected = {
            "clickhousebulk",
            "duckdb",
            "hanabulk",
            "msbulk",
            "mysqlbulk",
            "nzbulk",
            "orabulk",
            "oradirect",
            "pgcopy",
            "pgsql",
            "teradata",
        }
        assert caps.target_types == expected

    def test_registry_016_method_completeness(self):
        """Test that 0.16.0.0 registry has all expected parallelism methods."""
        caps = VERSION_REGISTRY["0.16.0.0"]
        expected = {
            "Ctid",
            "DataDriven",
            "Ntile",
            "NZDataSlice",
            "None",
            "Physloc",
            "Random",
            "RangeId",
            "Rowid",
        }
        assert caps.parallelism_methods == expected
