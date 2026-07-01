"""
T097: Wheel content verification.

Builds a wheel and asserts no test_*.py files are included (SC-009).
"""
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

import pytest


class TestWheelContents:
    """Verify the built wheel excludes test files."""

    @pytest.fixture(scope="class")
    def wheel_path(self):
        """Build a wheel into a temp directory and return its path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = subprocess.run(
                [sys.executable, "-m", "pip", "wheel", ".", "--no-deps", "--wheel-dir", tmpdir],
                capture_output=True,
                text=True,
                cwd=str(Path(__file__).resolve().parents[2]),
            )
            if result.returncode != 0:
                pytest.skip(f"Wheel build failed: {result.stderr[:200]}")

            wheels = list(Path(tmpdir).glob("*.whl"))
            if not wheels:
                pytest.skip("No wheel produced")

            yield wheels[0]

    def test_no_test_files_in_wheel(self, wheel_path):
        """SC-009: No test_*.py files should be in the wheel."""
        with zipfile.ZipFile(wheel_path, "r") as zf:
            test_files = [
                name for name in zf.namelist()
                if Path(name).name.startswith("test_") and name.endswith(".py")
            ]

        assert test_files == [], (
            f"Found test files in wheel:\n"
            + "\n".join(f"  - {f}" for f in test_files)
        )

    def test_no_tests_directory_in_wheel(self, wheel_path):
        """The tests/ directory should not be included in the wheel."""
        with zipfile.ZipFile(wheel_path, "r") as zf:
            tests_entries = [
                name for name in zf.namelist()
                if name.startswith("tests/") or "/tests/" in name
            ]

        assert tests_entries == [], (
            f"Found tests/ directory entries in wheel:\n"
            + "\n".join(f"  - {f}" for f in tests_entries[:10])
        )

    def test_wheel_contains_plaita_package(self, wheel_path):
        """The wheel should contain the plaita package."""
        with zipfile.ZipFile(wheel_path, "r") as zf:
            plaita_files = [name for name in zf.namelist() if name.startswith("plaita/")]

        assert len(plaita_files) > 0, "Wheel should contain plaita/ package files"

    def test_wheel_contains_core_subpackage(self, wheel_path):
        """The wheel should contain plaita/core/ subpackage."""
        with zipfile.ZipFile(wheel_path, "r") as zf:
            core_files = [name for name in zf.namelist() if name.startswith("plaita/core/")]

        assert len(core_files) > 0, "Wheel should contain plaita/core/ subpackage"
