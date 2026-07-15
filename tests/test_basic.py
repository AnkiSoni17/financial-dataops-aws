from __future__ import annotations

from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    "directory",
    [
        ".github/workflows",
        "glue",
        "terraform",
        "tests",
    ],
)
def test_expected_directories_exist(directory: str) -> None:
    """Verify that the main project directories exist."""
    path = PROJECT_ROOT / directory

    assert path.exists(), f"Expected directory does not exist: {path}"
    assert path.is_dir(), f"Expected a directory but found something else: {path}"


@pytest.mark.parametrize(
    "file_path",
    [
        "README.md",
        "requirements.txt",
        ".gitignore",
        ".github/workflows/ci.yml",
        ".github/workflows/deploy.yml",
        "glue/raw_to_silver.py",
        "glue/fraud_json_to_silver.py",
        "terraform/main.tf",
        "terraform/provider.tf",
        "terraform/variables.tf",
        "terraform/versions.tf",
    ],
)
def test_expected_files_exist(file_path: str) -> None:
    """Verify that important project files exist."""
    path = PROJECT_ROOT / file_path

    assert path.exists(), f"Expected file does not exist: {path}"
    assert path.is_file(), f"Expected a file but found something else: {path}"


def test_readme_contains_project_documentation() -> None:
    """Verify that the README contains meaningful documentation."""
    path = PROJECT_ROOT / "README.md"
    content = path.read_text(encoding="utf-8").strip()

    assert len(content) >= 100, (
        "README.md should contain at least 100 characters of documentation"
    )