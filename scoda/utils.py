"""
Utility functions.

Copyright (C) 2025 Nicholas M. Synovic.

"""

from pathlib import Path


def resolve_path(filepath: str) -> Path:
    """
    Resolve the absolute path of the given file path.

    This function converts the provided file path string into a Path object
    and resolves it to its absolute path.

    Args:
        filepath (str): The file path to be resolved.

    Returns:
        Path: The resolved absolute path as a Path object.

    """
    return Path(filepath).resolve()
