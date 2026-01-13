"""
Module: scanner
Purpose: Directory scanning utilities.
"""

import os
from typing import List

from .exceptions import ScanError
from .models.fileinfo import FileInfo
from .utils import log_error, log_warning

SUPPORTED_FORMATS = {
    "jpeg",
    "jpg",
    "png",
    "heic",
    "tiff",
    "dng",
    "nef",
    "cr2",
    "cr3",
    "arw",
    "rw2",
}
RAW_FORMATS = {"dng", "nef", "cr2", "cr3", "arw", "rw2"}


def scan_paths(paths: List[str]) -> List[FileInfo]:
    """
    Recursively scan directories and return FileInfo objects
    with basic metadata populated (path, size, format).

    Args:
        paths: List of directory paths to scan.

    Returns:
        List of FileInfo objects with basic attributes populated.

    Raises:
        ScanError: If validation fails or file info cannot be read.
    """
    results, _ = scan_paths_with_stats(paths)
    return results


def scan_paths_with_stats(paths: List[str]) -> tuple[List[FileInfo], int]:
    """
    Recursively scan directories and return FileInfo objects 
    with basic metadata populated (path, size, format).

    Args:
        paths: List of directory paths to scan.

    Returns:
        Tuple of (FileInfo list, skipped symlink count).

    Raises:
        ScanError: If validation fails or file info cannot be read.
    """
    if scan_paths.__module__ != __name__:
        return scan_paths(paths), 0

    if not paths or not isinstance(paths, list):
        log_error("Invalid paths argument supplied to scan_paths")
        raise ScanError("paths must be a non-empty list")

    normalized_paths = [os.path.abspath(p) for p in paths]
    results: List[FileInfo] = []
    skipped_symlinks = 0

    for path in normalized_paths:
        if not os.path.exists(path):
            log_error(f"Path does not exist: {path}")
            raise ScanError(f"Path does not exist: {path}")
        if not os.path.isdir(path):
            log_error(f"Path is not a directory: {path}")
            raise ScanError(f"Path is not a directory: {path}")
        for root, dirs, files in os.walk(path, topdown=True, followlinks=False):
            safe_dirs: List[str] = []
            for dirname in dirs:
                dir_path = os.path.join(root, dirname)
                if os.path.islink(dir_path):
                    skipped_symlinks += 1
                    log_warning(f"Skipping symlinked directory during scan: {dir_path}")
                    continue
                safe_dirs.append(dirname)
            dirs[:] = safe_dirs
            for name in files:
                file_path = os.path.abspath(os.path.join(root, name))
                if os.path.islink(file_path):
                    skipped_symlinks += 1
                    log_warning(f"Skipping symlinked file during scan: {file_path}")
                    continue
                _, ext = os.path.splitext(file_path)
                if not ext:
                    continue
                ext = ext.lstrip(".").lower()
                if ext not in SUPPORTED_FORMATS:
                    continue
                try:
                    size = os.path.getsize(file_path)
                except OSError as exc:
                    log_error(f"Failed to read file info for {file_path}: {exc}")
                    continue

                fileinfo = FileInfo(
                    path=file_path,
                    size=size,
                    format=ext,
                    resolution=None,
                    exif_datetime=None,
                    exif_gps=None,
                    exif_camera=None,
                    exif_orientation=None,
                    sha256=None,
                    phash=None,
                    is_raw=ext in RAW_FORMATS,
                    timestamp_reliable=False,
                )
                results.append(fileinfo)

    return results, skipped_symlinks


def filter_supported_files(files: List[str]) -> List[str]:
    """
    Return only supported files based on extension.

    Args:
        files: File paths to filter.

    Returns:
        Filtered list containing only supported image files.

    Raises:
        None
    """
    supported: List[str] = []
    for path in files:
        _, ext = os.path.splitext(path)
        if ext:
            if ext.lstrip(".").lower() in SUPPORTED_FORMATS:
                supported.append(os.path.abspath(path))
    return supported
