"""Hashing helpers for duplicate detection."""

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import hashlib
import os
from typing import List, Optional

from PIL import Image

from .exceptions import HashingError, OversizedImageError
from .models.fileinfo import FileInfo
from .utils import ensure_heif_registered, enforce_pixel_limit, executor_mode, log_error, log_warning


def compute_sha256(path: str) -> str:
    """
    Compute SHA256 for exact duplicate detection.

    Args:
        path: Path to the file.

    Returns:
        Hexadecimal SHA256 digest.

    Raises:
        HashingError: If hashing fails.
    """
    try:
        normalized = os.path.abspath(path)
        sha = hashlib.sha256()
        with open(normalized, "rb") as handle:
            for chunk in iter(lambda: handle.read(65536), b""):
                sha.update(chunk)
        return sha.hexdigest()
    except Exception as exc:
        log_error(f"Failed to compute SHA256 for {path}: {exc}")
        raise HashingError(f"Failed to compute SHA256 for {path}") from exc


def compute_phash(path: str) -> str:
    """
    Compute perceptual hash for near-duplicate detection.

    Args:
        path: Path to the image file.

    Returns:
        String perceptual hash value.

    Raises:
        HashingError: If hashing fails.
    """
    try:
        normalized = os.path.abspath(path)
        ensure_heif_registered()
        enforce_pixel_limit()
        with Image.open(normalized) as img:
            # Simple average hash (aHash) implementation
            resized = img.convert("L").resize((8, 8), Image.Resampling.LANCZOS)
            pixels = list(resized.getdata())
            avg = sum(pixels) / len(pixels)
            bits = "".join("1" if px > avg else "0" for px in pixels)
            hash_int = int(bits, 2)
            return f"{hash_int:016x}"
    except Image.DecompressionBombError as exc:
        log_warning(
            f"Skipped perceptual hash for '{path}' due to decompression-bomb protection ({exc})."
        )
        raise OversizedImageError(f"Decompression bomb detected for {path}") from exc
    except Exception as exc:
        log_error(f"Failed to compute perceptual hash for {path}: {exc}")
        raise HashingError(f"Failed to compute perceptual hash for {path}") from exc


def phash_distance(a: str, b: str) -> int:
    """
    Compute the Hamming distance between two pHash strings.
    """
    try:
        int_a = int(a, 16)
        int_b = int(b, 16)
        return (int_a ^ int_b).bit_count()
    except ValueError:
        if len(a) != len(b):
            return max(len(a), len(b))
        return sum(ch1 != ch2 for ch1, ch2 in zip(a, b))


def _hash_file(fileinfo: FileInfo) -> Optional[FileInfo]:
    """
    Helper function to compute SHA256 and phash for a single file.
    Designed to be used with a process pool.
    """
    try:
        sha256 = compute_sha256(fileinfo.path)
        phash = None
        try:
            phash = compute_phash(fileinfo.path)
        except OversizedImageError as exc:
            log_warning(
                f"Skipped perceptual hash for '{fileinfo.path}' ({exc}). Keeping SHA256 only."
            )
        except HashingError as exc:
            log_error(f"Perceptual hash unavailable for {fileinfo.path}: {exc}")
        
        return FileInfo(
            path=fileinfo.path,
            size=fileinfo.size,
            format=fileinfo.format,
            resolution=fileinfo.resolution,
            exif_datetime=fileinfo.exif_datetime,
            exif_gps=fileinfo.exif_gps,
            exif_camera=fileinfo.exif_camera,
            exif_orientation=fileinfo.exif_orientation,
            sha256=sha256,
            phash=phash,
            is_raw=fileinfo.is_raw,
            timestamp_reliable=fileinfo.timestamp_reliable,
        )
    except HashingError as exc:
        log_error(f"Skipping file during hashing: {fileinfo.path} ({exc})")
        return None


def add_hashes(fileinfo_list: List[FileInfo]) -> List[FileInfo]:
    """
    Add sha256 and phash to FileInfo objects in parallel.

    Args:
        fileinfo_list: FileInfo instances to hash.

    Returns:
        New list with hash values populated.
    """
    hashed_list: List[FileInfo] = []
    results = []
    mode = executor_mode()
    if mode == "process":
        try:
            with ProcessPoolExecutor() as executor:
                results = list(executor.map(_hash_file, fileinfo_list))
        except (NotImplementedError, PermissionError, OSError, RuntimeError) as exc:
            log_warning(f"ProcessPool unavailable, falling back to ThreadPool for hashing: {exc}")
            with ThreadPoolExecutor() as executor:
                results = list(executor.map(_hash_file, fileinfo_list))
    else:
        with ThreadPoolExecutor() as executor:
            results = list(executor.map(_hash_file, fileinfo_list))

    hashed_list = [result for result in results if result is not None]

    return hashed_list
