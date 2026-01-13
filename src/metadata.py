"""
Module: metadata
Purpose: EXIF and resolution extraction utilities.
"""

import os
from dataclasses import replace
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import datetime
from typing import Dict, List

from PIL import ExifTags, Image

from .exceptions import MetadataError, OversizedImageError
from .models.fileinfo import FileInfo
from .utils import ensure_heif_registered, enforce_pixel_limit, executor_mode, log_error, log_warning


def _enrich_single_file(fileinfo: FileInfo) -> FileInfo | None:
    """
    Helper function to enrich a single FileInfo object with metadata.
    Designed for use with a process pool.
    """
    try:
        ensure_heif_registered()
        resolution = fileinfo.resolution
        exif_data: Dict = {}
        try:
            resolution = extract_resolution(fileinfo.path)
        except OversizedImageError:
            return None
        except MetadataError:
            pass  # Logged in extract_resolution
        try:
            exif_data = extract_exif(fileinfo.path)
        except OversizedImageError:
            return None
        except MetadataError:
            pass  # Logged in extract_exif

        timestamp_reliable = fileinfo.timestamp_reliable
        exif_datetime = exif_data.get("datetime") or fileinfo.exif_datetime
        if exif_data.get("datetime"):
            timestamp_reliable = True
        elif exif_datetime is None:
            try:
                exif_datetime = safe_modified_timestamp(fileinfo.path)
                timestamp_reliable = False
            except MetadataError:
                exif_datetime = fileinfo.exif_datetime

        return replace(
            fileinfo,
            path=os.path.abspath(fileinfo.path),
            resolution=resolution,
            exif_datetime=exif_datetime,
            exif_gps=exif_data.get("gps", fileinfo.exif_gps),
            exif_camera=exif_data.get("camera", fileinfo.exif_camera),
            exif_orientation=exif_data.get("orientation", fileinfo.exif_orientation),
            timestamp_reliable=timestamp_reliable,
        )
    except Exception as exc:
        log_error(f"Failed to enrich metadata for {fileinfo.path}: {exc}")
        return fileinfo


def enrich_metadata(fileinfo_list: List[FileInfo]) -> List[FileInfo]:
    """
    Populate EXIF and resolution fields for FileInfo objects in parallel.

    Args:
        fileinfo_list: Collection of FileInfo objects to enrich.

    Returns:
        Updated FileInfo list with metadata populated.
    """
    if not fileinfo_list:
        return []
    if len(fileinfo_list) == 1:
        result = _enrich_single_file(fileinfo_list[0])
        return [result] if result is not None else []
    results: List[FileInfo | None]
    mode = executor_mode()
    if mode == "process":
        try:
            with ProcessPoolExecutor() as executor:
                results = list(executor.map(_enrich_single_file, fileinfo_list))
        except (NotImplementedError, PermissionError, OSError, RuntimeError) as exc:
            log_warning(f"ProcessPool unavailable, falling back to ThreadPool for metadata: {exc}")
            with ThreadPoolExecutor() as executor:
                results = list(executor.map(_enrich_single_file, fileinfo_list))
    else:
        with ThreadPoolExecutor() as executor:
            results = list(executor.map(_enrich_single_file, fileinfo_list))

    return [result for result in results if result is not None]



def extract_exif(path: str) -> Dict:
    """
    Extract EXIF metadata from file.

    Args:
        path: File path to extract EXIF from.

    Returns:
        Dictionary with normalized EXIF fields.

    Raises:
        MetadataError: Reserved for critical failures; typical errors are logged and return {}.
    """
    try:
        ensure_heif_registered()
        enforce_pixel_limit()
        normalized = os.path.abspath(path)
        with Image.open(normalized) as image:
            raw_exif = image._getexif() or {}
        exif = {}

        tag_map = {ExifTags.TAGS.get(k, k): v for k, v in raw_exif.items()}

        datetime_value = tag_map.get("DateTimeOriginal") or tag_map.get("DateTime")
        if datetime_value:
            try:
                exif_datetime = datetime.strptime(datetime_value, "%Y:%m:%d %H:%M:%S")
                exif["datetime"] = exif_datetime
            except (ValueError, TypeError):
                exif["datetime"] = None
        gps_info = tag_map.get("GPSInfo")
        if gps_info:
            # GPSInfo may use numeric keys
            gps_tags = {}
            for key, value in gps_info.items():
                decoded = ExifTags.GPSTAGS.get(key, key)
                gps_tags[decoded] = value
            lat = gps_tags.get("GPSLatitude")
            lat_ref = gps_tags.get("GPSLatitudeRef")
            lon = gps_tags.get("GPSLongitude")
            lon_ref = gps_tags.get("GPSLongitudeRef")
            if lat and lon and lat_ref and lon_ref:
                exif["gps"] = (_convert_gps(lat, lat_ref), _convert_gps(lon, lon_ref))

        make = tag_map.get("Make")
        model = tag_map.get("Model")
        if make or model:
            camera_parts = [part for part in (make, model) if part]
            exif["camera"] = " ".join(camera_parts)
        orientation = tag_map.get("Orientation")
        if orientation:
            exif["orientation"] = str(orientation)

        return exif
    except Image.DecompressionBombError as exc:
        log_warning(f"Skipped EXIF extraction for '{path}' (decompression bomb detected: {exc}).")
        raise OversizedImageError(f"Decompression bomb attack detected for {path}") from exc
    except Exception as exc:
        log_error(f"Corrupted or unreadable EXIF for {path}: {exc}")
        return {}


def extract_resolution(path: str) -> tuple[int, int] | None:
    """
    Extract image resolution.

    Args:
        path: File path to inspect.

    Returns:
        Tuple of width and height, or None if unavailable.

    Raises:
        MetadataError: When image cannot be opened.
    """
    try:
        ensure_heif_registered()
        enforce_pixel_limit()
        normalized = os.path.abspath(path)
        with Image.open(normalized) as img:
            return img.size  # type: ignore[return-value]
    except Image.DecompressionBombError as exc:
        log_warning(f"Skipped resolution extraction for '{path}' (decompression bomb detected: {exc}).")
        raise OversizedImageError(f"Decompression bomb attack detected for {path}") from exc
    except Exception as exc:
        log_error(f"Failed to extract resolution for {path}: {exc}")
        raise MetadataError(f"Failed to extract resolution for {path}") from exc


def safe_modified_timestamp(path: str) -> datetime:
    """
    Fallback timestamp.

    Args:
        path: File path to inspect.

    Returns:
        Datetime derived from filesystem metadata.

    Raises:
        MetadataError: If timestamp cannot be read.
    """
    try:
        normalized = os.path.abspath(path)
        modified = os.path.getmtime(normalized)
        return datetime.fromtimestamp(modified)
    except OSError as exc:
        log_error(f"Failed to read modified timestamp for {path}: {exc}")
        raise MetadataError(f"Failed to read modified timestamp for {path}") from exc


def _convert_gps(value, ref):
    """

    Convert GPS coordinates to decimal degrees.
    """
    try:
        deg, minute, sec = value
        degrees = deg[0] / deg[1] if isinstance(deg, tuple) else float(deg)
        minutes = minute[0] / minute[1] if isinstance(minute, tuple) else float(minute)
        seconds = sec[0] / sec[1] if isinstance(sec, tuple) else float(sec)
        coord = degrees + (minutes / 60.0) + (seconds / 3600.0)
        if ref in ["S", "W"]:
            coord = -coord
        return coord
    except Exception:
        return None

