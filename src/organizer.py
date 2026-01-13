"""
Module: organizer
Purpose: Target path determination for organized library.
"""

import os
import re
from typing import Iterable, NamedTuple, Optional

from .exceptions import NolossiaError
from .models.fileinfo import FileInfo
from .review import (
    REVIEW_REASON_INVALID_CHRONOLOGY,
    REVIEW_REASON_MISSING_EXIF,
    REVIEW_REASON_UNRELIABLE_TIMESTAMP,
    describe_review_reason,
)
from .utils import ensure_directory, log_warning, path_violation_message


def determine_target_path(
    file: FileInfo,
    base_out: str,
    merge_mode: str = "off",
    source_root: str | None = None,
) -> str:
    """
    Compute target path based on organization mode.

    Args:
        file: FileInfo instance with datetime populated.
        base_out: Base output directory.
        merge_mode: "off" to preserve source hierarchy, "on" to use YEAR/YEAR-MONTH.
        source_root: Root path to preserve hierarchy when mode is off.

    Returns:
        Absolute target path for the file.

    Raises:
        None
    """
    effective_mode = merge_mode
    filename = os.path.basename(file.path)
    normalized_out = os.path.abspath(base_out)
    file.review_reason = None

    if effective_mode.lower() == "on":
        chronology = _existing_chronology(file.path)
        review_path = os.path.abspath(os.path.join(normalized_out, "REVIEW", filename))
        review_reason = _review_reason(file, chronology.invalid)
        if review_reason:
            file.review_reason = review_reason
            if review_reason != REVIEW_REASON_INVALID_CHRONOLOGY:
                log_warning(
                    f"{describe_review_reason(review_reason)} for '{file.path}'. Set aside for review."
                )
            return review_path
        if chronology.coordinates:
            year, month = chronology.coordinates
        else:
            dt = file.exif_datetime
            if dt is None:
                reason = REVIEW_REASON_MISSING_EXIF
                file.review_reason = reason
                log_warning(
                    f"{describe_review_reason(reason)} for '{file.path}'. Set aside for review."
                )
                return review_path
            year = dt.year
            month = dt.month
        folder = os.path.join(str(year), f"{year}-{month:02d}")
        return os.path.abspath(os.path.join(normalized_out, folder, filename))

    # mode off: preserve source hierarchy under target
    if source_root:
        try:
            rel_path = os.path.relpath(file.path, source_root)
        except ValueError:
            rel_path = filename
        if rel_path in (".", os.curdir, ""):
            rel_path = filename
    else:
        rel_path = os.path.relpath(file.path, os.path.dirname(file.path))
    return os.path.abspath(os.path.join(normalized_out, rel_path))


def _review_reason(file: FileInfo, chronology_invalid: bool) -> str | None:
    """
    Determine whether a file should be routed to REVIEW and why.
    """
    if chronology_invalid:
        return REVIEW_REASON_INVALID_CHRONOLOGY
    timestamp_reliable = getattr(file, "timestamp_reliable", True)
    if file.exif_datetime is None:
        return REVIEW_REASON_MISSING_EXIF
    if not timestamp_reliable:
        return REVIEW_REASON_UNRELIABLE_TIMESTAMP
    return None


def ensure_structure(
    base_out: str,
    merge_mode: str,
    *,
    folders: Iterable[str] | None = None,
) -> None:
    """
    Ensure that required directory structure exists based on merge mode.

    Args:
        base_out: Base output directory.
        merge_mode: "off" preserves hierarchy; "on" prepares chronological structure.

    Returns:
        None

    Raises:
        NolossiaError: If directory creation fails.
    """
    normalized_base = os.path.abspath(base_out)
    if os.path.islink(normalized_base):
        message = (
            f"Destination '{normalized_base}' is a symlink. "
            "Choose a real folder inside your merge target."
        )
        log_warning(message)
        raise NolossiaError(message)
    ensure_directory(normalized_base)
    if folders is None:
        folders = []
    lower_mode = merge_mode.lower()
    if lower_mode != "on":
        for folder in folders:
            ensure_directory(folder)
        return

    for folder in folders:
        normalized_folder = os.path.abspath(folder)
        violation = path_violation_message(
            normalized_folder, normalized_base, label="Destination folder"
        )
        if violation:
            log_warning(violation)
            raise NolossiaError(violation)
        try:
            common = os.path.commonpath([normalized_base, normalized_folder])
        except ValueError as exc:
            raise NolossiaError(f"Folder {normalized_folder} is outside destination {normalized_base}") from exc
        if common != normalized_base:
            raise NolossiaError(f"Folder {normalized_folder} is outside destination {normalized_base}")

        rel = os.path.relpath(normalized_folder, normalized_base)
        parts = [part for part in rel.split(os.sep) if part and part != "."]
        if not parts:
            continue
        if parts[0] in {"REVIEW", "QUARANTINE_EXACT"}:
            ensure_directory(os.path.join(normalized_base, parts[0]))
            ensure_directory(normalized_folder)
            continue

        year_segment = parts[0]
        if not re.fullmatch(r"(19|20)\d{2}", year_segment):
            raise NolossiaError(
                f"Invalid YEAR folder '{year_segment}' for destination {normalized_base}"
            )
        ensure_directory(os.path.join(normalized_base, year_segment))

        if len(parts) >= 2:
            month_segment = parts[1]
            if not re.fullmatch(rf"{year_segment}-([01]\d)", month_segment):
                raise NolossiaError(
                    f"Invalid YEAR-MONTH folder '{month_segment}' under {year_segment}"
                )
            ensure_directory(os.path.join(normalized_base, year_segment, month_segment))
        ensure_directory(normalized_folder)


class ChronologyResult(NamedTuple):
    coordinates: Optional[tuple[int, int]]
    invalid: bool


def _existing_chronology(path: str) -> ChronologyResult:
    """
    Detect existing YEAR / YEAR-MONTH patterns in the source path.
    """
    parts = [p for p in os.path.abspath(path).split(os.sep) if p]
    for idx, part in enumerate(parts[:-1]):  # ignore filename
        year_match = re.fullmatch(r"(19|20)\d{2}", part)
        if not year_match:
            continue
        if idx + 1 >= len(parts) - 1:
            continue
        next_part = parts[idx + 1]
        detection = _parse_month_segment(year_match.group(0), next_part)
        if detection == "invalid":
            log_warning(
                f"Invalid chronological folder '{next_part}' detected under '{part}' for file '{path}'. "
                "Set aside for review."
            )
            return ChronologyResult(None, True)
        if isinstance(detection, tuple):
            return ChronologyResult(detection, False)
    return ChronologyResult(None, False)


def _parse_month_segment(year_text: str, candidate: str) -> tuple[int, int] | str | None:
    """
    Parse a YEAR-MONTH pattern relative to the detected year segment.
    Returns:
        tuple[int, int]: valid (year, month) pair when the candidate encodes a valid month.
        "invalid": when a chronological pattern exists but the month is outside 01-12.
        None: when no chronological pattern exists for the candidate.
    """
    normalized_year = int(year_text)
    year_month_match = re.fullmatch(rf"{year_text}[-_]?([01]\d)", candidate)
    if year_month_match:
        month_val = int(year_month_match.group(1))
        if 1 <= month_val <= 12:
            return normalized_year, month_val
        return "invalid"

    month_only_match = re.fullmatch(r"([01]\d)", candidate)
    if month_only_match:
        month_val = int(month_only_match.group(1))
        if 1 <= month_val <= 12:
            return normalized_year, month_val
        return "invalid"
    return None
