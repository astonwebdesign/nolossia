"""
Module: review
Purpose: Shared constants and helpers for REVIEW bucket routing.
"""

from __future__ import annotations

REVIEW_REASON_MISSING_EXIF = "missing_exif_timestamp"
REVIEW_REASON_UNRELIABLE_TIMESTAMP = "timestamp_unreliable"
REVIEW_REASON_INVALID_CHRONOLOGY = "invalid_chronology"

_REASON_LABELS = {
    REVIEW_REASON_MISSING_EXIF: "Missing photo date (EXIF)",
    REVIEW_REASON_UNRELIABLE_TIMESTAMP: "Unreliable photo date (filesystem fallback)",
    REVIEW_REASON_INVALID_CHRONOLOGY: "Invalid YEAR/YEAR-MONTH destination",
}


def describe_review_reason(reason: str | None) -> str:
    """
    Return a human-readable description for a REVIEW routing reason.
    """
    if not reason:
        return "Set aside for review (date needs confirmation)"
    return _REASON_LABELS.get(reason, reason)
