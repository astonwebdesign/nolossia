"""
Module: fileinfo
Purpose: Dataclass representing file metadata.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Tuple


@dataclass
class FileInfo:
    """
    Dataclass representing a single file in the Nolossia pipeline.
    """

    path: str
    size: int
    format: str
    resolution: Optional[Tuple[int, int]]
    exif_datetime: Optional[datetime]
    exif_gps: Optional[Tuple[float, float]]
    exif_camera: Optional[str]
    exif_orientation: Optional[str]
    sha256: Optional[str]
    phash: Optional[str]
    is_raw: bool
    timestamp_reliable: bool = True
    review_reason: Optional[str] = None
    selection_reason: Optional[str] = None

    def __hash__(self):
        return hash(self.path)

    def __eq__(self, other):
        if not isinstance(other, FileInfo):
            return NotImplemented
        return self.path == other.path
