"""
Module: cluster
Purpose: Duplicate cluster dataclass.
"""

from dataclasses import dataclass
from typing import List, Optional

from .fileinfo import FileInfo


@dataclass
class DuplicateCluster:
    """
    Group of files that represent exact or near-duplicate photos.
    """

    cluster_id: str
    files: List[FileInfo]
    master: Optional[FileInfo]
    redundant: List[FileInfo]
