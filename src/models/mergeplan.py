"""
Module: mergeplan
Purpose: Merge plan dataclass definition.
"""

from dataclasses import dataclass
from typing import List
from .actions import MergeAction


@dataclass
class MergePlan:
    """
    Represents the full merge plan including required space
    and all file actions.
    """

    required_space: int
    destination_free: int
    duplicate_count: int
    total_files: int
    actions: List[MergeAction]
    destination_path: str
    skipped_files: int = 0
