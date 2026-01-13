"""
Module: actions
Purpose: Defines the data structures for merge plan actions.
"""

from dataclasses import dataclass, field

@dataclass
class MergeAction:
    """Base class for all merge actions."""
    type: str

@dataclass
class CreateFolderAction(MergeAction):
    """Action to create a folder."""
    path: str
    type: str = field(default="CREATE_FOLDER", init=False)

@dataclass
class MoveMasterAction(MergeAction):
    """Action to move a master file."""
    src: str
    dst: str
    sha256: str | None
    size: int
    review_reason: str | None = None
    type: str = field(default="MOVE_MASTER", init=False)

@dataclass
class MoveToQuarantineExactAction(MergeAction):
    """Action to move an exact duplicate to quarantine."""
    src: str
    dst: str
    sha256: str | None
    size: int
    type: str = field(default="MOVE_TO_QUARANTINE_EXACT", init=False)

@dataclass
class MarkNearDuplicateAction(MergeAction):
    """Action to mark a near-duplicate file."""
    src: str
    master: str
    sha256: str | None
    size: int
    type: str = field(default="MARK_NEAR_DUPLICATE", init=False)
