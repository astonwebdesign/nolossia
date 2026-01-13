"""
Module: exceptions
Purpose: Custom exception hierarchy for Nolossia.
"""


class NolossiaError(Exception):
    """Base exception for Nolossia."""

    pass


class ScanError(NolossiaError):
    pass


class MetadataError(NolossiaError):
    pass

class OversizedImageError(MetadataError):
    pass


class HashingError(NolossiaError):
    pass


class DuplicateDetectionError(NolossiaError):
    pass


class MergePlanError(NolossiaError):
    pass


class MergeExecutionError(NolossiaError):
    pass


class StorageError(NolossiaError):
    pass


class UndoError(NolossiaError):
    pass


class UndoInputError(UndoError):
    pass


class UndoSafetyError(UndoError):
    pass
