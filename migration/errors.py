"""Exceptions raised by the layout migration runner."""


class MigrationError(Exception):
    """Base class for every migration failure."""


class FieldMappingError(MigrationError):
    """A source record cannot be transformed with the supplied mapping."""


class PartitionConflictError(MigrationError):
    """A target file that the batch does not own already exists."""


class ValidationError(MigrationError):
    """The dual-read comparison between source and target failed."""


class CheckpointError(MigrationError):
    """The persisted migration state is missing or inconsistent."""
