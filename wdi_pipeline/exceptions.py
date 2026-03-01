from __future__ import annotations


class PipelineError(Exception):
    """Base class for all pipeline errors."""


class ManifestValidationError(PipelineError):
    """Raised when manifest.yaml fails validation."""


class ConnectorError(PipelineError):
    """Raised when a connector fails to fetch or process data."""


class SQLTemplateError(PipelineError):
    """Raised when a SQL template has unresolved {{key}} placeholders."""


class ExportError(PipelineError):
    """Raised when export to CSV or Parquet fails."""
