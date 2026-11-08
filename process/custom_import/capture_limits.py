# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure resource limits shared by capture and declarative acquisition."""

from __future__ import annotations

from dataclasses import dataclass

_DEFAULT_READ_CHUNK_BYTES = 64 * 1024


@dataclass(frozen=True)
class CaptureLimits:
    """Resource limits applied before custom-import source data is admitted.

    The per-record byte limit applies to raw delimited and JSON record bytes
    (including escapes) and to the cumulative scalar content of XML records.
    XML parser safety also applies fixed internal ceilings to unfinished markup
    and the document-wide expanded-name vocabulary.
    """

    maximum_compressed_bytes: int = 64 * 1024 * 1024
    maximum_decoded_bytes: int = 256 * 1024 * 1024
    maximum_record_bytes: int = 1024 * 1024
    maximum_records: int = 1_000_000
    maximum_fields_per_record: int = 1_024
    read_chunk_bytes: int = _DEFAULT_READ_CHUNK_BYTES

    def __post_init__(self) -> None:
        """Reject nonsensical or internally inconsistent resource limits."""

        for name, value in (
            ("maximum_compressed_bytes", self.maximum_compressed_bytes),
            ("maximum_decoded_bytes", self.maximum_decoded_bytes),
            ("maximum_record_bytes", self.maximum_record_bytes),
            ("maximum_records", self.maximum_records),
            ("maximum_fields_per_record", self.maximum_fields_per_record),
            ("read_chunk_bytes", self.read_chunk_bytes),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if self.maximum_decoded_bytes < self.maximum_record_bytes:
            raise ValueError("maximum_decoded_bytes must cover one complete record")
