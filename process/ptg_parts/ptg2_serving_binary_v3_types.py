# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Value objects shared by the PTG2 v3 compact codecs."""

from dataclasses import dataclass


@dataclass(frozen=True)
class PTG2V3CodeSetStats:
    """Describe the selected container encodings for one provider code set."""

    code_count: int
    container_count: int
    sparse_container_count: int
    run_container_count: int
    bitmap_container_count: int
    encoded_bytes: int


@dataclass(frozen=True)
class PTG2V3PriceAtomRecord:
    """Store one negotiated rate and its lean dictionary keys."""

    negotiated_rate: str | None
    attribute_keys: tuple[int | None, ...]


@dataclass(frozen=True)
class MembershipPayloadHeader:
    """Locate checkpointed price-membership records inside one payload."""

    version: int
    atom_key_bits: int
    entry_count: int
    records_offset: int
    checkpoint_interval: int
    checkpoints: tuple[tuple[int | None, int], ...]


@dataclass(frozen=True)
class AtomPayloadHeader:
    """Locate checkpointed price-atom records inside one payload."""

    version: int
    attribute_count: int
    entry_count: int
    records_offset: int
    checkpoint_interval: int
    checkpoint_offsets: tuple[int, ...]
