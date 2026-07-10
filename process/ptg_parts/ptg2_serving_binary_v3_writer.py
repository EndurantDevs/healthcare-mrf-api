# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded-memory, unwired reference writer for PostgreSQL PTG2 v3 blocks."""
from __future__ import annotations
from collections.abc import AsyncIterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any
from process.ptg_parts.ptg2_serving_binary import (
    PTG2_SERVING_BINARY_BY_CODE_DICTIONARY_KIND,
    _copy_serving_binary_records,
    _row_value,
    _serving_binary_copy_records,
    _serving_binary_record,
)
from process.ptg_parts.ptg2_serving_binary_v3 import (
    PTG2_V3_ATOM_KEY_24_BITS,
    PTG2_V3_ATOM_KEY_32_BITS,
    PTG2V3PriceAtomRecord,
    append_uvarint,
    encode_price_atoms,
    encode_price_memberships,
    encode_provider_code_set,
    select_atom_key_bits,
)
PTG2_V3_REFERENCE_FORMAT = "postgres_binary_v3"
PTG2_V3_PROVIDER_SET_CODES_ARTIFACT_KIND = "provider_set_codes_v3"
PTG2_V3_PRICE_SET_ATOM_MEMBERSHIPS_ARTIFACT_KIND = "price_set_atom_memberships_v3"
PTG2_V3_PRICE_ATOMS_ARTIFACT_KIND = "price_atoms_v3"
PTG2_V3_PROVIDER_SET_KEY_BLOCK_SPAN = 1024
PTG2_V3_PRICE_KEY_BLOCK_SPAN = PTG2_V3_ATOM_KEY_BLOCK_SPAN = 512
_SUPPORTED_ATOM_KEY_BITS = {PTG2_V3_ATOM_KEY_24_BITS, PTG2_V3_ATOM_KEY_32_BITS}
@dataclass
class _StorageCounts:
    record_count: int = 0
    entry_count: int = 0
    stored_payload_bytes: int = 0
    raw_payload_bytes: int = 0
    compressed_records: int = 0
    def observe(self, serving_record: tuple[Any, ...], raw_byte_count: int) -> None:
        """Include one physical table record in manifest counters."""
        self.record_count += 1
        self.entry_count += int(serving_record[3])
        self.stored_payload_bytes += len(serving_record[4])
        self.raw_payload_bytes += raw_byte_count
        self.compressed_records += int(serving_record[5] != "none")
    def manifest(self) -> dict[str, int]:
        """Return JSON-compatible stored and uncompressed byte counts."""
        return {
            "record_count": self.record_count,
            "entry_count": self.entry_count,
            "stored_payload_bytes": self.stored_payload_bytes,
            "raw_payload_bytes": self.raw_payload_bytes,
            "compressed_records": self.compressed_records,
            "compressed_saved_bytes": self.raw_payload_bytes - self.stored_payload_bytes,
        }
@dataclass
class _CopyBatch:
    schema_name: str
    table_name: str
    max_payload_bytes: int
    batch_size: int
    pending_records: list[tuple[Any, ...]] = field(default_factory=list)
    storage_counts: _StorageCounts = field(default_factory=_StorageCounts)
    async def append_logical_block(
        self,
        artifact_kind: str,
        block_key: int,
        entry_count: int,
        logical_payload: bytes,
    ) -> None:
        """Write capped raw fragments whose concatenation restores one logical payload."""
        if not logical_payload:
            raise RuntimeError(f"{artifact_kind} cannot emit an empty payload")
        for block_no, payload_fragment in enumerate(_raw_payload_chunks(
            logical_payload,
            self.max_payload_bytes,
        )):
            serving_record = _serving_binary_record(
                artifact_kind,
                block_key,
                block_no,
                entry_count if block_no == 0 else 0,
                payload_fragment,
            )
            self.storage_counts.observe(serving_record, len(payload_fragment))
            self.pending_records.append(serving_record)
            if len(self.pending_records) >= self.batch_size:
                await self.flush()
    async def flush(self) -> None:
        """COPY the current bounded record batch."""
        if not self.pending_records:
            return
        await _copy_serving_binary_records(
            schema_name=self.schema_name,
            table_name=self.table_name,
            records=self.pending_records,
        )
        self.pending_records = []
def _raw_payload_chunks(logical_payload: bytes, max_payload_bytes: int) -> tuple[bytes, ...]:
    """Split only the raw, re-concatenable logical payload before compression."""
    return tuple(
        logical_payload[offset : offset + max_payload_bytes]
        for offset in range(0, len(logical_payload), max_payload_bytes)
    )
def _new_copy_batch(
    schema_name: str, table_name: str,
    max_payload_bytes: int,
    copy_batch_size: int | None,
) -> _CopyBatch:
    batch_size = _serving_binary_copy_records() if copy_batch_size is None else int(copy_batch_size)
    if int(max_payload_bytes) <= 0:
        raise ValueError("max_payload_bytes must be positive")
    if batch_size <= 0:
        raise ValueError("copy_batch_size must be positive")
    return _CopyBatch(schema_name, table_name, int(max_payload_bytes), batch_size)
def _atom_key_bits(atom_key_bits: int) -> int:
    normalized_bits = int(atom_key_bits)
    if normalized_bits not in _SUPPORTED_ATOM_KEY_BITS:
        raise ValueError("atom_key_bits must be 24 or 32")
    return normalized_bits
def _artifact_summary(
    artifact_kind: str,
    block_span: int,
    copy_batch: _CopyBatch,
    **details: Any,
) -> dict[str, Any]:
    return {
        "format": PTG2_V3_REFERENCE_FORMAT,
        "artifact_kind": artifact_kind,
        "block_span": block_span,
        **details,
        "storage": copy_batch.storage_counts.manifest(),
    }
def _provider_block_payload(
    block_key: int,
    provider_entries: Sequence[tuple[int, bytes]],
) -> bytes:
    block_payload = bytearray()
    append_uvarint(block_payload, len(provider_entries))
    block_start = block_key * PTG2_V3_PROVIDER_SET_KEY_BLOCK_SPAN
    for provider_set_key, code_payload in provider_entries:
        append_uvarint(block_payload, provider_set_key - block_start)
        append_uvarint(block_payload, len(code_payload))
        block_payload.extend(code_payload)
    return bytes(block_payload)
@dataclass
class _ProviderCodeWriter:
    copy_batch: _CopyBatch
    previous_pair: tuple[int, int] | None = None
    current_provider_key: int | None = None
    current_code_keys: list[int] = field(default_factory=list)
    current_block_key: int | None = None
    provider_entries: list[tuple[int, bytes]] = field(default_factory=list)
    provider_set_count: int = 0
    code_count: int = 0
    container_count: int = 0
    async def add(self, provider_set_key: int, code_key: int) -> None:
        """Validate and append one provider/code edge."""
        normalized_pair = (int(provider_set_key), int(code_key))
        if normalized_pair[0] < 0:
            raise ValueError("provider_set_key cannot be negative")
        if self.previous_pair is not None and normalized_pair <= self.previous_pair:
            raise ValueError("provider/code rows must be strictly ordered by provider_set_key, code_key")
        if self.current_provider_key is not None and normalized_pair[0] != self.current_provider_key:
            await self.finish_provider()
        self.current_provider_key = normalized_pair[0]
        self.current_code_keys.append(normalized_pair[1])
        self.code_count += 1
        self.previous_pair = normalized_pair
    async def finish_provider(self) -> None:
        """Encode one provider's complete code container set."""
        if self.current_provider_key is None:
            return
        block_key = self.current_provider_key // PTG2_V3_PROVIDER_SET_KEY_BLOCK_SPAN
        if self.current_block_key != block_key:
            await self.flush_block()
            self.current_block_key = block_key
        code_payload, code_stats = encode_provider_code_set(self.current_code_keys)
        self.provider_entries.append((self.current_provider_key, code_payload))
        self.provider_set_count += 1
        self.container_count += code_stats.container_count
        self.current_provider_key = None
        self.current_code_keys = []
    async def flush_block(self) -> None:
        """Emit one provider-set-key logical block."""
        if not self.provider_entries:
            return
        await self.copy_batch.append_logical_block(
            PTG2_V3_PROVIDER_SET_CODES_ARTIFACT_KIND,
            int(self.current_block_key or 0),
            len(self.provider_entries),
            _provider_block_payload(int(self.current_block_key or 0), self.provider_entries),
        )
        self.provider_entries = []
    async def finish(self) -> None:
        """Flush the last provider, logical block, and COPY batch."""
        await self.finish_provider()
        await self.flush_block()
        await self.copy_batch.flush()
@dataclass
class _PriceMembershipWriter:
    copy_batch: _CopyBatch
    atom_key_bits: int
    previous_pair: tuple[int, int] | None = None
    current_price_key: int | None = None
    current_atom_keys: list[int] = field(default_factory=list)
    current_block_key: int | None = None
    memberships: list[tuple[int, tuple[int, ...]]] = field(default_factory=list)
    membership_count: int = 0
    maximum_price_key: int | None = None
    maximum_atom_key: int | None = None
    atom_reference_count: int = 0
    async def add(self, price_key: int, atom_key: int) -> None:
        """Validate and append one price/atom membership edge."""
        normalized_pair = (int(price_key), int(atom_key))
        if min(normalized_pair) < 0:
            raise ValueError("price and atom keys cannot be negative")
        if normalized_pair[1] >= 1 << self.atom_key_bits:
            raise ValueError(f"dense atom key does not fit in {self.atom_key_bits} bits")
        if self.previous_pair is not None and normalized_pair <= self.previous_pair:
            raise ValueError("price/atom rows must be strictly ordered by price_key, atom_key")
        if self.current_price_key is not None and normalized_pair[0] != self.current_price_key:
            await self.finish_membership()
        self.current_price_key = normalized_pair[0]
        self.current_atom_keys.append(normalized_pair[1])
        self.atom_reference_count += 1
        self.previous_pair = normalized_pair
    async def finish_membership(self) -> None:
        """Move one complete price membership into its fixed-span block."""
        if self.current_price_key is None:
            return
        block_key = self.current_price_key // PTG2_V3_PRICE_KEY_BLOCK_SPAN
        if self.current_block_key != block_key:
            await self.flush_block()
            self.current_block_key = block_key
        self.memberships.append((self.current_price_key, tuple(self.current_atom_keys)))
        self.membership_count += 1
        self.maximum_price_key = self.current_price_key
        self.maximum_atom_key = self.current_atom_keys[-1]
        self.current_price_key = None
        self.current_atom_keys = []
    async def flush_block(self) -> None:
        """Encode and emit one price-key logical block."""
        if not self.memberships:
            return
        await self.copy_batch.append_logical_block(
            PTG2_V3_PRICE_SET_ATOM_MEMBERSHIPS_ARTIFACT_KIND,
            int(self.current_block_key or 0),
            len(self.memberships),
            encode_price_memberships(self.memberships, self.atom_key_bits),
        )
        self.memberships = []
    async def finish(self) -> None:
        """Flush the last membership, logical block, and COPY batch."""
        await self.finish_membership()
        await self.flush_block()
        await self.copy_batch.flush()
@dataclass
class _DenseAtomWriter:
    copy_batch: _CopyBatch
    atom_key_bits: int
    expected_atom_key: int = 0
    current_block_key: int | None = None
    price_atoms: list[PTG2V3PriceAtomRecord] = field(default_factory=list)
    attribute_count: int | None = None
    async def add(
        self,
        atom_key: int,
        negotiated_rate: str | None,
        attribute_keys: Sequence[int | None],
    ) -> None:
        """Validate and append one dense atom row."""
        normalized_key = int(atom_key)
        if normalized_key != self.expected_atom_key:
            raise ValueError(
                "v3 atom keys must be dense and ordered from zero: "
                f"expected {self.expected_atom_key}, got {normalized_key}"
            )
        if normalized_key >= 1 << self.atom_key_bits:
            raise ValueError(f"dense atom key does not fit in {self.atom_key_bits} bits")
        normalized_attributes = tuple(attribute_keys)
        if self.attribute_count is None:
            self.attribute_count = len(normalized_attributes)
        elif len(normalized_attributes) != self.attribute_count:
            raise ValueError("all v3 atom rows must use the same attribute-key count")
        block_key = normalized_key // PTG2_V3_ATOM_KEY_BLOCK_SPAN
        if self.current_block_key != block_key:
            await self.flush_block()
            self.current_block_key = block_key
        self.price_atoms.append(PTG2V3PriceAtomRecord(negotiated_rate, normalized_attributes))
        self.expected_atom_key += 1
    async def flush_block(self) -> None:
        """Encode and emit one atom-key logical block."""
        if not self.price_atoms:
            return
        await self.copy_batch.append_logical_block(
            PTG2_V3_PRICE_ATOMS_ARTIFACT_KIND,
            int(self.current_block_key or 0),
            len(self.price_atoms),
            encode_price_atoms(self.price_atoms),
        )
        self.price_atoms = []
    async def finish(self) -> None:
        """Flush the last logical block and COPY batch."""
        await self.flush_block()
        await self.copy_batch.flush()
async def write_provider_set_code_blocks(
    *,
    schema_name: str,
    table_name: str,
    ordered_rows: AsyncIterable[Any],
    max_payload_bytes: int,
    copy_batch_size: int | None = None,
) -> dict[str, Any]:
    """Write provider-set code containers from strictly ordered edge rows."""
    copy_batch = _new_copy_batch(schema_name, table_name, max_payload_bytes, copy_batch_size)
    code_writer = _ProviderCodeWriter(copy_batch)
    async for code_row in ordered_rows:
        await code_writer.add(
            _row_value(code_row, "provider_set_key", 0),
            _row_value(code_row, "code_key", 1),
        )
    await code_writer.finish()
    return _artifact_summary(
        PTG2_V3_PROVIDER_SET_CODES_ARTIFACT_KIND,
        PTG2_V3_PROVIDER_SET_KEY_BLOCK_SPAN,
        copy_batch,
        provider_set_count=code_writer.provider_set_count,
        code_count=code_writer.code_count,
        container_count=code_writer.container_count,
    )
async def write_price_atom_membership_blocks(
    *,
    schema_name: str,
    table_name: str,
    ordered_rows: AsyncIterable[Any],
    atom_key_bits: int,
    max_payload_bytes: int,
    copy_batch_size: int | None = None,
) -> dict[str, Any]:
    """Write shared-price-key to dense-atom-key membership blocks."""
    normalized_bits = _atom_key_bits(atom_key_bits)
    copy_batch = _new_copy_batch(schema_name, table_name, max_payload_bytes, copy_batch_size)
    membership_writer = _PriceMembershipWriter(copy_batch, normalized_bits)
    async for membership_row in ordered_rows:
        await membership_writer.add(
            _row_value(membership_row, "price_key", 0),
            _row_value(membership_row, "atom_key", 1),
        )
    await membership_writer.finish()
    return _artifact_summary(
        PTG2_V3_PRICE_SET_ATOM_MEMBERSHIPS_ARTIFACT_KIND,
        PTG2_V3_PRICE_KEY_BLOCK_SPAN,
        copy_batch,
        atom_key_bits=normalized_bits,
        atom_key_bytes=normalized_bits // 8,
        membership_count=membership_writer.membership_count,
        maximum_price_key=membership_writer.maximum_price_key,
        maximum_atom_key=membership_writer.maximum_atom_key,
        atom_reference_count=membership_writer.atom_reference_count,
    )
async def write_dense_atom_payload_blocks(
    *,
    schema_name: str,
    table_name: str,
    ordered_rows: AsyncIterable[Any],
    atom_key_bits: int,
    max_payload_bytes: int,
    copy_batch_size: int | None = None,
) -> dict[str, Any]:
    """Write dense atom rows using the snapshot-wide selected atom-key width."""
    normalized_bits = _atom_key_bits(atom_key_bits)
    copy_batch = _new_copy_batch(schema_name, table_name, max_payload_bytes, copy_batch_size)
    atom_writer = _DenseAtomWriter(copy_batch, normalized_bits)
    async for atom_row in ordered_rows:
        await atom_writer.add(
            _row_value(atom_row, "atom_key", 0),
            _row_value(atom_row, "negotiated_rate", 1),
            _row_value(atom_row, "attribute_keys", 2),
        )
    await atom_writer.finish()
    return _artifact_summary(
        PTG2_V3_PRICE_ATOMS_ARTIFACT_KIND,
        PTG2_V3_ATOM_KEY_BLOCK_SPAN,
        copy_batch,
        atom_key_bits=normalized_bits,
        atom_key_bytes=normalized_bits // 8,
        atom_count=atom_writer.expected_atom_key,
        attribute_count=atom_writer.attribute_count or 0,
    )
def _aggregate_storage(artifact_summary_by_name: Mapping[str, Mapping[str, Any]]) -> dict[str, int]:
    storage_fields = (
        "record_count",
        "entry_count",
        "stored_payload_bytes",
        "raw_payload_bytes",
        "compressed_records",
        "compressed_saved_bytes",
    )
    return {
        storage_field: sum(
            int(summary["storage"][storage_field]) for summary in artifact_summary_by_name.values()
        )
        for storage_field in storage_fields
    }
async def write_v3_reference_blocks(
    *,
    schema_name: str, table_name: str,
    provider_code_rows: AsyncIterable[Any],
    price_membership_rows: AsyncIterable[Any],
    atom_payload_rows: AsyncIterable[Any],
    price_set_count: int,
    atom_count: int,
    max_payload_bytes: int,
) -> dict[str, Any]:
    """Write three v3 kinds while retaining the canonical v2 price dictionary."""
    normalized_price_set_count = int(price_set_count)
    if normalized_price_set_count < 0:
        raise ValueError("price_set_count cannot be negative")
    atom_key_bits = select_atom_key_bits(atom_count)
    common_option_map = {
        "schema_name": schema_name,
        "table_name": table_name,
        "max_payload_bytes": max_payload_bytes,
    }
    artifact_summary_by_name = {
        "provider_set_codes": await write_provider_set_code_blocks(
            ordered_rows=provider_code_rows,
            **common_option_map,
        ),
        "price_set_atom_memberships": await write_price_atom_membership_blocks(
            ordered_rows=price_membership_rows,
            atom_key_bits=atom_key_bits,
            **common_option_map,
        ),
        "price_atoms": await write_dense_atom_payload_blocks(
            ordered_rows=atom_payload_rows,
            atom_key_bits=atom_key_bits,
            **common_option_map,
        ),
    }
    actual_atom_count = int(artifact_summary_by_name["price_atoms"]["atom_count"])
    if actual_atom_count != int(atom_count):
        raise RuntimeError(f"dense atom row count mismatch: expected {atom_count}, got {actual_atom_count}")
    membership_summary = artifact_summary_by_name["price_set_atom_memberships"]
    maximum_price_key = membership_summary.get("maximum_price_key")
    if maximum_price_key is not None and int(maximum_price_key) >= normalized_price_set_count:
        raise RuntimeError("price membership references a key outside by_code_price_dictionary")
    maximum_atom_key = membership_summary.get("maximum_atom_key")
    if maximum_atom_key is not None and int(maximum_atom_key) >= actual_atom_count:
        raise RuntimeError("price membership references an atom outside the dense atom payload")
    return {
        "format": PTG2_V3_REFERENCE_FORMAT,
        "writer": "python_reference_unwired",
        "table": f"{schema_name}.{table_name}",
        "max_payload_bytes": int(max_payload_bytes),
        "atom_key_bits": atom_key_bits,
        "atom_key_bytes": atom_key_bits // 8,
        "forward_price_dictionary": {
            "artifact_kind": PTG2_SERVING_BINARY_BY_CODE_DICTIONARY_KIND,
            "price_set_count": normalized_price_set_count,
        },
        "artifacts": artifact_summary_by_name,
        "storage": _aggregate_storage(artifact_summary_by_name),
    }
write_ptg2_serving_binary_v3_reference = write_v3_reference_blocks
