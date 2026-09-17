# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded raw Parquet page preflight for custom-import capture."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import pyarrow as pa

_MAX_FOOTER_BYTES = 1024 * 1024
_MAX_CONTAINER_ITEMS = 64 * 1024
_MAX_HEADER_BYTES = 64 * 1024
_MAX_HEADER_DEPTH = 16
_MAX_HEADER_FIELDS = 64
_MAX_CHUNK_RANGES = 64 * 1024
_MAX_PAGE_COUNT = 64 * 1024
_MAX_PAGE_DECODED_BYTES = 16 * 1024 * 1024
_SINGLE_ROW_ARROW_OVERHEAD_BYTES = 256
_DICTIONARY_ENTRY_OVERHEAD_BYTES = 64

_COMPACT_STOP = 0
_COMPACT_TRUE = 1
_COMPACT_FALSE = 2
_COMPACT_BYTE = 3
_COMPACT_I16 = 4
_COMPACT_I32 = 5
_COMPACT_I64 = 6
_COMPACT_DOUBLE = 7
_COMPACT_BINARY = 8
_COMPACT_LIST = 9
_COMPACT_SET = 10
_COMPACT_MAP = 11
_COMPACT_STRUCT = 12
_COMPACT_UUID = 13

_PAGE_TYPE_DATA = 0
_PAGE_TYPE_INDEX = 1
_PAGE_TYPE_DICTIONARY = 2
_PAGE_TYPE_DATA_V2 = 3

_ENCODING_PLAIN = 0
_ENCODING_PLAIN_DICTIONARY = 2
_ENCODING_RLE = 3
_ENCODING_RLE_DICTIONARY = 8
_ALLOWED_VALUE_ENCODINGS = frozenset((_ENCODING_PLAIN, _ENCODING_PLAIN_DICTIONARY, _ENCODING_RLE_DICTIONARY))
_V1_INTEGER_FIELD_NAMES = {
    1: "number_of_values",
    2: "value_encoding",
    3: "definition_encoding",
    4: "repetition_encoding",
}
_V2_INTEGER_FIELD_NAMES = {
    1: "number_of_values",
    2: "number_of_nulls",
    3: "number_of_rows",
    4: "value_encoding",
    5: "definition_level_bytes",
    6: "repetition_level_bytes",
}


class ParquetPageError(ValueError):
    """Raw Parquet page data is malformed or exceeds the capture budget."""


@dataclass(frozen=True)
class _PageHeader:
    """The page-header facts needed before native page decoding."""

    page_type: int
    header_size: int
    compressed_page_size: int
    uncompressed_page_size: int
    num_values: int | None
    value_encoding: int | None
    definition_levels_byte_length: int = 0
    repetition_levels_byte_length: int = 0


@dataclass
class _PageHeaderFacts:
    """Mutable facts gathered while parsing one outer PageHeader struct."""

    page_type: int | None = None
    uncompressed_page_size: int | None = None
    compressed_page_size: int | None = None
    nested_field_id: int | None = None
    num_values: int | None = None
    value_encoding: int | None = None
    definition_levels_byte_length: int = 0
    repetition_levels_byte_length: int = 0


class _CompactPageReader:
    """Read bounded Compact-protocol values directly from a page-header window."""

    def __init__(self, source_bytes: bytes | memoryview, start_offset: int, end_offset: int) -> None:
        self.source_view = memoryview(source_bytes)
        self.current_offset = start_offset
        self.end_offset = end_offset

    @property
    def remaining_bytes(self) -> int:
        """Return the unread bytes in the fixed page-header window."""

        return self.end_offset - self.current_offset

    def read_byte(self) -> int:
        """Read one byte without crossing into a page body."""

        if self.current_offset >= self.end_offset:
            raise ParquetPageError("Parquet source payload has an invalid page header")
        byte_value = self.source_view[self.current_offset]
        self.current_offset += 1
        return byte_value

    def advance(self, byte_count: int) -> None:
        """Skip a validated range without materializing it."""

        if byte_count < 0 or byte_count > self.remaining_bytes:
            raise ParquetPageError("Parquet source payload has an invalid page header")
        self.current_offset += byte_count

    def read_unsigned(self, *, maximum_bytes: int, maximum_value: int) -> int:
        """Read one bounded Compact unsigned varint."""

        integer_value = 0
        bit_shift = 0
        for _ in range(maximum_bytes):
            byte_value = self.read_byte()
            integer_value |= (byte_value & 0x7F) << bit_shift
            if integer_value > maximum_value:
                raise ParquetPageError("Parquet source payload has an invalid page header")
            if not byte_value & 0x80:
                return integer_value
            bit_shift += 7
        raise ParquetPageError("Parquet source payload has an invalid page header")

    def read_i16(self) -> int:
        """Read one Compact ZigZag i16."""

        wire_value = self.read_unsigned(maximum_bytes=3, maximum_value=0xFFFF)
        return (wire_value >> 1) ^ -(wire_value & 1)

    def read_i32(self) -> int:
        """Read one Compact ZigZag i32."""

        wire_value = self.read_unsigned(maximum_bytes=5, maximum_value=0xFFFFFFFF)
        return (wire_value >> 1) ^ -(wire_value & 1)

    def read_i64(self) -> int:
        """Read one Compact ZigZag i64."""

        wire_value = self.read_unsigned(maximum_bytes=10, maximum_value=0xFFFFFFFFFFFFFFFF)
        return (wire_value >> 1) ^ -(wire_value & 1)

    def read_field(self, previous_field_id: int) -> tuple[int, int] | None:
        """Read one Compact struct field header."""

        header_byte = self.read_byte()
        if header_byte == _COMPACT_STOP:
            return None
        compact_type = header_byte & 0x0F
        field_delta = header_byte >> 4
        if compact_type == _COMPACT_STOP or compact_type > _COMPACT_UUID:
            raise ParquetPageError("Parquet source payload has an invalid page header")
        field_id = previous_field_id + field_delta if field_delta else self.read_i16()
        if field_id <= 0 or field_id > 0x7FFF:
            raise ParquetPageError("Parquet source payload has an invalid page header")
        return field_id, compact_type

    def skip_value(self, compact_type: int, depth: int, *, is_collection_item: bool = False) -> None:
        """Skip a bounded unknown Compact field without constructing its value."""

        if compact_type in (_COMPACT_TRUE, _COMPACT_FALSE):
            self._skip_boolean_item(is_collection_item)
            return
        if compact_type == _COMPACT_BYTE:
            self.advance(1)
            return
        if compact_type == _COMPACT_I16:
            self.read_i16()
            return
        if compact_type == _COMPACT_I32:
            self.read_i32()
            return
        if compact_type == _COMPACT_I64:
            self.read_i64()
            return
        if compact_type == _COMPACT_DOUBLE:
            self.advance(8)
            return
        if compact_type == _COMPACT_BINARY:
            self._skip_binary_value()
            return
        if compact_type in (_COMPACT_LIST, _COMPACT_SET):
            self._skip_collection(depth + 1)
            return
        if compact_type == _COMPACT_MAP:
            self._skip_map(depth + 1)
            return
        if compact_type == _COMPACT_STRUCT:
            self._skip_struct(depth + 1)
            return
        if compact_type == _COMPACT_UUID:
            self.advance(16)
            return
        raise ParquetPageError("Parquet source payload has an invalid page header")

    def _skip_boolean_item(self, is_collection_item: bool) -> None:
        """Consume an explicit collection boolean while retaining inline struct booleans."""

        if not is_collection_item:
            return
        boolean_byte = self.read_byte()
        if boolean_byte not in (_COMPACT_TRUE, _COMPACT_FALSE):
            raise ParquetPageError("Parquet source payload has an invalid page header")

    def _skip_binary_value(self) -> None:
        """Skip one bounded Compact binary value."""

        byte_count = self.read_unsigned(
            maximum_bytes=5,
            maximum_value=min(_MAX_HEADER_BYTES, self.remaining_bytes),
        )
        self.advance(byte_count)

    def _skip_collection(self, depth: int) -> None:
        """Skip one bounded Compact list or set."""

        _require_header_depth(depth)
        collection_header = self.read_byte()
        item_count = collection_header >> 4
        item_type = collection_header & 0x0F
        if item_count == 15:
            item_count = self.read_unsigned(maximum_bytes=5, maximum_value=_MAX_CONTAINER_ITEMS)
        if item_count > _MAX_CONTAINER_ITEMS or item_type == _COMPACT_STOP or item_type > _COMPACT_UUID:
            raise ParquetPageError("Parquet source payload has an invalid page header")
        for _ in range(item_count):
            self.skip_value(item_type, depth, is_collection_item=True)

    def _skip_map(self, depth: int) -> None:
        """Skip one bounded Compact map."""

        _require_header_depth(depth)
        item_count = self.read_unsigned(maximum_bytes=5, maximum_value=_MAX_CONTAINER_ITEMS)
        if item_count == 0:
            return
        compact_types = self.read_byte()
        key_type = compact_types >> 4
        mapped_type = compact_types & 0x0F
        _require_collection_types(key_type, mapped_type)
        for _ in range(item_count):
            self.skip_value(key_type, depth, is_collection_item=True)
            self.skip_value(mapped_type, depth, is_collection_item=True)

    def _skip_struct(self, depth: int) -> None:
        """Skip one recursively bounded Compact struct."""

        for _field_id, compact_type in _iter_compact_fields(self, depth):
            self.skip_value(compact_type, depth)


def parquet_footer_start(source_bytes: bytes | memoryview) -> int:
    """Return the first footer byte after validating the fixed Parquet envelope."""

    if len(source_bytes) < 12 or source_bytes[:4] != b"PAR1" or source_bytes[-4:] != b"PAR1":
        raise ParquetPageError("Parquet source payload has an invalid envelope")
    footer_size = int.from_bytes(source_bytes[-8:-4], byteorder="little")
    if footer_size == 0 or footer_size > _MAX_FOOTER_BYTES or footer_size > len(source_bytes) - 8:
        raise ParquetPageError("Parquet source payload has an invalid footer")
    return len(source_bytes) - 8 - footer_size


def _require_header_depth(depth: int) -> None:
    """Reject a Compact structure that exceeds the configured nesting depth."""

    if depth > _MAX_HEADER_DEPTH:
        raise ParquetPageError("Parquet source payload has an invalid page header")


def _require_collection_types(first_type: int, second_type: int) -> None:
    """Reject invalid Compact map key and value type tags."""

    if (
        first_type == _COMPACT_STOP
        or second_type == _COMPACT_STOP
        or first_type > _COMPACT_UUID
        or second_type > _COMPACT_UUID
    ):
        raise ParquetPageError("Parquet source payload has an invalid page header")


def _iter_compact_fields(reader: _CompactPageReader, depth: int) -> Iterator[tuple[int, int]]:
    """Yield unique Compact fields through the enclosing STOP marker."""

    _require_header_depth(depth)
    previous_field_id = 0
    seen_field_ids: set[int] = set()
    for _ in range(_MAX_HEADER_FIELDS):
        field = reader.read_field(previous_field_id)
        if field is None:
            return
        field_id, compact_type = field
        if field_id in seen_field_ids:
            raise ParquetPageError("Parquet source payload has an invalid page header")
        seen_field_ids.add(field_id)
        previous_field_id = field_id
        yield field
    raise ParquetPageError("Parquet source payload has an invalid page header")


def _require_compact_type(compact_type: int, expected_type: int) -> None:
    """Require the Compact wire type for one known page-header field."""

    if compact_type != expected_type:
        raise ParquetPageError("Parquet source payload has an invalid page header")


def _require_compact_boolean_type(compact_type: int) -> None:
    """Require an inline Compact boolean field type."""

    if compact_type not in (_COMPACT_TRUE, _COMPACT_FALSE):
        raise ParquetPageError("Parquet source payload has an invalid page header")


def _read_nonnegative_i32(reader: _CompactPageReader) -> int:
    """Read one nonnegative Compact i32 page property."""

    integer_value = reader.read_i32()
    if integer_value < 0:
        raise ParquetPageError("Parquet source payload has an invalid page header")
    return integer_value


@dataclass
class _V1PageFacts:
    """Mutable V1 fields gathered before their cross-field validation."""

    number_of_values: int | None = None
    value_encoding: int | None = None
    definition_encoding: int | None = None
    repetition_encoding: int | None = None


def _read_data_page_v1(reader: _CompactPageReader, depth: int) -> tuple[int, int]:
    """Read required V1 data-page facts and bounded optional statistics."""

    v1_facts = _V1PageFacts()
    for field_id, compact_type in _iter_compact_fields(reader, depth):
        _read_v1_field(reader, v1_facts, field_id, compact_type, depth)
    _require_v1_page_fields(v1_facts)
    assert v1_facts.number_of_values is not None
    assert v1_facts.value_encoding is not None
    return v1_facts.number_of_values, v1_facts.value_encoding


def _read_v1_field(
    reader: _CompactPageReader,
    v1_facts: _V1PageFacts,
    field_id: int,
    compact_type: int,
    depth: int,
) -> None:
    """Read one V1 field or skip a bounded extension field."""

    fact_name = _V1_INTEGER_FIELD_NAMES.get(field_id)
    if fact_name is not None:
        _require_compact_type(compact_type, _COMPACT_I32)
        setattr(v1_facts, fact_name, _read_nonnegative_i32(reader))
        return
    if field_id == 5:
        _require_compact_type(compact_type, _COMPACT_STRUCT)
        reader.skip_value(compact_type, depth)
        return
    reader.skip_value(compact_type, depth)


def _require_v1_page_fields(v1_facts: _V1PageFacts) -> None:
    """Require the fields and RLE levels supported for flat V1 pages."""

    if None in (
        v1_facts.number_of_values,
        v1_facts.value_encoding,
        v1_facts.definition_encoding,
        v1_facts.repetition_encoding,
    ):
        raise ParquetPageError("Parquet source payload has an invalid page header")
    if v1_facts.definition_encoding != _ENCODING_RLE or v1_facts.repetition_encoding != _ENCODING_RLE:
        raise ParquetPageError("Parquet source payload has an invalid page header")


def _read_data_page_v2(reader: _CompactPageReader, depth: int) -> tuple[int, int, int, int]:
    """Read required V2 data-page facts and bounded optional statistics."""

    v2_facts = _V2PageFacts()
    for field_id, compact_type in _iter_compact_fields(reader, depth):
        _read_v2_field(reader, v2_facts, field_id, compact_type, depth)
    _validate_v2_page_facts(v2_facts)
    assert v2_facts.number_of_values is not None
    assert v2_facts.value_encoding is not None
    assert v2_facts.definition_level_bytes is not None
    assert v2_facts.repetition_level_bytes is not None
    return (
        v2_facts.number_of_values,
        v2_facts.value_encoding,
        v2_facts.definition_level_bytes,
        v2_facts.repetition_level_bytes,
    )


@dataclass
class _V2PageFacts:
    """Mutable V2 fields gathered before their cross-field validation."""

    number_of_values: int | None = None
    number_of_nulls: int | None = None
    number_of_rows: int | None = None
    value_encoding: int | None = None
    definition_level_bytes: int | None = None
    repetition_level_bytes: int | None = None


def _read_v2_field(
    reader: _CompactPageReader,
    v2_facts: _V2PageFacts,
    field_id: int,
    compact_type: int,
    depth: int,
) -> None:
    """Read one V2 field or skip a bounded extension field."""

    fact_name = _V2_INTEGER_FIELD_NAMES.get(field_id)
    if fact_name is not None:
        _require_compact_type(compact_type, _COMPACT_I32)
        setattr(v2_facts, fact_name, _read_nonnegative_i32(reader))
        return
    if field_id == 7:
        _require_compact_boolean_type(compact_type)
        return
    if field_id == 8:
        _require_compact_type(compact_type, _COMPACT_STRUCT)
        reader.skip_value(compact_type, depth)
        return
    reader.skip_value(compact_type, depth)


def _validate_v2_page_facts(v2_facts: _V2PageFacts) -> None:
    """Validate required V2 counts for the flat source-column contract."""

    required_facts = (
        v2_facts.number_of_values,
        v2_facts.number_of_nulls,
        v2_facts.number_of_rows,
        v2_facts.value_encoding,
        v2_facts.definition_level_bytes,
        v2_facts.repetition_level_bytes,
    )
    if any(fact is None for fact in required_facts):
        raise ParquetPageError("Parquet source payload has an invalid page header")
    assert v2_facts.number_of_values is not None
    assert v2_facts.number_of_nulls is not None
    assert v2_facts.number_of_rows is not None
    if v2_facts.number_of_nulls > v2_facts.number_of_values:
        raise ParquetPageError("Parquet source payload has an invalid page header")
    if v2_facts.number_of_rows != v2_facts.number_of_values:
        raise ParquetPageError("Parquet source payload has an invalid page header")


def _read_dictionary_page(reader: _CompactPageReader, depth: int) -> int:
    """Read a PLAIN dictionary page with an allowed zero-entry count."""

    number_of_values: int | None = None
    dictionary_encoding: int | None = None
    for field_id, compact_type in _iter_compact_fields(reader, depth):
        if field_id == 1:
            _require_compact_type(compact_type, _COMPACT_I32)
            number_of_values = _read_nonnegative_i32(reader)
            continue
        if field_id == 2:
            _require_compact_type(compact_type, _COMPACT_I32)
            dictionary_encoding = _read_nonnegative_i32(reader)
            continue
        if field_id == 3:
            _require_compact_boolean_type(compact_type)
            continue
        reader.skip_value(compact_type, depth)
    if number_of_values is None or dictionary_encoding != _ENCODING_PLAIN:
        raise ParquetPageError("Parquet source payload has an invalid page header")
    return number_of_values


def _read_parquet_page_header(
    source_bytes: bytes | memoryview,
    start_offset: int,
    chunk_end_offset: int,
    header_limit: int,
) -> _PageHeader:
    """Read one raw PageHeader before a native page reader sees its size claims."""

    _validate_page_header_range(start_offset, chunk_end_offset, header_limit)
    reader = _CompactPageReader(source_bytes, start_offset, min(chunk_end_offset, start_offset + header_limit))
    header_facts = _PageHeaderFacts()
    for field_id, compact_type in _iter_compact_fields(reader, 1):
        _read_outer_page_field(reader, header_facts, field_id, compact_type)
    return _build_page_header(header_facts, reader.current_offset - start_offset)


def _validate_page_header_range(start_offset: int, chunk_end_offset: int, header_limit: int) -> None:
    """Validate the fixed raw range available to one Compact PageHeader."""

    if start_offset < 0 or chunk_end_offset < start_offset or header_limit <= 0:
        raise ParquetPageError("Parquet source payload has an invalid page header")


def _read_outer_page_field(
    reader: _CompactPageReader,
    header_facts: _PageHeaderFacts,
    field_id: int,
    compact_type: int,
) -> None:
    """Read one known outer PageHeader field or skip a bounded extension."""

    if field_id in (1, 2, 3):
        _read_outer_integer_field(reader, header_facts, field_id, compact_type)
    elif field_id == 4:
        _require_compact_type(compact_type, _COMPACT_I32)
        reader.read_i32()
    elif field_id in (5, 7, 8):
        _read_nested_page_field(reader, header_facts, field_id, compact_type)
    elif field_id == 6:
        raise ParquetPageError("Parquet source payload has an unsupported page type")
    else:
        reader.skip_value(compact_type, 1)


def _read_outer_integer_field(
    reader: _CompactPageReader,
    header_facts: _PageHeaderFacts,
    field_id: int,
    compact_type: int,
) -> None:
    """Read one required PageHeader i32 field."""

    _require_compact_type(compact_type, _COMPACT_I32)
    integer_value = _read_nonnegative_i32(reader)
    if field_id == 1:
        header_facts.page_type = integer_value
    elif field_id == 2:
        header_facts.uncompressed_page_size = integer_value
    else:
        header_facts.compressed_page_size = integer_value


def _read_nested_page_field(
    reader: _CompactPageReader,
    header_facts: _PageHeaderFacts,
    field_id: int,
    compact_type: int,
) -> None:
    """Read exactly one nested page-type struct."""

    _require_compact_type(compact_type, _COMPACT_STRUCT)
    if header_facts.nested_field_id is not None:
        raise ParquetPageError("Parquet source payload has an invalid page header")
    header_facts.nested_field_id = field_id
    if field_id == 5:
        header_facts.num_values, header_facts.value_encoding = _read_data_page_v1(reader, 2)
    elif field_id == 7:
        header_facts.num_values = _read_dictionary_page(reader, 2)
    else:
        (
            header_facts.num_values,
            header_facts.value_encoding,
            header_facts.definition_levels_byte_length,
            header_facts.repetition_levels_byte_length,
        ) = _read_data_page_v2(reader, 2)


def _build_page_header(header_facts: _PageHeaderFacts, header_size: int) -> _PageHeader:
    """Validate cross-field PageHeader facts and return the immutable result."""

    _require_outer_page_fields(header_facts)
    assert header_facts.page_type is not None
    assert header_facts.uncompressed_page_size is not None
    assert header_facts.compressed_page_size is not None
    assert header_facts.nested_field_id is not None
    expected_nested_field = _expected_nested_field(header_facts.page_type)
    if header_facts.nested_field_id != expected_nested_field:
        raise ParquetPageError("Parquet source payload has an invalid page header")
    _validate_data_page_header_facts(header_facts)
    return _PageHeader(
        page_type=header_facts.page_type,
        header_size=header_size,
        compressed_page_size=header_facts.compressed_page_size,
        uncompressed_page_size=header_facts.uncompressed_page_size,
        num_values=header_facts.num_values,
        value_encoding=header_facts.value_encoding,
        definition_levels_byte_length=header_facts.definition_levels_byte_length,
        repetition_levels_byte_length=header_facts.repetition_levels_byte_length,
    )


def _require_outer_page_fields(header_facts: _PageHeaderFacts) -> None:
    """Require all outer PageHeader fields used by the raw page walk."""

    if None in (
        header_facts.page_type,
        header_facts.uncompressed_page_size,
        header_facts.compressed_page_size,
        header_facts.nested_field_id,
    ):
        raise ParquetPageError("Parquet source payload has an invalid page header")


def _expected_nested_field(page_type: int) -> int:
    """Return the required nested header field for one supported page type."""

    nested_by_page_type = {
        _PAGE_TYPE_DATA: 5,
        _PAGE_TYPE_DICTIONARY: 7,
        _PAGE_TYPE_DATA_V2: 8,
    }
    try:
        return nested_by_page_type[page_type]
    except KeyError as error:
        raise ParquetPageError("Parquet source payload has an unsupported page type") from error


def _validate_data_page_header_facts(header_facts: _PageHeaderFacts) -> None:
    """Validate data-page count and V2 level-length relations."""

    if header_facts.page_type in (_PAGE_TYPE_DATA, _PAGE_TYPE_DATA_V2) and header_facts.num_values is None:
        raise ParquetPageError("Parquet source payload has an invalid page header")
    if header_facts.page_type == _PAGE_TYPE_DATA_V2:
        level_bytes = header_facts.definition_levels_byte_length + header_facts.repetition_levels_byte_length
        if level_bytes > header_facts.compressed_page_size:
            raise ParquetPageError("Parquet source payload has an invalid page header")


@dataclass(frozen=True)
class _ChunkRange:
    """The raw page interval declared for one row-group column chunk."""

    start_offset: int
    end_offset: int
    row_group_index: int
    column_index: int


@dataclass(frozen=True)
class _ChunkContext:
    """Validated footer facts and per-page limits for one physical chunk."""

    expected_value_count: int
    expected_compressed_bytes: int
    expected_decoded_bytes: int
    dictionary_offset: int | None
    first_data_offset: int
    page_header_limit: int
    page_decoded_limit: int
    data_type: pa.DataType
    maximum_decoded_bytes: int


@dataclass
class _ChunkScanState:
    """Incremental raw-page totals and retained-page estimates for one chunk."""

    current_offset: int
    page_count: int = 0
    observed_compressed_bytes: int = 0
    observed_decoded_bytes: int = 0
    observed_value_count: int = 0
    has_dictionary_page: bool = False
    has_data_page: bool = False
    dictionary_page_bytes: int = 0
    dictionary_entry_bytes: int = 0
    maximum_data_page_bytes: int = 0
    maximum_output_page_bytes: int = 0


def validate_page_layout(
    source_bytes: bytes | memoryview,
    file_metadata: Any,
    arrow_schema: pa.Schema,
    *,
    maximum_decoded_bytes: int,
) -> None:
    """Validate raw page ranges and headers before the Arrow batch reader is used."""

    if file_metadata is None:
        raise ParquetPageError("Parquet source payload has no metadata")
    footer_offset = parquet_footer_start(source_bytes)
    chunk_ranges = _collect_chunk_ranges(file_metadata, arrow_schema, footer_offset)
    _ensure_nonoverlapping_chunk_ranges(chunk_ranges)
    _validate_chunk_ranges(source_bytes, file_metadata, arrow_schema, chunk_ranges, maximum_decoded_bytes)


def _collect_chunk_ranges(
    file_metadata: Any,
    arrow_schema: pa.Schema,
    footer_offset: int,
) -> list[_ChunkRange]:
    """Collect bounded page intervals after validating footer row-group facts."""

    row_group_count = _nonnegative_integer(file_metadata.num_row_groups)
    expected_column_count = len(arrow_schema)
    if _nonnegative_integer(file_metadata.num_columns) != expected_column_count:
        raise ParquetPageError("Parquet source payload has inconsistent column metadata")
    if row_group_count * expected_column_count > _MAX_CHUNK_RANGES:
        raise ParquetPageError("Parquet source payload has too many page intervals")
    chunk_ranges: list[_ChunkRange] = []
    for row_group_index in range(row_group_count):
        row_group_metadata = file_metadata.row_group(row_group_index)
        _collect_row_group_ranges(
            row_group_metadata,
            row_group_index,
            expected_column_count,
            footer_offset,
            chunk_ranges,
        )
    return chunk_ranges


def _collect_row_group_ranges(
    row_group_metadata: Any,
    row_group_index: int,
    expected_column_count: int,
    footer_offset: int,
    chunk_ranges: list[_ChunkRange],
) -> None:
    """Collect nonempty raw chunk intervals for one footer row group."""

    row_count = _nonnegative_integer(row_group_metadata.num_rows)
    if _nonnegative_integer(row_group_metadata.num_columns) != expected_column_count:
        raise ParquetPageError("Parquet source payload has inconsistent column metadata")
    for column_index in range(expected_column_count):
        column_metadata = row_group_metadata.column(column_index)
        _require_chunk_value_count(column_metadata, row_count)
        compressed_bytes = _nonnegative_integer(column_metadata.total_compressed_size)
        decoded_bytes = _nonnegative_integer(column_metadata.total_uncompressed_size)
        if compressed_bytes == 0:
            _require_empty_chunk(row_count, decoded_bytes)
        else:
            chunk_ranges.append(
                _make_chunk_range(
                    column_metadata,
                    compressed_bytes,
                    footer_offset,
                    row_group_index,
                    column_index,
                )
            )


def _require_chunk_value_count(column_metadata: Any, row_count: int) -> None:
    """Require flat columns to expose one value slot per row-group record."""

    if _nonnegative_integer(column_metadata.num_values) != row_count:
        raise ParquetPageError("Parquet source payload has inconsistent row metadata")


def _require_empty_chunk(row_count: int, decoded_bytes: int) -> None:
    """Allow a zero-size chunk only for an empty row group."""

    if decoded_bytes != 0 or row_count != 0:
        raise ParquetPageError("Parquet source payload has inconsistent page metadata")


def _make_chunk_range(
    column_metadata: Any,
    compressed_bytes: int,
    footer_offset: int,
    row_group_index: int,
    column_index: int,
) -> _ChunkRange:
    """Return one checked raw interval from its data or dictionary offset."""

    start_offset = _chunk_start_offset(column_metadata)
    if start_offset < 4 or start_offset > footer_offset or compressed_bytes > footer_offset - start_offset:
        raise ParquetPageError("Parquet source payload has invalid page metadata")
    return _ChunkRange(start_offset, start_offset + compressed_bytes, row_group_index, column_index)


def _ensure_nonoverlapping_chunk_ranges(chunk_ranges: list[_ChunkRange]) -> None:
    """Reject overlapping footer intervals while permitting page-index gaps."""

    previous_end_offset = 4
    for chunk_range in sorted(chunk_ranges, key=lambda candidate: candidate.start_offset):
        if chunk_range.start_offset < previous_end_offset or chunk_range.end_offset < chunk_range.start_offset:
            raise ParquetPageError("Parquet source payload has overlapping page metadata")
        previous_end_offset = chunk_range.end_offset


def _validate_chunk_ranges(
    source_bytes: bytes | memoryview,
    file_metadata: Any,
    arrow_schema: pa.Schema,
    chunk_ranges: list[_ChunkRange],
    maximum_decoded_bytes: int,
) -> None:
    """Scan each chunk and aggregate a conservative working estimate by row group."""

    row_group_working_bytes = [0] * _nonnegative_integer(file_metadata.num_row_groups)
    for chunk_range in chunk_ranges:
        row_group_metadata = file_metadata.row_group(chunk_range.row_group_index)
        column_metadata = row_group_metadata.column(chunk_range.column_index)
        column_working_bytes = _scan_chunk_pages(
            source_bytes,
            column_metadata,
            arrow_schema.field(chunk_range.column_index).type,
            chunk_range,
            maximum_decoded_bytes,
        )
        row_group_working_bytes[chunk_range.row_group_index] += column_working_bytes
        if row_group_working_bytes[chunk_range.row_group_index] > maximum_decoded_bytes:
            raise ParquetPageError("Parquet source payload exceeds the decoded-byte limit")


def _scan_chunk_pages(
    source_bytes: bytes | memoryview,
    column_metadata: Any,
    data_type: pa.DataType,
    chunk_range: _ChunkRange,
    maximum_decoded_bytes: int,
) -> int:
    """Walk one raw column chunk and return its conservative one-row working estimate."""

    chunk_context = _make_chunk_context(column_metadata, data_type, maximum_decoded_bytes)
    scan_state = _ChunkScanState(current_offset=chunk_range.start_offset)
    while scan_state.current_offset < chunk_range.end_offset:
        page_header = _read_parquet_page_header(
            source_bytes,
            scan_state.current_offset,
            chunk_range.end_offset,
            chunk_context.page_header_limit,
        )
        next_page_offset = _validate_page_span(
            page_header, scan_state.current_offset, chunk_range.end_offset, chunk_context
        )
        _record_page_totals(scan_state, page_header, chunk_context)
        _record_page_facts(scan_state, page_header, chunk_context)
        scan_state.current_offset = next_page_offset
    _require_complete_chunk_scan(scan_state, chunk_context, chunk_range.end_offset)
    return _estimate_chunk_working_bytes(scan_state)


def _make_chunk_context(
    column_metadata: Any,
    data_type: pa.DataType,
    maximum_decoded_bytes: int,
) -> _ChunkContext:
    """Build checked footer facts and fixed limits for one chunk walk."""

    return _ChunkContext(
        expected_value_count=_nonnegative_integer(column_metadata.num_values),
        expected_compressed_bytes=_nonnegative_integer(column_metadata.total_compressed_size),
        expected_decoded_bytes=_nonnegative_integer(column_metadata.total_uncompressed_size),
        dictionary_offset=_optional_nonnegative_integer(column_metadata.dictionary_page_offset),
        first_data_offset=_nonnegative_integer(column_metadata.data_page_offset),
        page_header_limit=min(_MAX_HEADER_BYTES, maximum_decoded_bytes),
        page_decoded_limit=min(_MAX_PAGE_DECODED_BYTES, maximum_decoded_bytes),
        data_type=data_type,
        maximum_decoded_bytes=maximum_decoded_bytes,
    )


def _validate_page_span(
    page_header: _PageHeader,
    page_offset: int,
    chunk_end_offset: int,
    chunk_context: _ChunkContext,
) -> int:
    """Validate one raw body range and return the next page offset."""

    if page_header.uncompressed_page_size > chunk_context.page_decoded_limit:
        raise ParquetPageError("Parquet source payload exceeds the decoded-byte limit")
    body_start_offset = page_offset + page_header.header_size
    if body_start_offset > chunk_end_offset:
        raise ParquetPageError("Parquet source payload has invalid page metadata")
    if page_header.compressed_page_size > chunk_end_offset - body_start_offset:
        raise ParquetPageError("Parquet source payload has invalid page metadata")
    return body_start_offset + page_header.compressed_page_size


def _record_page_totals(
    scan_state: _ChunkScanState,
    page_header: _PageHeader,
    chunk_context: _ChunkContext,
) -> None:
    """Accumulate page header-plus-body totals and require their footer bounds."""

    scan_state.page_count += 1
    if scan_state.page_count > _MAX_PAGE_COUNT:
        raise ParquetPageError("Parquet source payload has too many data pages")
    scan_state.observed_compressed_bytes += page_header.header_size + page_header.compressed_page_size
    scan_state.observed_decoded_bytes += page_header.header_size + page_header.uncompressed_page_size
    if scan_state.observed_compressed_bytes > chunk_context.expected_compressed_bytes:
        raise ParquetPageError("Parquet source payload has inconsistent page metadata")
    if scan_state.observed_decoded_bytes > chunk_context.expected_decoded_bytes:
        raise ParquetPageError("Parquet source payload has inconsistent page metadata")


def _record_page_facts(
    scan_state: _ChunkScanState,
    page_header: _PageHeader,
    chunk_context: _ChunkContext,
) -> None:
    """Record dictionary or data-page facts after its raw byte span is bounded."""

    if page_header.page_type == _PAGE_TYPE_DICTIONARY:
        _record_dictionary_page(scan_state, page_header, chunk_context)
    else:
        _record_data_page(scan_state, page_header, chunk_context)


def _record_dictionary_page(
    scan_state: _ChunkScanState,
    page_header: _PageHeader,
    chunk_context: _ChunkContext,
) -> None:
    """Require one leading dictionary and include its entry bookkeeping estimate."""

    if (
        scan_state.has_dictionary_page
        or scan_state.has_data_page
        or chunk_context.dictionary_offset is None
        or scan_state.current_offset != chunk_context.dictionary_offset
    ):
        raise ParquetPageError("Parquet source payload has invalid dictionary page metadata")
    if page_header.num_values is None:
        raise ParquetPageError("Parquet source payload has an invalid dictionary page header")
    scan_state.has_dictionary_page = True
    scan_state.dictionary_page_bytes = page_header.header_size + page_header.uncompressed_page_size
    scan_state.dictionary_entry_bytes = _dictionary_entry_working_bytes(
        chunk_context.data_type,
        page_header.num_values,
        chunk_context.maximum_decoded_bytes,
    )


def _record_data_page(
    scan_state: _ChunkScanState,
    page_header: _PageHeader,
    chunk_context: _ChunkContext,
) -> None:
    """Require valid data-page offsets, encodings, and flat value counts."""

    _require_first_data_offset(scan_state, chunk_context)
    _require_allowed_value_encoding(page_header.value_encoding, chunk_context.data_type)
    _require_dictionary_for_data_encoding(page_header.value_encoding, scan_state.has_dictionary_page)
    if page_header.num_values is None:
        raise ParquetPageError("Parquet source payload has inconsistent page metadata")
    if page_header.num_values > chunk_context.expected_value_count - scan_state.observed_value_count:
        raise ParquetPageError("Parquet source payload has inconsistent page metadata")
    scan_state.observed_value_count += page_header.num_values
    page_decoded_bytes = page_header.header_size + page_header.uncompressed_page_size
    scan_state.maximum_data_page_bytes = max(scan_state.maximum_data_page_bytes, page_decoded_bytes)
    page_output_bytes = _single_row_output_bytes(page_header, chunk_context.data_type, scan_state.dictionary_page_bytes)
    scan_state.maximum_output_page_bytes = max(scan_state.maximum_output_page_bytes, page_output_bytes)


def _require_first_data_offset(scan_state: _ChunkScanState, chunk_context: _ChunkContext) -> None:
    """Require the first actual data page to begin at its declared footer offset."""

    if not scan_state.has_data_page and scan_state.current_offset != chunk_context.first_data_offset:
        raise ParquetPageError("Parquet source payload has invalid data-page metadata")
    scan_state.has_data_page = True


def _require_allowed_value_encoding(value_encoding: int | None, data_type: pa.DataType) -> None:
    """Allow only strict-v1 source value encodings with bounded page behavior."""

    if value_encoding in _ALLOWED_VALUE_ENCODINGS:
        return
    if value_encoding == _ENCODING_RLE and pa.types.is_boolean(data_type):
        return
    raise ParquetPageError("Parquet source payload has an unsupported page encoding")


def _require_dictionary_for_data_encoding(value_encoding: int | None, has_dictionary_page: bool) -> None:
    """Require a preceding dictionary for dictionary-indexed data pages."""

    if value_encoding in (_ENCODING_PLAIN_DICTIONARY, _ENCODING_RLE_DICTIONARY) and not has_dictionary_page:
        raise ParquetPageError("Parquet source payload has invalid dictionary page metadata")


def _single_row_output_bytes(
    page_header: _PageHeader,
    data_type: pa.DataType,
    dictionary_page_bytes: int,
) -> int:
    """Estimate one emitted Arrow scalar from a page or retained dictionary."""

    page_decoded_bytes = page_header.header_size + page_header.uncompressed_page_size
    source_bytes = (
        dictionary_page_bytes
        if page_header.value_encoding in (_ENCODING_PLAIN_DICTIONARY, _ENCODING_RLE_DICTIONARY)
        else page_decoded_bytes
    )
    return _scalar_output_bytes(data_type, source_bytes)


def _require_complete_chunk_scan(
    scan_state: _ChunkScanState,
    chunk_context: _ChunkContext,
    chunk_end_offset: int,
) -> None:
    """Require exact footer totals, value counts, and dictionary offset agreement."""

    if scan_state.current_offset != chunk_end_offset:
        raise ParquetPageError("Parquet source payload has inconsistent page metadata")
    if scan_state.observed_compressed_bytes != chunk_context.expected_compressed_bytes:
        raise ParquetPageError("Parquet source payload has inconsistent page metadata")
    if scan_state.observed_decoded_bytes != chunk_context.expected_decoded_bytes:
        raise ParquetPageError("Parquet source payload has inconsistent page metadata")
    if scan_state.observed_value_count != chunk_context.expected_value_count:
        raise ParquetPageError("Parquet source payload has inconsistent page metadata")
    if scan_state.has_dictionary_page != (chunk_context.dictionary_offset is not None):
        raise ParquetPageError("Parquet source payload has inconsistent page metadata")


def _estimate_chunk_working_bytes(scan_state: _ChunkScanState) -> int:
    """Return the bounded page, dictionary, row, and Arrow-bookkeeping estimate."""

    return (
        scan_state.dictionary_page_bytes
        + scan_state.dictionary_entry_bytes
        + scan_state.maximum_data_page_bytes
        + scan_state.maximum_output_page_bytes
        + _SINGLE_ROW_ARROW_OVERHEAD_BYTES
    )


def _dictionary_entry_working_bytes(
    data_type: pa.DataType,
    number_of_values: int,
    maximum_decoded_bytes: int,
) -> int:
    """Bound dictionary entry bookkeeping implied by a raw dictionary count."""

    entry_bytes = max(_DICTIONARY_ENTRY_OVERHEAD_BYTES, _scalar_output_bytes(data_type, 0))
    if number_of_values > maximum_decoded_bytes // entry_bytes:
        raise ParquetPageError("Parquet source payload exceeds the decoded-byte limit")
    return number_of_values * entry_bytes


def _scalar_output_bytes(data_type: pa.DataType, source_bytes: int) -> int:
    """Estimate one flat scalar without relying on a multi-row output batch."""

    if pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
        return source_bytes
    if pa.types.is_decimal(data_type):
        return data_type.byte_width
    if pa.types.is_integer(data_type):
        return data_type.bit_width // 8
    if pa.types.is_boolean(data_type):
        return 1
    if pa.types.is_null(data_type):
        return 0
    return source_bytes


def _chunk_start_offset(column_metadata: Any) -> int:
    """Choose dictionary or data offset without using deprecated file_offset metadata."""

    dictionary_offset = _optional_nonnegative_integer(column_metadata.dictionary_page_offset)
    if dictionary_offset is not None:
        return dictionary_offset
    return _nonnegative_integer(column_metadata.data_page_offset)


def _optional_nonnegative_integer(candidate: object) -> int | None:
    """Accept a missing optional integer or one nonnegative integer."""

    if candidate is None:
        return None
    return _nonnegative_integer(candidate)


def _nonnegative_integer(candidate: object) -> int:
    """Reject booleans, non-integers, and negative footer properties."""

    if isinstance(candidate, bool) or not isinstance(candidate, int) or candidate < 0:
        raise ParquetPageError("Parquet source payload has invalid metadata")
    return candidate
