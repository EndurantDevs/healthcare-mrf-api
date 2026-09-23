# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Immutable persisted source bindings for Snowflake bundle operators."""

from __future__ import annotations

import hashlib
import hmac
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models.custom_import import (
    CustomImportDefinitionRevision,
    CustomImportSchemaRevision,
    CustomImportSourceBindingRevision,
)
from process.custom_import.definition import (
    MAX_REVISION_NUMBER,
    CustomImportDefinition,
    DefinitionError,
    SourceStream,
    canonical_json,
    load_json_definition,
)
from process.custom_import.definition_store import RegisteredDefinition, register_definition
from process.custom_import.read_identity import verified_definition
from process.custom_import.snowflake import (
    MAX_APPROVED_RELATIONS,
    MAX_SELECTED_COLUMNS,
    SnowflakeApprovedRelation,
    SnowflakeConnectorError,
    SnowflakeDeclaredColumn,
    SnowflakeRelation,
)
from process.custom_import.snowflake_bundle import (
    SnowflakeBundleBinding,
    SnowflakeBundleError,
    SnowflakeBundleRequest,
)

__all__ = (
    "LoadedSnowflakeSourceBinding",
    "SNOWFLAKE_SOURCE_BINDING_CONNECTOR",
    "SOURCE_BINDING_CONTRACT",
    "SnowflakeSourceBinding",
    "SnowflakeSourceBindingError",
    "SnowflakeSourceBindingReceipt",
    "SnowflakeSourceBindingUnavailableError",
    "load_snowflake_source_binding",
    "register_snowflake_source_binding",
)


SOURCE_BINDING_CONTRACT = "custom-import/source-binding/v1"
SNOWFLAKE_SOURCE_BINDING_CONNECTOR = "snowflake_bundle"
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]{0,254}$", flags=re.ASCII)
_FIELD_ID = re.compile(r"^[a-z][a-z0-9_]{0,62}$", flags=re.ASCII)
_SHA256 = re.compile(r"^[0-9a-f]{64}$", flags=re.ASCII)
_SOURCE_OBJECT_VERSION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$", flags=re.ASCII)


class SnowflakeSourceBindingError(ValueError):
    """A source binding is incomplete, unsafe, or mismatched."""


class SnowflakeSourceBindingUnavailableError(SnowflakeSourceBindingError):
    """Persisted source-binding evidence cannot be used for an operator run."""


@dataclass(frozen=True)
class SnowflakeSourceBindingReceipt:
    """Stable identity for one newly persisted or exactly replayed binding."""

    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int
    source_binding_revision_id: int
    revision_number: int
    source_binding_sha256: bytes
    created: bool


def _field_id(value: object) -> str:
    if not isinstance(value, str) or _FIELD_ID.fullmatch(value) is None:
        raise SnowflakeSourceBindingError("source binding field identifiers are invalid")
    return value


def _identifier(value: object) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise SnowflakeSourceBindingError("source binding Snowflake identifiers are invalid")
    return value.upper()


def _sha256(value: object) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise SnowflakeSourceBindingError("source binding digests are invalid")
    return value


def _source_object_version(value: object) -> str:
    if not isinstance(value, str) or _SOURCE_OBJECT_VERSION.fullmatch(value) is None:
        raise SnowflakeSourceBindingError("source binding source-object version is invalid")
    return value


def _positive_id(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 < value < 2**63:
        raise SnowflakeSourceBindingUnavailableError("source binding identifiers are invalid")
    return value


def _exact_mapping(value: object, keys: frozenset[str]) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != keys:
        raise SnowflakeSourceBindingError("source binding object shape is invalid")
    return value


def _relation(value: object) -> SnowflakeRelation:
    if not isinstance(value, list) or len(value) != 3:
        raise SnowflakeSourceBindingError("source binding relation is invalid")
    try:
        return SnowflakeRelation(database=value[0], schema=value[1], name=value[2])
    except (SnowflakeConnectorError, TypeError) as exc:
        raise SnowflakeSourceBindingError("source binding relation is invalid") from exc


def _declared_column(value: object) -> SnowflakeDeclaredColumn:
    column = _exact_mapping(value, frozenset({"field_id", "column_identifier"}))
    try:
        return SnowflakeDeclaredColumn(
            field_id=_field_id(column["field_id"]),
            column_identifier=_identifier(column["column_identifier"]),
        )
    except (SnowflakeConnectorError, TypeError) as exc:
        raise SnowflakeSourceBindingError("source binding column is invalid") from exc


@dataclass(frozen=True)
class _SourceObject:
    fingerprint_sha256: str
    version: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "fingerprint_sha256", _sha256(self.fingerprint_sha256))
        object.__setattr__(self, "version", _source_object_version(self.version))


@dataclass(frozen=True)
class _SnapshotBinding:
    relation: SnowflakeRelation
    selector: str
    column_identifier: str

    def __post_init__(self) -> None:
        if not isinstance(self.relation, SnowflakeRelation):
            raise SnowflakeSourceBindingError("source binding snapshot relation is invalid")
        selector = _field_id(self.selector)
        try:
            column = SnowflakeDeclaredColumn(
                field_id=selector,
                column_identifier=_identifier(self.column_identifier),
            )
        except (SnowflakeConnectorError, TypeError) as exc:
            raise SnowflakeSourceBindingError("source binding snapshot column is invalid") from exc
        object.__setattr__(self, "selector", selector)
        object.__setattr__(self, "column_identifier", column.column_identifier)


@dataclass(frozen=True)
class _StreamBinding:
    stream_id: str
    relation: SnowflakeRelation
    snapshot: _SnapshotBinding
    columns: tuple[SnowflakeDeclaredColumn, ...]

    def __post_init__(self) -> None:
        stream_id = _field_id(self.stream_id)
        if not isinstance(self.relation, SnowflakeRelation):
            raise SnowflakeSourceBindingError("source binding stream relation is invalid")
        if not isinstance(self.snapshot, _SnapshotBinding):
            raise SnowflakeSourceBindingError("source binding stream snapshot is invalid")
        if (
            not isinstance(self.columns, tuple)
            or not 1 <= len(self.columns) <= MAX_SELECTED_COLUMNS
            or not all(isinstance(column, SnowflakeDeclaredColumn) for column in self.columns)
        ):
            raise SnowflakeSourceBindingError("source binding stream columns are invalid")
        field_ids = tuple(column.field_id for column in self.columns)
        column_identifiers = tuple(column.column_identifier for column in self.columns)
        if len(field_ids) != len(set(field_ids)) or len(column_identifiers) != len(set(column_identifiers)):
            raise SnowflakeSourceBindingError("source binding stream columns are not unique")
        object.__setattr__(self, "stream_id", stream_id)


def _stream_binding(value: object) -> _StreamBinding:
    stream = _exact_mapping(
        value,
        frozenset(
            {
                "columns",
                "relation",
                "semantic_token_metadata_key",
                "source_snapshot_token_column_identifier",
                "source_snapshot_token_relation",
                "stream_id",
            }
        ),
    )
    raw_columns = stream["columns"]
    if not isinstance(raw_columns, list):
        raise SnowflakeSourceBindingError("source binding stream columns are invalid")
    return _StreamBinding(
        stream_id=_field_id(stream["stream_id"]),
        relation=_relation(stream["relation"]),
        snapshot=_SnapshotBinding(
            relation=_relation(stream["source_snapshot_token_relation"]),
            selector=stream["semantic_token_metadata_key"],
            column_identifier=stream["source_snapshot_token_column_identifier"],
        ),
        columns=tuple(_declared_column(column) for column in raw_columns),
    )


@dataclass(frozen=True)
class SnowflakeSourceBinding:
    """One immutable connector configuration without credentials or executable input."""

    definition_sha256: str
    schema_sha256: str
    source_object: _SourceObject
    role: str
    warehouse: str
    streams: tuple[_StreamBinding, ...]
    canonical: str = field(init=False)
    digest: str = field(init=False)

    def __post_init__(self) -> None:
        definition_sha256 = _sha256(self.definition_sha256)
        schema_sha256 = _sha256(self.schema_sha256)
        if not isinstance(self.source_object, _SourceObject):
            raise SnowflakeSourceBindingError("source binding source object is invalid")
        role = _identifier(self.role)
        warehouse = _identifier(self.warehouse)
        if (
            not isinstance(self.streams, tuple)
            or not 1 <= len(self.streams) <= MAX_APPROVED_RELATIONS
            or not all(isinstance(stream, _StreamBinding) for stream in self.streams)
        ):
            raise SnowflakeSourceBindingError("source binding streams are invalid")
        stream_ids = tuple(stream.stream_id for stream in self.streams)
        if len(stream_ids) != len(set(stream_ids)):
            raise SnowflakeSourceBindingError("source binding stream identifiers are not unique")
        object.__setattr__(self, "definition_sha256", definition_sha256)
        object.__setattr__(self, "schema_sha256", schema_sha256)
        object.__setattr__(self, "role", role)
        object.__setattr__(self, "warehouse", warehouse)
        canonical = canonical_json(self._document())
        object.__setattr__(self, "canonical", canonical)
        object.__setattr__(
            self,
            "digest",
            hashlib.sha256(f"{SOURCE_BINDING_CONTRACT}:".encode("ascii") + canonical.encode("utf-8")).hexdigest(),
        )

    @classmethod
    def from_json(cls, serialized: str | bytes) -> SnowflakeSourceBinding:
        """Parse one canonical source-binding document."""

        try:
            return cls.from_mapping(load_json_definition(serialized))
        except DefinitionError as exc:
            raise SnowflakeSourceBindingError("source binding JSON is invalid") from exc

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any]) -> SnowflakeSourceBinding:
        """Validate one decoded source-binding document."""
        document = _exact_mapping(
            mapping,
            frozenset(
                {
                    "connector",
                    "contract",
                    "definition_sha256",
                    "role",
                    "schema_sha256",
                    "source_object",
                    "streams",
                    "warehouse",
                }
            ),
        )
        if (
            document["contract"] != SOURCE_BINDING_CONTRACT
            or document["connector"] != SNOWFLAKE_SOURCE_BINDING_CONNECTOR
        ):
            raise SnowflakeSourceBindingError("source binding contract is unsupported")
        source_object = _exact_mapping(document["source_object"], frozenset({"fingerprint_sha256", "version"}))
        raw_streams = document["streams"]
        if not isinstance(raw_streams, list):
            raise SnowflakeSourceBindingError("source binding streams are invalid")
        return cls(
            definition_sha256=_sha256(document["definition_sha256"]),
            schema_sha256=_sha256(document["schema_sha256"]),
            source_object=_SourceObject(
                fingerprint_sha256=_sha256(source_object["fingerprint_sha256"]),
                version=_source_object_version(source_object["version"]),
            ),
            role=_identifier(document["role"]),
            warehouse=_identifier(document["warehouse"]),
            streams=tuple(_stream_binding(raw_stream) for raw_stream in raw_streams),
        )

    def _document(self) -> dict[str, object]:
        return {
            "connector": SNOWFLAKE_SOURCE_BINDING_CONNECTOR,
            "contract": SOURCE_BINDING_CONTRACT,
            "definition_sha256": self.definition_sha256,
            "role": self.role,
            "schema_sha256": self.schema_sha256,
            "source_object": {
                "fingerprint_sha256": self.source_object.fingerprint_sha256,
                "version": self.source_object.version,
            },
            "streams": [
                {
                    "columns": [
                        {"column_identifier": column.column_identifier, "field_id": column.field_id}
                        for column in stream.columns
                    ],
                    "relation": list(stream.relation.parts),
                    "semantic_token_metadata_key": stream.snapshot.selector,
                    "source_snapshot_token_column_identifier": stream.snapshot.column_identifier,
                    "source_snapshot_token_relation": list(stream.snapshot.relation.parts),
                    "stream_id": stream.stream_id,
                }
                for stream in self.streams
            ],
            "warehouse": self.warehouse,
        }

    def bundle_components(
        self,
        definition: CustomImportDefinition,
    ) -> tuple[tuple[SnowflakeApprovedRelation, ...], tuple[SnowflakeBundleBinding, ...]]:
        """Derive the fixed bundle allowlist from one validated definition."""

        binding_by_stream = self._validated_stream_bindings(definition)
        columns_by_relation: dict[tuple[str, str, str], dict[str, SnowflakeDeclaredColumn]] = {}
        relation_by_key: dict[tuple[str, str, str], SnowflakeRelation] = {}
        bundle_bindings = tuple(
            self._stream_bundle_binding(
                definition,
                source_stream,
                binding_by_stream,
                columns_by_relation,
                relation_by_key,
            )
            for source_stream in definition.source_streams
        )
        try:
            SnowflakeBundleRequest(definition=definition, bindings=bundle_bindings)
            approved_relations = tuple(
                SnowflakeApprovedRelation(
                    relation=relation_by_key[key],
                    columns=tuple(sorted(columns_by_relation[key].values(), key=lambda column: column.field_id)),
                )
                for key in sorted(relation_by_key)
            )
        except (SnowflakeBundleError, SnowflakeConnectorError, TypeError, ValueError) as exc:
            raise SnowflakeSourceBindingError("source binding cannot form a fixed bundle") from exc
        return approved_relations, bundle_bindings

    def _validated_stream_bindings(self, definition: CustomImportDefinition) -> dict[str, _StreamBinding]:
        if not isinstance(definition, CustomImportDefinition):
            raise SnowflakeSourceBindingError("source binding definition is invalid")
        if self.definition_sha256 != definition.digest or self.schema_sha256 != definition.schema_digest:
            raise SnowflakeSourceBindingError("source binding definition identity does not match")
        binding_by_stream = {stream.stream_id: stream for stream in self.streams}
        if set(binding_by_stream) != {stream.stream_id for stream in definition.source_streams}:
            raise SnowflakeSourceBindingError("source binding stream coverage does not match the definition")
        for source_stream in definition.source_streams:
            snapshot = binding_by_stream[source_stream.stream_id].snapshot
            if snapshot.selector in definition.fields_by_id:
                raise SnowflakeSourceBindingError("source binding snapshot selector collides with a field")
            if snapshot.selector != source_stream.snapshot_token:
                raise SnowflakeSourceBindingError("source binding snapshot selector does not match the declared stream")
        return binding_by_stream

    def _stream_bundle_binding(
        self,
        definition: CustomImportDefinition,
        source_stream: SourceStream,
        binding_by_stream: Mapping[str, _StreamBinding],
        columns_by_relation: dict[tuple[str, str, str], dict[str, SnowflakeDeclaredColumn]],
        relation_by_key: dict[tuple[str, str, str], SnowflakeRelation],
    ) -> SnowflakeBundleBinding:
        if (
            source_stream.format != "parquet"
            or source_stream.compression != "none"
            or source_stream.record_path is not None
        ):
            raise SnowflakeSourceBindingError("source binding requires fixed Parquet stream shapes")
        stream_binding = binding_by_stream[source_stream.stream_id]
        expected_fields = tuple(
            field for field in definition.fields if field.collection == source_stream.child_collection
        )
        columns_by_field = {column.field_id: column for column in stream_binding.columns}
        if set(columns_by_field) != {field.field_id for field in expected_fields}:
            raise SnowflakeSourceBindingError("source binding field coverage does not match the stream")
        for alias in definition.aliases:
            if alias.stream_id == source_stream.stream_id and (
                columns_by_field[alias.field_id].column_identifier != alias.source_label
            ):
                raise SnowflakeSourceBindingError("source binding aliases do not match selected columns")
        for column in stream_binding.columns:
            _register_approved_column(columns_by_relation, relation_by_key, stream_binding.relation, column)
        _register_approved_column(
            columns_by_relation,
            relation_by_key,
            stream_binding.snapshot.relation,
            SnowflakeDeclaredColumn(
                field_id=stream_binding.snapshot.selector,
                column_identifier=stream_binding.snapshot.column_identifier,
            ),
        )
        return SnowflakeBundleBinding(
            stream_id=source_stream.stream_id,
            relation=stream_binding.relation,
            source_snapshot_token_relation=stream_binding.snapshot.relation,
            selected_field_ids=tuple(field.field_id for field in expected_fields),
            semantic_token_metadata_key=stream_binding.snapshot.selector,
        )


def _register_approved_column(
    columns_by_relation: dict[tuple[str, str, str], dict[str, SnowflakeDeclaredColumn]],
    relation_by_key: dict[tuple[str, str, str], SnowflakeRelation],
    relation: SnowflakeRelation,
    column: SnowflakeDeclaredColumn,
) -> None:
    key = relation.parts
    relation_by_key[key] = relation
    columns_by_field = columns_by_relation.setdefault(key, {})
    existing = columns_by_field.get(column.field_id)
    if existing is not None and existing != column:
        raise SnowflakeSourceBindingError("source binding field mapping is inconsistent")
    if any(
        existing_column.column_identifier == column.column_identifier and existing_column.field_id != column.field_id
        for existing_column in columns_by_field.values()
    ):
        raise SnowflakeSourceBindingError("source binding physical column mapping is inconsistent")
    columns_by_field[column.field_id] = column


@dataclass(frozen=True)
class LoadedSnowflakeSourceBinding:
    """Exact persisted binding and derived allowlist ready for operator composition."""

    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int
    source_binding_revision_id: int
    source_binding_sha256: bytes
    definition: CustomImportDefinition
    binding: SnowflakeSourceBinding
    approved_relations: tuple[SnowflakeApprovedRelation, ...]
    bundle_bindings: tuple[SnowflakeBundleBinding, ...]


def _has_matching_digest(value: object, expected: bytes) -> bool:
    try:
        return hmac.compare_digest(bytes(value), expected)
    except TypeError, ValueError:
        return False


def _loaded_snowflake_source_binding(
    binding_row: CustomImportSourceBindingRevision,
    definition_row: CustomImportDefinitionRevision,
    schema_row: CustomImportSchemaRevision,
) -> LoadedSnowflakeSourceBinding:
    """Fail closed unless canonical persisted rows form one exact source capability."""

    try:
        dataset_id = _positive_id(binding_row.dataset_id)
        definition_revision_id = _positive_id(binding_row.definition_revision_id)
        schema_revision_id = _positive_id(binding_row.schema_revision_id)
        source_binding_revision_id = _positive_id(binding_row.source_binding_revision_id)
        _positive_id(binding_row.revision_number)
        if (
            binding_row.binding_contract != SOURCE_BINDING_CONTRACT
            or binding_row.connector_kind != SNOWFLAKE_SOURCE_BINDING_CONNECTOR
            or definition_row.dataset_id != dataset_id
            or definition_row.definition_revision_id != definition_revision_id
            or definition_row.schema_revision_id != schema_revision_id
            or schema_row.dataset_id != dataset_id
            or schema_row.schema_revision_id != schema_revision_id
        ):
            raise SnowflakeSourceBindingUnavailableError("source binding row identity is invalid")
        definition = verified_definition(definition_row, schema_row)
        binding = SnowflakeSourceBinding.from_json(binding_row.canonical_binding)
        binding_sha256 = bytes.fromhex(binding.digest)
        if (
            binding_row.canonical_binding != binding.canonical
            or not _has_matching_digest(binding_row.binding_sha256, binding_sha256)
            or not _has_matching_digest(binding_row.definition_sha256, bytes.fromhex(binding.definition_sha256))
            or not _has_matching_digest(binding_row.schema_sha256, bytes.fromhex(binding.schema_sha256))
            or not _has_matching_digest(
                binding_row.source_object_fingerprint_sha256,
                bytes.fromhex(binding.source_object.fingerprint_sha256),
            )
            or binding_row.source_object_version != binding.source_object.version
            or binding.definition_sha256 != definition.digest
            or binding.schema_sha256 != definition.schema_digest
        ):
            raise SnowflakeSourceBindingUnavailableError("source binding persisted identity is invalid")
        approved_relations, bundle_bindings = binding.bundle_components(definition)
    except (AttributeError, TypeError, ValueError, SnowflakeConnectorError, SnowflakeBundleError) as exc:
        if isinstance(exc, SnowflakeSourceBindingUnavailableError):
            raise
        raise SnowflakeSourceBindingUnavailableError("source binding persisted identity is invalid") from exc
    return LoadedSnowflakeSourceBinding(
        dataset_id=dataset_id,
        definition_revision_id=definition_revision_id,
        schema_revision_id=schema_revision_id,
        source_binding_revision_id=source_binding_revision_id,
        source_binding_sha256=binding_sha256,
        definition=definition,
        binding=binding,
        approved_relations=approved_relations,
        bundle_bindings=bundle_bindings,
    )


async def load_snowflake_source_binding(
    session: AsyncSession,
    *,
    definition_revision_id: int,
    source_binding_revision_id: int,
) -> LoadedSnowflakeSourceBinding:
    """Load one immutable binding without accepting a source-controlled selector."""

    definition_revision_id = _positive_id(definition_revision_id)
    source_binding_revision_id = _positive_id(source_binding_revision_id)
    statement = (
        select(
            CustomImportSourceBindingRevision,
            CustomImportDefinitionRevision,
            CustomImportSchemaRevision,
        )
        .join(
            CustomImportDefinitionRevision,
            and_(
                CustomImportDefinitionRevision.definition_revision_id
                == CustomImportSourceBindingRevision.definition_revision_id,
                CustomImportDefinitionRevision.dataset_id == CustomImportSourceBindingRevision.dataset_id,
                CustomImportDefinitionRevision.schema_revision_id
                == CustomImportSourceBindingRevision.schema_revision_id,
            ),
        )
        .join(
            CustomImportSchemaRevision,
            and_(
                CustomImportSchemaRevision.schema_revision_id == CustomImportSourceBindingRevision.schema_revision_id,
                CustomImportSchemaRevision.dataset_id == CustomImportSourceBindingRevision.dataset_id,
            ),
        )
        .where(CustomImportSourceBindingRevision.definition_revision_id == definition_revision_id)
        .where(CustomImportSourceBindingRevision.source_binding_revision_id == source_binding_revision_id)
    )
    query_result = await session.execute(statement)
    binding_rows = tuple(query_result.all())
    if len(binding_rows) != 1:
        raise SnowflakeSourceBindingUnavailableError("source binding is unavailable")
    binding_row, definition_row, schema_row = binding_rows[0]
    return _loaded_snowflake_source_binding(binding_row, definition_row, schema_row)


def _validated_registration_binding(
    definition: object,
    binding: object,
) -> tuple[CustomImportDefinition, SnowflakeSourceBinding]:
    """Canonicalize the declarative inputs before definition persistence begins."""

    if not isinstance(definition, CustomImportDefinition) or not isinstance(binding, SnowflakeSourceBinding):
        raise SnowflakeSourceBindingError("source binding registration inputs are invalid")
    try:
        canonical_binding = SnowflakeSourceBinding.from_json(binding.canonical)
    except (AttributeError, TypeError, ValueError) as exc:
        raise SnowflakeSourceBindingError("source binding registration inputs are invalid") from exc
    if canonical_binding != binding:
        raise SnowflakeSourceBindingError("source binding registration inputs are not canonical")
    try:
        canonical_binding.bundle_components(definition)
    except SnowflakeSourceBindingError:
        raise
    except (AttributeError, TypeError, ValueError) as exc:
        raise SnowflakeSourceBindingError("source binding registration inputs are invalid") from exc
    return definition, canonical_binding


async def _locked_source_binding_revisions(
    session: AsyncSession,
    definition_registration: RegisteredDefinition,
) -> tuple[CustomImportSourceBindingRevision, ...]:
    """Lock every binding revision after definition registration holds the dataset."""

    result = await session.execute(
        select(CustomImportSourceBindingRevision)
        .where(
            CustomImportSourceBindingRevision.definition_revision_id == definition_registration.definition_revision_id
        )
        .order_by(CustomImportSourceBindingRevision.revision_number)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    binding_revisions = tuple(result.scalars().all())
    for binding_revision in binding_revisions:
        if (
            binding_revision.dataset_id != definition_registration.dataset_id
            or binding_revision.definition_revision_id != definition_registration.definition_revision_id
            or binding_revision.schema_revision_id != definition_registration.schema_revision_id
            or isinstance(binding_revision.revision_number, bool)
            or not isinstance(binding_revision.revision_number, int)
            or not 0 < binding_revision.revision_number <= MAX_REVISION_NUMBER
        ):
            raise SnowflakeSourceBindingUnavailableError("source binding revision state is invalid")
    return binding_revisions


def _matching_binding_revision(
    binding_revisions: tuple[CustomImportSourceBindingRevision, ...],
    binding: SnowflakeSourceBinding,
) -> CustomImportSourceBindingRevision | None:
    """Return the one stored digest match, leaving exact validation to readback."""

    binding_sha256 = bytes.fromhex(binding.digest)
    matching_revisions = tuple(
        binding_revision
        for binding_revision in binding_revisions
        if _has_matching_digest(binding_revision.binding_sha256, binding_sha256)
    )
    if len(matching_revisions) > 1:
        raise SnowflakeSourceBindingUnavailableError("source binding identity is ambiguous")
    if not matching_revisions:
        if any(binding_revision.canonical_binding == binding.canonical for binding_revision in binding_revisions):
            raise SnowflakeSourceBindingUnavailableError("source binding canonical state is invalid")
        return None
    return matching_revisions[0]


def _next_binding_revision(binding_revisions: tuple[CustomImportSourceBindingRevision, ...]) -> int:
    """Allocate the next per-definition revision from already locked rows."""

    revision_number = max((binding_revision.revision_number for binding_revision in binding_revisions), default=0) + 1
    if revision_number > MAX_REVISION_NUMBER:
        raise SnowflakeSourceBindingUnavailableError("source binding revision limit is exhausted")
    return revision_number


async def _readback_receipt(
    session: AsyncSession,
    *,
    definition: CustomImportDefinition,
    binding: SnowflakeSourceBinding,
    definition_registration: RegisteredDefinition,
    binding_revision: CustomImportSourceBindingRevision,
    created: bool,
) -> SnowflakeSourceBindingReceipt:
    """Read the stored row through the normal fail-closed loader before receipt."""

    source_binding_revision_id = _positive_id(binding_revision.source_binding_revision_id)
    loaded = await load_snowflake_source_binding(
        session,
        definition_revision_id=definition_registration.definition_revision_id,
        source_binding_revision_id=source_binding_revision_id,
    )
    binding_sha256 = bytes.fromhex(binding.digest)
    if (
        loaded.dataset_id != definition_registration.dataset_id
        or loaded.definition_revision_id != definition_registration.definition_revision_id
        or loaded.schema_revision_id != definition_registration.schema_revision_id
        or loaded.source_binding_revision_id != source_binding_revision_id
        or not _has_matching_digest(loaded.source_binding_sha256, binding_sha256)
        or loaded.definition != definition
        or loaded.binding != binding
    ):
        raise SnowflakeSourceBindingUnavailableError("source binding readback does not match registration")
    return SnowflakeSourceBindingReceipt(
        dataset_id=loaded.dataset_id,
        definition_revision_id=loaded.definition_revision_id,
        schema_revision_id=loaded.schema_revision_id,
        source_binding_revision_id=source_binding_revision_id,
        revision_number=binding_revision.revision_number,
        source_binding_sha256=binding_sha256,
        created=created,
    )


async def register_snowflake_source_binding(
    session: AsyncSession,
    *,
    dataset_key: str,
    definition: CustomImportDefinition,
    binding: SnowflakeSourceBinding,
) -> SnowflakeSourceBindingReceipt:
    """Persist one bounded binding revision or return its exact immutable replay.

    The caller owns the active transaction.  Definition registration takes the
    dataset lock before this function locks existing binding revisions, so a
    concurrent binding append cannot choose the same revision number.
    """

    canonical_definition, canonical_binding = _validated_registration_binding(definition, binding)
    definition_registration = await register_definition(session, dataset_key, canonical_definition)
    binding_revisions = await _locked_source_binding_revisions(session, definition_registration)
    binding_revision = _matching_binding_revision(binding_revisions, canonical_binding)
    is_created = binding_revision is None
    if binding_revision is None:
        binding_revision = CustomImportSourceBindingRevision(
            dataset_id=definition_registration.dataset_id,
            definition_revision_id=definition_registration.definition_revision_id,
            schema_revision_id=definition_registration.schema_revision_id,
            revision_number=_next_binding_revision(binding_revisions),
            binding_contract=SOURCE_BINDING_CONTRACT,
            connector_kind=SNOWFLAKE_SOURCE_BINDING_CONNECTOR,
            definition_sha256=bytes.fromhex(canonical_binding.definition_sha256),
            schema_sha256=bytes.fromhex(canonical_binding.schema_sha256),
            source_object_fingerprint_sha256=bytes.fromhex(canonical_binding.source_object.fingerprint_sha256),
            source_object_version=canonical_binding.source_object.version,
            canonical_binding=canonical_binding.canonical,
            binding_sha256=bytes.fromhex(canonical_binding.digest),
        )
        session.add(binding_revision)
        await session.flush()
    return await _readback_receipt(
        session,
        definition=canonical_definition,
        binding=canonical_binding,
        definition_registration=definition_registration,
        binding_revision=binding_revision,
        created=is_created,
    )
