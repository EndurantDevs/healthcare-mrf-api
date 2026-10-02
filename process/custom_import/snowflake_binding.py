# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure immutable source bindings for Snowflake bundle statements."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from process.custom_import.definition import (
    CustomImportDefinition,
    DefinitionError,
    SourceStream,
    canonical_json,
    load_json_definition,
)
from process.custom_import.processing_policy import ProcessingPolicy
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
    "SNOWFLAKE_SOURCE_BINDING_CONNECTOR",
    "SOURCE_BINDING_CONTRACT",
    "SOURCE_BINDING_CONTRACTS",
    "SOURCE_BINDING_V2_CONTRACT",
    "SnowflakeSourceBinding",
    "SnowflakeSourceBindingError",
)


SOURCE_BINDING_CONTRACT = "custom-import/source-binding/v1"
SOURCE_BINDING_V2_CONTRACT = "custom-import/source-binding/v2"
SOURCE_BINDING_CONTRACTS = frozenset({SOURCE_BINDING_CONTRACT, SOURCE_BINDING_V2_CONTRACT})
SNOWFLAKE_SOURCE_BINDING_CONNECTOR = "snowflake_bundle"
_BINDING_KEYS = frozenset(
    {"connector", "contract", "definition_sha256", "role", "schema_sha256", "source_object", "streams", "warehouse"}
)
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]{0,254}$", flags=re.ASCII)
_FIELD_ID = re.compile(r"^[a-z][a-z0-9_]{0,62}$", flags=re.ASCII)
_SHA256 = re.compile(r"^[0-9a-f]{64}$", flags=re.ASCII)
_SOURCE_OBJECT_VERSION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$", flags=re.ASCII)


class SnowflakeSourceBindingError(ValueError):
    """A source binding is incomplete, unsafe, or mismatched."""


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
    relation: SnowflakeRelation | None
    selector: str
    column_identifier: str | None

    def __post_init__(self) -> None:
        if self.relation is None:
            if self.column_identifier is not None:
                raise SnowflakeSourceBindingError("source binding snapshot column is invalid")
        elif not isinstance(self.relation, SnowflakeRelation):
            raise SnowflakeSourceBindingError("source binding snapshot relation is invalid")
        selector = _field_id(self.selector)
        if self.relation is None:
            column_identifier = None
        else:
            try:
                column = SnowflakeDeclaredColumn(
                    field_id=selector,
                    column_identifier=_identifier(self.column_identifier),
                )
            except (SnowflakeConnectorError, TypeError) as exc:
                raise SnowflakeSourceBindingError("source binding snapshot column is invalid") from exc
            column_identifier = column.column_identifier
        object.__setattr__(self, "selector", selector)
        object.__setattr__(self, "column_identifier", column_identifier)


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
        if self.snapshot.relation == self.relation:
            column_identifiers += (self.snapshot.column_identifier,)
        if len(field_ids) != len(set(field_ids)) or len(column_identifiers) != len(set(column_identifiers)):
            raise SnowflakeSourceBindingError("source binding stream columns are not unique")
        object.__setattr__(self, "stream_id", stream_id)


def _stream_binding(stream_mapping: object) -> _StreamBinding:
    stream_document = _exact_mapping(
        stream_mapping,
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
    raw_columns = stream_document["columns"]
    if not isinstance(raw_columns, list):
        raise SnowflakeSourceBindingError("source binding stream columns are invalid")
    return _StreamBinding(
        stream_id=_field_id(stream_document["stream_id"]),
        relation=_relation(stream_document["relation"]),
        snapshot=_SnapshotBinding(
            relation=(
                None
                if stream_document["source_snapshot_token_relation"] is None
                else _relation(stream_document["source_snapshot_token_relation"])
            ),
            selector=stream_document["semantic_token_metadata_key"],
            column_identifier=stream_document["source_snapshot_token_column_identifier"],
        ),
        columns=tuple(_declared_column(column) for column in raw_columns),
    )


def _processing_policy(document: Mapping[str, Any]) -> ProcessingPolicy | None:
    if document["contract"] == SOURCE_BINDING_CONTRACT:
        return None
    try:
        return ProcessingPolicy.from_mapping(document["processing_policy"])
    except ValueError as exc:
        raise SnowflakeSourceBindingError("source binding processing policy is invalid") from exc


def _validated_processing_policy(policy: object) -> ProcessingPolicy | None:
    if policy is None:
        return None
    if not isinstance(policy, ProcessingPolicy):
        raise SnowflakeSourceBindingError("source binding processing policy is invalid")
    try:
        return ProcessingPolicy.from_mapping(policy.to_mapping())
    except ValueError as exc:
        raise SnowflakeSourceBindingError("source binding processing policy is invalid") from exc


@dataclass(frozen=True)
class SnowflakeSourceBinding:
    """One immutable connector configuration without credentials or executable input."""

    definition_sha256: str
    schema_sha256: str
    source_object: _SourceObject
    role: str
    warehouse: str
    streams: tuple[_StreamBinding, ...]
    processing_policy: ProcessingPolicy | None = None
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
        object.__setattr__(self, "processing_policy", _validated_processing_policy(self.processing_policy))
        canonical = canonical_json(self._document())
        object.__setattr__(self, "canonical", canonical)
        object.__setattr__(
            self,
            "digest",
            hashlib.sha256(f"{self.contract}:".encode("ascii") + canonical.encode("utf-8")).hexdigest(),
        )

    @property
    def contract(self) -> str:
        """Select the version from the validated retained declaration."""

        return SOURCE_BINDING_CONTRACT if self.processing_policy is None else SOURCE_BINDING_V2_CONTRACT

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

        if not isinstance(mapping, Mapping):
            raise SnowflakeSourceBindingError("source binding object shape is invalid")
        contract = mapping.get("contract")
        keys = _BINDING_KEYS | {"processing_policy"} if contract == SOURCE_BINDING_V2_CONTRACT else _BINDING_KEYS
        document = _exact_mapping(mapping, keys)
        if (
            not isinstance(contract, str)
            or contract not in SOURCE_BINDING_CONTRACTS
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
            processing_policy=_processing_policy(document),
        )

    def _document(self) -> dict[str, object]:
        document_by_field = {
            "connector": SNOWFLAKE_SOURCE_BINDING_CONNECTOR,
            "contract": self.contract,
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
                    "source_snapshot_token_relation": (
                        None if stream.snapshot.relation is None else list(stream.snapshot.relation.parts)
                    ),
                    "stream_id": stream.stream_id,
                }
                for stream in self.streams
            ],
            "warehouse": self.warehouse,
        }
        if self.processing_policy is not None:
            document_by_field["processing_policy"] = self.processing_policy.to_mapping()
        return document_by_field

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
            SnowflakeBundleRequest(
                definition=definition, bindings=bundle_bindings, processing_policy=self.processing_policy
            )
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
        supports_query_identity_snapshot = (
            len(definition.source_streams) == 1 and definition.source_streams[0].record_kind == "root"
        )
        for source_stream in definition.source_streams:
            snapshot = binding_by_stream[source_stream.stream_id].snapshot
            if snapshot.relation is None and not supports_query_identity_snapshot:
                raise SnowflakeSourceBindingError(
                    "source binding query-identity snapshot requires exactly one root stream"
                )
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
        if stream_binding.snapshot.relation is not None:
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
    columns_by_field[column.field_id] = column
