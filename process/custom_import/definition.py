# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Strict, canonical ``custom-import/v1`` definition parsing.

Definitions are declarative data only.  They cannot carry credentials, SQL,
paths, environment-variable references, transforms, or executable selectors.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import yaml

CONTRACT_VERSION = "custom-import/v1"
MAX_DEFINITION_BYTES = 1024 * 1024
MAX_DEFINITION_DEPTH = 32
MAX_DEFINITION_NODES = 10_000
MAX_HOT_FIELDS = 20
MAX_CHILD_COLLECTIONS = 8
MAX_SELECTION_PROFILES = 4
MAX_CONTEXT_DIMENSIONS = 2
MAX_SELECTION_TERMS = 3
MAX_ORDER_TERMS = 3

_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_FIELD_TYPES = frozenset(
    {"string", "integer", "decimal", "boolean", "date", "timestamp"}
)
_FORMATS = frozenset({"csv", "tsv", "json", "ndjson", "xml", "parquet"})
_COMPRESSIONS = frozenset({"none", "gzip"})
_DIRECTIONS = frozenset({"asc", "desc"})
_NULLS = frozenset({"first", "last"})
_REFRESH_MODES = frozenset({"upsert", "snapshot"})


class DefinitionError(ValueError):
    """A definition is malformed, unsafe, or exceeds the v1 surface."""


class _StrictYamlLoader(yaml.SafeLoader):
    """Safe loader which rejects duplicate keys and non-string mapping keys."""


def _construct_mapping(loader: _StrictYamlLoader, node, deep: bool = False):
    mapping_by_key: dict[str, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if not isinstance(key, str):
            raise DefinitionError("YAML object keys must be strings")
        if key in mapping_by_key:
            raise DefinitionError(f"duplicate object key: {key}")
        mapping_by_key[key] = loader.construct_object(value_node, deep=deep)
    return mapping_by_key


_StrictYamlLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_mapping,
)


def load_json_definition(serialized: str | bytes) -> Mapping[str, Any]:
    """Parse bounded JSON while rejecting duplicate object keys."""

    text = _bounded_text(serialized, "JSON")
    try:
        value = json.loads(text, object_pairs_hook=_json_object)
    except json.JSONDecodeError as exc:
        raise DefinitionError(f"invalid JSON definition: {exc.msg}") from exc
    return _validate_wire_value(value)


def load_yaml_definition(serialized: str | bytes) -> Mapping[str, Any]:
    """Parse bounded YAML without aliases, duplicate keys, or non-JSON types."""

    text = _bounded_text(serialized, "YAML")
    try:
        for event in yaml.parse(text, Loader=_StrictYamlLoader):
            if isinstance(event, yaml.events.AliasEvent):
                raise DefinitionError("YAML aliases are not allowed")
        value = yaml.load(text, Loader=_StrictYamlLoader)
    except DefinitionError:
        raise
    except yaml.YAMLError as exc:
        raise DefinitionError("invalid YAML definition") from exc
    return _validate_wire_value(value)


def canonical_json(value: Mapping[str, Any]) -> str:
    """Return a domain-independent canonical JSON representation."""

    checked = _validate_wire_value(value)
    return json.dumps(
        checked,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def canonical_sha256(value: Mapping[str, Any], *, domain: str = "definition") -> str:
    """Hash canonical JSON with a v1 domain separator."""

    if domain not in {"definition", "schema", "profile", "payload", "event"}:
        raise ValueError("unsupported custom-import digest domain")
    data = canonical_json(value).encode("utf-8")
    prefix = f"{CONTRACT_VERSION}\x00{domain}\x00".encode("ascii")
    return hashlib.sha256(prefix + data).hexdigest()


def _bounded_text(value: str | bytes, label: str) -> str:
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise DefinitionError(f"{label} definition must be UTF-8") from exc
    if not isinstance(value, str):
        raise DefinitionError(f"{label} definition must be text")
    if len(value.encode("utf-8")) > MAX_DEFINITION_BYTES:
        raise DefinitionError("definition exceeds the v1 byte limit")
    return value


def _json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    object_by_key: dict[str, Any] = {}
    for key, value in pairs:
        if key in object_by_key:
            raise DefinitionError(f"duplicate object key: {key}")
        object_by_key[key] = value
    return object_by_key


def _validate_wire_value(
    value: Any, *, depth: int = 0, nodes: list[int] | None = None
) -> Any:
    if nodes is None:
        nodes = [0]
    nodes[0] += 1
    if depth > MAX_DEFINITION_DEPTH or nodes[0] > MAX_DEFINITION_NODES:
        raise DefinitionError("definition exceeds structural limits")
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        raise DefinitionError("definitions cannot contain floating-point values")
    if isinstance(value, list):
        return [
            _validate_wire_value(item, depth=depth + 1, nodes=nodes) for item in value
        ]
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise DefinitionError("definition object keys must be strings")
        return {
            key: _validate_wire_value(item, depth=depth + 1, nodes=nodes)
            for key, item in value.items()
        }
    raise DefinitionError("definitions must contain JSON-compatible values only")


def _mapping(value: Any, path: str, *, keys: set[str]) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise DefinitionError(f"{path} must be an object")
    unknown = set(value) - keys
    if unknown:
        raise DefinitionError(f"{path} has unknown key: {min(unknown)}")
    return value


def _array(value: Any, path: str) -> tuple[Any, ...]:
    if not isinstance(value, list):
        raise DefinitionError(f"{path} must be an array")
    return tuple(value)


def _identifier(value: Any, path: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise DefinitionError(f"{path} must be lower_snake_case")
    return value


def _integer(value: Any, path: str, *, minimum: int, maximum: int | None = None) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        raise DefinitionError(f"{path} must be an integer >= {minimum}")
    if maximum is not None and value > maximum:
        raise DefinitionError(f"{path} must be an integer <= {maximum}")
    return value


def _required(value: Mapping[str, Any], key: str, path: str) -> Any:
    if key not in value:
        raise DefinitionError(f"{path}.{key} is required")
    return value[key]


@dataclass(frozen=True)
class Field:
    """Stable dataset slot and schema-specific scalar projection capability."""

    field_id: str
    field_slot: int
    value_type: str
    nullable: bool
    projection_slot: int | None
    collection: str | None


@dataclass(frozen=True)
class KeyPart:
    """Ordered mapping from a child field to a root-key field."""

    child_field: str
    root_field: str


@dataclass(frozen=True)
class ChildCollection:
    """One v1 child collection with a complete parent and child identity."""

    name: str
    parent_key: tuple[KeyPart, ...]
    child_key: tuple[str, ...]


@dataclass(frozen=True)
class SourceStream:
    """Declarative input format shape, without a connector or secrets."""

    stream_id: str
    record_kind: str
    child_collection: str | None
    format: str
    compression: str
    snapshot_token: str


@dataclass(frozen=True)
class FieldAlias:
    """Exact source label mapped within one stream's record scope."""

    stream_id: str
    source_label: str
    field_id: str


@dataclass(frozen=True)
class SortTerm:
    """Bounded selection or result order term."""

    field_id: str
    direction: str
    nulls: str


@dataclass(frozen=True)
class QueryContract:
    """Permitted fields and ordering for one root plus optional child context."""

    root_fields: tuple[str, ...]
    child_collection: str | None
    child_fields: tuple[str, ...]
    order_terms: tuple[SortTerm, ...]


@dataclass(frozen=True)
class SelectionProfile:
    """Winner selection before threshold filtering, capped by v1 limits."""

    profile_id: str
    selection_terms: tuple[SortTerm, ...]
    context_dimensions: tuple[str, ...]


@dataclass(frozen=True)
class CustomImportDefinition:
    """Fully validated, immutable declaration shared across engine entry points."""

    definition_revision: int
    schema_revision: int
    refresh_mode: str
    root_logical_key: tuple[str, ...]
    entity_field: str
    source_streams: tuple[SourceStream, ...]
    root_fields: tuple[Field, ...]
    child_fields: tuple[Field, ...]
    child_collections: tuple[ChildCollection, ...]
    aliases: tuple[FieldAlias, ...]
    query: QueryContract
    selection_profiles: tuple[SelectionProfile, ...]
    canonical: str
    digest: str
    schema_canonical: str
    schema_digest: str

    @property
    def fields(self) -> tuple[Field, ...]:
        """Return every declared root and child field in stable slot order."""
        return (*self.root_fields, *self.child_fields)

    @property
    def fields_by_id(self) -> dict[str, Field]:
        """Return declared fields indexed by their globally stable identifier."""
        return {field.field_id: field for field in self.fields}

    @property
    def collections_by_name(self) -> dict[str, ChildCollection]:
        """Return child collection declarations indexed by name."""
        return {collection.name: collection for collection in self.child_collections}

    @classmethod
    def from_json(
        cls,
        serialized: str | bytes,
        *,
        previous: CustomImportDefinition | None = None,
    ) -> CustomImportDefinition:
        """Parse a bounded JSON definition and optionally validate its revision."""
        return cls.from_mapping(load_json_definition(serialized), previous=previous)

    @classmethod
    def from_yaml(
        cls,
        serialized: str | bytes,
        *,
        previous: CustomImportDefinition | None = None,
    ) -> CustomImportDefinition:
        """Parse a bounded YAML definition and optionally validate its revision."""
        return cls.from_mapping(load_yaml_definition(serialized), previous=previous)

    @classmethod
    def from_mapping(
        cls,
        definition_value: Mapping[str, Any],
        *,
        previous: CustomImportDefinition | None = None,
    ) -> CustomImportDefinition:
        """Parse an in-memory declaration into its immutable v1 representation."""
        definition = _parse_definition_header(definition_value)
        parsed = _parse_definition_contents(definition)
        if previous is not None:
            _validate_revision_transition(previous, parsed)
        return parsed


def _parse_definition_header(definition_value: Mapping[str, Any]) -> Mapping[str, Any]:
    definition = _mapping(
        definition_value,
        "definition",
        keys={
            "contract",
            "revision",
            "refresh_mode",
            "streams",
            "schema",
            "aliases",
            "query",
            "selection_profiles",
        },
    )
    if _required(definition, "contract", "definition") != CONTRACT_VERSION:
        raise DefinitionError(f"definition.contract must equal {CONTRACT_VERSION}")
    return definition


def _parse_definition_contents(definition: Mapping[str, Any]) -> CustomImportDefinition:
    definition_revision, schema_revision = _parse_revision_numbers(definition)
    refresh_mode = _parse_refresh_mode(definition)
    schema = _mapping(
        _required(definition, "schema", "definition"),
        "definition.schema",
        keys={"root", "children"},
    )
    root_schema, root_fields, root_logical_key, entity_field = _parse_root(
        _required(schema, "root", "definition.schema")
    )
    child_collections, child_fields = _parse_children(
        schema.get("children", []), root_fields, root_logical_key
    )
    _validate_field_identity(root_fields, child_fields)
    source_streams = _parse_streams(
        _required(definition, "streams", "definition"),
        {collection.name for collection in child_collections},
    )
    aliases = _parse_aliases(
        definition.get("aliases", {}), source_streams, root_fields, child_fields
    )
    query = _parse_query(
        definition.get("query", {}), root_fields, child_fields, child_collections
    )
    selection_profiles = _parse_profiles(
        definition.get("selection_profiles", []), query, (*root_fields, *child_fields)
    )
    schema_by_scope = {"root": root_schema, "children": schema.get("children", [])}
    return CustomImportDefinition(
        definition_revision=definition_revision,
        schema_revision=schema_revision,
        refresh_mode=refresh_mode,
        root_logical_key=root_logical_key,
        entity_field=entity_field,
        source_streams=source_streams,
        root_fields=root_fields,
        child_fields=child_fields,
        child_collections=child_collections,
        aliases=aliases,
        query=query,
        selection_profiles=selection_profiles,
        canonical=canonical_json(definition),
        digest=canonical_sha256(definition),
        schema_canonical=canonical_json(schema_by_scope),
        schema_digest=canonical_sha256(schema_by_scope, domain="schema"),
    )


def _parse_revision_numbers(definition: Mapping[str, Any]) -> tuple[int, int]:
    revision = _mapping(
        _required(definition, "revision", "definition"),
        "definition.revision",
        keys={"definition", "schema"},
    )
    definition_revision = _integer(
        _required(revision, "definition", "definition.revision"),
        "definition.revision.definition",
        minimum=1,
    )
    schema_revision = _integer(
        _required(revision, "schema", "definition.revision"),
        "definition.revision.schema",
        minimum=1,
    )
    return definition_revision, schema_revision


def _parse_refresh_mode(definition: Mapping[str, Any]) -> str:
    refresh_mode = _required(definition, "refresh_mode", "definition")
    if refresh_mode not in _REFRESH_MODES:
        raise DefinitionError("definition.refresh_mode must be upsert or snapshot")
    return refresh_mode


def _parse_root(
    root_value: Any,
) -> tuple[Mapping[str, Any], tuple[Field, ...], tuple[str, ...], str]:
    root_schema = _mapping(
        root_value, "definition.schema.root", keys={"logical_key", "entity", "fields"}
    )
    root_fields = _parse_fields(
        _required(root_schema, "fields", "definition.schema.root"),
        "definition.schema.root.fields",
    )
    root_fields_by_id = {field.field_id: field for field in root_fields}
    logical_key_field_ids = tuple(
        _identifier(field_id, "definition.schema.root.logical_key")
        for field_id in _array(
            _required(root_schema, "logical_key", "definition.schema.root"),
            "definition.schema.root.logical_key",
        )
    )
    if not logical_key_field_ids or len(logical_key_field_ids) != len(
        set(logical_key_field_ids)
    ):
        raise DefinitionError(
            "definition.schema.root.logical_key must be a non-empty unique array"
        )
    for field_id in logical_key_field_ids:
        field = root_fields_by_id.get(field_id)
        if field is None or field.nullable:
            raise DefinitionError(
                "root logical-key fields must be declared required root fields"
            )
    entity_mapping = _mapping(
        _required(root_schema, "entity", "definition.schema.root"),
        "definition.schema.root.entity",
        keys={"adapter", "field"},
    )
    if _required(entity_mapping, "adapter", "definition.schema.root.entity") != "npi":
        raise DefinitionError("definition.schema.root.entity.adapter must be npi in v1")
    entity_field = _identifier(
        _required(entity_mapping, "field", "definition.schema.root.entity"),
        "definition.schema.root.entity.field",
    )
    entity_field_definition = root_fields_by_id.get(entity_field)
    if (
        entity_field_definition is None
        or entity_field_definition.nullable
        or entity_field_definition.value_type != "string"
    ):
        raise DefinitionError("the NPI entity field must be a required root string")
    return root_schema, root_fields, logical_key_field_ids, entity_field


def _parse_children(
    child_definitions_value: Any,
    root_fields: tuple[Field, ...],
    root_key_field_ids: tuple[str, ...],
) -> tuple[tuple[ChildCollection, ...], tuple[Field, ...]]:
    child_definitions = _array(child_definitions_value, "definition.schema.children")
    if len(child_definitions) > MAX_CHILD_COLLECTIONS:
        raise DefinitionError("definition.schema.children exceeds the v1 limit")
    root_fields_by_id = {field.field_id: field for field in root_fields}
    child_collections: list[ChildCollection] = []
    child_field_definitions: list[Field] = []
    for ordinal, child_definition_value in enumerate(child_definitions):
        path = f"definition.schema.children[{ordinal}]"
        child_collection, collection_fields = _parse_child_collection(
            child_definition_value,
            path,
            root_fields_by_id,
            root_key_field_ids,
        )
        child_collections.append(child_collection)
        child_field_definitions.extend(collection_fields)
    if len({collection.name for collection in child_collections}) != len(
        child_collections
    ):
        raise DefinitionError("child collection names must be unique")
    return tuple(child_collections), tuple(child_field_definitions)


def _parse_child_collection(
    child_definition_value: Any,
    path: str,
    root_fields_by_id: Mapping[str, Field],
    root_key_field_ids: tuple[str, ...],
) -> tuple[ChildCollection, tuple[Field, ...]]:
    child_mapping = _mapping(
        child_definition_value, path, keys={"name", "parent_key", "child_key", "fields"}
    )
    collection_name = _identifier(
        _required(child_mapping, "name", path), f"{path}.name"
    )
    collection_fields = _parse_fields(
        _required(child_mapping, "fields", path),
        f"{path}.fields",
        collection=collection_name,
    )
    child_fields_by_id = {field.field_id: field for field in collection_fields}
    parent_key = _parse_parent_key(
        child_mapping,
        path,
        child_fields_by_id,
        root_fields_by_id,
        root_key_field_ids,
    )
    child_key_field_ids = _parse_child_key(child_mapping, path, child_fields_by_id)
    return (
        ChildCollection(
            name=collection_name,
            parent_key=parent_key,
            child_key=child_key_field_ids,
        ),
        collection_fields,
    )


def _parse_parent_key(
    child_mapping: Mapping[str, Any],
    path: str,
    child_fields_by_id: Mapping[str, Field],
    root_fields_by_id: Mapping[str, Field],
    root_key_field_ids: tuple[str, ...],
) -> tuple[KeyPart, ...]:
    parent_key_definitions = _array(
        _required(child_mapping, "parent_key", path), f"{path}.parent_key"
    )
    parent_key_parts: list[KeyPart] = []
    for ordinal, pair_definition_value in enumerate(parent_key_definitions):
        pair_path = f"{path}.parent_key[{ordinal}]"
        pair_mapping = _mapping(
            pair_definition_value, pair_path, keys={"child", "root"}
        )
        child_field_id = _identifier(
            _required(pair_mapping, "child", pair_path), f"{pair_path}.child"
        )
        root_field_id = _identifier(
            _required(pair_mapping, "root", pair_path), f"{pair_path}.root"
        )
        expected_root_field_id = (
            root_key_field_ids[ordinal] if ordinal < len(root_key_field_ids) else None
        )
        if root_field_id != expected_root_field_id:
            raise DefinitionError(
                "child parent_key must preserve root logical-key order"
            )
        child_field = child_fields_by_id.get(child_field_id)
        root_field = root_fields_by_id.get(root_field_id)
        if (
            child_field is None
            or root_field is None
            or child_field.nullable
            or child_field.value_type != root_field.value_type
        ):
            raise DefinitionError(
                "child parent keys must be required and type-compatible"
            )
        parent_key_parts.append(
            KeyPart(child_field=child_field_id, root_field=root_field_id)
        )
    if len(parent_key_parts) != len(root_key_field_ids):
        raise DefinitionError(
            "child parent_key must cover the complete root logical key"
        )
    return tuple(parent_key_parts)


def _parse_child_key(
    child_mapping: Mapping[str, Any], path: str, child_fields_by_id: Mapping[str, Field]
) -> tuple[str, ...]:
    child_key_field_ids = tuple(
        _identifier(field_id, f"{path}.child_key")
        for field_id in _array(
            _required(child_mapping, "child_key", path), f"{path}.child_key"
        )
    )
    if (
        not child_key_field_ids
        or len(child_key_field_ids) > 3
        or len(child_key_field_ids) != len(set(child_key_field_ids))
    ):
        raise DefinitionError("child_key must contain one to three unique fields")
    if any(
        field_id not in child_fields_by_id or child_fields_by_id[field_id].nullable
        for field_id in child_key_field_ids
    ):
        raise DefinitionError("child-key fields must be declared required child fields")
    return child_key_field_ids


def _parse_fields(
    raw: Any, path: str, *, collection: str | None = None
) -> tuple[Field, ...]:
    fields: list[Field] = []
    for ordinal, raw_field in enumerate(_array(raw, path)):
        item_path = f"{path}[{ordinal}]"
        field = _mapping(
            raw_field,
            item_path,
            keys={"id", "slot", "type", "nullable", "projection_slot"},
        )
        value_type = _required(field, "type", item_path)
        if value_type not in _FIELD_TYPES:
            raise DefinitionError(f"{item_path}.type is not a v1 scalar type")
        nullable = _required(field, "nullable", item_path)
        if not isinstance(nullable, bool):
            raise DefinitionError(f"{item_path}.nullable must be a boolean")
        projection = field.get("projection_slot")
        if projection is not None:
            projection = _integer(
                projection, f"{item_path}.projection_slot", minimum=1, maximum=20
            )
        fields.append(
            Field(
                field_id=_identifier(
                    _required(field, "id", item_path), f"{item_path}.id"
                ),
                field_slot=_integer(
                    _required(field, "slot", item_path),
                    f"{item_path}.slot",
                    minimum=1,
                    maximum=32767,
                ),
                value_type=value_type,
                nullable=nullable,
                projection_slot=projection,
                collection=collection,
            )
        )
    return tuple(fields)


def _validate_field_identity(
    root_fields: tuple[Field, ...], child_fields: tuple[Field, ...]
) -> None:
    fields = (*root_fields, *child_fields)
    if not root_fields:
        raise DefinitionError("schema.root.fields must not be empty")
    ids = [field.field_id for field in fields]
    slots = [field.field_slot for field in fields]
    projections = [
        field.projection_slot for field in fields if field.projection_slot is not None
    ]
    if len(ids) != len(set(ids)) or len(slots) != len(set(slots)):
        raise DefinitionError("field ids and stable field slots must be unique")
    if len(projections) != len(set(projections)) or len(projections) > MAX_HOT_FIELDS:
        raise DefinitionError("hot projection slots must be unique and capped at 20")


def _parse_streams(raw: Any, child_names: set[str]) -> tuple[SourceStream, ...]:
    streams: list[SourceStream] = []
    for ordinal, raw_stream in enumerate(_array(raw, "definition.streams")):
        path = f"definition.streams[{ordinal}]"
        stream = _mapping(
            raw_stream,
            path,
            keys={"id", "kind", "child", "format", "compression", "snapshot_token"},
        )
        kind = _required(stream, "kind", path)
        if kind not in {"root", "child"}:
            raise DefinitionError(f"{path}.kind must be root or child")
        child = stream.get("child")
        if kind == "root" and child is not None:
            raise DefinitionError("root streams cannot declare child")
        if kind == "child":
            child = _identifier(child, f"{path}.child")
            if child not in child_names:
                raise DefinitionError(f"{path}.child is not declared")
        format_name = _required(stream, "format", path)
        compression = _required(stream, "compression", path)
        if format_name not in _FORMATS or compression not in _COMPRESSIONS:
            raise DefinitionError(
                f"{path} declares an unsupported format or compression"
            )
        streams.append(
            SourceStream(
                stream_id=_identifier(_required(stream, "id", path), f"{path}.id"),
                record_kind=kind,
                child_collection=child,
                format=format_name,
                compression=compression,
                snapshot_token=_identifier(
                    _required(stream, "snapshot_token", path), f"{path}.snapshot_token"
                ),
            )
        )
    if not streams or len({stream.stream_id for stream in streams}) != len(streams):
        raise DefinitionError("streams must be non-empty with unique ids")
    root_streams = [stream for stream in streams if stream.record_kind == "root"]
    child_collection_names = [
        stream.child_collection for stream in streams if stream.record_kind == "child"
    ]
    if (
        len(root_streams) != 1
        or set(child_collection_names) != child_names
        or len(child_collection_names) != len(child_names)
    ):
        raise DefinitionError(
            "v1 requires exactly one root stream and one stream per child collection"
        )
    return tuple(streams)


def _parse_aliases(
    raw: Any,
    streams: tuple[SourceStream, ...],
    root_fields: tuple[Field, ...],
    child_fields: tuple[Field, ...],
) -> tuple[FieldAlias, ...]:
    aliases = _mapping(
        raw, "definition.aliases", keys={stream.stream_id for stream in streams}
    )
    root_ids = {field.field_id for field in root_fields}
    field_ids_by_child_collection = {field.collection: set() for field in child_fields}
    for field in child_fields:
        field_ids_by_child_collection[field.collection].add(field.field_id)
    parsed_aliases: list[FieldAlias] = []
    for stream in streams:
        labels = aliases.get(stream.stream_id, {})
        if not isinstance(labels, Mapping):
            raise DefinitionError(
                f"definition.aliases.{stream.stream_id} must be an object"
            )
        permitted = (
            root_ids
            if stream.record_kind == "root"
            else field_ids_by_child_collection[stream.child_collection]
        )
        for label, field_id in labels.items():
            if (
                not isinstance(label, str)
                or not label
                or len(label.encode("utf-8")) > 255
                or any(ord(char) < 32 or ord(char) == 127 for char in label)
            ):
                raise DefinitionError("source aliases must be bounded printable text")
            field_id = _identifier(
                field_id, f"definition.aliases.{stream.stream_id}.{label}"
            )
            if field_id not in permitted:
                raise DefinitionError(
                    "a source alias must target a field in its stream scope"
                )
            parsed_aliases.append(FieldAlias(stream.stream_id, label, field_id))
    return tuple(
        sorted(parsed_aliases, key=lambda alias: (alias.stream_id, alias.source_label))
    )


def _parse_query(
    raw: Any,
    root_fields: tuple[Field, ...],
    child_fields: tuple[Field, ...],
    children: tuple[ChildCollection, ...],
) -> QueryContract:
    query = _mapping(raw, "definition.query", keys={"root_fields", "child", "order"})
    root_ids = {
        field.field_id for field in root_fields if field.projection_slot is not None
    }
    child_by_collection: dict[str, set[str]] = {}
    for field in child_fields:
        if field.projection_slot is not None:
            child_by_collection.setdefault(field.collection, set()).add(field.field_id)
    root_query_fields = tuple(
        _identifier(field_id, "definition.query.root_fields")
        for field_id in _array(
            query.get("root_fields", []), "definition.query.root_fields"
        )
    )
    if len(root_query_fields) != len(set(root_query_fields)) or not set(
        root_query_fields
    ).issubset(root_ids):
        raise DefinitionError("query root fields must be unique projected root fields")
    raw_child = query.get("child")
    child_collection: str | None = None
    query_child_fields: tuple[str, ...] = ()
    if raw_child is not None:
        child = _mapping(
            raw_child, "definition.query.child", keys={"collection", "fields"}
        )
        child_collection = _identifier(
            _required(child, "collection", "definition.query.child"),
            "definition.query.child.collection",
        )
        if child_collection not in {collection.name for collection in children}:
            raise DefinitionError("query child collection is not declared")
        query_child_fields = tuple(
            _identifier(field_id, "definition.query.child.fields")
            for field_id in _array(
                _required(child, "fields", "definition.query.child"),
                "definition.query.child.fields",
            )
        )
        if len(query_child_fields) != len(set(query_child_fields)) or not set(
            query_child_fields
        ).issubset(child_by_collection.get(child_collection, set())):
            raise DefinitionError(
                "query child fields must be unique projected fields in one collection"
            )
    order = _parse_sort_terms(
        query.get("order", []), "definition.query.order", maximum=MAX_ORDER_TERMS
    )
    permitted = set(root_query_fields) | set(query_child_fields)
    if any(term.field_id not in permitted for term in order):
        raise DefinitionError("query order terms must use permitted query fields")
    return QueryContract(root_query_fields, child_collection, query_child_fields, order)


def _parse_profiles(
    raw: Any, query: QueryContract, fields: tuple[Field, ...]
) -> tuple[SelectionProfile, ...]:
    profile_definitions = _array(raw, "definition.selection_profiles")
    if len(profile_definitions) > MAX_SELECTION_PROFILES:
        raise DefinitionError("selection profile count exceeds v1 limit")
    projected_field_ids = {
        field.field_id for field in fields if field.projection_slot is not None
    }
    permitted = set(query.root_fields) | set(query.child_fields)
    profiles: list[SelectionProfile] = []
    for ordinal, raw_profile in enumerate(profile_definitions):
        path = f"definition.selection_profiles[{ordinal}]"
        profile = _mapping(
            raw_profile, path, keys={"id", "selection", "context_dimensions"}
        )
        terms = _parse_sort_terms(
            _required(profile, "selection", path),
            f"{path}.selection",
            maximum=MAX_SELECTION_TERMS,
        )
        dimensions = tuple(
            _identifier(field_id, f"{path}.context_dimensions")
            for field_id in _array(
                profile.get("context_dimensions", []), f"{path}.context_dimensions"
            )
        )
        if len(dimensions) > MAX_CONTEXT_DIMENSIONS or len(dimensions) != len(
            set(dimensions)
        ):
            raise DefinitionError(
                "profile context dimensions must contain at most two unique fields"
            )
        references = {term.field_id for term in terms} | set(dimensions)
        if not references.issubset(projected_field_ids) or not references.issubset(
            permitted
        ):
            raise DefinitionError(
                "profile fields must be projected and use the one permitted query context"
            )
        profiles.append(
            SelectionProfile(
                profile_id=_identifier(_required(profile, "id", path), f"{path}.id"),
                selection_terms=terms,
                context_dimensions=dimensions,
            )
        )
    if len({profile.profile_id for profile in profiles}) != len(profiles):
        raise DefinitionError("selection profile ids must be unique")
    return tuple(profiles)


def _parse_sort_terms(raw: Any, path: str, *, maximum: int) -> tuple[SortTerm, ...]:
    values = _array(raw, path)
    if len(values) > maximum:
        raise DefinitionError(f"{path} exceeds the v1 term limit")
    terms: list[SortTerm] = []
    for ordinal, raw_term in enumerate(values):
        term_path = f"{path}[{ordinal}]"
        term = _mapping(raw_term, term_path, keys={"field", "direction", "nulls"})
        direction = _required(term, "direction", term_path)
        nulls = _required(term, "nulls", term_path)
        if direction not in _DIRECTIONS or nulls not in _NULLS:
            raise DefinitionError(f"{term_path} has invalid direction or null ordering")
        terms.append(
            SortTerm(
                field_id=_identifier(
                    _required(term, "field", term_path), f"{term_path}.field"
                ),
                direction=direction,
                nulls=nulls,
            )
        )
    if len({term.field_id for term in terms}) != len(terms):
        raise DefinitionError(f"{path} cannot repeat a field")
    return tuple(terms)


def _validate_revision_transition(
    previous: CustomImportDefinition, current: CustomImportDefinition
) -> None:
    if current.definition_revision <= previous.definition_revision:
        raise DefinitionError("definition revision must increase")
    if (
        current.schema_digest != previous.schema_digest
        and current.schema_revision <= previous.schema_revision
    ):
        raise DefinitionError(
            "schema/key/type/relationship changes require a new schema revision"
        )
    if (
        current.schema_digest == previous.schema_digest
        and current.schema_revision != previous.schema_revision
    ):
        raise DefinitionError("unchanged schema must retain its schema revision")
    field_id_by_previous_slot = {
        field.field_slot: field.field_id for field in previous.fields
    }
    field_id_by_current_slot = {
        field.field_slot: field.field_id for field in current.fields
    }
    if any(
        field_id_by_current_slot.get(slot) not in {None, field_id}
        for slot, field_id in field_id_by_previous_slot.items()
    ):
        raise DefinitionError("stable field slots cannot be rebound")
