# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain only the three owner-issued rollback keysets on the ordinary byte meter."""

import re
from dataclasses import dataclass, field

from process.provider_directory_cms_preparation import OwnedRelation

KINDS = ("npi", "location", "group")
_COLUMNS = {
    "npi": (("npi", 20),),
    "location": (("location_key", 1043),),
    "group": (
        ("entity_type", 1043),
        ("entity_id", 1043),
        ("entity_npi", 20),
        ("address_key", 2950),
        ("street_key", 1043),
        ("city_key", 1043),
        ("state_key", 1043),
        ("zip_key", 1043),
        ("country_key", 1043),
    ),
}
_GROUP_INDEX = (
    "entity_npi",
    "entity_type",
    "entity_id",
    "address_key",
    "street_key",
    "city_key",
    "state_key",
    "zip_key",
    "country_key",
)


def _require(condition):
    if not condition:
        raise RuntimeError("provider_directory_ordinary_retained_scope_changed")


@dataclass(frozen=True)
class OrdinaryRetainedScope:
    """An actual issued scope and its native copy, never a new selection authority."""

    kind: str
    schema: str
    relation: str
    oid: int
    owner_oid: int
    schema_oid: int
    retention_plan_sha256: str
    layout_sha256: str
    source: tuple[str, str, int]
    required_kinds: tuple[str, ...]
    prepared: object = field(repr=False, compare=False)


def require_scope_layout(kind, relation, attributes, indexes):
    """Keep exact source columns and actual uniqueness without inventing a group PK."""
    _require(kind in KINDS)
    columns = [entry for entry in attributes if entry["relation_oid"] == relation.oid]
    _require(tuple((entry["attname"], entry["atttypid"]) for entry in columns) == _COLUMNS[kind])
    _require(all(not entry["default_expression"] for entry in columns))
    names_by_number = {entry["attnum"]: entry["attname"] for entry in columns}
    keys = _GROUP_INDEX if kind == "group" else tuple(name for name, _type in _COLUMNS[kind])
    matching_indexes = [
        index
        for index in indexes
        if index["relation_oid"] == relation.oid
        and index["index_am"] == "btree"
        and not index["index_predicate"]
        and not index["index_expressions"]
        and tuple(names_by_number.get(int(key)) for key in index["indkey"].split()) == keys
    ]
    _require(len(matching_indexes) == 1)
    _require(matching_indexes[0]["indisunique"] is (kind != "group"))
    if kind == "npi":
        _require(matching_indexes[0]["indisprimary"] and columns[0]["attnotnull"])
    elif kind == "group":
        _require(not any(index["indisprimary"] for index in indexes if index["relation_oid"] == relation.oid))


async def retained_scope_layout_sha256(fhir, kind, relation):
    """Use the same native semantic layout hash before and after bounded COPY."""
    from process import provider_directory_ordinary_retention_capacity as retention

    catalog, _toast_oid = await retention._catalog(fhir, relation, scope_kind=kind)
    return retention._layout_digest(catalog)


def _issued_source(admission, prepared, kind):
    from process.provider_directory_ordinary_row_retention import retained_scope_source

    _require(kind in KINDS and prepared.nonprofile_admission is admission)
    _require(prepared.nonprofile_admission.captured_inputs is admission.captured_inputs)
    source = retained_scope_source(prepared, kind)
    _require(type(source) is tuple and len(source) == 3)
    required_kinds = tuple(name for name in KINDS if retained_scope_source(prepared, name) is not None)
    _require({"location", "group"} <= set(required_kinds) and kind in required_kinds)
    return source, required_kinds


def require_annotation(admission, annotation):
    """Revalidate the actual prepared carrier even after its scratch is cleaned."""
    _require(type(annotation) is OrdinaryRetainedScope)
    source, required = _issued_source(admission, annotation.prepared, annotation.kind)
    _require(source == annotation.source and required == annotation.required_kinds)
    scope = admission.captured_inputs.scope_payload
    _require(annotation.retention_plan_sha256 == scope["retention_plan_sha256"])
    _require(annotation.schema_oid == scope["schema_oid"] and annotation.owner_oid == scope["owner_oid"])
    _require(re.fullmatch("pd_" + annotation.kind + r"_scope_before_[0-9a-f]{20}", annotation.relation) is not None)
    _require(annotation.schema == source[0] and annotation.oid != source[2])


async def register_ordinary_retained_scope(admission, fhir, *, prepared, kind, schema, name, oid):
    """Capture exact descriptor, native clone custody and structure before meter registration."""
    from process import provider_directory_ordinary_retention_capacity as retention
    from process.provider_directory_ordinary_address_admission import require_capture

    capture = require_capture(fhir, admission.plan, admission.captured_inputs)
    source_coordinate, required = _issued_source(admission, prepared, kind)
    await capture.recheck(fhir)
    probe = OwnedRelation(schema, name, oid, 0, "p")
    custody = await retention._custody(fhir, probe)
    _require(custody["actor_oid"] == custody["owner_oid"] == capture.scope_payload["owner_oid"])
    clone = OwnedRelation(schema, name, oid, 0, custody["persistence"])
    original = OwnedRelation(*source_coordinate, 0, "u")
    original_custody = await retention._custody(fhir, original)
    _require(
        original_custody["owner_oid"] == custody["owner_oid"]
        and original_custody["schema_oid"] == custody["schema_oid"]
    )
    original = OwnedRelation(*source_coordinate, 0, original_custody["persistence"])
    digest = await retained_scope_layout_sha256(fhir, kind, clone)
    _require(digest == await retained_scope_layout_sha256(fhir, kind, original))
    annotation = OrdinaryRetainedScope(
        kind,
        schema,
        name,
        oid,
        custody["owner_oid"],
        custody["schema_oid"],
        capture.scope_payload["retention_plan_sha256"],
        digest,
        source_coordinate,
        required,
        prepared,
    )
    require_annotation(admission, annotation)
    await admission.register_external_relation(fhir, schema, name, oid, ordinary_relation=annotation)


async def capture_scope_layout(producer, relation, annotation):
    """Check only the retained native copy after candidate cleanup; never discount its bytes."""
    from process import provider_directory_ordinary_retention_capacity as retention

    require_annotation(annotation.prepared.nonprofile_admission, annotation)
    _require(annotation.prepared.nonprofile_admission.captured_inputs is producer.captured_inputs)
    custody = await retention._custody(producer.fhir, relation)
    _require(
        (custody["owner_oid"], custody["schema_oid"], custody["actor_oid"])
        == (annotation.owner_oid, annotation.schema_oid, annotation.owner_oid)
    )
    catalog, toast_oid = await retention._catalog(producer.fhir, relation, scope_kind=annotation.kind)
    _require(retention._layout_digest(catalog) == annotation.layout_sha256)
    return retention.native_layout._observed_layout(producer.fhir, relation, catalog, toast_oid)


def require_complete_scopes(annotations, *, cutover):
    """A complete retained old/new closure is required before any live common cut."""
    scopes = [annotation for annotation in annotations if type(annotation) is OrdinaryRetainedScope]
    kinds = [annotation.kind for annotation in scopes]
    _require(len(kinds) == len(set(kinds)))
    if scopes:
        required = scopes[0].required_kinds
        _require(all(annotation.required_kinds == required for annotation in scopes))
        if cutover:
            _require(set(kinds) == set(required))
    elif cutover:
        _require(False)
