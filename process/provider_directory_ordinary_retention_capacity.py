# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Account exact ordinary before-images on the existing nonprofile admission."""

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass, replace

from process import provider_directory_cms_native_layout as native_layout
from process import provider_directory_serving_capacity_contract as contract
from process.provider_directory_cms_preparation import OwnedRelation
from process.provider_directory_ordinary_address_admission import require_capture

_FAMILIES = {
    "profile": ("provider_directory_profile", ("npi",)),
    "evidence": ("provider_directory_profile_evidence", ("evidence_key",)),
    "overlay": ("provider_directory_address_overlay", ("source_record_id",)),
    "unified": ("entity_address_unified", ("location_key",)),
    "address_evidence": ("entity_address_evidence", ("evidence_id",)),
    "plan_bridge": ("entity_address_plan_bridge", ("location_key", "entity_type", "entity_id", "plan_id")),
    "network_bridge": ("entity_address_network_bridge", ("location_key", "entity_type", "entity_id", "network_id")),
    "procedure_bridge": ("entity_address_procedure_bridge", ("location_key", "npi", "code_system", "code")),
    "medication_bridge": ("entity_address_medication_bridge", ("location_key", "npi", "code_system", "code")),
    "facility_candidate": ("facility_anchor_npi_candidate", ("candidate_id",)),
}
_PLAN_CONTRACT = "provider-directory-serving-retention.v2"
_ENTRY_FIELDS = {
    "family",
    "source_schema",
    "source_relation",
    "source_oid",
    "source_layout_sha256",
    "key_columns",
    "scope_kind",
    "retention_mode",
}
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z_0-9]{0,62}\Z")


@dataclass(frozen=True)
class OrdinaryRetainedRelation:
    """Physical identity captured from the original database, never from worker JSON."""

    family: str
    schema: str
    relation: str
    oid: int
    owner_oid: int
    schema_oid: int
    retention_plan_sha256: str
    layout_sha256: str
    index_oids: tuple[int, ...] = ()


def _require(condition):
    if not condition:
        raise RuntimeError("provider_directory_ordinary_retention_changed")


def _plan_payload(raw):
    _require(isinstance(raw, Mapping) and set(raw) == {"contract_id", "affected_scope_sha256", "families"})
    _require(raw["contract_id"] == _PLAN_CONTRACT and contract.is_valid_hash(raw["affected_scope_sha256"]))
    _require(isinstance(raw["families"], (tuple, list)) and len(raw["families"]) == len(_FAMILIES))
    entries, families, source_oids = [], set(), set()
    for entry in raw["families"]:
        _require(isinstance(entry, Mapping) and set(entry) == _ENTRY_FIELDS)
        family = entry["family"]
        _require(type(family) is str and family in _FAMILIES and family not in families)
        _require(entry["source_relation"] == _FAMILIES[family][0])
        _require(type(entry["source_schema"]) is str and _IDENTIFIER.fullmatch(entry["source_schema"]) is not None)
        _require(
            type(entry["source_oid"]) is int
            and 0 < entry["source_oid"] < 2**32
            and entry["source_oid"] not in source_oids
        )
        _require(contract.is_valid_hash(entry["source_layout_sha256"]))
        _require(
            isinstance(entry["key_columns"], (tuple, list)) and tuple(entry["key_columns"]) == _FAMILIES[family][1]
        )
        _require(entry["retention_mode"] in {"owned_copy", "existing_initial_target"})
        if entry["retention_mode"] == "existing_initial_target":
            _require(family in {"profile", "evidence"} and entry["scope_kind"] == "initial_full_root")
        else:
            _require(
                entry["scope_kind"]
                == ("affected_npi" if family in {"profile", "evidence", "overlay"} else "affected_location")
            )
        families.add(family)
        source_oids.add(entry["source_oid"])
        entries.append({**entry, "key_columns": list(entry["key_columns"])})
    return {"contract_id": _PLAN_CONTRACT, "affected_scope_sha256": raw["affected_scope_sha256"], "families": entries}


def retention_plan_sha256(raw):
    """Bind the complete captured recipe, including every Source slice and original root."""
    return hashlib.sha256(
        _PLAN_CONTRACT.encode("ascii") + b"\0" + contract.canonical(_plan_payload(raw)).encode("ascii")
    ).hexdigest()


def _captured_plan(fhir, plan, capture):
    captured = require_capture(fhir, plan, capture)
    payload = _plan_payload(captured.retention_plan)
    scope = captured.scope_payload
    _require(retention_plan_sha256(payload) == scope["retention_plan_sha256"])
    _require(payload["affected_scope_sha256"] == scope["affected_scope_sha256"])
    if any(entry["retention_mode"] == "existing_initial_target" for entry in payload["families"]):
        _require(scope["retention_kind"] == "snapshot")
    return {entry["family"]: entry for entry in payload["families"]}


def _family(relation):
    for family, (source_name, _key) in _FAMILIES.items():
        if relation.relation == source_name or re.fullmatch(
            "pd_" + family + r"_before_[0-9a-f]{20}", relation.relation
        ):
            return family
    raise RuntimeError("provider_directory_ordinary_retention_changed")


async def _catalog(fhir, relation, *, scope_kind=None, initial_family=None):
    """Reuse native census and preserve all essential constraints without admitting FKs or hooks."""
    _require(type(relation) is OwnedRelation and relation.persistence in {"u", "p"})
    relation_metadata, toast_oid = await fhir._profile_capacity_relation_row(relation.oid, relation.persistence, 0)
    _require(
        (relation_metadata["schema_name"], relation_metadata["relation_name"], relation_metadata["relation_oid"])
        == (relation.schema, relation.relation, relation.oid)
    )
    sections = await native_layout._native_catalog_rows(fhir, [relation.oid] + ([toast_oid] if toast_oid else []))
    attributes, indexes, constraints, triggers, database_oid = sections
    native_layout._validate_native_catalog_members((relation.oid, toast_oid), sections[:4])
    _require(
        not triggers
        and all(
            entry["constraint_type"] in {"p", "n", "u", "c"}
            and not entry["condeferrable"]
            and not entry["condeferred"]
            and entry["convalidated"]
            for entry in constraints
        )
    )
    for index in indexes:
        native_layout._assert_index_physical(index, relation)
    if initial_family is not None:
        _require(scope_kind is None and initial_family in {"profile", "evidence"})
        _require(relation.relation in {_FAMILIES[initial_family][0], f"pdsrv_r_{relation.oid:x}"})
    if scope_kind is None:
        _require_family_key(relation, attributes, indexes, initial_family)
    else:
        from process.provider_directory_ordinary_scope_retention import require_scope_layout

        require_scope_layout(scope_kind, relation, attributes, indexes)
    return {
        "contract": "ordinary-retention-observed-layout.v1",
        "database_oid": database_oid,
        "relation": relation_metadata,
        "attributes": attributes,
        "indexes": indexes,
        "constraints": constraints,
        "triggers": triggers,
    }, toast_oid


def _require_family_key(relation, attributes, indexes, initial_family):
    family = initial_family or _family(relation)
    primary_indexes = [index for index in indexes if index["relation_oid"] == relation.oid and index["indisprimary"]]
    names_by_number = {
        entry["attnum"]: entry["attname"] for entry in attributes if entry["relation_oid"] == relation.oid
    }
    if not primary_indexes and family == "overlay":
        _require_overlay_unique_key(relation, attributes, indexes, names_by_number)
        return
    if not (
        len(primary_indexes) == 1 and primary_indexes[0]["indisunique"] and primary_indexes[0]["index_am"] == "btree"
    ):
        raise RuntimeError("provider_directory_ordinary_retention_key_changed:" + family)
    _require(
        tuple(names_by_number.get(int(key)) for key in primary_indexes[0]["indkey"].split()) == _FAMILIES[family][1]
    )


def _require_overlay_unique_key(relation, attributes, indexes, names_by_number):
    """The existing Overlay stage installer retains its unique index instead of a PK."""
    keys = _FAMILIES["overlay"][1]
    _require(
        all(
            any(
                row["relation_oid"] == relation.oid and row["attname"] == key and row["attnotnull"]
                for row in attributes
            )
            for key in keys
        )
    )
    matching_indexes = [
        index
        for index in indexes
        if index["relation_oid"] == relation.oid
        and index["indisunique"]
        and index["index_am"] == "btree"
        and all(index[field] is True for field in ("indisvalid", "indisready", "indislive", "indimmediate"))
        and not index["index_predicate"]
        and not index["index_expressions"]
        and index["indnatts"] == index["indnkeyatts"] == len(keys)
        and tuple(names_by_number.get(int(key)) for key in index["indkey"].split()) == keys
    ]
    _require(len(matching_indexes) == 1)


def _layout_digest(catalog):
    structure = native_layout._structural_catalog(catalog)
    structure["relation"]["relpersistence"] = "p"
    if structure["has_toast"]:
        structure["relation"]["toast_relpersistence"] = "p"
    for index in structure["indexes"]:
        index["index_persistence"] = "p"
    return hashlib.sha256(
        b"provider-directory-serving-retention-layout.v1\0" + contract.canonical(structure).encode("ascii")
    ).hexdigest()


async def retention_layout_sha256(fhir, relation):
    """Capture uses this same native structure digest before any retained copy exists."""
    catalog, _toast_oid = await _catalog(fhir, relation)
    return _layout_digest(catalog)


async def _custody(fhir, relation):
    row = await fhir.db.first(
        "SELECT c.relowner::bigint AS owner_oid,c.relnamespace::bigint AS schema_oid,"
        "c.relpersistence::text AS persistence,(SELECT oid::bigint FROM pg_roles WHERE rolname=current_user) AS actor_oid "
        "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE c.oid=CAST(:oid AS oid) AND n.nspname=:schema AND c.relname=:name",
        oid=relation.oid,
        schema=relation.schema,
        name=relation.relation,
    )
    _require(row is not None)
    return dict(fhir._pagination_checkpoint_row_mapping(row))


def _require_coordinate(entry, relation, scope, custody):
    _require(relation.schema == entry["source_schema"] and custody["schema_oid"] == scope["schema_oid"])
    if entry["retention_mode"] == "existing_initial_target":
        _require(
            relation.relation in {entry["source_relation"], f"pdsrv_r_{entry['source_oid']:x}"}
            and (relation.oid, custody["persistence"]) == (entry["source_oid"], "p")
        )
    else:
        _require(re.fullmatch("pd_" + entry["family"] + r"_before_[0-9a-f]{20}", relation.relation) is not None)
        _require(
            relation.oid != entry["source_oid"] and custody["owner_oid"] == custody["actor_oid"] == scope["owner_oid"]
        )


async def register_ordinary_retained_relation(admission, fhir, *, family, schema, name, oid):
    """Read original physical custody before registering it with the existing aggregate meter."""
    entries = _captured_plan(fhir, admission.plan, admission.captured_inputs)
    _require(family in entries)
    await admission.captured_inputs.recheck(fhir)
    probe = OwnedRelation(schema, name, oid, 0, "p")
    custody = await _custody(fhir, probe)
    relation = OwnedRelation(schema, name, oid, 0, custody["persistence"])
    _require_coordinate(entries[family], relation, admission.captured_inputs.scope_payload, custody)
    catalog, _toast_oid = await _catalog(fhir, relation)
    layout = _layout_digest(catalog)
    _require(layout == entries[family]["source_layout_sha256"])
    annotation = OrdinaryRetainedRelation(
        family,
        schema,
        name,
        oid,
        custody["owner_oid"],
        custody["schema_oid"],
        admission.captured_inputs.scope_payload["retention_plan_sha256"],
        layout,
        tuple(sorted(index["index_oid"] for index in catalog["indexes"]))
        if entries[family]["retention_mode"] == "existing_initial_target"
        else (),
    )
    await admission.register_external_relation(fhir, schema, name, oid, ordinary_relation=annotation)


def _require_annotation(admission, annotation, entries):
    from process import provider_directory_ordinary_scope_retention as scopes

    if type(annotation) is scopes.OrdinaryRetainedScope:
        scopes.require_annotation(admission, annotation)
        return
    _require(type(annotation) is OrdinaryRetainedRelation and annotation.family in entries)
    _require(annotation.retention_plan_sha256 == admission.captured_inputs.scope_payload["retention_plan_sha256"])
    _require(annotation.layout_sha256 == entries[annotation.family]["source_layout_sha256"])


def record_annotation(admission, fhir, annotation, coordinate, raw, native):
    """Keep ordinary row and key copies disjoint from CMS and on their original meter."""
    previous = admission._ordinary_relations.get(coordinate[:2])
    if annotation is None:
        _require(previous is None)
        return
    entries = _captured_plan(fhir, admission.plan, admission.captured_inputs)
    _require(raw is None and native is None)
    _require_annotation(admission, annotation, entries)
    _require((annotation.schema, annotation.relation, annotation.oid) == coordinate)
    _require(previous is None or previous == annotation)
    admission._ordinary_relations[coordinate[:2]] = annotation


def _checked_annotations(producer, request, entries, measured_by_coordinate):
    from process import provider_directory_ordinary_scope_retention as scopes

    annotation_by_coordinate, families = {}, set()
    for annotation in request.ordinary_relations:
        if type(annotation) is scopes.OrdinaryRetainedScope:
            admission = annotation.prepared.nonprofile_admission
            _require(admission.captured_inputs is producer.captured_inputs)
            _require_annotation(admission, annotation, entries)
        else:
            _require(type(annotation) is OrdinaryRetainedRelation and annotation.family not in families)
            _require_annotation(producer, annotation, entries)
            families.add(annotation.family)
        coordinate = (annotation.schema, annotation.relation)
        _require(coordinate in measured_by_coordinate and coordinate not in annotation_by_coordinate)
        _require(measured_by_coordinate[coordinate].oid == annotation.oid)
        if request.phase == "cutover":
            _require(measured_by_coordinate[coordinate].persistence == "p")
        annotation_by_coordinate[coordinate] = annotation
    if request.phase == "cutover":
        _require(families == set(entries))
    scopes.require_complete_scopes(request.ordinary_relations, cutover=request.phase == "cutover")
    return annotation_by_coordinate


def retained_relations(producer, request):
    """Count every row and key copy once; require the whole old/new closure at the cut."""
    from process.provider_directory_ordinary_scope_retention import KINDS

    if producer.captured_inputs is None:
        _require(not request.ordinary_relations)
        return {}
    entries = _captured_plan(producer.fhir, producer.plan, producer.captured_inputs)
    measured_by_coordinate = {(relation.schema, relation.relation): relation for relation in request.relations}
    annotations = _checked_annotations(producer, request, entries, measured_by_coordinate)
    _require(
        sum(measured_by_coordinate[coordinate].total_bytes for coordinate in annotations)
        <= producer.captured_inputs.scope_payload["max_retention_data_bytes"]
    )
    prefixes = (*_FAMILIES, *(kind + "_scope" for kind in KINDS))
    for coordinate, relation in measured_by_coordinate.items():
        if re.fullmatch("pd_(" + "|".join(prefixes) + r")_before_[0-9a-f]{20}", relation.relation):
            _require(coordinate in annotations)
    return annotations


def resident_retained_bytes(producer, request):
    """Exclude only authenticated already-resident initial targets from new allocation."""
    if producer.captured_inputs is None:
        return 0
    annotations = retained_relations(producer, request)
    entries = _captured_plan(producer.fhir, producer.plan, producer.captured_inputs)
    measured_by_coordinate = {(relation.schema, relation.relation): relation for relation in request.relations}
    resident = 0
    for coordinate, annotation in annotations.items():
        if type(annotation) is not OrdinaryRetainedRelation:
            continue
        entry = entries[annotation.family]
        if entry["retention_mode"] != "existing_initial_target":
            continue
        relation = measured_by_coordinate[coordinate]
        _require(
            relation.relation in {entry["source_relation"], f"pdsrv_r_{entry['source_oid']:x}"}
            and (relation.oid, relation.persistence) == (entry["source_oid"], "p")
        )
        expected = producer.captured_inputs.predecessor["profile"][annotation.family + "_target_bytes"]
        _require(type(expected) is int and expected >= 0 and relation.total_bytes == expected)
        resident += expected
    ceiling = producer.captured_inputs.scope_payload["resident_retention_data_bytes"]
    _require(resident <= ceiling and (request.phase != "cutover" or resident == ceiling))
    return resident


def allocated_data_bytes(producer, request):
    """Charge new physical allocation while retaining the full rollback quota."""
    return sum(relation.total_bytes for relation in request.relations) - resident_retained_bytes(producer, request)


async def capture_retained_layout(producer, relation, annotation):
    """Recheck the exact original owner, OID, native layout and tablespace on every phase."""
    from process import provider_directory_ordinary_scope_retention as scopes

    if type(annotation) is scopes.OrdinaryRetainedScope:
        return await scopes.capture_scope_layout(producer, relation, annotation)
    entries = _captured_plan(producer.fhir, producer.plan, producer.captured_inputs)
    custody = await _custody(producer.fhir, relation)
    _require((custody["owner_oid"], custody["schema_oid"]) == (annotation.owner_oid, annotation.schema_oid))
    _require_coordinate(entries[annotation.family], relation, producer.captured_inputs.scope_payload, custody)
    initial_family = (
        annotation.family if entries[annotation.family]["retention_mode"] == "existing_initial_target" else None
    )
    catalog, toast_oid = await _catalog(producer.fhir, relation, initial_family=initial_family)
    _require(_layout_digest(catalog) == annotation.layout_sha256)
    if entries[annotation.family]["retention_mode"] == "existing_initial_target":
        _require(
            annotation.index_oids
            and tuple(sorted(index["index_oid"] for index in catalog["indexes"])) == annotation.index_oids
        )
    return native_layout._observed_layout(producer.fhir, relation, catalog, toast_oid)


async def retained_scope_layout_sha256(fhir, kind, relation):
    """Observe the issued native keyset schema without imposing a fabricated primary key."""
    from process import provider_directory_ordinary_scope_retention as scopes

    return await scopes.retained_scope_layout_sha256(fhir, kind, relation)


async def register_ordinary_retained_scope(admission, fhir, *, prepared, kind, schema, name, oid):
    """Register one native keyset copy on the same original signed retention meter."""
    from process import provider_directory_ordinary_scope_retention as scopes

    await scopes.register_ordinary_retained_scope(
        admission, fhir, prepared=prepared, kind=kind, schema=schema, name=name, oid=oid
    )


def measured_data_ceiling(plan):
    """The wrapper permits signed resident bytes; its producer verifies each exact discount."""
    from process.provider_directory_serving_capacity_plan import OrdinaryServingAdmissionPlan

    ceiling = dict(plan.reservation_bytes)["data"]
    if type(plan) is OrdinaryServingAdmissionPlan:
        ceiling += plan.payload["ordinary_scope"]["resident_retention_data_bytes"]
    return ceiling


async def check_ordinary_retained_growth(admission, fhir):
    """Check local aggregate growth between bounded COPY pages without authority transport."""
    from process.provider_directory_cms_nonprofile_capacity import _CapacityProducer
    from process.provider_directory_cms_preparation import NonprofileAdmissionCheck

    producer = getattr(admission.check_phase, "__self__", None)
    _require(type(producer) is _CapacityProducer and producer.fhir is fhir)
    _require(producer.plan is admission.plan and producer.lease is admission.lease)
    _require(producer.captured_inputs is admission.captured_inputs)
    _require(not producer._cutover_active and producer.resumed_profile is None and admission._started)
    require_capture(fhir, admission.plan, admission.captured_inputs)
    admission._assert_lease()
    relations = await admission.measure(fhir, fhir._schema())
    request = NonprofileAdmissionCheck(
        "readiness",
        admission.lease,
        admission.plan,
        relations,
        ordinary_relations=tuple(admission._ordinary_relations.values()),
    )
    _require(allocated_data_bytes(producer, request) <= dict(admission.plan.reservation_bytes)["data"])
    await producer._assert_consumption()
    await producer._assert_wal_budget(request)


def _initial_rename_context(admission, fhir, family):
    from process.provider_directory_cms_nonprofile_capacity import _CapacityProducer

    producer = getattr(admission.check_cutover, "__self__", None)
    _require(type(producer) is _CapacityProducer and producer.fhir is fhir and producer._cutover_active)
    _require(producer.plan is admission.plan and producer.captured_inputs is admission.captured_inputs)
    _require(producer.lease is admission.lease)
    _require(producer._cutover_binding is not None and producer._cutover_binding is fhir.db._transaction_binding())
    entries = _captured_plan(fhir, admission.plan, admission.captured_inputs)
    _require(family in {"profile", "evidence"} and entries[family]["retention_mode"] == "existing_initial_target")
    entry = entries[family]
    old_coordinate = (entry["source_schema"], entry["source_relation"])
    annotation = admission._ordinary_relations.get(old_coordinate)
    _require(type(annotation) is OrdinaryRetainedRelation and annotation.family == family)
    _require(annotation.oid == entry["source_oid"] and annotation.index_oids)
    _require(
        admission._relations.get(old_coordinate) == annotation.oid and old_coordinate in admission._external_relations
    )
    return producer, entry, old_coordinate, annotation


def _renamed_cutover_request(producer, annotation, name):
    """Keep the original phase witness while following one verified same-OID rename."""
    from process.provider_directory_cms_preparation import NonprofileAdmissionCheck

    request = producer._cutover_request
    _require(
        type(request) is NonprofileAdmissionCheck
        and request.phase == "cutover"
        and request.lease is producer.lease
        and request.plan is producer.plan
        and not request.logging_relations
    )
    original = (annotation.schema, annotation.relation, annotation.oid)
    renamed = (annotation.schema, name)
    _require(sum((item.schema, item.relation, item.oid) == original for item in request.relations) == 1)
    _require(all((item.schema, item.relation) != renamed for item in request.relations))
    _require(sum(item is annotation for item in request.ordinary_relations) == 1)
    _require(sum((item.schema, item.relation) == original[:2] for item in request.ordinary_relations) == 1)
    relations = tuple(
        replace(item, relation=name) if (item.schema, item.relation, item.oid) == original else item
        for item in request.relations
    )
    return replace(
        request,
        relations=tuple(sorted(relations, key=lambda item: (item.schema, item.relation))),
        ordinary_relations=tuple(
            replace(item, relation=name) if item is annotation else item for item in request.ordinary_relations
        ),
    )


async def acknowledge_initial_retained_relation(admission, fhir, *, family):
    """Follow only an authentic initial target through the existing deterministic retention rename."""
    producer, entry, old_coordinate, annotation = _initial_rename_context(admission, fhir, family)
    name = f"pdsrv_r_{annotation.oid:x}"
    coordinate = (annotation.schema, name)
    _require(coordinate not in admission._relations)
    relation = OwnedRelation(annotation.schema, name, annotation.oid, 0, "p")
    custody = await _custody(fhir, relation)
    _require(
        (custody["owner_oid"], custody["schema_oid"], custody["persistence"])
        == (annotation.owner_oid, annotation.schema_oid, "p")
    )
    _require_coordinate(entry, relation, admission.captured_inputs.scope_payload, custody)
    catalog, _toast_oid = await _catalog(fhir, relation, initial_family=family)
    _require(_layout_digest(catalog) == annotation.layout_sha256)
    _require(tuple(sorted(index["index_oid"] for index in catalog["indexes"])) == annotation.index_oids)
    _require(
        all(
            index["index_name"] == f"pdsrv_i_{index['index_oid']:x}"
            for index in catalog["indexes"]
            if index["relation_oid"] == annotation.oid
        )
    )
    actual_original_name_oid = await fhir.db.scalar(
        "SELECT to_regclass(:name)::oid::bigint", name=fhir._unscoped_qt(*old_coordinate)
    )
    _require(actual_original_name_oid != annotation.oid)
    request = _renamed_cutover_request(producer, annotation, name)
    del admission._relations[old_coordinate]
    del admission._ordinary_relations[old_coordinate]
    admission._external_relations.remove(old_coordinate)
    admission._relations[coordinate] = annotation.oid
    admission._ordinary_relations[coordinate] = next(
        retained for retained in request.ordinary_relations if (retained.schema, retained.relation) == coordinate
    )
    admission._external_relations.add(coordinate)
    if old_coordinate in admission._logged_relations:
        admission._logged_relations.remove(old_coordinate)
        admission._logged_relations.add(coordinate)
    producer._cutover_request = request
