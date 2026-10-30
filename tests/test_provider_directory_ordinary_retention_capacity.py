# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Before-image identity, native structure and aggregate cost refusals."""

from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_artifact_bundle_preparation as coordinator
from process import provider_directory_cms_nonprofile_capacity as capacity
from process import provider_directory_ordinary_retention_capacity as retention
from process import provider_directory_ordinary_scope_retention as scopes
from process import provider_directory_serving_capacity_contract as contract
from process.provider_directory_cms_preparation import NonprofileAdmissionCheck, OwnedRelation
from tests.provider_directory_serving_capacity_signing_test_support import serving_case
from tests.test_provider_directory_cms_retained_native_layout import _fixture as native_fixture
from tests.test_provider_directory_cms_retained_native_layout import _native_catalog_transport


@pytest.fixture(autouse=True)
def configured_node(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")


def _catalog_case(monkeypatch):
    directory, source, source_catalog, clone_catalog = native_fixture()
    source = replace(source, relation="entity_address_unified")
    source_catalog["relation"]["relation_name"] = source.relation
    clone = OwnedRelation("fixture", "pd_unified_before_" + "a" * 20, 21, 100, "u")
    clone_catalog["relation"].update(schema_name="fixture", relation_name=clone.relation, relpersistence="u")
    for index in clone_catalog["indexes"]:
        index["index_persistence"] = "u"
        if index["index_schema"] != "pg_toast":
            index["index_schema"] = "fixture"
    active_catalogs = [source_catalog]
    custody_by_field = {"owner_oid": 16400, "schema_oid": 16401, "actor_oid": 16400, "persistence": "u"}

    async def first(query, **_values):
        return (
            custody_by_field if query.startswith("SELECT c.relowner") else _native_catalog_transport(active_catalogs[0])
        )

    directory.db.first = AsyncMock(side_effect=first)
    directory._profile_capacity_relation_row = AsyncMock(
        side_effect=lambda *_args: (active_catalogs[0]["relation"], active_catalogs[0]["relation"]["toast_oid"])
    )
    return directory, source, clone, source_catalog, clone_catalog, active_catalogs, custody_by_field


@pytest.mark.parametrize(
    "damage", [None, "nullable", "predicate", "expression", "column", "nonunique", "notready", "other_family"]
)
def test_existing_overlay_unique_key_is_recognized_without_weakening_other_family_keys(damage):
    relation = OwnedRelation("fixture", "provider_directory_address_overlay", 11, 0, "p")
    attributes = [dict(relation_oid=11, attnum=1, attname="source_record_id", attnotnull=damage != "nullable")]
    index_by_field = dict(
        relation_oid=11,
        indisprimary=False,
        indisunique=damage != "nonunique",
        index_am="btree",
        indisvalid=True,
        indisready=damage != "notready",
        indislive=True,
        indimmediate=True,
        index_predicate="source_record_id IS NOT NULL" if damage == "predicate" else "",
        index_expressions="lower(source_record_id)" if damage == "expression" else "",
        indnatts=1,
        indnkeyatts=1,
        indkey="2" if damage == "column" else "1",
    )
    if damage == "other_family":
        relation = replace(relation, relation="provider_directory_profile")
        attributes[0]["attname"] = "npi"
    if damage is None:
        retention._require_family_key(relation, attributes, [index_by_field], None)
    else:
        with pytest.raises(RuntimeError, match="provider_directory_ordinary_retention"):
            retention._require_family_key(relation, attributes, [index_by_field], None)


def _captured_owner(monkeypatch, directory, layout_digest, *, initial=False):
    case = serving_case(initial_mode=initial)
    scope = case.plan.payload["ordinary_scope"]
    source_oid_by_family = {family: ordinal for ordinal, family in enumerate(retention._FAMILIES, 1)}
    source_oid_by_family["unified"] = 11
    entries = [
        dict(
            family=family,
            source_schema="fixture",
            source_relation=names[0],
            source_oid=source_oid_by_family[family],
            source_layout_sha256=layout_digest,
            key_columns=list(names[1]),
            scope_kind="initial_full_root"
            if initial and family in {"profile", "evidence"}
            else ("affected_npi" if family in {"profile", "evidence", "overlay"} else "affected_location"),
            retention_mode="existing_initial_target" if initial and family in {"profile", "evidence"} else "owned_copy",
        )
        for family, names in retention._FAMILIES.items()
    ]
    plan_by_field = dict(
        contract_id=retention._PLAN_CONTRACT, affected_scope_sha256=scope["affected_scope_sha256"], families=entries
    )
    scope["retention_plan_sha256"] = retention.retention_plan_sha256(plan_by_field)
    scope["input_sha256"] = contract.input_sha256({name: scope[name] for name in contract.INPUT_FIELDS})
    plan = replace(case.plan, ordinary_scope_json=contract.canonical(scope))

    class CapturedServingInputs:
        database = directory.db
        scope_payload = scope
        retention_plan = plan_by_field
        recheck = AsyncMock()

    monkeypatch.setattr(coordinator, "CapturedServingInputs", CapturedServingInputs, raising=False)
    captured = CapturedServingInputs()
    owner = SimpleNamespace(fhir=directory, plan=plan, captured_inputs=captured)
    admission = SimpleNamespace(
        plan=plan, captured_inputs=captured, register_external_relation=AsyncMock(), _ordinary_relations={}
    )
    owner.admission = admission
    owner.required_scopes = ("location", "group") if initial else scopes.KINDS

    def issued_source(original, prepared, kind):
        assert original is admission and prepared.nonprofile_admission is admission
        return ("fixture", "issued_" + kind, 500 + scopes.KINDS.index(kind)), owner.required_scopes

    monkeypatch.setattr(scopes, "_issued_source", issued_source)
    return owner, admission


async def test_native_clone_preserves_structure_across_logging_and_physical_names(monkeypatch):
    directory, source, clone, _source_catalog, clone_catalog, active_catalogs, _custody = _catalog_case(monkeypatch)
    original = await retention.retention_layout_sha256(directory, source)
    active_catalogs[0] = clone_catalog
    assert await retention.retention_layout_sha256(directory, clone) == original
    clone_catalog["relation"]["relpersistence"] = "p"
    for index in clone_catalog["indexes"]:
        index["index_persistence"] = "p"
    assert await retention.retention_layout_sha256(directory, replace(clone, persistence="p")) == original


@pytest.mark.parametrize("changed", ["default", "constraint", "operator", "predicate", "expression", "column"])
async def test_retention_digest_keeps_native_semantics(monkeypatch, changed):
    directory, source, clone, _source_catalog, clone_catalog, active_catalogs, _custody = _catalog_case(monkeypatch)
    original = await retention.retention_layout_sha256(directory, source)
    if changed == "default":
        clone_catalog["attributes"][0]["default_expression"] = "'changed'::text"
    if changed == "constraint":
        clone_catalog["constraints"][0]["constraint_definition"] += " changed"
    if changed == "operator":
        clone_catalog["indexes"][1]["opclass_names"] = ["changed_ops"]
    if changed == "predicate":
        clone_catalog["indexes"][1]["index_predicate"] = "npi IS NULL"
    if changed == "expression":
        clone_catalog["indexes"][1]["index_expressions"] = "npi + 1"
    if changed == "column":
        clone_catalog["attributes"][1]["attname"] = "changed_column"
    active_catalogs[0] = clone_catalog
    assert await retention.retention_layout_sha256(directory, clone) != original


@pytest.mark.parametrize("kind", ["n", "u", "c", "f"])
async def test_native_constraints_are_retained_but_foreign_keys_refuse(monkeypatch, kind):
    directory, source, _clone, catalog, _clone_catalog, _current, _custody = _catalog_case(monkeypatch)
    original = await retention.retention_layout_sha256(directory, source)
    catalog["constraints"].append(
        {**catalog["constraints"][0], "constraint_type": kind, "constraint_definition": kind + " native clause"}
    )
    if kind == "f":
        with pytest.raises(RuntimeError, match="retention_changed"):
            await retention.retention_layout_sha256(directory, source)
    else:
        assert await retention.retention_layout_sha256(directory, source) != original


async def test_registered_before_image_is_read_from_actual_owner_and_captured_plan(monkeypatch):
    directory, source, clone, _source_catalog, clone_catalog, active_catalogs, _custody = _catalog_case(monkeypatch)
    digest = await retention.retention_layout_sha256(directory, source)
    owner, admission = _captured_owner(monkeypatch, directory, digest)
    active_catalogs[0] = clone_catalog
    await retention.register_ordinary_retained_relation(
        admission, directory, family="unified", schema=clone.schema, name=clone.relation, oid=clone.oid
    )
    annotation = admission.register_external_relation.await_args.kwargs["ordinary_relation"]
    assert type(annotation) is retention.OrdinaryRetainedRelation
    assert (annotation.owner_oid, annotation.schema_oid, annotation.layout_sha256) == (16400, 16401, digest)
    owner.captured_inputs.recheck.assert_awaited_once_with(directory)
    layout = await retention.capture_retained_layout(owner, clone, annotation)
    assert layout.relation_oid == clone.oid and layout.effective_tablespace_oids == (42,)


@pytest.mark.parametrize("tablespace", [42, 43])
async def test_existing_phase_meter_observes_exact_ordinary_catalog(monkeypatch, tablespace):
    directory, source, clone, _original, clone_catalog, active_catalogs, _custody = _catalog_case(monkeypatch)
    digest = await retention.retention_layout_sha256(directory, source)
    owner, admission = _captured_owner(monkeypatch, directory, digest)
    active_catalogs[0] = clone_catalog
    await retention.register_ordinary_retained_relation(
        admission, directory, family="unified", schema=clone.schema, name=clone.relation, oid=clone.oid
    )
    annotation = admission.register_external_relation.await_args.kwargs["ordinary_relation"]
    directory.db._transaction_binding = lambda: object()
    owner._assert_wal_budget = AsyncMock()
    owner._checked_relation_policies = lambda request: capacity._CapacityProducer._checked_relation_policies(
        owner, request
    )
    owner._raw_relation_policies = lambda _request: {}
    owner._native_relation_policies = lambda _request: {}
    owner._native_catalog_observations = AsyncMock(return_value={})
    owner._capture_relation_layout = AsyncMock(side_effect=AssertionError("ordinary custody must be checked"))
    request = NonprofileAdmissionCheck("readiness", object(), owner.plan, (clone,), ordinary_relations=(annotation,))
    if tablespace == 43:
        with pytest.raises(RuntimeError, match="physical_tablespace_changed"):
            await capacity._CapacityProducer._assert_physical(owner, request, {"data_tablespace_oid": tablespace})
    else:
        await capacity._CapacityProducer._assert_physical(owner, request, {"data_tablespace_oid": tablespace})
    owner._assert_wal_budget.assert_awaited_once_with(request)
    owner._capture_relation_layout.assert_not_awaited()


@pytest.mark.parametrize("changed", ["owner", "schema", "actor", "layout", "captured_plan"])
async def test_wrong_native_custody_or_plan_cannot_register(monkeypatch, changed):
    directory, source, clone, _source_catalog, clone_catalog, active_catalogs, custody = _catalog_case(monkeypatch)
    digest = await retention.retention_layout_sha256(directory, source)
    _owner, admission = _captured_owner(monkeypatch, directory, digest)
    active_catalogs[0] = clone_catalog
    if changed in {"owner", "schema", "actor"}:
        custody[changed + "_oid"] += 1
    if changed == "layout":
        clone_catalog["indexes"][1]["index_predicate"] = "npi IS NULL"
    if changed == "captured_plan":
        admission.captured_inputs.retention_plan["families"][0]["source_oid"] = 999
    with pytest.raises(RuntimeError, match="retention_changed"):
        await retention.register_ordinary_retained_relation(
            admission, directory, family="unified", schema=clone.schema, name=clone.relation, oid=clone.oid
        )
    admission.register_external_relation.assert_not_awaited()


def _measured_request(owner):
    relations, annotations = [], []
    for ordinal, family in enumerate(retention._FAMILIES, 21):
        name = "pd_" + family + "_before_" + "a" * 20
        relations.append(OwnedRelation("fixture", name, ordinal, 100, "p"))
        annotations.append(
            retention.OrdinaryRetainedRelation(
                family,
                "fixture",
                name,
                ordinal,
                16400,
                16401,
                owner.captured_inputs.scope_payload["retention_plan_sha256"],
                "ab" * 32,
            )
        )
    prepared = SimpleNamespace(nonprofile_admission=owner.admission)
    for kind in owner.required_scopes:
        name = "pd_" + kind + "_scope_before_" + "a" * 20
        ordinal = 100 + scopes.KINDS.index(kind)
        relations.append(OwnedRelation("fixture", name, ordinal, 100, "p"))
        annotations.append(
            scopes.OrdinaryRetainedScope(
                kind,
                "fixture",
                name,
                ordinal,
                16400,
                16401,
                owner.captured_inputs.scope_payload["retention_plan_sha256"],
                "ab" * 32,
                ("fixture", "issued_" + kind, 500 + scopes.KINDS.index(kind)),
                owner.required_scopes,
                prepared,
            )
        )
    return NonprofileAdmissionCheck(
        "cutover", object(), owner.plan, tuple(relations), ordinary_relations=tuple(annotations)
    )


@pytest.mark.parametrize("changed", [None, "missing", "duplicate", "oid", "unlogged", "budget", "unannotated"])
def test_atomic_cut_requires_all_retained_families_and_aggregate_bound(monkeypatch, changed):
    directory = SimpleNamespace(db=object())
    owner, _admission = _captured_owner(monkeypatch, directory, "ab" * 32)
    request = _measured_request(owner)
    if changed == "missing":
        request = replace(request, ordinary_relations=request.ordinary_relations[:-1])
    if changed == "duplicate":
        request = replace(request, ordinary_relations=(*request.ordinary_relations, request.ordinary_relations[0]))
    if changed == "oid":
        request = replace(request, relations=(replace(request.relations[0], oid=999), *request.relations[1:]))
    if changed == "unlogged":
        request = replace(request, relations=(replace(request.relations[0], persistence="u"), *request.relations[1:]))
    if changed == "budget":
        request = replace(request, relations=(replace(request.relations[0], total_bytes=8192), *request.relations[1:]))
    if changed == "unannotated":
        request = replace(
            request,
            relations=(
                *request.relations,
                replace(request.relations[0], relation="pd_profile_before_" + "b" * 20, oid=999),
            ),
        )
    if changed:
        with pytest.raises(RuntimeError, match="retention_changed|retained_scope_changed"):
            retention.retained_relations(owner, request)
    else:
        assert len(retention.retained_relations(owner, request)) == 13


def test_initial_retention_keeps_existing_profile_identity_and_original_owner(monkeypatch):
    directory = SimpleNamespace(db=object())
    owner, _admission = _captured_owner(monkeypatch, directory, "ab" * 32, initial=True)
    entry = retention._captured_plan(directory, owner.plan, owner.captured_inputs)["profile"]
    relation = OwnedRelation("fixture", "provider_directory_profile", 1, 100, "p")
    custody_by_field = dict(owner_oid=25000, schema_oid=16401, actor_oid=16400, persistence="p")
    retention._require_coordinate(entry, relation, owner.captured_inputs.scope_payload, custody_by_field)
    with pytest.raises(RuntimeError, match="retention_changed"):
        retention._require_coordinate(
            entry, replace(relation, oid=2), owner.captured_inputs.scope_payload, custody_by_field
        )
    custody_by_field["persistence"] = "u"
    with pytest.raises(RuntimeError, match="retention_changed"):
        retention._require_coordinate(entry, relation, owner.captured_inputs.scope_payload, custody_by_field)
