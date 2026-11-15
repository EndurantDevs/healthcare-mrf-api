# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named provider data and control regressions using synthetic catalog rows."""

import asyncio
import copy
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import network_em_provider_inputs as inputs
from process.network_compact_layout import CompactAddressRelation
from process.network_membership_copy import MembershipCopyTarget
from process.network_serving_read import PinnedNetworkServingManifest


def family():
    return inputs.EMProviderInputs(
        tuple(
            inputs.EMProviderInputRelation(role, "retained_inputs", "held_" + str(i), "generation-1")
            for i, role in enumerate(inputs._ROLES)
        )
    )


def catalog():
    catalog_rows = []
    for role in inputs._ROLES:
        columns = [
            dict(
                attnum=n,
                attname=name,
                type_name=kind,
                type_schema="public" if kind == "geometry" else "pg_catalog",
                canonical_geometry=kind == "geometry",
                geometry_srid=4269 if kind == "geometry" else None,
                geometry_type="MultiPolygon" if kind == "geometry" else None,
            )
            for n, (name, kind) in enumerate(inputs._required_columns(role).items(), 1)
        ]
        key = {
            "npi": ["npi"],
            "npi_taxonomy": ["npi", "checksum"],
            "nucc_taxonomy": ["code"],
            "geo_zip_lookup": ["zip_code"],
        }.get(role)
        catalog_rows.append(
            dict(
                role=role,
                relkind="r",
                relpersistence="p",
                relispartition=False,
                inheritance_ok=True,
                columns_json=json.dumps(columns),
                keys_json=json.dumps([dict(columns=key)] if key else []),
            )
        )
    return catalog_rows


def connection(rows=None):
    return SimpleNamespace(
        is_in_transaction=lambda: True,
        fetch=AsyncMock(return_value=rows or catalog()),
        fetchval=AsyncMock(side_effect=lambda sql, *args: "repeatable read" if sql.startswith("SHOW") else True),
        fetchrow=AsyncMock(),
        execute=AsyncMock(),
    )


def prepare(conn=None):
    return asyncio.run(
        inputs.prepare_em_provider_inputs(conn or connection(), family(), runtime_roles=("runtime_reader",))
    )


def target():
    ids = [str(UUID(int=i)) for i in range(1, 5)]
    return MembershipCopyTarget(*ids, "network_candidate_" + UUID(ids[-1]).hex)


def candidate(receipt, **updates):
    return dict(
        source_generations={"em_provider_inputs": receipt["receipt_sha256"]},
        validation_json=None,
        state="open",
        **updates,
    )


def manifest(receipt):
    return PinnedNetworkServingManifest(
        17,
        target().candidate_id,
        target().schema_name,
        1,
        {"em_provider_inputs": receipt["receipt_sha256"]},
        3,
        "a" * 64,
        CompactAddressRelation(target().schema_name, "entity_address_unified"),
    )


def test_closed_family_renders_only_held_relations_and_shared_geo_bindings():
    held = family()
    assert held.relation("npi") == '"retained_inputs"."held_3"'
    bindings = held.geo_bindings("address_snapshot")
    assert len(bindings) == 6 and bindings["tiger.zcta5"]["table_name"] == "held_7"
    assert bindings["address_snapshot.npi_address"] == {"schema_name": "retained_inputs", "table_name": "held_4"}
    with pytest.raises(ValueError):
        held.relation("npi; DROP TABLE x")


@pytest.mark.parametrize("change", ["missing", "extra", "order", "duplicate_name", "list"])
def test_roster_is_exact_sorted_tuple_with_unique_named_relations(change):
    rows = list(family().relations)
    if change == "missing":
        rows.pop()
    if change == "extra":
        rows.append(rows[-1])
    if change == "order":
        rows.reverse()
    if change == "duplicate_name":
        rows[1] = replace(rows[1], table_name=rows[0].table_name)
    with pytest.raises(ValueError):
        inputs.EMProviderInputs(rows if change == "list" else tuple(rows))


@pytest.mark.parametrize(
    "field,value",
    [
        ("role", "unknown"),
        ("schema_name", "a.b"),
        ("table_name", 'x"'),
        ("generation_id", ""),
        ("generation_id", "x" * 129),
        ("generation_id", "x\n"),
    ],
)
def test_relation_identity_refuses_unbounded_or_executable_values(field, value):
    with pytest.raises(ValueError):
        replace(family().relations[0], **{field: value})


def test_preparation_batches_native_column_and_key_checks_without_locks_or_writes():
    conn = connection()
    receipt = prepare(conn)
    assert inputs._receipt(receipt) == family()
    assert receipt["revision"] == 2
    assert set(receipt["relations"][6]) == {"role", "schema_name", "table_name", "generation_id"}
    assert len(json.dumps(receipt)) < 32768
    assert conn.fetch.await_count == 1
    sql, encoded = conn.fetch.await_args.args
    assert json.loads(encoded)[3]["table_name"] == "held_3"
    assert "pg_attribute" in sql and "pg_index" in sql
    assert "NOT EXISTS(SELECT 1 FROM pg_catalog.pg_inherits" in sql
    assert "to_regclass('tiger.zcta5')" in sql
    assert all(term not in sql for term in ("pg_roles", "pg_trigger", "filenode", "has_table_privilege"))
    assert conn.fetchval.await_count == 1
    conn.execute.assert_not_awaited()
    assert inputs._required_columns("nucc_taxonomy")["definition"] == "text"


@pytest.mark.parametrize(
    "field,value",
    [
        ("relkind", "v"),
        ("relpersistence", "u"),
        ("relispartition", True),
        ("inheritance_ok", False),
    ],
)
def test_native_relation_structure_damage_fails_before_writes(field, value):
    rows = catalog()
    rows[0][field] = value
    conn = connection(rows)
    with pytest.raises(ValueError):
        prepare(conn)
    assert conn.fetchval.await_count == 1
    conn.execute.assert_not_awaited()


@pytest.mark.parametrize(
    "role,damage",
    [
        ("npi", "missing"),
        ("npi_taxonomy", "type"),
        ("npi_address", "namespace"),
        ("tiger.zcta5", "geometry"),
        ("tiger.zcta5", "srid"),
        ("tiger.zcta5", "geometry_type"),
        ("geo_zip_lookup", "key"),
        ("nucc_taxonomy", "key"),
        ("npi", "columns_bound"),
        ("npi", "keys_bound"),
    ],
)
def test_semantic_columns_and_relevant_unique_keys_are_required(role, damage):
    rows = catalog()
    row = rows[inputs._ROLES.index(role)]
    columns = json.loads(row["columns_json"])
    keys = json.loads(row["keys_json"])
    if damage == "missing":
        columns.pop()
    if damage == "type":
        columns[0]["type_name"] = "varchar"
    if damage == "namespace":
        columns[0]["type_schema"] = "untrusted"
    if damage == "geometry":
        columns[-1]["canonical_geometry"] = False
    if damage == "srid":
        columns[-1]["geometry_srid"] = 4326
    if damage == "geometry_type":
        columns[-1]["geometry_type"] = "Point"
    if damage == "key":
        keys = []
    if damage == "columns_bound":
        columns *= 257
    if damage == "keys_bound":
        keys *= 65
    row.update(columns_json=json.dumps(columns), keys_json=json.dumps(keys))
    with pytest.raises(ValueError):
        prepare(connection(rows))


def test_repeatable_read_is_required_before_native_structure_checks():
    conn = connection()
    conn.is_in_transaction = lambda: False
    with pytest.raises(ValueError):
        prepare(conn)
    conn.fetch.assert_not_awaited()
    conn = connection()
    conn.fetchval.return_value = "read committed"
    conn.fetchval.side_effect = None
    with pytest.raises(ValueError):
        prepare(conn)
    conn.fetch.assert_not_awaited()


@pytest.mark.parametrize("damage", ["extra", "revision_bool", "missing_role", "digest", "custody_extra"])
def test_receipt_parser_is_closed_and_hash_checked(damage):
    receipt = prepare()
    if damage == "extra":
        receipt["unexpected"] = True
    if damage == "revision_bool":
        receipt["revision"] = True
    if damage == "missing_role":
        receipt["relations"].pop()
    if damage == "digest":
        receipt["receipt_sha256"] = "0" * 64
    if damage == "custody_extra":
        receipt["relations"][0]["sql"] = "SELECT 1"
    with pytest.raises(ValueError):
        inputs._receipt(receipt)


def test_record_updates_validation_only_under_exact_candidate_lock_and_replays(monkeypatch):
    receipt = prepare()
    conn = connection()
    saved = candidate(receipt)
    lock = AsyncMock(return_value=saved)
    monkeypatch.setattr(inputs, "_locked_candidate", lock)
    assert asyncio.run(inputs.record_em_provider_inputs(conn, target(), receipt, "control")) == receipt
    lock.assert_awaited_once_with(conn, target(), '"control"')
    sql, identity, encoded = conn.execute.await_args.args
    assert identity == UUID(target().candidate_id) and json.loads(encoded) == receipt
    assert "SET validation_json=" in sql and "source_generations=" not in sql
    assert "state='open'" in sql
    saved["validation_json"] = {"em_provider_inputs": receipt}
    conn.execute.reset_mock()
    assert asyncio.run(inputs.record_em_provider_inputs(conn, target(), receipt, "control")) == receipt
    conn.execute.assert_not_awaited()


@pytest.mark.parametrize("damage", ["closed", "digest", "collision", "scope"])
def test_record_refuses_closed_scope_digest_or_receipt_collision(monkeypatch, damage):
    receipt = prepare()
    conn = connection()
    saved = candidate(receipt)
    if damage == "closed":
        saved["state"] = "published"
    if damage == "digest":
        saved["source_generations"] = {"em_provider_inputs": "0" * 64}
    if damage == "collision":
        saved["validation_json"] = {"em_provider_inputs": {}}
    lock = AsyncMock(return_value=saved, side_effect=ValueError("wrong scope") if damage == "scope" else None)
    monkeypatch.setattr(inputs, "_locked_candidate", lock)
    with pytest.raises(ValueError):
        asyncio.run(inputs.record_em_provider_inputs(conn, target(), receipt))
    conn.execute.assert_not_awaited()


def resolver(monkeypatch, *, receipt=None):
    receipt = receipt or prepare()
    pinned = manifest(receipt)
    conn = connection()
    conn.fetchrow.return_value = {
        "source_generations": pinned.source_generations,
        "validation_json": {"em_provider_inputs": copy.deepcopy(receipt)},
    }
    resolve = AsyncMock(return_value=pinned)
    monkeypatch.setattr(inputs, "resolve_network_serving_manifest", resolve)
    return conn, pinned, resolve


def test_resolver_rechecks_retained_full_manifest_and_named_structure_readonly(monkeypatch):
    conn, pinned, resolve = resolver(monkeypatch)
    assert asyncio.run(inputs.resolve_em_provider_inputs(conn, pinned, "control")) == family()
    resolve.assert_awaited_once_with(conn, generation_id=17, control_schema="control")
    sql, cid, schema = conn.fetchrow.await_args.args
    assert "state='published'" in sql and "FOR UPDATE" not in sql
    assert (cid, schema) == (UUID(pinned.candidate_id), pinned.schema_name)
    assert conn.fetch.await_count == 1
    conn.execute.assert_not_awaited()


@pytest.mark.parametrize(
    "damage",
    [
        "manifest",
        "missing",
        "candidate_sources",
        "receipt_missing",
        "receipt_digest",
        "shape",
        "keys",
    ],
)
def test_resolver_refuses_stale_or_absent_logical_data_without_fallback(monkeypatch, damage):
    conn, pinned, resolve = resolver(monkeypatch)
    if damage == "manifest":
        resolve.return_value = replace(pinned, manifest_sha256="b" * 64)
    if damage == "missing":
        conn.fetchrow.return_value = None
    if damage == "candidate_sources":
        conn.fetchrow.return_value["source_generations"] = {}
    if damage == "receipt_missing":
        conn.fetchrow.return_value["validation_json"] = {}
    if damage == "receipt_digest":
        conn.fetchrow.return_value["validation_json"]["em_provider_inputs"]["receipt_sha256"] = "0" * 64
    rows = catalog()
    if damage == "shape":
        columns = json.loads(rows[0]["columns_json"])
        columns[0]["type_name"] = "invalid"
        rows[0]["columns_json"] = json.dumps(columns)
    if damage == "keys":
        rows[inputs._ROLES.index("npi")]["keys_json"] = "[]"
    conn.fetch.return_value = rows
    with pytest.raises(inputs.PlanPricingProjectionUnavailable, match="unavailable"):
        asyncio.run(inputs.resolve_em_provider_inputs(conn, pinned))
    conn.execute.assert_not_awaited()


def test_cancellation_remains_cancellation(monkeypatch):
    conn, pinned, resolve = resolver(monkeypatch)
    resolve.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(inputs.resolve_em_provider_inputs(conn, pinned))


@pytest.mark.parametrize("field", ["dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name"])
def test_record_actual_control_helper_enforces_all_candidate_scope_fields(field):
    receipt = prepare()
    conn = connection()
    requested = target()
    saved = candidate(receipt)
    saved.update(
        {
            name: getattr(requested, name)
            for name in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
        }
    )
    saved[field] = "wrong_schema" if field == "schema_name" else str(UUID(int=99))
    conn.fetchrow.return_value = saved
    with pytest.raises(ValueError):
        asyncio.run(inputs.record_em_provider_inputs(conn, requested, receipt, "control"))
    sql, identity = conn.fetchrow.await_args.args
    assert "FOR UPDATE" in sql and identity == UUID(requested.candidate_id)
    conn.fetchrow.assert_awaited_once()
    conn.execute.assert_not_awaited()


def test_actual_control_lock_accepts_exact_scope_without_mutating_generations():
    receipt = prepare()
    conn = connection()
    requested = target()
    saved = candidate(receipt)
    saved.update(
        {
            name: getattr(requested, name)
            for name in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
        }
    )
    generations = copy.deepcopy(saved["source_generations"])
    conn.fetchrow.return_value = saved
    assert asyncio.run(inputs.record_em_provider_inputs(conn, requested, receipt, "control")) == receipt
    conn.fetchrow.assert_awaited_once()
    conn.execute.assert_awaited_once()
    assert saved["source_generations"] == generations


def test_resolver_refuses_manifest_generation_digest_even_with_valid_receipt(monkeypatch):
    conn, pinned, resolve = resolver(monkeypatch)
    wrong = replace(pinned, source_generations={"em_provider_inputs": "0" * 64})
    resolve.return_value = wrong
    conn.fetchrow.return_value["source_generations"] = wrong.source_generations
    with pytest.raises(inputs.PlanPricingProjectionUnavailable):
        asyncio.run(inputs.resolve_em_provider_inputs(conn, wrong))
    conn.fetch.assert_not_awaited()
    conn.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_api_input_resolution_requires_a_typed_manifest_before_catalog_reads():
    conn = connection()
    with pytest.raises(inputs.PlanPricingProjectionUnavailable):
        await inputs._resolve_em_provider_inputs_api(conn, SimpleNamespace(), "control")
    conn.fetchrow.assert_not_awaited()
    conn.fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_session_input_read_refuses_missing_root_transaction_before_resolution(monkeypatch):
    resolve = AsyncMock()
    monkeypatch.setattr(inputs, "resolve_em_provider_inputs", resolve)
    session = SimpleNamespace(get_transaction=lambda: None)
    with pytest.raises(inputs.PlanPricingProjectionUnavailable):
        await inputs.resolve_em_provider_inputs_in_session(
            session,
            manifest(
                await inputs.prepare_em_provider_inputs(connection(), family(), runtime_roles=("runtime_reader",))
            ),
            "control",
        )
    resolve.assert_not_awaited()


def _query_session():
    driver = SimpleNamespace(fetchrow=AsyncMock())
    transaction = object()
    connection = SimpleNamespace(get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver)))
    return SimpleNamespace(
        info={}, get_transaction=lambda: transaction, connection=AsyncMock(return_value=connection)
    ), driver


@pytest.mark.asyncio
async def test_session_input_read_resolves_the_recipe_on_its_configured_session(monkeypatch):
    held = family()
    session, driver = _query_session()
    pinned = manifest(
        await inputs.prepare_em_provider_inputs(connection(), family(), runtime_roles=("runtime_reader",))
    )
    resolve = AsyncMock(return_value=held)
    monkeypatch.setattr(inputs, "resolve_em_provider_inputs", resolve)
    assert await inputs.resolve_em_provider_inputs_in_session(session, pinned, "control") is held
    resolve.assert_awaited_once_with(driver, pinned, "control", api_read=True)
    assert session.info == {}
    driver.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_canceled_input_resolution_preserves_the_transaction_owner(monkeypatch):
    session, _driver = _query_session()
    entered = asyncio.Event()

    async def pending(*_arguments, **_options):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(inputs, "resolve_em_provider_inputs", pending)
    pinned = manifest(
        await inputs.prepare_em_provider_inputs(connection(), family(), runtime_roles=("runtime_reader",))
    )
    transaction = session.get_transaction()
    task = asyncio.create_task(inputs.resolve_em_provider_inputs_in_session(session, pinned, "control"))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert session.get_transaction() is transaction and session.info == {}


def _legacy_provider_receipt():
    legacy = prepare()
    legacy.update(revision=1, runtime_roles=["old_reader"])
    for relation in legacy["relations"]:
        relation.update(
            relation_oid=1,
            relfilenode=2,
            schema_oid=3,
            owner_oid=4,
            owner_role="old_owner",
            shape_sha256="a" * 64,
            keys_sha256="b" * 64,
        )
    legacy["receipt_sha256"] = inputs._digest({key: value for key, value in legacy.items() if key != "receipt_sha256"})
    return legacy


def test_retained_legacy_provider_receipt_projects_names_and_ignores_physical_drift(monkeypatch):
    legacy = _legacy_provider_receipt()
    conn, pinned, _ = resolver(monkeypatch, receipt=legacy)
    for row in conn.fetch.return_value:
        row.update(relation_oid=999, relfilenode=999, owner_oid=999, owner_role="copied_owner")
    assert asyncio.run(inputs.resolve_em_provider_inputs(conn, pinned)) == family()
    assert legacy["relations"][0]["owner_oid"] == 4
    assert conn.fetch.await_count == 1
    assert "pg_roles" not in conn.fetch.await_args.args[0]
    changed = copy.deepcopy(legacy)
    changed["relations"][0]["table_name"] = "wrong_table"
    with pytest.raises(ValueError):
        inputs._receipt(changed)


def test_named_geo_rendering_decodes_legacy_receipts_without_reconstructing_physical_identity():
    from api import ptg2_geo_projection as projection

    decoded = inputs._receipt(_legacy_provider_receipt())
    bindings = decoded.geo_bindings("mrf")
    assert bindings == family().geo_bindings("mrf")
    assert all(set(binding) == {"schema_name", "table_name"} for binding in bindings.values())
    rendered = " ".join(
        projection.projection_dependency_relation_sql("mrf", name, dependency_bindings=bindings) for name in bindings
    )
    evidence = projection.legacy_evidence_source_id_sql(
        "addr", schema_name="mrf", dependency_bindings=bindings, unified_schema_name="network_snapshot"
    )
    assert '"retained_inputs"."held_4"' in rendered
    assert 'FROM "network_snapshot"."entity_address_unified" AS geo_nppes_anchor' in evidence
    for forbidden in ("relation_oid", "filenode", "pg_class", "pg_roles", "pg_backend_pid", "to_regclass"):
        assert forbidden not in rendered + evidence


def test_named_geo_validator_returns_a_detached_complete_qname_map():
    from api import ptg2_geo_projection as projection

    bindings = family().geo_bindings("mrf")
    normalized = projection.validate_projection_dependency_qnames("mrf", bindings)
    bindings["mrf.npi_address"]["table_name"] = "other_table"
    assert normalized == family().geo_bindings("mrf")


@pytest.mark.parametrize(
    "damage",
    ["missing", "extra_role", "extra_field", "legacy", "mixed", "schema_type", "table_type", "sql", "duplicate"],
)
def test_named_geo_validator_and_renderer_refuse_open_or_unsafe_maps(damage):
    from api import ptg2_geo_projection as projection

    bindings = family().geo_bindings("mrf")
    entry = bindings["mrf.npi_address"]
    match damage:
        case "missing":
            bindings.pop("tiger.zcta5")
        case "extra_role":
            bindings["mrf.unreviewed"] = dict(entry)
        case "extra_field":
            entry["sql"] = "SELECT 1"
        case "legacy" | "mixed":
            for index, binding in enumerate(bindings.values()):
                if damage == "legacy" or binding is entry:
                    binding.update(relation_oid=index + 1, relfilenode=index + 101)
        case "schema_type":
            entry["schema_name"] = 42
        case "table_type":
            entry["table_name"] = True
        case "sql":
            entry["table_name"] = "table; SELECT 1"
        case _:
            bindings["mrf.mrf_address"] = dict(entry)
    with pytest.raises(ValueError):
        projection.validate_projection_dependency_qnames("mrf", bindings)
    if damage != "legacy":
        with pytest.raises(ValueError):
            projection.projection_dependency_relation_sql("mrf", "mrf.npi_address", dependency_bindings=bindings)


@pytest.mark.parametrize("schema", [None, True, 42, "mrf; SELECT 1", "mrf.schema"])
def test_named_geo_validator_requires_a_safe_string_schema(schema):
    from api import ptg2_geo_projection as projection

    with pytest.raises(ValueError):
        projection.validate_projection_dependency_qnames(schema, family().geo_bindings("mrf"))


def test_named_geo_bindings_use_shared_signature_and_state_paths(monkeypatch):
    from api import ptg2_geo_projection as projection
    from process import entity_address_dependency_bindings as state

    bindings = family().geo_bindings("mrf")
    with pytest.raises(ValueError):
        projection.validate_projection_dependency_bindings("mrf", bindings)
    assert projection.validate_projection_dependency_qnames("mrf", bindings) == bindings
    match_sql = projection.projection_dependency_bindings_match_sql("mrf", bindings)
    lock_sql = projection.projection_dependency_lock_sql("mrf", dependency_bindings=bindings)
    signature_sql = projection.projection_relation_signature_sql("mrf", dependency_bindings=bindings)
    state_sql = state.record_geo_assurance_candidate_sql("mrf", "address_stage", 1, dependency_bindings=bindings)
    for binding in bindings.values():
        qualified_name = f'"{binding["schema_name"]}"."{binding["table_name"]}"'
        assert qualified_name in lock_sql and qualified_name in match_sql
        assert repr(binding["schema_name"]) in signature_sql and repr(binding["table_name"]) in signature_sql
    assert "ACCESS SHARE MODE" in lock_sql
    assert "relfilenode" not in state_sql and '"relation_oid"' not in state_sql
    assert "RETURNING candidate_geo_assurance_version" in state_sql
    invalid_binding_by_name = dict(bindings, unreviewed={"schema_name": "mrf", "table_name": "unreviewed"})
    with pytest.raises(ValueError):
        state.record_geo_assurance_candidate_sql("mrf", "address_stage", 1, dependency_bindings=invalid_binding_by_name)


def test_named_geo_signature_keeps_complete_qualified_dependency_map():
    from api import ptg2_geo_projection as projection

    bindings = family().geo_bindings("mrf")
    signature = projection.projection_relation_signature_sql("mrf", dependency_bindings=bindings)
    assert signature.startswith("jsonb_build_object(")
    for canonical, binding in bindings.items():
        assert repr(canonical) in signature
        assert f"jsonb_build_array({binding['schema_name']!r}, {binding['table_name']!r})" in signature
    assert "to_regclass" not in signature and "pg_relation_filenode" not in signature


def test_legacy_geo_token_changes_preserve_recorded_names_and_raw_editions():
    from api import ptg2_geo_projection as projection

    named = family().geo_bindings("mrf")
    legacy_by_name = {name: {**binding, "relation_oid": 17, "relfilenode": 29} for name, binding in named.items()}
    raw = copy.deepcopy(legacy_by_name)
    expected = projection.projection_relation_signature_sql("mrf", dependency_bindings=named)
    assert projection.projection_relation_signature_sql("mrf", dependency_bindings=legacy_by_name) == expected
    checks = projection.projection_dependency_bindings_match_sql("mrf", legacy_by_name)
    assert checks == projection.projection_named_dependency_match_sql("mrf", named)
    assert legacy_by_name == raw
    changed = copy.deepcopy(legacy_by_name)
    changed["mrf.npi_address"]["table_name"] = "another_named_heap"
    assert projection.projection_relation_signature_sql("mrf", dependency_bindings=changed) != expected


def test_geo_api_uses_named_projection_with_complete_semantic_fallback():
    from api import ptg2_geo_projection as projection

    state = projection.projection_state_available_sql("mrf")
    available = projection.projected_evidence_available_sql("address", schema_name="mrf")
    evidence = projection.projected_evidence_level_sql(
        "address", schema_name="mrf", legacy_level_sql="validated_evidence_level"
    )
    boolean = projection.projected_boolean_sql(
        "address", "has_geo_assurance", schema_name="mrf", legacy_sql="validated_row_predicate"
    )
    assert available == "FALSE"
    assert evidence == "(validated_evidence_level)"
    assert boolean == "(validated_row_predicate)"
    for forbidden in ("pg_relation_filenode", "pg_roles", "relowner", "nspowner", "active_table_oid"):
        assert forbidden not in state
    assert "active_relation_signature" in state and "active_dependency_bindings" in state
