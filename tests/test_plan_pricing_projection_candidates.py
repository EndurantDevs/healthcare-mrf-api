# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected candidate inputs reuse ordinary projection semantics and identity."""

import json
from collections import OrderedDict
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import plan_pricing_projection_build as build
from api import plan_pricing_projection_contract as contract
from api import plan_pricing_projection_source as source
from api import ptg2_serving as serving
from api import ptg2_tables
from api.ptg2_candidate_audit import PTG2CandidateAuditAccess
from process.entity_address_snapshot_destination import EntityAddressGeoAssurancePreparation
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


def candidate_inputs():
    """Return named inputs and synthetic portable family publications."""
    inputs = contract.ProjectionCandidateInputs(
        (PTG2CandidateAuditAccess("candidate-snapshot", "source", "plan", "group"),),
        tuple(
            (role, contract.SCHEMA if role == "entity_address_unified" else f"candidate_{index}", role.split(".")[-1])
            for index, role in enumerate(contract._CANDIDATE_PROVIDER_RELATIONS)
        ),
        provider_generations=tuple(
            (family, f"00000000-0000-0000-0000-{index + 1:012d}", index + 1)
            for index, family in enumerate(contract._PROVIDER_GENERATIONS)
        ),
    )
    geo = EntityAddressGeoAssurancePreparation(
        None,
        5,
        tuple((name, entry["schema_name"], entry["table_name"]) for name, entry in inputs.geo_bindings().items()),
        stage_relation=(contract.SCHEMA, "entity_address_unified"),
    )
    return replace(inputs, geo_preparation=geo)


def binding():
    return {
        "snapshot_id": "candidate-snapshot",
        "source_key": "source",
        "plan_id": "plan",
        "market_type": "group",
        "role": "in_network",
        "ordinal": 0,
    }


class _Result:
    def __init__(self, rows=(), scalar=None, row=None):
        self.rows, self.scalar, self.row = rows, scalar, row

    def __iter__(self):
        return iter(self.rows)

    def scalar_one(self):
        return self.scalar

    def scalar_one_or_none(self):
        return self.scalar

    def mappings(self):
        return self

    def one_or_none(self):
        return self.row


def provider_session(inputs, *, change=None):
    statements = []
    signature_by_relation = {name: [schema, table] for name, schema, table in inputs.geo_preparation.relation_signature}
    state_by_field = {
        "candidate_geo_assurance_version": 1,
        "candidate_table_oid": inputs.geo_preparation.stage_table_oid,
        "candidate_projected_rows": 5,
        "candidate_relation_signature": signature_by_relation,
        "current_signature": signature_by_relation,
        "candidate_dependency_bindings": inputs.geo_bindings(),
        "bindings_match": True,
    }
    if change == "geo-state":
        state_by_field["candidate_projected_rows"] += 1

    async def execute(statement, parameters=None):
        sql = str(statement)
        statements.append((sql, parameters))
        if sql == "SHOW transaction_isolation":
            return _Result(scalar="read committed" if change == "isolation" else "repeatable read")
        if "pg_catalog.pg_class relation" in sql:
            rows = [entry[1:] for entry in inputs.relations]
            return _Result(rows=rows[:-1] if change == "catalog" else rows)
        if "geo_assurance_state" in sql:
            return _Result(row=state_by_field)
        if "jsonb_build_object" in sql:
            identity_by_field = {
                "contract": contract._PROVIDER_SIGNATURE_CONTRACT,
                "generations": json.loads(parameters["candidate_generations"]),
                "relations_available": True,
                "geo_assurance": json.loads(parameters["candidate_geo"]),
                "geo_assurance_ready": True,
            }
            return _Result(scalar=json.dumps(identity_by_field))
        return _Result()

    return SimpleNamespace(execute=AsyncMock(side_effect=execute), in_transaction=lambda: True, statements=statements)


@pytest.mark.parametrize("change", ["partial", "duplicate", "identifier", "tuple", "untrusted-access"])
def test_candidate_inputs_refuse_open_or_ambiguous_named_scope(change):
    inputs = candidate_inputs()
    relations = list(inputs.relations)
    if change == "partial":
        relations.pop()
    elif change == "duplicate":
        relations[-1] = (relations[-1][0], *relations[0][1:])
    elif change == "identifier":
        relations[0] = (relations[0][0], 'unsafe"; SELECT 1', *relations[0][2:])
    elif change == "tuple":
        relations[0] = (*relations[0], None)
    with pytest.raises(ValueError):
        replace(
            inputs,
            relations=tuple(relations),
            candidate_access=(object(),) if change == "untrusted-access" else inputs.candidate_access,
        )


@pytest.mark.asyncio
async def test_changed_selected_names_require_new_geo_preparation():
    inputs = candidate_inputs()
    original = provider_session(inputs)
    digest = await contract.provider_signature(original, candidate_inputs=inputs)
    renamed = replace(
        inputs,
        relations=tuple(
            (role, "published", f"live_{index}") for index, (role, _schema, _relation) in enumerate(inputs.relations)
        ),
    )
    assert len(digest) == 64
    with pytest.raises(ValueError, match="geo (authority changed|stage differs)"):
        await contract.provider_signature(provider_session(renamed), candidate_inputs=renamed)
    sql, parameters = original.statements[-1]
    assert "active_geo_assurance_version" not in sql
    assert parameters["npi_relation"] == inputs.relation("npi")
    assert original.statements[0][0] == "SHOW transaction_isolation"
    assert original.statements[1][0].startswith("LOCK TABLE ONLY ")
    catalog_sql, catalog_parameters = original.statements[2]
    assert "NOT relation.relispartition OR" in catalog_sql
    assert catalog_parameters["tiger_names"] == [
        f"{schema}.{relation}" for role, schema, relation in inputs.relations if role.startswith("tiger.")
    ]
    assert "relfilenode" not in catalog_sql and "relation.oid::" not in catalog_sql
    assert any("FOR SHARE" in statement for statement, _parameters in original.statements)


@pytest.mark.asyncio
async def test_candidate_signature_matches_transferred_publication_origins():
    """Stage names and discarded legacy tuple tails cannot change publication identity."""
    inputs = candidate_inputs()
    candidate = provider_session(inputs)
    candidate_digest = await contract.provider_signature(candidate, candidate_inputs=inputs)
    published_by_field = {
        "contract": contract._PROVIDER_SIGNATURE_CONTRACT,
        "generations": json.loads(candidate.statements[-1][1]["candidate_generations"]),
        "relations_available": True,
        "geo_assurance": {
            "version": 1,
            "signature": inputs.geo_bindings(),
        },
        "geo_assurance_ready": True,
    }
    published = SimpleNamespace(
        execute=AsyncMock(return_value=_Result(scalar=json.dumps(published_by_field))),
        scalar=AsyncMock(return_value=inputs.geo_bindings()),
    )
    assert candidate_digest == await contract.provider_signature(published)
    assert json.loads(candidate.statements[-1][1]["candidate_geo"]) == published_by_field["geo_assurance"]
    assert inputs.geo_preparation.stage_table_oid is None
    published_sql = str(published.execute.await_args.args[0])
    assert "origin_lineage_id" in published_sql and "origin_generation" in published_sql
    assert "active_table_oid" not in published_sql and "pg_relation_filenode" not in published_sql
    renamed = replace(
        inputs,
        relations=tuple(
            (
                role,
                "relocated"
                if role
                not in {
                    "entity_address_unified",
                    *(name.removeprefix(f"{contract.SCHEMA}.") for name in inputs.geo_bindings()),
                }
                else schema,
                name,
            )
            for role, schema, name in inputs.relations
        ),
    )
    assert await contract.provider_signature(provider_session(renamed), candidate_inputs=renamed) == candidate_digest
    for family in contract._PROVIDER_GENERATIONS:
        changed_by_field = deepcopy(published_by_field)
        changed_by_field["generations"][family]["origin_generation"] += 1
        assert contract._validated_provider_signature(json.dumps(changed_by_field)) != candidate_digest


def test_legacy_candidate_tuple_tails_are_discarded_without_identity_checks():
    inputs = candidate_inputs()
    legacy = replace(inputs, relations=tuple((*entry, None, "unrelated") for entry in inputs.relations))
    assert legacy == inputs
    assert all(len(entry) == 3 for entry in legacy.relations)


@pytest.mark.parametrize("change", ["missing", "lineage", "generation", "boolean", "extra"])
def test_candidate_requires_complete_portable_origins(change):
    inputs = candidate_inputs()
    generations = list(inputs.provider_generations)
    if change == "missing":
        generations.pop()
    elif change == "extra":
        generations.append(("unknown", generations[0][1], 1))
    else:
        family, lineage, generation = generations[0]
        generations[0] = (
            family,
            "unknown" if change == "lineage" else lineage,
            True if change == "boolean" else 0 if change == "generation" else generation,
        )
    with pytest.raises(ValueError, match="provider"):
        replace(inputs, provider_generations=tuple(generations))


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [False, True])
async def test_final_cut_recaptures_the_ordinary_signature_under_native_metadata_locks(monkeypatch, changed):
    """Final publication must match the prepared heaps, without opening another transaction."""
    session = SimpleNamespace(execute=AsyncMock(), in_transaction=lambda: True)
    signature = AsyncMock(return_value=("c" if changed else "b") * 64)
    monkeypatch.setattr(contract, "provider_signature", signature)
    if changed:
        with pytest.raises(ValueError, match="provider generation changed"):
            await contract.require_published_provider_generation(session, expected_signature="b" * 64)
    else:
        await contract.require_published_provider_generation(session, expected_signature="b" * 64)
    signature.assert_awaited_once_with(session)
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert len(statements) == 3
    assert all(statement.startswith("LOCK TABLE ") for statement in statements[:2])
    assert all(" IN ACCESS SHARE MODE" in statement for statement in statements[:2])
    assert "entity_address_geo_assurance_state" in statements[-1] and statements[-1].endswith("FOR SHARE")
    assert all("SET TRANSACTION" not in statement for statement in statements)


@pytest.mark.asyncio
async def test_final_provider_fence_requires_a_caller_transaction():
    session = SimpleNamespace(execute=AsyncMock(), in_transaction=lambda: False)
    with pytest.raises(ValueError, match="bound transaction"):
        await contract.require_published_provider_generation(session, expected_signature="b" * 64)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["catalog", "geo-state", "isolation"])
async def test_changed_native_inputs_refuse_before_signature_or_provider_reads(change):
    inputs = candidate_inputs()
    session = provider_session(inputs, change=change)
    with pytest.raises((ValueError, RuntimeError)):
        await contract.provider_signature(session, candidate_inputs=inputs)
    assert not any("CAST(:candidate_geo AS jsonb)" in statement for statement, _parameters in session.statements)


@pytest.mark.asyncio
async def test_geo_recapture_preserves_the_sealed_selected_input_digest(monkeypatch):
    """The projection must not silently discard destination-selected geo authority."""
    from process import entity_address_snapshot_destination as destination

    inputs = candidate_inputs()
    expected = SimpleNamespace(
        stage_table_oid=inputs.geo_preparation.stage_table_oid,
        projected_rows=inputs.geo_preparation.projected_rows,
        publisher_selected_inputs_sha256="a" * 64,
        stage_relation=inputs.geo_preparation.stage_relation,
    )
    inputs = replace(inputs, geo_preparation=expected)
    capture = AsyncMock(return_value=expected)
    monkeypatch.setattr(destination, "EntityAddressGeoAssurancePreparation", SimpleNamespace)
    monkeypatch.setattr(destination, "_capture_geo_preparation", capture)
    session = object()
    await contract._candidate_geo_identity(session, inputs, None)
    capture.assert_awaited_once_with(
        session,
        db_schema=contract.SCHEMA,
        stage_table_oid=expected.stage_table_oid,
        stage_relation_name=expected.stage_relation[1],
        projected_rows=expected.projected_rows,
        dependency_bindings=inputs.geo_bindings(),
        publisher_selected_inputs_sha256="a" * 64,
    )


def _retained_candidate_inputs(binding_mode):
    inputs = candidate_inputs()
    dependency_roles = {name.removeprefix(f"{contract.SCHEMA}.") for name in inputs.geo_bindings()}
    relations = []
    for role, schema, relation in inputs.relations:
        if role in dependency_roles or role == "entity_address_unified":
            schema, relation = role.split(".") if role.startswith("tiger.") else (contract.SCHEMA, role)
        if role in dependency_roles and binding_mode.startswith("held"):
            schema = "held_inputs"
        relations.append((role, schema, relation))
    inputs = replace(inputs, relations=tuple(relations), geo_preparation=None)
    active_bindings_by_name = inputs.geo_bindings()
    if binding_mode == "canonical_null":
        active_bindings_by_name = None
    elif binding_mode == "held_legacy":
        active_bindings_by_name = {
            name: {**entry, "relation_oid": None, "relfilenode": 1} for name, entry in active_bindings_by_name.items()
        }
    return inputs, {"active_geo_assurance_version": 1, "active_dependency_bindings": active_bindings_by_name}


@pytest.mark.asyncio
@pytest.mark.parametrize("binding_mode", ["canonical_null", "canonical_named", "held", "held_legacy"])
async def test_retained_geo_requires_matching_named_active_inputs(binding_mode):
    inputs, state = _retained_candidate_inputs(binding_mode)
    session = SimpleNamespace(execute=AsyncMock(return_value=_Result(scalar=True)))
    identity = await contract._candidate_geo_identity(session, inputs, state)
    assert identity["signature"] == inputs.geo_bindings()
    assert identity["version"] == 1
    readiness_sql = str(session.execute.await_args.args[0])
    assert ("FROM mrf.npi_result_generation" in readiness_sql) is not binding_mode.startswith("held")


@pytest.mark.asyncio
@pytest.mark.parametrize("binding_mode", ["canonical_null", "held"])
@pytest.mark.parametrize(
    "changed_role",
    [
        "entity_address_unified",
        "npi_address",
        "mrf_address",
        "doctor_clinician_address",
        "geo_zip_lookup",
        "tiger.zip_state",
        "tiger.zcta5",
    ],
)
async def test_retained_geo_refuses_a_different_address_or_dependency(binding_mode, changed_role):
    inputs, state = _retained_candidate_inputs(binding_mode)
    inputs = replace(
        inputs,
        relations=tuple(
            (role, schema, relation + "_older" if role == changed_role else relation)
            for role, schema, relation in inputs.relations
        ),
    )
    session = SimpleNamespace(execute=AsyncMock(return_value=_Result(scalar=True)))
    with pytest.raises(ValueError, match="retained geo authority changed"):
        await contract._candidate_geo_identity(session, inputs, state)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_retained_geo_keeps_the_logical_generation_readiness_check():
    inputs, state = _retained_candidate_inputs("canonical_named")
    session = SimpleNamespace(execute=AsyncMock(return_value=_Result(scalar=False)))
    with pytest.raises(ValueError, match="retained geo authority changed"):
        await contract._candidate_geo_identity(session, inputs, state)
    assert "origin_generation" in str(session.execute.await_args.args[0])


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["missing_state", "version", "incomplete_map", "address_schema"])
async def test_retained_geo_refuses_incomplete_or_unrelated_publication_state(change):
    inputs, state = _retained_candidate_inputs("canonical_named")
    if change == "missing_state":
        state = None
    elif change == "version":
        state["active_geo_assurance_version"] = 2
    elif change == "incomplete_map":
        state["active_dependency_bindings"].pop(f"{contract.SCHEMA}.mrf_address")
    else:
        inputs = replace(
            inputs,
            relations=tuple(
                (role, "older" if role == "entity_address_unified" else schema, relation)
                for role, schema, relation in inputs.relations
            ),
        )
    session = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(ValueError):
        await contract._candidate_geo_identity(session, inputs, state)
    session.execute.assert_not_awaited()


def test_candidate_provider_and_provenance_queries_use_the_same_closed_relations():
    inputs = candidate_inputs()
    provider_sql = source._provider_rows_sql(candidate_inputs=inputs)
    lineage_sql = serving._address_provenance_sql(inputs)
    for role in ("npi", "npi_taxonomy", "nucc_taxonomy", "entity_address_unified"):
        assert inputs.relation(role) in provider_sql
    for role in (
        "entity_address_unified",
        "entity_address_evidence",
        "npi_address",
        "mrf_address",
        "doctor_clinician_address",
    ):
        assert inputs.relation(role) in lineage_sql
    assert "active_table_oid" not in provider_sql
    assert "geo_evidence_source_id" in provider_sql
    assert "{8}" in lineage_sql and "$unified_relation" not in lineage_sql
    assert serving._address_provenance_sql() == serving._ADDRESS_PROVENANCE_SQL


@pytest.mark.asyncio
async def test_local_network_descriptors_are_resolved_in_each_read_transaction(monkeypatch):
    """A cached physical inventory is not a transaction's native read fence."""
    tables = SimpleNamespace(snapshot_id="snapshot-a", physical_binding=object())
    cache = OrderedDict()
    resolver = AsyncMock(return_value=tables)
    monkeypatch.setattr(serving, "_PTG2_NETWORK_SERVING_TABLES_CACHE", cache)
    monkeypatch.setattr(serving, "snapshot_serving_tables", resolver)
    for session in (object(), object()):
        assert await serving._network_tables_by_snapshot_id(session, [("source", "snapshot-a")]) == {
            "snapshot-a": tables
        }
        assert resolver.await_args.args == (session, "snapshot-a")
        assert not cache
    assert resolver.await_count == 2
    assert not serving._is_network_serving_tables_current(tables, {})


@pytest.mark.asyncio
async def test_local_descriptor_reresolution_uses_only_explicit_candidate_access(monkeypatch):
    """A descriptor can request the same metadata, but cannot mint read authority."""
    physical = SimpleNamespace(relation_oids=(("code", 21),), sequence_oids=())
    cached = SimpleNamespace(
        snapshot_id="candidate-snapshot", physical_binding=physical, provider_tax_identity_source_publication=object()
    )
    refreshed = SimpleNamespace(snapshot_id=cached.snapshot_id, physical_binding=physical)
    resolver = AsyncMock(return_value=refreshed)
    monkeypatch.setattr(ptg2_tables, "snapshot_serving_tables", resolver)
    for access in (None, candidate_inputs().candidate_access[0]):
        session = object()
        assert (
            await ptg2_tables.read_serving_tables(
                session, cached.snapshot_id, serving_tables=cached, candidate_audit_access=access
            )
            is refreshed
        )
        resolver.assert_awaited_with(
            session, cached.snapshot_id, candidate_audit_access=access, include_billing_tax_identity_source=True
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["requested-snapshot", "resolved-snapshot", "heap", "sequence", "missing-binding"])
async def test_local_descriptor_rejects_changed_native_identity(monkeypatch, changed):
    physical = SimpleNamespace(relation_oids=(("code", 21),), sequence_oids=(("key_seq", 22),))
    cached = SimpleNamespace(snapshot_id="candidate-snapshot", physical_binding=physical)
    refreshed = SimpleNamespace(snapshot_id=cached.snapshot_id, physical_binding=SimpleNamespace(**vars(physical)))
    if changed == "resolved-snapshot":
        refreshed.snapshot_id = "another-snapshot"
    elif changed == "heap":
        refreshed.physical_binding.relation_oids = (("code", 23),)
    elif changed == "sequence":
        refreshed.physical_binding.sequence_oids = (("key_seq", 24),)
    elif changed == "missing-binding":
        refreshed.physical_binding = None
    resolver = AsyncMock(return_value=refreshed)
    monkeypatch.setattr(ptg2_tables, "snapshot_serving_tables", resolver)
    with pytest.raises(PTG2ManifestArtifactError, match="physical read"):
        await ptg2_tables.read_serving_tables(
            object(),
            "another-snapshot" if changed == "requested-snapshot" else cached.snapshot_id,
            serving_tables=cached,
        )
    assert resolver.await_count == (0 if changed == "requested-snapshot" else 1)


@pytest.mark.asyncio
async def test_legacy_descriptor_reuse_and_uncached_resolution_are_unchanged(monkeypatch):
    tables = SimpleNamespace(snapshot_id="legacy-snapshot", physical_binding=None)
    resolver = AsyncMock(return_value=tables)
    monkeypatch.setattr(ptg2_tables, "snapshot_serving_tables", resolver)
    session = object()
    assert await ptg2_tables.read_serving_tables(session, tables.snapshot_id, serving_tables=tables) is tables
    resolver.assert_not_awaited()
    assert await ptg2_tables.read_serving_tables(session, tables.snapshot_id) is tables
    resolver.assert_awaited_once_with(session, tables.snapshot_id, candidate_audit_access=None)


@pytest.mark.asyncio
@pytest.mark.parametrize("reader", ["forward", "reverse", "plan-code", "projection"])
async def test_cached_local_readers_refuse_custody_loss_before_payload_query(monkeypatch, reader):
    cached = SimpleNamespace(snapshot_id="candidate-snapshot", physical_binding=object())
    resolver = AsyncMock(side_effect=PTG2ManifestArtifactError("physical custody lost"))
    monkeypatch.setattr(ptg2_tables, "snapshot_serving_tables", resolver)
    monkeypatch.setattr(serving, "_ptg2_manifest_plan_code_values", lambda _args: ("plan", "CPT", "99213"))
    session = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(PTG2ManifestArtifactError, match="physical custody lost"):
        if reader == "forward":
            await serving._search_one_ptg2_snapshot(session, cached.snapshot_id, {}, None, serving_tables=cached)
        elif reader == "reverse":
            await serving._search_ptg2_provider_procedures_snapshot(
                session, 1234567890, {}, None, snapshot_id=cached.snapshot_id, serving_tables=cached
            )
        elif reader == "plan-code":
            await serving._has_snapshot_plan_code(session, cached.snapshot_id, {}, serving_tables=cached)
        else:
            await source.binding_projection(session, binding(), serving_tables=cached)
    resolver.assert_awaited_once_with(session, cached.snapshot_id, candidate_audit_access=None)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_candidate_binding_never_falls_back_to_unprivileged_serving(monkeypatch):
    inputs = candidate_inputs()
    resolver = AsyncMock(side_effect=RuntimeError("protected candidate unavailable"))
    monkeypatch.setattr(source, "snapshot_serving_tables", resolver)
    with pytest.raises(RuntimeError, match="protected candidate unavailable"):
        await source.binding_source(object(), binding(), candidate_audit_access=inputs.access_for(binding()))
    assert resolver.await_args.kwargs == {"candidate_audit_access": inputs.candidate_access[0]}
    with pytest.raises(ValueError, match="authority differs"):
        await source.binding_source(
            object(), binding(), candidate_audit_access=replace(inputs.candidate_access[0], source_key="other")
        )
    assert resolver.await_count == 1


@pytest.mark.asyncio
async def test_candidate_read_dedup_includes_authenticated_physical_family_not_just_source_layout(monkeypatch):
    """Independent source databases may legitimately reuse snapshot keys and coverage scopes."""
    physical = SimpleNamespace(
        dataset_id="candidate-dataset",
        schema_name="payload_one",
        payload_snapshot_id="source-snapshot",
        relations=(("payload_one", "code"),),
    )
    tables = SimpleNamespace(
        snapshot_id="candidate-snapshot",
        source_key="source",
        storage_generation="shared_blocks_v4",
        uses_shared_blocks=True,
        shared_snapshot_key=7,
        coverage_scope_id="c" * 64,
        physical_binding=physical,
    )
    monkeypatch.setattr(source, "snapshot_serving_tables", AsyncMock(return_value=tables))
    monkeypatch.setattr(serving, "_require_strict_shared_v3", lambda _tables: None)
    session = SimpleNamespace(execute=AsyncMock(return_value=_Result(scalar="group")))
    _, first_identity = await source.binding_source(session, binding())
    tables.snapshot_id = "another-candidate"
    _, reused_identity = await source.binding_source(session, {**binding(), "snapshot_id": tables.snapshot_id})
    assert first_identity == reused_identity
    physical.schema_name = "payload_two"
    _, separate_identity = await source.binding_source(session, {**binding(), "snapshot_id": tables.snapshot_id})
    assert separate_identity != first_identity
    physical.schema_name = "payload_one"
    physical.relations = (("payload_one", "code_v2"),)
    _, replaced_identity = await source.binding_source(session, {**binding(), "snapshot_id": tables.snapshot_id})
    assert replaced_identity != first_identity
    tables.physical_binding = None
    _, legacy_identity = await source.binding_source(session, {**binding(), "snapshot_id": tables.snapshot_id})
    assert legacy_identity == ("shared_blocks_v4", 7, "c" * 64, "plan", "group")


@pytest.mark.asyncio
async def test_ready_projection_reuse_still_requires_native_candidate_read_authority(monkeypatch):
    inputs = candidate_inputs()
    session = SimpleNamespace(execute=AsyncMock())
    monkeypatch.setattr(build, "provider_signature", AsyncMock(return_value="b" * 64))
    reader = AsyncMock(side_effect=RuntimeError("candidate withdrawn"))
    existing = AsyncMock(return_value={"state": "ready"})
    monkeypatch.setattr(build, "binding_source", reader)
    monkeypatch.setattr(build, "_existing_candidate_receipt", existing)
    with pytest.raises(RuntimeError, match="candidate withdrawn"):
        await build.build_in_session(
            session, binding_manifest_digest="a" * 64, bindings=[binding()], candidate_inputs=inputs
        )
    existing.assert_not_awaited()
    session.execute.assert_not_awaited()
    assert reader.await_args.kwargs == {"candidate_audit_access": inputs.candidate_access[0]}


@pytest.mark.asyncio
async def test_candidate_build_reuses_authenticated_reader_and_provider_inputs(monkeypatch):
    """The ordinary materializer receives the same selected sources, never an incumbent fallback."""
    inputs = candidate_inputs()
    session = SimpleNamespace(execute=AsyncMock())
    serving_tables = object()
    read_identity = ("candidate-snapshot", 1, "provider", "price", "map")
    reader = AsyncMock(return_value=(serving_tables, read_identity))
    provider_signature = AsyncMock(return_value="b" * 64)
    code_projection = SimpleNamespace(raw_code_row_count=1)
    materializer = AsyncMock(return_value=object())
    monkeypatch.setattr(build, "provider_signature", provider_signature)
    monkeypatch.setattr(build, "binding_source", reader)
    monkeypatch.setattr(build, "binding_projection", AsyncMock(return_value=code_projection))
    monkeypatch.setattr(build, "_existing_candidate_receipt", AsyncMock(return_value=None))
    monkeypatch.setattr(build, "_insert_candidate", AsyncMock())
    monkeypatch.setattr(build, "materialize_factorized_projection", materializer)
    monkeypatch.setattr(build, "validate_stored_aggregate_packs", AsyncMock())
    monkeypatch.setattr(build, "_seal_candidate", AsyncMock(return_value={"state": "ready"}))
    assert await build.build_in_session(
        session, binding_manifest_digest="a" * 64, bindings=[binding()], candidate_inputs=inputs
    ) == {"state": "ready"}
    provider_signature.assert_awaited_once_with(session, candidate_inputs=inputs)
    reader.assert_awaited_once_with(session, binding(), candidate_audit_access=inputs.candidate_access[0])
    assert build.binding_projection.await_args.kwargs["candidate_audit_access"] == inputs.candidate_access[0]
    assert materializer.await_args.args[2] == [code_projection]
    assert materializer.await_args.kwargs == {"candidate_inputs": inputs}
