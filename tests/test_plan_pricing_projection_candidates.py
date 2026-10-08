# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected candidate inputs reuse ordinary projection semantics and identity."""

import json
from collections import OrderedDict
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
    """Return synthetic identities, not a native custody or authorization fixture."""
    inputs = contract.ProjectionCandidateInputs(
        (PTG2CandidateAuditAccess("candidate-snapshot", "source", "plan", "group"),),
        tuple(
            (role, f"candidate_{index}", role.split(".")[-1], index + 10, index + 100)
            for index, role in enumerate(contract._CANDIDATE_PROVIDER_RELATIONS)
        ),
    )
    geo = EntityAddressGeoAssurancePreparation(
        next(entry[3] for entry in inputs.relations if entry[0] == "entity_address_unified"),
        5,
        tuple((name, entry["relation_oid"], entry["relfilenode"]) for name, entry in inputs.geo_bindings().items()),
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
    signature_by_relation = {
        name: [entry["relation_oid"], entry["relfilenode"]] for name, entry in inputs.geo_bindings().items()
    }
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
        state_by_field["candidate_table_oid"] += 1

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
            identity_by_role = {entry[0]: list(entry[3:]) for entry in inputs.relations}
            identity_by_field = {
                "npi": identity_by_role["npi"],
                "taxonomy": identity_by_role["npi_taxonomy"],
                "vocabulary": identity_by_role["nucc_taxonomy"],
                "address": identity_by_role["entity_address_unified"],
                "address_evidence": identity_by_role["entity_address_evidence"]
                + identity_by_role["entity_address_unified"],
                "zip": identity_by_role["geo_zip_lookup"],
                "geo_assurance": json.loads(parameters["candidate_geo"]),
                "geo_assurance_ready": True,
            }
            return _Result(scalar=json.dumps(identity_by_field))
        return _Result()

    return SimpleNamespace(execute=AsyncMock(side_effect=execute), in_transaction=lambda: True, statements=statements)


@pytest.mark.parametrize("change", ["partial", "duplicate", "identifier", "oid", "untrusted-access"])
def test_candidate_inputs_refuse_open_or_ambiguous_physical_scope(change):
    inputs = candidate_inputs()
    relations = list(inputs.relations)
    if change == "partial":
        relations.pop()
    elif change == "duplicate":
        relations[-1] = (*relations[-1][:3], relations[0][3], relations[-1][4])
    elif change == "identifier":
        relations[0] = (relations[0][0], 'unsafe"; SELECT 1', *relations[0][2:])
    elif change == "oid":
        relations[0] = (*relations[0][:3], True, relations[0][4])
    with pytest.raises(ValueError):
        replace(
            inputs,
            relations=tuple(relations),
            candidate_access=(object(),) if change == "untrusted-access" else inputs.candidate_access,
        )


@pytest.mark.asyncio
async def test_future_provider_signature_survives_physical_relation_rename():
    inputs = candidate_inputs()
    original = provider_session(inputs)
    digest = await contract.provider_signature(original, candidate_inputs=inputs)
    renamed = replace(
        inputs,
        relations=tuple(
            (role, "published", f"live_{index}", oid, filenode)
            for index, (role, _schema, _relation, oid, filenode) in enumerate(inputs.relations)
        ),
    )
    assert await contract.provider_signature(provider_session(renamed), candidate_inputs=renamed) == digest
    sql, parameters = original.statements[-1]
    assert "active_geo_assurance_version" not in sql
    assert parameters["npi_relation"] == inputs.relation("npi")
    assert original.statements[0][0] == "SHOW transaction_isolation"
    assert original.statements[1][0].startswith("LOCK TABLE ONLY ")
    catalog_sql, catalog_parameters = original.statements[2]
    assert "NOT relation.relispartition OR relation.oid=ANY" in catalog_sql
    assert catalog_parameters["tiger_oids"] == [entry[3] for entry in inputs.relations if entry[0].startswith("tiger.")]
    assert any("FOR SHARE" in statement for statement, _parameters in original.statements)


@pytest.mark.asyncio
async def test_candidate_signature_matches_published_oid_encoding():
    """Match PostgreSQL's oid JSON string without changing the numeric custody identity."""
    inputs = candidate_inputs()
    candidate = provider_session(inputs)
    candidate_digest = await contract.provider_signature(candidate, candidate_inputs=inputs)
    identities_by_role = {entry[0]: list(entry[3:]) for entry in inputs.relations}
    published_by_field = {
        "npi": identities_by_role["npi"],
        "taxonomy": identities_by_role["npi_taxonomy"],
        "vocabulary": identities_by_role["nucc_taxonomy"],
        "address": identities_by_role["entity_address_unified"],
        "address_evidence": identities_by_role["entity_address_evidence"]
        + identities_by_role["entity_address_unified"],
        "zip": identities_by_role["geo_zip_lookup"],
        "geo_assurance": {
            "version": 1,
            "table_oid": str(inputs.geo_preparation.stage_table_oid),
            "signature": {
                name: [entry["relation_oid"], entry["relfilenode"]] for name, entry in inputs.geo_bindings().items()
            },
        },
        "geo_assurance_ready": True,
    }
    published = SimpleNamespace(execute=AsyncMock(return_value=_Result(scalar=json.dumps(published_by_field))))
    assert candidate_digest == await contract.provider_signature(published)
    assert json.loads(candidate.statements[-1][1]["candidate_geo"]) == published_by_field["geo_assurance"]
    assert type(inputs.geo_preparation.stage_table_oid) is int
    published_sql = str(published.execute.await_args.args[0])
    assert "'table_oid', active_table_oid," in published_sql
    assert "active_table_oid::" not in published_sql


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
        projected_rows=expected.projected_rows,
        dependency_bindings=inputs.geo_bindings(),
        publisher_selected_inputs_sha256="a" * 64,
    )


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
        schema_oid=11,
        payload_snapshot_id="source-snapshot",
        relation_oids=(("code", 21),),
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
    physical.schema_oid += 1
    _, separate_identity = await source.binding_source(session, {**binding(), "snapshot_id": tables.snapshot_id})
    assert separate_identity != first_identity
    physical.schema_oid -= 1
    physical.relation_oids = (("code", 22),)
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
