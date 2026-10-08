# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Host refusal regressions; native roles, SQL effects and rollback need PostgreSQL qualification."""

from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import reference_family_archive as archive
from process import reference_family_dictionary as dictionary
from process import reference_family_result_generation as generation
from tests.test_reference_family_dictionary import _ownership, _session
from tests.test_reference_family_source_copy import (
    _nucc_abandoned_run,
    _nucc_completed_handoff,
    _nucc_stage_receipt,
)


def _resign_stage(stage):
    stage["stage_sha256"] = archive.nucc_native_digest(
        {key: value for key, value in stage.items() if key != "stage_sha256"}
    )
    return stage


def _legacy_stage():
    stage = _nucc_stage_receipt()
    stage["contract"] = "nucc-native-stage.v1"
    del stage["incumbent"], stage["incumbent_relation_oid"]
    return _resign_stage(stage)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed_field", (None, "relation_oid", "owner_oid"))
async def test_immutable_storage_binds_live_relation_and_protected_owner(monkeypatch, changed_field):
    session = _session()
    stage = _nucc_stage_receipt()["stage"]
    if changed_field is not None:
        stage[changed_field] += 1
    seal = AsyncMock(return_value=45)
    catalog = AsyncMock(return_value=stage)
    monkeypatch.setattr(generation, "_require_sealed_nucc_storage", seal)
    monkeypatch.setattr(archive, "_nucc_native_stage", catalog)
    if changed_field is None:
        assert (
            await generation.require_immutable_nucc_storage(session, schema_name="mrf", expected_relation_oid=43)
            is stage
        )
    else:
        with pytest.raises(RuntimeError, match="storage identity differs"):
            await generation.require_immutable_nucc_storage(session, schema_name="mrf", expected_relation_oid=43)
    seal.assert_awaited_once_with(session, "mrf", 43)
    catalog.assert_awaited_once_with(session, "mrf", "nucc_taxonomy")
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("transaction,oid", ((False, 43), (True, True), (True, 0), (True, None)))
async def test_sealed_storage_rejects_invalid_scope_before_catalog_or_lock(transaction, oid):
    session = _session()
    session.in_transaction = lambda: transaction
    with pytest.raises(RuntimeError, match="transaction or identity is invalid"):
        await generation._require_sealed_nucc_storage(session, "mrf", oid)
    session.scalar.assert_not_awaited()
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("owner,relation", ((None, 43), (0, 43), (True, 43), (45, None), (45, 44)))
async def test_sealed_storage_rejects_missing_owner_or_replaced_heap(monkeypatch, owner, relation):
    session = _session()
    session.scalar.side_effect = [owner, relation]
    columns = AsyncMock()
    monkeypatch.setattr(archive, "_require_nucc_native_columns", columns)
    with pytest.raises(RuntimeError, match="protected owner is unavailable|storage identity differs"):
        await generation._require_sealed_nucc_storage(session, "mrf", 43)
    columns.assert_not_awaited()
    assert session.execute.await_count == int(type(owner) is int and owner > 0)


@pytest.mark.asyncio
@pytest.mark.parametrize("adopt,failure", ((False, "cas"), (True, "cas"), (True, "stale"), (False, "exhausted")))
async def test_immutable_generation_cannot_accept_failed_cas_or_exhausted_authority(monkeypatch, adopt, failure):
    original = archive._nucc_result_authority(_nucc_stage_receipt()["incumbent"], allow_untracked=True)
    if failure == "exhausted":
        original = replace(original, local_generation=generation._MAX_GENERATION)
    expected = original.as_dict()
    if failure == "stale":
        expected["local_generation"] += 1
    monkeypatch.setattr(archive, "protected_publisher_owner", AsyncMock(return_value=45))
    monkeypatch.setattr(generation, "require_immutable_nucc_storage", AsyncMock())
    monkeypatch.setattr(
        generation, "read_reference_family_result_generation_authority", AsyncMock(return_value=original)
    )
    update = AsyncMock(return_value=None)
    monkeypatch.setattr(generation, "_first", update)
    arguments_by_field = {"schema_name": "mrf", "expected_authority": expected, "expected_relation_oid": 43}
    with pytest.raises(RuntimeError, match="changed|predecessor differs"):
        if adopt:
            await generation.adopt_immutable_nucc_generation(
                _session(),
                source_generation={
                    "origin_lineage_id": original.local_lineage_id,
                    "origin_generation": 1,
                    "published_at": "2026-10-01T00:00:00Z",
                },
                **arguments_by_field,
            )
        else:
            await generation.publish_immutable_nucc_generation(_session(), **arguments_by_field)
    assert update.await_count == int(failure == "cas")
    if failure == "cas":
        assert update.await_args.kwargs["prior"] == 0
        assert update.await_args.kwargs["lineage"] == original.local_lineage_id
        assert update.await_args.kwargs["oids"] == [43]


@pytest.mark.asyncio
@pytest.mark.parametrize("safe", (True, False, None, 1))
async def test_native_builder_requires_explicit_genuine_login_and_exact_owner(safe):
    session = _session()
    session.scalar.return_value = safe
    if safe is True:
        await generation._require_nucc_native_builder(session, expected_owner_oid=45)
    else:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="Builder authority differs"):
            await generation._require_nucc_native_builder(session, expected_owner_oid=45)
    query, parameters = session.scalar.await_args.args
    assert parameters == {"expected": 45}
    assert "session_user=current_user" in str(query)
    assert "NOT pg_has_role(builder.oid,namespace.nspowner,'USAGE')" in str(query)
    assert "builder.oid=CAST(:expected AS bigint)" in str(query)
    assert "current_setting('session_replication_role')='origin'" in str(query)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault,match",
    (
        (None, None),
        ("legacy", None),
        ("finished", "attempt is closed"),
        ("handoff", "attempt is closed"),
        ("missing-receipt", "persisted custody differs"),
        ("changed-receipt", "persisted custody differs"),
        ("builder", "Builder authority differs"),
        ("catalog", "custody catalog differs"),
        ("marker", "custody marker differs"),
    ),
)
async def test_precreated_stage_rechecks_locked_attempt_persisted_receipt_and_heap(monkeypatch, fault, match):
    stage = _legacy_stage() if fault == "legacy" else _nucc_stage_receipt()
    run = _nucc_abandoned_run(deepcopy(stage), "running")
    if fault == "finished":
        run["finished_at"] = "2026-10-01T00:01:00Z"
    elif fault == "handoff":
        run["metrics"]["nucc_handoff"] = {}
    elif fault == "missing-receipt":
        run["metrics"].clear()
    elif fault == "changed-receipt":
        run["metrics"]["nucc_native_stage"]["stage"]["relation_oid"] += 1
    observed_stage = deepcopy(stage["stage"])
    if fault == "catalog":
        observed_stage["relfilenode"] += 1
    lock, location, catalog = AsyncMock(return_value=run), AsyncMock(), AsyncMock(return_value=observed_stage)
    monkeypatch.setattr(archive, "_nucc_locked_attempt", lock)
    monkeypatch.setattr(archive, "_require_nucc_native_location", location)
    monkeypatch.setattr(archive, "_nucc_native_stage", catalog)
    marker = None if fault == "marker" else archive._canonical_json(stage).decode("ascii")
    session = _session()
    session.scalar.side_effect = [marker] if fault == "legacy" else [fault != "builder", marker]
    if match is None:
        await generation._require_nucc_precreated_stage(session, stage)
    else:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match=match):
            await generation._require_nucc_precreated_stage(session, stage)
    lock.assert_awaited_once_with(session, stage, ("running",))
    location.assert_awaited_once_with(session, stage, run)
    assert catalog.await_count == int(fault in {None, "legacy", "catalog", "marker"})
    assert session.scalar.await_count == (
        0
        if fault in {"finished", "handoff", "missing-receipt", "changed-receipt"}
        else 1
        if fault in {"legacy", "builder", "catalog"}
        else 2
    )
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ("finished", "handoff", "existing", "indexed"))
async def test_precreation_refuses_closed_attempt_existing_heap_or_premature_indexes(monkeypatch, fault):
    stage = _nucc_stage_receipt()
    run = _nucc_abandoned_run(stage, "running")
    run["metrics"] = {"nucc_handoff": {}} if fault == "handoff" else {}
    if fault == "finished":
        run["finished_at"] = "2026-10-01T00:01:00Z"
    monkeypatch.setattr(archive, "_nucc_locked_attempt", AsyncMock(return_value=run))
    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(side_effect=[43 if fault == "existing" else None, 42]))
    create = AsyncMock()
    monkeypatch.setattr(archive, "_create_model_heaps", create)
    monkeypatch.setattr(archive, "_nucc_native_stage", AsyncMock(return_value={**stage["stage"], "indexes": [47]}))
    session = _session()
    session.scalar.return_value = 41
    session.connection = AsyncMock()
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="already closed|already exists|not index-free"):
        await generation._precreate_nucc_attempt(
            session, "mrf", stage["run_id"], stage["attempt_id"], stage["attempt_started_at"], stage["import_date"]
        )
    assert create.await_count == int(fault == "indexed")
    session.connection.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ("boolean-oid", "missing-oid", "prior-custody", "lost-reservation"))
async def test_stage_reservation_cannot_overwrite_prior_custody_or_lose_attempt_cas(monkeypatch, fault):
    stage = _nucc_stage_receipt()
    run = _nucc_abandoned_run(stage, "running")
    if fault != "prior-custody":
        run["metrics"] = {}
    session = _session()
    session.scalar.side_effect = [True, 41, None]
    relation = AsyncMock(return_value=42)
    monkeypatch.setattr(archive, "_relation_oid", relation)
    oid = True if fault == "boolean-oid" else None if fault == "missing-oid" else 46
    with pytest.raises(
        archive.ReferenceFamilyArchiveError, match="incumbent identity|prior custody|reservation changed"
    ):
        await generation._reserve_nucc_stage(session, deepcopy(stage), run, stage["incumbent"], oid)
    assert relation.await_count == int(fault == "lost-reservation")
    if fault == "lost-reservation":
        query, parameters = session.scalar.await_args.args
        assert parameters["slot"] == "nucc_native_stage_reservation"
        assert parameters["attempt_id"] == stage["attempt_id"]
        assert "metrics->'nucc_native_stage' IS NULL" in str(query)
        assert "metrics->'nucc_handoff' IS NULL" in str(query)
    session.execute.assert_not_awaited()


@pytest.mark.parametrize(
    "path,value,match",
    (
        (("contract",), "unknown", "custody differs"),
        (("extra",), True, "custody differs"),
        (("incumbent_relation_oid",), True, "incumbent identity"),
        (("incumbent_relation_oid",), 0, "incumbent identity"),
        (("schema_name",), " mrf ", "catalog is invalid"),
        (("stage", "relation_oid"), True, "catalog is invalid"),
        (("stage", "relfilenode"), 0, "catalog is invalid"),
        (("stage", "owner_oid"), 2**32, "catalog is invalid"),
        (("stage", "indexes"), [47], "catalog is invalid"),
        (("database_oid",), 0, "catalog is invalid"),
        (("import_run_oid",), True, "catalog is invalid"),
        (("import_date",), "other", "custody attempt differs"),
        (("stage", "table_name"), "nucc_taxonomy_other", "custody attempt differs"),
    ),
)
def test_signed_stage_metadata_still_requires_closed_schema_and_exact_attempt(path, value, match):
    stage = _nucc_stage_receipt()
    target = stage if len(path) == 1 else stage[path[0]]
    target[path[-1]] = value
    _resign_stage(stage)
    with pytest.raises(archive.ReferenceFamilyArchiveError, match=match):
        generation._nucc_precreated_stage_value(stage)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ("legacy", "owner", "callback"))
async def test_stage_cleanup_requires_original_private_owner_and_reference_callback(fault):
    stage = _legacy_stage() if fault == "legacy" else _nucc_stage_receipt()
    session = _session()
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="trusted stage cleanup is required"):
        await generation.cleanup_nucc_native_stage(
            session,
            stage,
            runtime_owner_oids=(46,) if fault == "owner" else (45,),
            assert_unreferenced=None if fault == "callback" else AsyncMock(return_value=True),
        )
    session.execute.assert_not_awaited()
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", ("missing", "engine", "importer", "metrics", "progress", "params", "receipt", "published", "handoff")
)
async def test_stage_retirement_refuses_foreign_run_changed_receipt_and_published_custody(monkeypatch, fault):
    stage = _nucc_stage_receipt()
    run = _nucc_abandoned_run(stage, "failed")
    match fault:
        case "missing":
            run = None
        case "engine" | "importer":
            run[fault] = "other"
        case "metrics" | "progress" | "params":
            run[fault] = []
        case "receipt":
            run["metrics"]["nucc_native_stage"] = {}
        case "published":
            run["metrics"]["nucc_native_publication"] = {}
        case "handoff":
            other = _nucc_stage_receipt()
            other["incumbent_relation_oid"] += 1
            run["metrics"]["nucc_handoff"] = _nucc_completed_handoff(_resign_stage(other))
    session = _session()
    session.execute.return_value = SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: run))
    location = AsyncMock()
    monkeypatch.setattr(archive, "_require_nucc_native_location", location)
    with pytest.raises(
        archive.ReferenceFamilyArchiveError, match="cleanup run differs|cleanup custody differs|handoff differs"
    ):
        await generation._require_nucc_stage_retirement(session, stage)
    query, parameters = session.execute.await_args.args
    assert parameters == {"run_id": stage["run_id"]}
    assert "FOR UPDATE NOWAIT" in str(query)
    assert "octet_length(params::text)<=131072" in str(query)
    assert "octet_length(metrics::text)<=393216" in str(query)
    assert location.await_count == int(fault in {"receipt", "published", "handoff"})
    session.scalar.assert_not_awaited()


@pytest.mark.parametrize(
    "fault,accepted",
    (
        ("canceling", True),
        ("cancel-phase", False),
        ("cancel-message", False),
        ("replacement", True),
        ("replacement-error", False),
        ("replacement-malformed", False),
        ("foreign-terminal", False),
    ),
)
def test_abandoned_attempt_requires_complete_cancel_facts_or_authentic_running_replacement(fault, accepted):
    stage = _nucc_stage_receipt()
    run = _nucc_abandoned_run(stage, "running")
    if fault.startswith("cancel"):
        run.update(status="canceling", phase_detail="cancel requested")
        run["progress"]["message"] = "cancel requested"
        if fault == "cancel-phase":
            run["phase_detail"] = "running"
        elif fault == "cancel-message":
            run["progress"]["message"] = "running"
    else:
        run["progress"]["attempt_id"] = stage["run_id"] + ":" + "2" * 32
        if fault == "replacement-error":
            run["error"] = "failed"
        elif fault == "replacement-malformed":
            run["progress"]["attempt_id"] = "other:" + "2" * 32
        elif fault == "foreign-terminal":
            run.update(status="failed", finished_at="2026-10-01T00:01:00Z")
    if accepted:
        generation._require_nucc_abandoned_attempt(stage, run)
    else:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="not abandoned|controlled attempt is invalid"):
            generation._require_nucc_abandoned_attempt(stage, run)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ("catalog", "marker"))
async def test_cleanup_storage_refuses_replaced_heap_or_marker_before_privilege_check(monkeypatch, fault):
    stage = _nucc_stage_receipt()
    observed = {**stage["stage"], "relation_oid": 99} if fault == "catalog" else stage["stage"]
    monkeypatch.setattr(archive, "_nucc_native_stage", AsyncMock(return_value=observed))
    custody = AsyncMock()
    monkeypatch.setattr(archive, "_require_nucc_cleanup_custody", custody)
    session = _session()
    session.scalar.return_value = None
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="cleanup identity differs|cleanup marker differs"):
        await generation._require_nucc_stage_cleanup_storage(session, stage, None, 45)
    assert str(session.execute.await_args.args[0]) == (
        f'LOCK TABLE ONLY "mrf"."{stage["stage"]["table_name"]}" IN ACCESS EXCLUSIVE MODE NOWAIT'
    )
    custody.assert_not_awaited()
    assert session.scalar.await_count == int(fault == "marker")


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ("referenced", "ended", "transaction", "record-cas"))
async def test_stage_cleanup_cannot_cross_reference_or_transaction_fences(monkeypatch, fault):
    stage = _nucc_stage_receipt()
    monkeypatch.setattr(generation, "_require_nucc_stage_retirement", AsyncMock(return_value=None))
    monkeypatch.setattr(archive, "_nucc_native_publisher_owner", AsyncMock(return_value=45))
    monkeypatch.setattr(generation, "_require_nucc_stage_cleanup_storage", AsyncMock(return_value=stage["stage"]))
    session = _session()
    session.scalar.side_effect = ["initial", "different" if fault == "transaction" else "initial", None]
    calls = []

    async def is_unreferenced(actual_session, actual_stage):
        assert actual_session is session and actual_stage == stage
        calls.append(actual_stage)
        if fault == "ended" and len(calls) == 2:
            session.in_transaction = lambda: False
        return fault != "referenced"

    with pytest.raises(
        archive.ReferenceFamilyArchiveError, match="remains referenced|transaction changed|fence changed"
    ):
        await generation.cleanup_nucc_native_stage(
            session, stage, runtime_owner_oids=(45,), assert_unreferenced=is_unreferenced
        )
    assert len(calls) == (1 if fault == "referenced" else 2)
    assert session.execute.await_count == int(fault == "record-cas")
    if fault == "record-cas":
        assert str(session.execute.await_args.args[0]).endswith(" RESTRICT")
        query, parameters = session.scalar.await_args.args
        assert "metrics::jsonb->'nucc_native_stage'=CAST(:stage AS jsonb)" in str(query)
        assert "metrics::jsonb->'nucc_handoff' IS NOT DISTINCT FROM CAST(:handoff AS jsonb)" in str(query)
        assert parameters["stage"] == archive._canonical_json(stage).decode("ascii")
        assert parameters["handoff"] is None


@pytest.mark.parametrize(
    "field,value,match",
    (
        ("control_run_id", "other", "attempt is invalid"),
        ("_control_attempt_id", "bad", "attempt is invalid"),
        ("test_mode", True, "attempt is invalid"),
        ("_control_attempt_started_at", "bad", "time is invalid"),
        ("_control_attempt_started_at", "2026-10-01T00:00:00", "time is invalid"),
    ),
)
def test_attempt_parser_requires_control_identity_and_timezone(field, value, match):
    stage = _nucc_stage_receipt()
    context_by_field = {
        "control_run_id": stage["run_id"],
        "context": {
            "_control_attempt_id": stage["attempt_id"],
            "_control_attempt_started_at": stage["attempt_started_at"],
        },
    }
    (context_by_field if field == "control_run_id" else context_by_field["context"])[field] = value
    with pytest.raises(archive.ReferenceFamilyArchiveError, match=match):
        generation._nucc_attempt(context_by_field)


@pytest.mark.parametrize("fault", ("missing", "extra", "untracked"))
def test_nucc_authority_decoder_rejects_open_shapes_and_untracked_publication(fault):
    authority = _nucc_stage_receipt()["incumbent"]
    if fault == "missing":
        del authority["relation_oids"]
    elif fault == "extra":
        authority["extra"] = True
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="predecessor differs|generation is untracked"):
        generation._nucc_result_authority(authority)


@pytest.mark.asyncio
@pytest.mark.parametrize("immutable", (True, False))
async def test_handoff_generation_dispatch_preserves_legacy_and_immutable_authorities(monkeypatch, immutable):
    handoff = _nucc_completed_handoff(_nucc_stage_receipt())
    if not immutable:
        handoff["contract"] = archive.NUCC_HANDOFF_CONTRACT
    modern, legacy = AsyncMock(return_value="modern"), AsyncMock(return_value="legacy")
    monkeypatch.setattr(generation, "publish_immutable_nucc_generation", modern)
    monkeypatch.setattr(generation, "publish_local_reference_family_generation", legacy)
    result = await generation._publish_nucc_handoff_generation("session", handoff)
    assert result == ("modern" if immutable else "legacy")
    assert modern.await_count == int(immutable) and legacy.await_count == int(not immutable)
    if immutable:
        assert modern.await_args.kwargs["expected_authority"] == handoff["incumbent"]
        assert modern.await_args.kwargs["expected_relation_oid"] == 43
    else:
        legacy.assert_awaited_once_with("session", importer_id="nucc", schema_name="mrf")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "importer,contract",
    (
        ("nucc", archive.IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT),
        ("nucc", archive.GUARDED_SOURCE_CAPTURE_CONTRACT),
        ("label", archive.IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT),
    ),
)
async def test_activation_reads_immutable_predecessor_only_for_exact_family_contract(monkeypatch, importer, contract):
    read = AsyncMock(return_value="authority")
    monkeypatch.setattr(generation, "read_reference_family_result_generation_authority", read)
    manifest = SimpleNamespace(importer_id=importer, source_capture_contract=contract)
    result = await generation._read_immutable_activation_predecessor(
        "session", manifest, SimpleNamespace(schema_name="mrf")
    )
    is_selected = importer == "nucc" and contract == archive.IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT
    assert result == ("authority" if is_selected else None)
    assert read.await_count == int(is_selected)
    if is_selected:
        read.assert_awaited_once_with("session", importer_id="nucc", schema_name="mrf", lock=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("adopted_oid", (43, 44))
async def test_immutable_adoption_rechecks_exact_activated_relation_binding(monkeypatch, adopted_oid):
    original = archive._nucc_result_authority(_nucc_stage_receipt()["incumbent"], allow_untracked=True)
    incoming_by_field = {
        "origin_lineage_id": original.local_lineage_id,
        "origin_generation": 1,
        "published_at": "2026-10-01T00:00:00Z",
    }
    published = replace(
        original,
        serving_generation=generation.validate_reference_family_serving_generation(incoming_by_field),
        relation_oids=(adopted_oid,),
    )
    adopt = AsyncMock(return_value=published)
    legacy = AsyncMock(side_effect=AssertionError("immutable adoption cannot install revision hooks"))
    monkeypatch.setattr(generation, "adopt_immutable_nucc_generation", adopt)
    monkeypatch.setattr(generation, "publish_adopted_reference_family_generation", legacy)
    arguments = (
        "session",
        SimpleNamespace(importer_id="nucc", source_capture_contract=archive.IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT),
        SimpleNamespace(schema_name="mrf"),
        incoming_by_field,
        (("nucc_taxonomy", 43),),
        original,
    )
    if adopted_oid == 43:
        assert await generation._adopt_validated_family_generation(*arguments) is published
    else:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="adopted generation OIDs differ"):
            await generation._adopt_validated_family_generation(*arguments)
    adopt.assert_awaited_once_with(
        "session",
        schema_name="mrf",
        source_generation=incoming_by_field,
        expected_authority=original.as_dict(),
        expected_relation_oid=43,
    )
    legacy.assert_not_awaited()


@pytest.mark.parametrize("fault", ("extra", "contract", "node", "heap", "incumbent", "legacy"))
def test_completed_handoff_cannot_substitute_original_custody_or_reuse_incumbent(fault):
    stage = _nucc_stage_receipt()
    handoff = _nucc_completed_handoff(stage)
    if fault in {"extra", "contract"}:
        handoff["extra" if fault == "extra" else "contract"] = "unknown"
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="handoff differs"):
            generation._require_nucc_handoff_fields(handoff)
        return
    if fault == "node":
        handoff["node_id"] = "other"
    elif fault == "heap":
        handoff["stage"]["relfilenode"] += 1
    elif fault == "incumbent":
        stage["incumbent_relation_oid"] = stage["stage"]["relation_oid"]
        handoff["precreated_stage"] = _resign_stage(stage)
    else:
        handoff["precreated_stage"] = _legacy_stage()
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="original custody differs|reuses its predecessor"):
        generation._require_nucc_precreated_handoff_binding(handoff)


@pytest.mark.asyncio
@pytest.mark.parametrize("failed_model", (0, 1))
async def test_claims_slice_readback_failure_cannot_report_publication_success(monkeypatch, failed_model):
    session = _session()
    monkeypatch.setattr(dictionary, "_lock_reference_dictionary", AsyncMock(return_value=(201, 202)))
    monkeypatch.setattr(archive, "validate_claims_dictionary_closure", AsyncMock())
    equality = AsyncMock(side_effect=[True] * failed_model + [False])
    monkeypatch.setattr(archive, "_is_model_table_equal", equality)
    events = []
    session.scalar.side_effect = lambda *_args: events.append("check") or False
    session.execute.side_effect = lambda *_args: events.append("write")
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="claims dictionary publication differs"):
        await dictionary.replace_claims_dictionary_slice(
            session, incoming_schema="candidate", current_schema=None, destination_schema="mrf"
        )
    assert events == ["check"] * 4 + ["write"] * (2 * (failed_model + 1))
    for offset in range(failed_model + 1):
        statement, parameters = session.execute.await_args_list[2 * offset].args
        assert str(statement).endswith("WHERE source=:source")
        assert parameters == {"source": dictionary._CLAIMS_DICTIONARY_SOURCE}
    assert equality.await_args.kwargs["right_schema"] == "candidate"


@pytest.mark.asyncio
async def test_prescription_rollup_refuses_correct_aggregate_with_wrong_provider_binding():
    session = _session()
    session.scalar.side_effect = [False, True]
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="autocomplete binding differs"):
        await dictionary.validate_prescription_rollup(session, "candidate", require_binding=True)
    assert session.scalar.await_count == 2
    assert session.scalar.await_args.args[1] == {"provider": '"candidate"."pricing_provider_prescription"'}
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("oids", ((None, 202), (201, None), (201, 202, 203, 202), (201, 202, 201, None)))
async def test_dictionary_lock_refuses_missing_or_replaced_relation_before_catalog(monkeypatch, oids):
    session = _session()
    relation, lock, catalog = AsyncMock(side_effect=oids), AsyncMock(), AsyncMock()
    monkeypatch.setattr(archive, "_relation_oid", relation)
    monkeypatch.setattr(archive, "_lock_family", lock)
    monkeypatch.setattr(archive, "require_native_read_catalog", catalog)
    with pytest.raises(
        archive.ReferenceFamilyArchiveError, match="dictionary is unavailable|dictionary identity changed"
    ):
        await dictionary._lock_reference_dictionary(session, "mrf")
    catalog.assert_not_awaited()
    assert relation.await_count == len(oids)
    assert lock.await_count == int(len(oids) == 4)
    if len(oids) == 4:
        lock.assert_awaited_once_with(
            session, "mrf", ("code_catalog", "code_crosswalk"), "SHARE ROW EXCLUSIVE", nowait=True
        )
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "importer,budget", (("nucc", 1000), ("drug-claims", True), ("drug-claims", 0), ("drug-claims", None))
)
async def test_effect_preparation_rejects_wrong_family_or_invalid_budget_before_ownership(
    monkeypatch, importer, budget
):
    session = _session()
    ownership = replace(_ownership(), importer_id=importer)
    verify = AsyncMock()
    monkeypatch.setattr(archive, "verify_reference_family_stage_ownership", verify)
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="effect scope is invalid"):
        await dictionary.prepare_reference_dictionary_effects(session, ownership, max_bytes=budget)
    verify.assert_not_awaited()
    session.execute.assert_not_awaited()
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("registered", (True, False))
async def test_existing_dictionary_effects_require_registered_predecessor_before_capture(monkeypatch, registered):
    ownership, session = _ownership(), _session()
    session.scalar.return_value = 1000
    current = SimpleNamespace(importer_id="drug-claims", relation_oids=ownership.relation_oids)
    monkeypatch.setattr(archive, "verify_reference_family_stage_ownership", AsyncMock())
    monkeypatch.setattr(dictionary, "validate_prescription_dictionary_closure", AsyncMock())
    monkeypatch.setattr(archive, "capture_reference_family_incumbent", AsyncMock(return_value=current))
    binding = AsyncMock(
        side_effect=None if registered else archive.ReferenceFamilyArchiveError("predecessor unregistered")
    )
    empty, lock, capture = AsyncMock(), AsyncMock(return_value=(201, 202)), AsyncMock()
    monkeypatch.setattr(dictionary, "_require_terminal_current_binding", binding)
    monkeypatch.setattr(dictionary, "require_terminal_empty_incumbent", empty)
    monkeypatch.setattr(dictionary, "_lock_reference_dictionary", lock)
    monkeypatch.setattr(dictionary, "_capture_dictionary_effect_table", capture)
    if registered:
        await dictionary.prepare_reference_dictionary_effects(session, ownership, max_bytes=1000)
    else:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="predecessor unregistered"):
            await dictionary.prepare_reference_dictionary_effects(session, ownership, max_bytes=1000)
    binding.assert_awaited_once_with(session, current)
    empty.assert_not_awaited()
    assert lock.await_count == int(registered)
    assert capture.await_count == 2 * int(registered)
    assert str(session.execute.await_args.args[0]).lstrip().startswith('UPDATE "candidate".')
    if registered:
        assert all(call.args[-1] is True for call in capture.await_args_list)
        assert str(session.scalar.await_args.args[0]).count("sum(pg_column_size(row_value))") == 7


@pytest.mark.asyncio
@pytest.mark.parametrize("current_present", (True, False))
async def test_dictionary_effect_capture_refuses_aggregate_row_bound(current_present):
    session = _session()
    session.scalar.side_effect = [False, False, True]
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="publication bound is exceeded"):
        await dictionary._capture_dictionary_effect_table(
            session,
            "candidate",
            "mrf",
            dictionary._DRUG_SCOPED_MODELS[0],
            dictionary._DRUG_EFFECT_MODELS[0],
            201,
            current_present,
        )
    session.execute.assert_awaited_once()
    assert str(session.execute.await_args.args[0]).lstrip().startswith('INSERT INTO "candidate".')
    assert "SELECT count(*)>100000" in str(session.scalar.await_args.args[0])


@pytest.mark.asyncio
async def test_portable_dictionary_inventory_cannot_claim_destination_effect_custody():
    session, ownership = _session(), _ownership()
    portable_names = set(archive.reference_family_spec("drug-claims").table_names)
    ownership = replace(
        ownership, relation_oids=tuple(pair for pair in ownership.relation_oids if pair[0] in portable_names)
    )
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="effect inventory is incomplete"):
        await dictionary.validate_reference_dictionary_effects(session, ownership)
    session.scalar.assert_not_awaited()
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failed_model", (0, 1))
async def test_dictionary_effect_closure_refuses_either_set_without_mutation(monkeypatch, failed_model):
    session = _session()
    session.scalar.side_effect = [False] * failed_model + [True]
    rollup = AsyncMock()
    monkeypatch.setattr(dictionary, "validate_prescription_rollup", rollup)
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="effect closure differs"):
        await dictionary.validate_reference_dictionary_effects(session, _ownership())
    rollup.assert_awaited_once_with(session, "candidate", require_binding=True)
    assert session.scalar.await_count == failed_model + 1
    query = str(session.scalar.await_args.args[0])
    assert dictionary._DRUG_EFFECT_MODELS[failed_model].__tablename__ in query
    assert "count(DISTINCT destination_oid)>1" in query
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("rollback", (False, True))
@pytest.mark.parametrize("failed_model", (0, 1))
async def test_dictionary_readback_failure_stops_publication_and_rollback(monkeypatch, rollback, failed_model):
    session = _session()
    monkeypatch.setattr(dictionary, "_lock_reference_dictionary", AsyncMock(return_value=(201, 202)))
    fence_count = 4 if rollback else 2
    session.scalar.side_effect = [False] * (fence_count + failed_model) + [True]
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="dictionary publication differs"):
        await dictionary.apply_reference_dictionary_effects(
            session, incoming_schema="candidate", current_schema="mrf", rollback=rollback
        )
    assert session.execute.await_count == 2 * (failed_model + 1)
    query = str(session.scalar.await_args.args[0])
    assert "effect.image IS DISTINCT FROM to_jsonb(live)" in query
    assert ("current.baseline_image" in query) is rollback
    for call in session.execute.await_args_list:
        assert str(call.args[0]).startswith(('DELETE FROM "mrf".', 'INSERT INTO "mrf".'))
