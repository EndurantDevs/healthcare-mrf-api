# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named NUCC scope, data/generation CAS and abandoned-candidate refusal checks."""

from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import reference_family_archive as archive
from process import reference_family_dictionary as dictionary
from process import reference_family_result_generation as generation
from tests.test_reference_family_dictionary import _ownership, _session
from tests.test_reference_family_dictionary import named_effects as named_effects
from tests.test_reference_family_source_copy import (
    _nucc_abandoned_run,
    _nucc_completed_handoff,
    _nucc_owning_transaction,
    _nucc_stage_receipt,
)


def _resign_stage(stage):
    stage["stage_sha256"] = archive.nucc_native_digest({k: v for k, v in stage.items() if k != "stage_sha256"})
    return stage


def _legacy_stage():
    stage = _nucc_stage_receipt()
    stage["contract"] = "nucc-native-stage.v1"
    del stage["incumbent"], stage["incumbent_relation"]
    return _resign_stage(stage)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ("r", "p"))
async def test_immutable_storage_dispatches_only_named_native_heap_or_partition(monkeypatch, kind):
    from process import nucc_snapshot_partition as partition

    session = _session()
    session.scalar.return_value = kind
    lock = AsyncMock()
    heap = AsyncMock(return_value={"table_name": "nucc_taxonomy"})
    parent = AsyncMock(return_value={"parent": ("mrf", "nucc_taxonomy")})
    monkeypatch.setattr(generation, "_require_sealed_nucc_storage", lock)
    monkeypatch.setattr(archive, "_observe_nucc_native_stage", heap)
    monkeypatch.setattr(partition, "capture_nucc_parent", parent)
    result = await generation.require_immutable_nucc_storage(
        session, schema_name="mrf", expected_relation_oid="opaque legacy token"
    )
    lock.assert_awaited_once_with(session, "mrf")
    assert result == ({"parent": ("mrf", "nucc_taxonomy")} if kind == "p" else {"table_name": "nucc_taxonomy"})
    assert heap.await_count == int(kind == "r") and parent.await_count == int(kind == "p")


@pytest.mark.asyncio
@pytest.mark.parametrize("address", (None, False, 0, 42))
async def test_named_storage_lock_precedes_actual_model_catalog_and_native_read_checks(monkeypatch, address):
    session = _session()
    session.scalar.return_value = address
    columns, catalog = AsyncMock(), AsyncMock()
    monkeypatch.setattr(archive, "_require_nucc_native_columns", columns)
    monkeypatch.setattr(archive, "require_native_read_catalog", catalog)
    if type(address) is int and address > 0:
        assert await generation._require_sealed_nucc_storage(session, "mrf", "opaque old ID") == (
            "mrf",
            "nucc_taxonomy",
        )
        columns.assert_awaited_once_with(session, 42)
        catalog.assert_awaited_once_with(session, (42,))
    else:
        with pytest.raises(RuntimeError, match="storage is unavailable"):
            await generation._require_sealed_nucc_storage(session, "mrf", "opaque old ID")
        columns.assert_not_awaited()
        catalog.assert_not_awaited()
    assert (
        str(session.execute.await_args.args[0]) == 'LOCK TABLE ONLY "mrf"."nucc_taxonomy" IN ACCESS SHARE MODE NOWAIT'
    )
    sql, params = session.scalar.await_args.args
    assert params == {"relation": '"mrf"."nucc_taxonomy"'}
    assert "relowner" not in str(sql) and "relfilenode" not in str(sql)


@pytest.mark.asyncio
@pytest.mark.parametrize("adopt", (False, True))
@pytest.mark.parametrize("failure", ("cas", "stale", "exhausted"))
async def test_nucc_generation_keeps_logical_counter_lineage_cas_and_no_physical_writer(monkeypatch, adopt, failure):
    prior = archive._nucc_result_authority(_nucc_stage_receipt()["incumbent"], allow_untracked=True)
    if failure == "exhausted":
        prior = replace(prior, local_generation=generation._MAX_GENERATION)
    expected = prior.as_dict()
    if failure == "stale":
        expected["local_lineage_id"] = "55555555-5555-4555-8555-555555555555"
    monkeypatch.setattr(archive, "model_family_roles", lambda: ("snapshot_owner", ("publisher", "reader")))
    monkeypatch.setattr(
        generation, "require_immutable_nucc_storage", AsyncMock(return_value={"table_name": "nucc_taxonomy"})
    )
    monkeypatch.setattr(generation, "read_reference_family_result_generation_authority", AsyncMock(return_value=prior))
    update = AsyncMock(return_value=None)
    monkeypatch.setattr(generation, "_first", update)
    with pytest.raises(RuntimeError, match="changed|predecessor differs|generation is untracked"):
        if adopt:
            await generation.adopt_immutable_nucc_generation(
                _session(),
                schema_name="mrf",
                expected_authority=expected,
                source_generation={
                    "origin_lineage_id": prior.local_lineage_id,
                    "origin_generation": 1,
                    "published_at": "2026-10-01T00:00:00Z",
                },
            )
        else:
            await generation.publish_immutable_nucc_generation(
                _session(), schema_name="mrf", expected_authority=expected
            )
    assert update.await_count == int(failure == "cas")
    if failure == "cas":
        assert update.await_args.kwargs["prior"] == 0 and update.await_args.kwargs["lineage"] == prior.local_lineage_id
        assert update.await_args.kwargs["serving_schema"] == "mrf"
        assert update.await_args.kwargs["serving_names"] == ["nucc_taxonomy"]
        assert "oids" not in update.await_args.kwargs


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", (None, "finished", "handoff", "missing-receipt", "changed-receipt", "source", "name", "indexes")
)
async def test_precreated_stage_rechecks_exact_locked_run_source_and_named_empty_heap(monkeypatch, fault):
    stage = _nucc_stage_receipt()
    run = _nucc_abandoned_run(deepcopy(stage), "running")
    if fault == "finished":
        run["finished_at"] = "2026-10-01T00:01:00Z"
    if fault == "handoff":
        run["metrics"]["nucc_handoff"] = {}
    if fault == "missing-receipt":
        run["metrics"].clear()
    if fault == "changed-receipt":
        run["metrics"]["nucc_native_stage"]["attempt_id"] = "other"
    observed = deepcopy(stage["stage"])
    if fault == "name":
        observed["table_name"] = "other"
    if fault == "indexes":
        observed["indexes"] = [{"index_name": "premature", "definition": "native shape"}]
    monkeypatch.setattr(archive, "_nucc_locked_attempt", AsyncMock(return_value=run))
    location = AsyncMock(side_effect=RuntimeError("source changed") if fault == "source" else None)
    catalog = AsyncMock(return_value=observed)
    monkeypatch.setattr(archive, "_require_nucc_native_location", location)
    monkeypatch.setattr(archive, "_nucc_native_stage", catalog)
    session = _session()
    if fault:
        with pytest.raises(RuntimeError, match="closed|persisted stage differs|source changed|catalog differs"):
            await generation._require_nucc_precreated_stage(session, stage)
    else:
        await generation._require_nucc_precreated_stage(session, stage)
    session.scalar.assert_not_awaited()
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ("prior-stage", "reservation-cas"))
async def test_stage_reservation_never_overwrites_an_operation_or_loses_attempt_cas(monkeypatch, fault):
    stage = _nucc_stage_receipt()
    run = _nucc_abandoned_run(stage, "running")
    if fault == "reservation-cas":
        run["metrics"] = {}
    session = _session()
    session.scalar.return_value = None
    monkeypatch.setattr(generation, "_require_nucc_native_builder", AsyncMock())
    with pytest.raises(
        archive.ReferenceFamilyArchiveError, match="prior stage requires retirement|reservation changed"
    ):
        await generation._reserve_nucc_stage(session, deepcopy(stage), run, stage["incumbent"])
    if fault == "reservation-cas":
        sql, params = session.scalar.await_args.args
        assert params["slot"] == "nucc_native_stage_reservation" and params["attempt_id"] == stage["attempt_id"]
        assert "metrics->'nucc_native_stage' IS NULL" in str(sql) and "metrics->'nucc_handoff' IS NULL" in str(sql)
        assert "database_oid" not in params["receipt"] and "incumbent_relation_oid" not in params["receipt"]
    else:
        session.scalar.assert_not_awaited()


@pytest.mark.parametrize(
    "path,value",
    [
        (("contract",), "unknown"),
        (("extra",), True),
        (("schema_name",), " mrf "),
        (("node_id",), ""),
        (("node_id",), 123),
        (("node_id",), " padded "),
        (("node_id",), "x" * 129),
        (("source_contract_sha256",), None),
        (("import_date",), "other"),
        (("stage", "table_name"), "nucc_taxonomy_other"),
        (("stage", "indexes"), ["premature"]),
        (("incumbent_relation",), ["other", "nucc_taxonomy"]),
    ],
)
def test_named_stage_metadata_requires_closed_scope_and_exact_attempt(path, value):
    stage = _nucc_stage_receipt()
    target = stage if len(path) == 1 else stage[path[0]]
    target[path[-1]] = value
    _resign_stage(stage)
    with pytest.raises(archive.ReferenceFamilyArchiveError):
        generation._nucc_precreated_stage_value(stage)


def test_named_stage_receipt_rejects_oversized_metadata_before_parsing():
    stage = _nucc_stage_receipt()
    stage["node_id"] = "x" * 131073
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="metadata exceeds its bound"):
        generation._nucc_precreated_stage_value(stage)


@pytest.mark.parametrize("legacy", (False, True))
def test_nucc_receipt_editions_keep_names_and_raw_legacy_tokens_without_rewriting(legacy):
    stage = _nucc_stage_receipt(legacy=legacy)
    before = deepcopy(stage)
    assert generation._nucc_precreated_stage_value(stage) == before == stage
    handoff = _nucc_completed_handoff(stage)
    saved = deepcopy(handoff)
    assert archive.validate_nucc_native_handoff(handoff) == saved == handoff
    if legacy:
        stage["stage"]["owner_oid"] = {"opaque": "unchanged historical type"}
        assert generation._nucc_precreated_stage_value(stage) == stage
    else:
        assert "database_oid" not in handoff and set(handoff["stage"]) == {"table_name", "indexes"}


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ("referenced", "ended", "transaction", "record-cas"))
async def test_stage_cleanup_keeps_reference_fences_original_transaction_and_exact_record_cas(monkeypatch, fault):
    stage = _nucc_stage_receipt()
    session = _nucc_owning_transaction(_session())
    transaction = object()
    current_entries = [transaction]
    session.get_transaction = lambda: current_entries[0]
    monkeypatch.setattr(generation, "_require_nucc_stage_retirement", AsyncMock(return_value=None))
    monkeypatch.setattr(generation, "_require_nucc_stage_cleanup_storage", AsyncMock(return_value=stage["stage"]))
    session.scalar.return_value = None
    calls = []

    async def is_unreferenced(actual, recorded):
        assert actual is session and recorded == stage
        calls.append(recorded)
        if len(calls) == 2:
            if fault == "ended":
                session.in_transaction = lambda: False
            if fault == "transaction":
                current_entries[0] = object()
        return fault != "referenced"

    with pytest.raises(
        archive.ReferenceFamilyArchiveError, match="remains referenced|transaction changed|fence changed"
    ):
        await generation.cleanup_nucc_native_stage(session, stage, assert_unreferenced=is_unreferenced)
    assert len(calls) == (1 if fault == "referenced" else 2)
    assert session.execute.await_count == int(fault == "record-cas")
    if fault == "record-cas":
        assert str(session.execute.await_args.args[0]).endswith(" RESTRICT")
        sql, params = session.scalar.await_args.args
        assert "metrics::jsonb->'nucc_native_stage'=CAST(:stage AS jsonb)" in str(sql)
        assert params["stage"] == archive._canonical_json(stage).decode("ascii")


@pytest.mark.parametrize("fault", ("missing", "extra", "untracked", "different-family"))
def test_nucc_authority_requires_closed_logical_generation_and_family(fault):
    authority = _nucc_stage_receipt()["incumbent"]
    if fault == "missing":
        del authority["relations"]
    elif fault == "extra":
        authority["extra"] = True
    elif fault == "different-family":
        authority["importer_id"] = "label"
    with pytest.raises(archive.ReferenceFamilyArchiveError):
        generation._nucc_result_authority(authority)


@pytest.mark.asyncio
@pytest.mark.parametrize("immutable", (True, False))
async def test_completed_handoff_publishes_named_frozen_generation_without_new_revision_hook(monkeypatch, immutable):
    handoff = _nucc_completed_handoff(_nucc_stage_receipt())
    if not immutable:
        handoff["contract"] = archive.NUCC_HANDOFF_CONTRACT
    session = SimpleNamespace(execute=AsyncMock())
    publish = AsyncMock(return_value="named generation")

    async def mark_taxonomy(schema, execute, replaced_table):
        publish.assert_awaited_once()
        assert schema == "mrf"
        assert replaced_table == "nucc_taxonomy"
        await execute("SELECT 1")

    monkeypatch.setattr(generation, "publish_immutable_nucc_generation", publish)
    monkeypatch.setattr(generation, "mark_procedure_taxonomy_inputs_changed", mark_taxonomy)
    assert await generation._publish_nucc_handoff_generation(session, handoff) == "named generation"
    publish.assert_awaited_once_with(session, schema_name="mrf", expected_authority=handoff["incumbent"])
    session.execute.assert_awaited_once()
    assert str(session.execute.await_args.args[0]) == "SELECT 1"


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
            other["node_id"] = "other-node"
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
    assert session.scalar.await_args.args[1] == {"provider": "mrf.pricing_provider_prescription"}
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("addresses", ((None, 202), (201, None), (201, 202)))
async def test_dictionary_lock_uses_names_then_checks_native_read_shape(monkeypatch, addresses):
    session = _session()
    events = []

    async def address(_session, schema, name):
        events.append(("catalog", schema, name))
        return addresses[("code_catalog", "code_crosswalk").index(name)]

    async def lock(*_args, **_options):
        events.append(("lock",))

    catalog = AsyncMock()
    monkeypatch.setattr(archive, "_relation_oid", address)
    monkeypatch.setattr(archive, "_lock_family", lock)
    monkeypatch.setattr(archive, "require_native_read_catalog", catalog)
    if None in addresses:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="dictionary is unavailable"):
            await dictionary._lock_reference_dictionary(session, "mrf")
        catalog.assert_not_awaited()
    else:
        assert await dictionary._lock_reference_dictionary(session, "mrf") == (
            ("mrf", "code_catalog"),
            ("mrf", "code_crosswalk"),
        )
        catalog.assert_awaited_once_with(session, addresses)
    assert events == [("lock",), ("catalog", "mrf", "code_catalog"), ("catalog", "mrf", "code_crosswalk")]
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
async def test_existing_dictionary_effects_require_registered_predecessor_before_capture(
    monkeypatch, registered, named_effects
):
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
async def test_dictionary_effect_capture_refuses_aggregate_row_bound(current_present, named_effects):
    session = _session()
    session.scalar.side_effect = [False] * (3 if current_present else 2) + [True]
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="publication bound is exceeded"):
        await dictionary._capture_dictionary_effect_table(
            session,
            "candidate",
            "mrf",
            dictionary._DRUG_SCOPED_MODELS[0],
            dictionary._DRUG_EFFECT_MODELS[0],
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
async def test_dictionary_effect_closure_refuses_either_set_without_mutation(monkeypatch, failed_model, named_effects):
    session = _session()
    session.scalar.side_effect = [False] * failed_model + [True]
    rollup = AsyncMock()
    monkeypatch.setattr(dictionary, "validate_prescription_rollup", rollup)
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="effect closure differs"):
        await dictionary.validate_reference_dictionary_effects(session, _ownership())
    rollup.assert_awaited_once_with(session, "candidate", require_binding=True, provider_schema="mrf")
    assert session.scalar.await_count == failed_model + 1
    query = str(session.scalar.await_args.args[0])
    assert dictionary._DRUG_EFFECT_MODELS[failed_model].__tablename__ in query
    assert "count(DISTINCT (destination_schema,destination_name))>1" in query
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("rollback", (False, True))
@pytest.mark.parametrize("failed_model", (0, 1))
async def test_dictionary_readback_failure_stops_publication_and_rollback(
    monkeypatch, rollback, failed_model, named_effects
):
    session = _session()
    monkeypatch.setattr(dictionary, "_lock_reference_dictionary", AsyncMock(return_value=(201, 202)))
    fence_count = 8 if rollback else 4
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
