# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure receipt refusals; synthetic shapes never establish native publication authority."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import nucc_protected_control as control


def publication():
    return {
        "contract": control.PUBLICATION_CONTRACT,
        "preparation_id": "11111111-1111-4111-8111-111111111111",
        "node_id": "test-node",
        "run_id": "test-run",
        "attempt_id": "test-run:" + "1" * 32,
        "history": ["mrf", "import_run"],
        "parent": ["mrf", "nucc_taxonomy"],
        "parent_index": ["mrf", "nucc_taxonomy_pkey"],
        "child": ["reference_family_archive_" + "1" * 32, "nucc_taxonomy"],
        "previous_child": ["reference_family_archive_" + "0" * 32, "nucc_taxonomy"],
        "generation_id": "22222222-2222-4222-8222-222222222222",
        "publication_fence": "33333333-3333-4333-8333-333333333333",
        "admission_sha256": "a" * 64,
        "handoff_sha256": "b" * 64,
        "inventory_sha256": "c" * 64,
        "validation_sha256": "d" * 64,
        "row_count": 42,
        "semantic_sha256": "e" * 64,
        "schema_sha256": "f" * 64,
        "source_predecessor": source_generation(4, schema="reference_family_archive_" + "0" * 32),
        "source_authority": source_generation(5, schema="reference_family_archive_" + "1" * 32),
        "published_at": "2026-10-17T05:00:00Z",
        "purpose": "source",
        "operation_id": None,
        "request_id": None,
    }


def source_generation(counter, _legacy_address=None, *, schema="reference_family_archive_" + "a" * 32):
    lineage = "44444444-4444-4444-8444-444444444444"
    return {
        "importer_id": "nucc",
        "local_lineage_id": lineage,
        "local_generation": counter,
        "serving_generation": None
        if counter == 0
        else {"origin_lineage_id": lineage, "origin_generation": counter, "published_at": "2026-10-17T05:00:00Z"},
        "relations": None if counter == 0 else [{"schema_name": schema, "relation_name": "nucc_taxonomy"}],
    }


def handoff():
    receipt = publication()
    handoff_by_field = {key: item for key, item in receipt.items() if key in control.HANDOFF_FIELDS}
    handoff_by_field.update(
        contract=control.HANDOFF_CONTRACT,
        attempt_started_at="2026-10-17T04:00:00.000000+00:00",
        source_contract_sha256="a" * 64,
    )
    handoff_by_field["handoff_sha256"] = control.receipt_digest(
        {key: item for key, item in handoff_by_field.items() if key != "handoff_sha256"}
    )
    return handoff_by_field


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["test-run", None, "other"])
async def test_source_ack_stores_utc_and_keeps_the_exact_original_cas(changed):
    receipt = publication()
    receipt["published_at"] = "2001-02-03T05:06:07.123456Z"
    receipt["source_authority"]["serving_generation"]["published_at"] = receipt["published_at"]
    receipt["source_predecessor"]["serving_generation"]["published_at"] = "2001-02-03T04:00:00Z"
    receipt = control.validate_publication(receipt)
    candidate = handoff()
    run_by_field = {"status": "finalizing", "finished_at": None, "error": None, "params": {}}
    session = SimpleNamespace(scalar=AsyncMock(return_value=changed))
    if changed == receipt["run_id"]:
        await control._acknowledge_run(session, run_by_field, candidate, receipt)
    else:
        with pytest.raises(control.NuccProtectedControlError, match="Source run ACK changed"):
            await control._acknowledge_run(session, run_by_field, candidate, receipt)
    statement, parameters = session.scalar.await_args.args
    assert "heartbeat_at=clock_timestamp() AT TIME ZONE 'UTC'" in str(statement)
    assert "finished_at=CAST(:published AS timestamptz) AT TIME ZONE 'UTC'" in str(statement)
    assert "AND status=:prior_status" in str(statement) and parameters["prior_status"] == "finalizing"
    assert "metrics::jsonb->'nucc_handoff'=CAST(:handoff AS jsonb)" in str(statement)
    assert "params::jsonb=CAST(:params AS jsonb)" in str(statement)
    assert type(parameters["published"]) is control.datetime
    assert parameters["published"] == control.datetime(2001, 2, 3, 5, 6, 7, 123456, tzinfo=control.timezone.utc)
    assert parameters["published"].microsecond == 123456 and parameters["attempt_id"] == receipt["attempt_id"]


def test_publication_is_closed_and_stable():
    value = publication()
    original = deepcopy(value)
    assert control.validate_publication(value) == original
    assert value == original
    assert control.receipt_digest(value) == control.receipt_digest(dict(reversed(list(value.items()))))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("contract", "nucc-native-publication.v2"),
        ("history", ["other", "import_run"]),
        ("child", "invalid"),
        ("previous_child", ["reference_family_archive_" + "1" * 32, "nucc_taxonomy"]),
        ("parent_index", ["mrf", "nucc_taxonomy"]),
        ("parent", ["other", "nucc_taxonomy"]),
        ("history", ["mrf", "other"]),
        ("row_count", False),
        ("row_count", 10001),
        ("parent_index", ["other", "index"]),
        ("child", ["reference_family_archive_" + "1" * 32, "other"]),
        ("previous_child", ["mrf", "nucc_taxonomy"]),
        ("attempt_id", "other-run:" + "1" * 32),
        ("node_id", " test-node"),
        ("child", ["mrf", "nucc_taxonomy"]),
        ("semantic_sha256", "A" * 64),
        ("generation_id", "{22222222-2222-4222-8222-222222222222}"),
    ],
)
def test_publication_rejects_malformed_identity(field, value):
    receipt = publication()
    receipt[field] = value
    with pytest.raises(control.NuccProtectedControlError):
        control.validate_publication(receipt)


@pytest.mark.parametrize("field", ["claim_token", "lease_token", "admission", "credential", "secret"])
def test_publication_rejects_unreviewed_fields(field):
    receipt = publication()
    receipt[field] = "unreviewed"
    with pytest.raises(control.NuccProtectedControlError, match="fields differ"):
        control.validate_publication(receipt)


def test_event_projection_excludes_secret_json():
    assert set(control.SOURCE_ACK_COLUMNS).isdisjoint(
        {
            "admission",
            "handoff",
            "publication",
            "claim_token",
            "claim_expires_at",
            "lease_token",
        }
    )
    assert len(control.SOURCE_ACK_COLUMNS) == len(set(control.SOURCE_ACK_COLUMNS)) == 9


def test_handoff_binds_source_contract():
    value = handoff()
    assert control.validate_handoff(value) == value
    changed = deepcopy(value)
    changed["row_count"] += 1
    with pytest.raises(control.NuccProtectedControlError, match="handoff digest differs"):
        control.validate_handoff(changed)
    from process.reference_family_archive import _nucc_source_contract

    params_by_name = {
        "source_url": "https://example.invalid/nucc.csv",
        "test_mode": False,
    }
    assert control.source_contract_digest(value, params_by_name) == _nucc_source_contract(
        {**value, "schema_name": "mrf"}, params_by_name
    )


@pytest.mark.parametrize("field", ["source_predecessor", "source_authority"])
@pytest.mark.parametrize("change", ["lineage", "counter", "child", "importer"])
def test_publication_rejects_wrong_generation(field, change):
    receipt = publication()
    generation = receipt[field]
    if change == "lineage":
        generation["local_lineage_id"] = "55555555-5555-4555-8555-555555555555"
    elif change == "counter":
        generation["local_generation"] += 2
    elif change == "child":
        generation["relations"] = [{"schema_name": "other", "relation_name": "nucc_taxonomy"}]
    else:
        generation["importer_id"] = "geo"
    with pytest.raises(control.NuccProtectedControlError):
        control.validate_publication(receipt)


def test_receive_preserves_signed_origin():
    receipt = publication()
    receipt.update(
        purpose="receive",
        preparation_id=None,
        run_id=None,
        attempt_id=None,
        operation_id="66666666-6666-4666-8666-666666666666",
    )
    receipt["source_authority"]["serving_generation"]["origin_lineage_id"] = "55555555-5555-4555-8555-555555555555"
    receipt["source_authority"]["serving_generation"]["origin_generation"] = 17
    assert control.validate_publication(receipt) == receipt


def test_source_rejects_foreign_origin():
    receipt = publication()
    receipt["source_authority"]["serving_generation"]["origin_lineage_id"] = "55555555-5555-4555-8555-555555555555"
    with pytest.raises(control.NuccProtectedControlError, match="native Source origin differs"):
        control.validate_publication(receipt)


@pytest.mark.parametrize("purpose", ["receive", "restore"])
def test_generic_publication_has_real_identity(purpose):
    receipt = publication()
    receipt.update(
        purpose=purpose,
        preparation_id=None,
        run_id=None,
        attempt_id=None,
        operation_id="66666666-6666-4666-8666-666666666666",
    )
    if purpose == "restore":
        receipt["request_id"] = "77777777-7777-4777-8777-777777777777"
    assert control.validate_publication(receipt) == receipt
    receipt["run_id"] = "invented-run"
    with pytest.raises(control.NuccProtectedControlError, match="invents a Source"):
        control.validate_publication(receipt)


def event():
    receipt = publication()
    return {
        "fence_token": receipt["publication_fence"],
        "node_id": receipt["node_id"],
        "importer_id": "nucc",
        "dataset_key": "nucc",
        "generation_id": receipt["generation_id"],
        "expected_generation_id": None,
        "expected_fence_token": None,
        "binding_sha256": "a" * 64,
        "nucc_source_ack": receipt,
    }


def test_event_binds_closed_receipt():
    assert control.validate_event_ack(event()) == publication()


@pytest.mark.parametrize("field", ["fence_token", "node_id", "importer_id", "dataset_key", "generation_id"])
def test_event_rejects_identity_mismatch(field):
    captured = event()
    captured[field] = "different"
    with pytest.raises(control.NuccProtectedControlError):
        control.validate_event_ack(captured)


def test_event_rejects_partial_predecessor():
    captured = event()
    captured["expected_generation_id"] = "77777777-7777-4777-8777-777777777777"
    with pytest.raises(control.NuccProtectedControlError, match="predecessor"):
        control.validate_event_ack(captured)


def test_source_run_binds_real_parameters():
    candidate = handoff()
    params_by_name = {"test_mode": False, "source": "synthetic"}
    candidate["source_contract_sha256"] = control.source_contract_digest(candidate, params_by_name)
    candidate["handoff_sha256"] = control.receipt_digest(
        {key: item for key, item in candidate.items() if key != "handoff_sha256"}
    )
    run_by_field = {
        "run_id": candidate["run_id"],
        "node_id": candidate["node_id"],
        "params": params_by_name,
        "progress": {name: candidate[name] for name in ("attempt_id", "attempt_started_at")},
        "metrics": {"nucc_handoff": candidate},
    }
    assert control._run_handoff(run_by_field) == candidate
    params_by_name["source"] = "changed"
    with pytest.raises(control.NuccProtectedControlError, match="parameters differ"):
        control._run_handoff(run_by_field)


@pytest.mark.asyncio
async def test_publisher_authority_rejects_serialized_actor():
    with pytest.raises(control.NuccProtectedControlError, match="original Publisher transaction required"):
        await control._publisher_transaction({"pid": 321, "owner_oid": 102})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source_active,publisher_active,changed",
    [(True, True, False), (False, True, False), (True, False, False), (True, True, True)],
)
async def test_pre_cutover_requires_live_owning_transactions_without_database_identity(
    source_active, publisher_active, changed
):
    source_transaction = object()
    publisher_transaction = SimpleNamespace(is_active=publisher_active)
    session = SimpleNamespace(
        in_transaction=lambda: source_active,
        get_transaction=lambda: object() if changed else source_transaction,
        execute=AsyncMock(),
        scalar=AsyncMock(),
    )
    arguments = (
        session,
        {"transaction": source_transaction},
        {"transaction": publisher_transaction},
        ("mrf", "nucc_taxonomy"),
    )
    if source_active and publisher_active and not changed:
        await control._require_publisher_fence(*arguments, run_id="test-run")
    else:
        with pytest.raises(control.NuccProtectedControlError, match="pre-cutover transaction"):
            await control._require_publisher_fence(*arguments, run_id="test-run")
    session.execute.assert_not_awaited()
    session.scalar.assert_not_awaited()


def _eligibility_connection(events, expected):
    from tests.test_nucc_snapshot_partition import parent_catalog

    class Connection:
        async def scalar(self, statement):
            events.append("catalog")
            return parent_catalog()

    return Connection(), control._source_generation(expected, predecessor=True)


@pytest.mark.asyncio
async def test_source_generation_row_is_locked_after_named_catalog_and_before_publication(monkeypatch):
    from process import reference_family_result_generation as generations

    events = []
    expected = source_generation(4)
    connection, current = _eligibility_connection(events, expected)
    source_actor_by_field = {"node_id": "test-node"}
    publisher_actor_by_field = {"role": "publisher"}
    publisher_session = object()
    monkeypatch.setattr(control, "_source_transaction", AsyncMock(return_value=source_actor_by_field))
    monkeypatch.setattr(control, "_publisher_transaction", AsyncMock(return_value=publisher_actor_by_field))

    async def fence(_session, source, publisher, parent):
        assert (source, publisher, parent) == (
            source_actor_by_field,
            publisher_actor_by_field,
            ("mrf", "nucc_taxonomy"),
        )
        events.append("before-cutover")

    async def controls(*_args):
        events.append("controls")

    async def generation(_session, **options):
        assert options == {"importer_id": "nucc", "schema_name": "mrf", "lock": True}
        events.append("generation-row-lock")
        return current

    async def prior(*_args, **kwargs):
        assert kwargs == {"publisher_session": publisher_session}
        events.append("prior-ack")

    monkeypatch.setattr(control, "_require_publisher_fence", fence)
    monkeypatch.setattr(control, "_require_source_controls", controls)
    monkeypatch.setattr(control, "_require_prior_owner_ack", prior)
    monkeypatch.setattr(control, "_recheck_source_transaction", AsyncMock())
    monkeypatch.setattr(control, "_recheck_publisher_transaction", AsyncMock())
    monkeypatch.setattr(generations, "read_reference_family_result_generation_authority", generation)
    assert (
        await control.lock_generation_eligibility(
            connection,
            expected,
            expected_child=("reference_family_archive_" + "a" * 32, "nucc_taxonomy"),
            publisher_session=publisher_session,
        )
        == expected
    )
    assert events == ["before-cutover", "catalog", "controls", "generation-row-lock", "prior-ack"]


@pytest.mark.asyncio
async def test_publisher_transaction_refusal_precedes_catalog_deparser():
    session = SimpleNamespace(in_transaction=lambda: True, get_transaction=lambda: None, scalar=AsyncMock())
    with pytest.raises(control.NuccProtectedControlError, match="pre-cutover transaction"):
        await control._require_publisher_fence(
            session, {"transaction": None}, {"transaction": SimpleNamespace(is_active=False)}, ("mrf", "nucc_taxonomy")
        )
    session.scalar.assert_not_awaited()


def _reconciliation_run():
    candidate = handoff()
    params_by_name = {"test_mode": False}
    candidate["source_contract_sha256"] = control.source_contract_digest(candidate, params_by_name)
    candidate["handoff_sha256"] = control.receipt_digest(
        {key: field for key, field in candidate.items() if key != "handoff_sha256"}
    )
    run_by_field = {
        "node_id": "test-node",
        "metrics": {"nucc_handoff": candidate},
        "params": params_by_name,
        "run_id": "test-run",
        "progress": {key: candidate[key] for key in ("attempt_id", "attempt_started_at")},
    }
    return run_by_field, candidate


@pytest.fixture
def reconciliation_case(monkeypatch):
    from process import control_lifecycle

    events = []
    run_by_field, candidate = _reconciliation_run()
    replies_by_phase = {"fence": None, "run": None}

    class Connection:
        async def scalar(self, _statement):
            return 101

    connection = Connection()

    async def actor(session):
        assert session is connection
        events.append("actor")
        return {"node_id": "test-node"}

    async def action(executor, run_id):
        assert executor._connection is connection and run_id == "test-run"
        events.append("action")

    async def controls(*_arguments):
        events.append("controls")

    async def locked(*_arguments):
        events.append("row")
        return run_by_field

    async def find_fence(*_arguments):
        events.append("owner-fence")
        return replies_by_phase["fence"]

    async def find_run(*_arguments):
        events.append("owner-run")
        return replies_by_phase["run"]

    async def unchanged(*_arguments):
        events.append("recheck")

    async def ack(session, run_id, fence):
        assert session is connection and run_id == "test-run" and fence == candidate["publication_fence"]
        events.append("ack")
        return replies_by_phase["fence"] or replies_by_phase["run"]

    callbacks_by_name = {
        "_source_transaction": actor,
        "_require_source_controls": controls,
        "_locked_source_run": locked,
        "_find_owner_event": find_fence,
        "_find_run_owner_event": find_run,
        "_recheck_source_transaction": unchanged,
        "acknowledge_source_publication": ack,
    }
    for name, callback in callbacks_by_name.items():
        monkeypatch.setattr(control, name, callback)
    monkeypatch.setattr(control_lifecycle, "acquire_control_run_worker_action_lock", action)
    return connection, run_by_field, replies_by_phase, events


@pytest.mark.asyncio
async def test_reconciliation_absence_is_authenticated_under_action_before_row(reconciliation_case):
    connection, _run, _replies, events = reconciliation_case
    assert await control.reconcile_source_publication(connection, "test-run") is None
    assert events == ["actor", "action", "controls", "row", "owner-fence", "owner-run", "recheck"]


@pytest.mark.asyncio
async def test_pre_handoff_reconciliation_still_queries_protected_run_event(reconciliation_case):
    connection, run_by_field, _replies, events = reconciliation_case
    run_by_field["metrics"] = {}
    assert await control.reconcile_source_publication(connection, "test-run") is None
    assert events == ["actor", "action", "controls", "row", "owner-run", "recheck"]


@pytest.mark.asyncio
async def test_reconciliation_present_outcome_uses_exact_ack(reconciliation_case):
    connection, _run, replies_by_phase, events = reconciliation_case
    replies_by_phase["fence"] = publication()
    assert await control.reconcile_source_publication(connection, "test-run") == publication()
    assert events == ["actor", "action", "controls", "row", "owner-fence", "ack"]


@pytest.mark.asyncio
async def test_reconciliation_custody_failure_never_becomes_absence(reconciliation_case, monkeypatch):
    connection, _run, _replies, events = reconciliation_case
    failure = control.NuccProtectedControlError("genuine ledger custody failure")

    async def broken(*_arguments):
        raise failure

    monkeypatch.setattr(control, "_find_owner_event", broken)
    with pytest.raises(control.NuccProtectedControlError) as rejected:
        await control.reconcile_source_publication(connection, "test-run")
    assert rejected.value is failure and "owner-run" not in events and "ack" not in events


@pytest.mark.asyncio
@pytest.mark.parametrize("contract", ["nucc-native-handoff.v1", "nucc-native-handoff.v2"])
async def test_reconciliation_refuses_legacy_handoff_without_event_fallback(reconciliation_case, contract):
    connection, run_by_field, _replies, events = reconciliation_case
    run_by_field["metrics"]["nucc_handoff"]["contract"] = contract
    with pytest.raises(control.NuccProtectedControlError, match="handoff contract differs"):
        await control.reconcile_source_publication(connection, "test-run")
    assert events == ["actor", "action", "controls", "row"]


@pytest.mark.asyncio
async def test_reconciliation_refuses_wrong_owner_locator(reconciliation_case):
    connection, _run, replies_by_phase, events = reconciliation_case
    replies_by_phase["run"] = {**publication(), "publication_fence": "99999999-9999-4999-8999-999999999999"}
    with pytest.raises(control.NuccProtectedControlError, match="outcome locator differs"):
        await control.reconcile_source_publication(connection, "test-run")
    assert "ack" not in events


@pytest.mark.asyncio
async def test_reconciliation_does_not_accept_metrics_as_owner_authority(reconciliation_case):
    connection, run_by_field, _replies, events = reconciliation_case
    run_by_field["metrics"]["nucc_native_publication"] = publication()
    with pytest.raises(control.NuccProtectedControlError, match="mirrored Owner outcome unavailable"):
        await control.reconcile_source_publication(connection, "test-run")
    assert "ack" not in events


@pytest.mark.asyncio
@pytest.mark.parametrize("finder", ["fence", "run"])
async def test_present_oversized_event_refuses_instead_of_becoming_absent(monkeypatch, finder):
    event_by_field = {key: None for key in control.SOURCE_ACK_COLUMNS}
    event_by_field.update(fence_token=publication()["publication_fence"], nucc_source_ack=None)

    class QueryReply:
        def mappings(self):
            return self

        def one_or_none(self):
            return event_by_field

        def all(self):
            return [event_by_field]

    class Connection:
        async def execute(self, statement, _parameters):
            sql = str(statement)
            projection, where = sql.split(" FROM hp_snapshot_retention.publication WHERE", 1)
            assert "CASE WHEN pg_catalog.octet_length(nucc_source_ack::text)<=131072" in projection
            assert "THEN nucc_source_ack ELSE NULL END AS nucc_source_ack" in projection
            assert "octet_length" not in where
            return QueryReply()

    async def custody(*_arguments):
        return None

    monkeypatch.setattr(control, "_lock_event_custody", custody)
    query = control._find_owner_event if finder == "fence" else control._find_run_owner_event
    locator = publication()["publication_fence"] if finder == "fence" else "test-run"
    with pytest.raises(control.NuccProtectedControlError, match="publication fields differ"):
        await query(Connection(), {}, locator)


def _received_publication(purpose):
    receipt = publication()
    receipt.update(
        purpose=purpose,
        preparation_id=None,
        run_id=None,
        attempt_id=None,
        operation_id="66666666-6666-4666-8666-666666666666",
        request_id="77777777-7777-4777-8777-777777777777" if purpose == "restore" else None,
    )
    receipt["published_at"] = "2026-10-17T06:00:00Z"
    receipt["source_authority"]["serving_generation"]["origin_lineage_id"] = "55555555-5555-4555-8555-555555555555"
    receipt["source_authority"]["serving_generation"]["origin_generation"] = 17
    return receipt


@pytest.mark.parametrize("purpose", ["receive", "restore"])
def test_signed_origin_time_survives_later_local_owner_commit(purpose):
    receipt = _received_publication(purpose)
    original = deepcopy(receipt)
    assert control.validate_publication(receipt) == original
    assert receipt["source_authority"]["serving_generation"]["published_at"] == "2026-10-17T05:00:00Z"
    assert receipt["published_at"] == "2026-10-17T06:00:00Z"


def test_genuine_source_time_still_matches_its_owner_event():
    receipt = publication()
    receipt["published_at"] = "2026-10-17T06:00:00Z"
    with pytest.raises(control.NuccProtectedControlError, match="publication time differs"):
        control.validate_publication(receipt)


@pytest.mark.asyncio
@pytest.mark.parametrize("purpose", ["receive", "restore"])
async def test_generation_ack_projects_signed_origin_time_not_receive_clock(monkeypatch, purpose):
    from process import reference_family_result_generation as generations

    receipt = _received_publication(purpose)
    receipt["source_predecessor"]["serving_generation"]["published_at"] = "2001-02-03T04:00:00Z"
    receipt["source_authority"]["serving_generation"]["published_at"] = "2001-02-03T05:06:07.123456Z"
    receipt["published_at"] = "2001-02-04T06:07:08.654321Z"
    receipt = control.validate_publication(receipt)
    observed_parameters = []

    async def original(_session, **_options):
        return control._source_generation(receipt["source_predecessor"], predecessor=True)

    class Session:
        async def scalar(self, statement, parameters):
            assert str(statement).startswith("UPDATE mrf.reference_family_result_generation")
            assert type(parameters["published"]) is control.datetime
            observed_parameters.append(parameters)
            return "nucc"

    monkeypatch.setattr(generations, "read_reference_family_result_generation_authority", original)
    await control._project_generation(Session(), receipt)
    [parameters] = observed_parameters
    assert parameters["published"] == control.datetime(2001, 2, 3, 5, 6, 7, 123456, tzinfo=control.timezone.utc)
    assert parameters["published"].microsecond == 123456
    assert parameters["published"] != control.datetime.fromisoformat(receipt["published_at"].replace("Z", "+00:00"))


@pytest.fixture
def initial_baseline_case(monkeypatch):
    from tests.test_nucc_protected_bootstrap import baseline
    from tests.test_nucc_snapshot_partition import parent_catalog

    catalog = parent_catalog()
    marker, names, _authority = baseline()
    authority = source_generation(0)
    marker["source_authority"] = authority
    marker["source_revision_tracked"] = False
    marker["baseline_sha256"] = control.receipt_digest(
        {k: field_value for k, field_value in marker.items() if k != "baseline_sha256"}
    )
    evidence_by_field = {"marker": marker, "no_events": True, "tracked": False, "content": (4, "c" * 64)}
    actor_by_field = {"node_id": "test-node"}

    class Session:
        async def scalar(self, statement, parameters=None):
            sql = str(statement)
            if "NOT EXISTS(SELECT FROM hp_snapshot_retention.publication" in sql:
                return evidence_by_field["no_events"]
            if sql.startswith("SELECT obj_description"):
                assert parameters == {"parent": '"mrf"."nucc_taxonomy"'}
                return evidence_by_field["marker"]
            if "source_revision_tracked" in sql:
                return evidence_by_field["tracked"]
            raise AssertionError(sql)

    from process import reference_family_archive as native

    publisher_session = object()

    async def content(selected, schema, relation, **kwargs):
        assert selected is publisher_session
        assert (schema, relation) == names["child"]
        assert kwargs == {"row_json_sql": "to_jsonb(row_value)"}
        return evidence_by_field["content"]

    monkeypatch.setattr(native, "_projected_row_identity", content)
    monkeypatch.setattr(control, "_recheck_source_transaction", AsyncMock())
    return Session(), actor_by_field, authority, catalog, evidence_by_field, publisher_session


@pytest.mark.asyncio
async def test_initial_baseline_preserves_untracked_zero_and_compares_complete_content(initial_baseline_case):
    session, actor, authority, catalog, _evidence, publisher_session = initial_baseline_case
    await control._require_initial_baseline(session, actor, authority, catalog, publisher_session=publisher_session)
    assert authority["local_generation"] == 0 and authority["serving_generation"] is authority["relations"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ("event", "source", "content", "scope", "tracking"))
async def test_initial_baseline_refuses_consumed_changed_source_content_scope_or_tracking(
    initial_baseline_case, change
):
    session, actor, authority_by_field, catalog, evidence, publisher_session = initial_baseline_case
    if change == "event":
        evidence["no_events"] = False
    elif change == "source":
        authority_by_field = {**authority_by_field, "local_generation": 1}
    elif change == "content":
        evidence["content"] = (4, "d" * 64)
    elif change == "scope":
        evidence["marker"]["child"] = ["reference_family_archive_" + "b" * 32, "nucc_taxonomy"]
    else:
        evidence["tracked"] = True
    evidence["marker"]["baseline_sha256"] = control.receipt_digest(
        {k: field_value for k, field_value in evidence["marker"].items() if k != "baseline_sha256"}
    )
    with pytest.raises((control.NuccProtectedControlError, RuntimeError)):
        await control._require_initial_baseline(
            session, actor, authority_by_field, catalog, publisher_session=publisher_session
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("denied", (False, True))
async def test_event_access_uses_only_declared_columns_and_propagates_native_permission_denial(denied):
    session = SimpleNamespace(
        in_transaction=lambda: True, execute=AsyncMock(side_effect=PermissionError("SELECT denied") if denied else None)
    )
    if denied:
        with pytest.raises(PermissionError, match="SELECT denied"):
            await control._lock_event_custody(session, {})
    else:
        await control._lock_event_custody(session, {})
    sql = str(session.execute.await_args.args[0])
    assert (
        sql == "SELECT " + ",".join(control.SOURCE_ACK_COLUMNS) + " FROM hp_snapshot_retention.publication WHERE false"
    )
    assert "aclexplode" not in sql and "oid" not in sql


@pytest.mark.asyncio
async def test_initial_ack_forwards_publisher_only_for_baseline_content(monkeypatch):
    result = SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: None))
    source_session = SimpleNamespace(execute=AsyncMock(return_value=result))
    publisher_session = object()
    actor_by_field, authority_by_field, parent_catalog = {}, source_generation(0), {}
    event_lock, baseline = AsyncMock(), AsyncMock()
    monkeypatch.setattr(control, "_lock_event_custody", event_lock)
    monkeypatch.setattr(control, "_require_initial_baseline", baseline)
    await control._require_prior_owner_ack(
        source_session,
        actor_by_field,
        authority_by_field,
        "test-node",
        parent_catalog,
        publisher_session=publisher_session,
    )
    event_lock.assert_awaited_once_with(source_session, actor_by_field)
    assert source_session.execute.await_args.args[1] == {"node": "test-node"}
    baseline.assert_awaited_once_with(
        source_session, actor_by_field, authority_by_field, parent_catalog, publisher_session=publisher_session
    )
