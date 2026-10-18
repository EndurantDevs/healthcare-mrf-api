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
        "database_oid": 100,
        "history_oid": 101,
        "owner_oid": 102,
        "parent_oid": 103,
        "parent_index_oid": 104,
        "child_oid": 105,
        "child_schema": "reference_family_archive_" + "1" * 32,
        "child_schema_oid": 106,
        "child_filenode": 107,
        "previous_child_oid": 108,
        "generation_id": "22222222-2222-4222-8222-222222222222",
        "publication_fence": "33333333-3333-4333-8333-333333333333",
        "admission_sha256": "a" * 64,
        "handoff_sha256": "b" * 64,
        "inventory_sha256": "c" * 64,
        "validation_sha256": "d" * 64,
        "row_count": 42,
        "semantic_sha256": "e" * 64,
        "schema_sha256": "f" * 64,
        "transaction_id": "1234",
        "source_predecessor": source_generation(4, 108),
        "source_authority": source_generation(5, 105),
        "published_at": "2026-10-17T05:00:00Z",
        "purpose": "source",
        "operation_id": None,
        "request_id": None,
    }


def source_generation(counter, oid):
    lineage = "44444444-4444-4444-8444-444444444444"
    return {
        "importer_id": "nucc",
        "local_lineage_id": lineage,
        "local_generation": counter,
        "serving_generation": {
            "origin_lineage_id": lineage,
            "origin_generation": counter,
            "published_at": "2026-10-17T05:00:00Z",
        },
        "relation_oids": [oid],
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
        ("database_oid", True),
        ("child_oid", "105"),
        ("previous_child_oid", 105),
        ("parent_index_oid", 103),
        ("owner_oid", 0),
        ("history_oid", 2**32),
        ("row_count", False),
        ("row_count", 10001),
        ("transaction_id", "01234"),
        ("transaction_id", str(2**64)),
        ("transaction_id", 1234),
        ("attempt_id", "other-run:" + "1" * 32),
        ("node_id", " test-node"),
        ("child_schema", "mrf"),
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
        generation["relation_oids"] = [999]
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


def custody():
    return {
        "oid": 100,
        "owner_oid": 101,
        "schema_owner_oid": 101,
        "acl": [
            [101, 101, privilege, False]
            for privilege in (
                "SELECT",
                "INSERT",
                "UPDATE",
                "DELETE",
                "TRUNCATE",
                "REFERENCES",
                "TRIGGER",
                "MAINTAIN",
            )
        ],
        "column_acl": [[name, 102, 101, "SELECT", False] for name in control.SOURCE_ACK_COLUMNS],
    }


def test_custody_requires_exact_column_grants():
    control.validate_event_custody(custody(), owner_oid=101, builder_oid=102)


@pytest.mark.parametrize(("column", "replacement"), [(3, "UPDATE"), (4, True), (2, 102), (1, 103)])
def test_custody_rejects_column_authority(column, replacement):
    captured = custody()
    captured["column_acl"][0][column] = replacement
    with pytest.raises(control.NuccProtectedControlError):
        control.validate_event_custody(captured, owner_oid=101, builder_oid=102)


@pytest.mark.parametrize("surface", ["acl", "column_acl"])
def test_custody_rejects_added_grants(surface):
    captured = custody()
    extra_entries = [102, 101, "SELECT", False]
    captured[surface].append(extra_entries if surface == "acl" else ["created_at", *extra_entries])
    with pytest.raises(control.NuccProtectedControlError):
        control.validate_event_custody(captured, owner_oid=101, builder_oid=102)


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
    "xid,expected_xid",
    [
        ("0", 0),
        ("1", 1),
        (str(2**32 - 1), 2**32 - 1),
        (str(2**32), 0),
        (str(2**32 + 1), 1),
        (str(2**32 + 321), 321),
        (str(2**64 - 1), 2**32 - 1),
    ],
)
async def test_native_fence_binds_exact_backend_transaction_without_parent_ax(xid, expected_xid):
    queries = []

    class Connection:
        is_held = True

        async def scalar(self, statement, parameters):
            assert type(parameters["xid"]) is int
            queries.append((str(statement), parameters))
            return self.is_held

    source_actor_by_field = {"database_oid": 100, "owner_oid": 102, "pid": 201}
    publisher_actor_by_field = {
        "database_oid": 100,
        "owner_oid": 102,
        "pid": 202,
        "xid": xid,
        "virtualtransaction": "4/99",
    }
    await control._require_publisher_fence(
        Connection(),
        source_actor_by_field,
        publisher_actor_by_field,
        103,
        run_id="test-run",
    )
    sql, parameters = queries[0]
    assert parameters == {
        "pid": 202,
        "virtualtransaction": "4/99",
        "xid": expected_xid,
        "database": 100,
        "parent": 103,
        "name": "control-run-worker-action:v1:test-run",
    }
    assert "transaction_lock.transactionid=CAST(:xid AS xid)" in sql
    assert "action_lock.virtualtransaction=:virtualtransaction" in sql
    assert "AND NOT EXISTS(SELECT FROM pg_locks parent_lock" in sql
    assert "parent_lock.mode='AccessExclusiveLock'" in sql


def _eligibility_connection(events, expected):
    from process import nucc_snapshot_partition as partition

    class Connection:
        async def scalar(self, statement):
            sql = str(statement)
            if sql == partition.CATALOG_SQL:
                events.append("catalog")
                return {"parent_oid": 103, "child_oid": 108}
            return 103 if "nucc_taxonomy" in sql else 101

    class Generation:
        relation_oids = (108,)

        def as_dict(self):
            return expected

    return Connection(), Generation()


@pytest.mark.asyncio
async def test_source_catalog_runs_before_publisher_parent_ax(monkeypatch):
    from process import nucc_snapshot_partition as partition
    from process import reference_family_result_generation as generations

    events = []
    source_actor_by_field = {"node_id": "test-node"}
    publisher_actor_by_field = {"pid": 202}
    expected = source_generation(4, 108)

    connection, generation_row = _eligibility_connection(events, expected)

    async def actor(_session):
        return source_actor_by_field

    async def publisher_actor(_session):
        return publisher_actor_by_field

    async def fence(_session, source_actor, publisher_actor, parent_oid):
        assert source_actor is source_actor_by_field and publisher_actor is publisher_actor_by_field
        assert parent_oid == 103
        events.append("pre-ax-fence")

    async def controls(*_arguments):
        events.append("controls")

    async def generation(_session, **options):
        assert options == {"importer_id": "nucc", "schema_name": "mrf", "lock": True}
        events.append("generation-row-lock")
        return generation_row

    async def prior(*_arguments):
        events.append("prior-ack")

    async def unchanged(*_arguments):
        return None

    monkeypatch.setattr(control, "_source_transaction", actor)
    monkeypatch.setattr(control, "_publisher_transaction", publisher_actor)
    monkeypatch.setattr(control, "_require_publisher_fence", fence)
    monkeypatch.setattr(control, "_require_source_controls", controls)
    monkeypatch.setattr(control, "_require_prior_owner_ack", prior)
    monkeypatch.setattr(control, "_recheck_source_transaction", unchanged)
    monkeypatch.setattr(control, "_recheck_publisher_transaction", unchanged)
    monkeypatch.setattr(partition, "validate_parent_catalog", lambda catalog: catalog)
    monkeypatch.setattr(generations, "read_reference_family_result_generation_authority", generation)
    assert (
        await control.lock_generation_eligibility(
            connection,
            expected,
            expected_child_oid=108,
            publisher_session=object(),
        )
        == expected
    )
    assert events == ["pre-ax-fence", "catalog", "pre-ax-fence", "controls", "generation-row-lock", "prior-ack"]


@pytest.mark.asyncio
async def test_parent_ax_refusal_precedes_catalog_deparser():
    class Connection:
        is_held = False

        async def scalar(self, statement, parameters):
            assert "pg_get_partkeydef" not in str(statement)
            return self.is_held

    source_actor_by_field = {"database_oid": 100, "owner_oid": 102, "pid": 201}
    publisher_actor_by_field = {
        "database_oid": 100,
        "owner_oid": 102,
        "pid": 202,
        "xid": "321",
        "virtualtransaction": "4/99",
    }
    with pytest.raises(control.NuccProtectedControlError, match="pre-cutover fence"):
        await control._require_publisher_fence(Connection(), source_actor_by_field, publisher_actor_by_field, 103)


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
    import json

    from process import nucc_snapshot_partition as partition
    from tests.test_nucc_protected_bootstrap import baseline
    from tests.test_nucc_snapshot_partition import parent_catalog

    marker, _physical, source_authority = baseline()
    source_authority.update(local_generation=0, serving_generation=None, relation_oids=None)
    captured = parent_catalog()
    marker.update(
        source_authority=source_authority,
        source_revision_tracked=False,
        protected_native_sha256=partition.catalog_digest(captured),
    )
    marker["baseline_sha256"] = control.receipt_digest(
        {key: field_value for key, field_value in marker.items() if key != "baseline_sha256"}
    )
    captured["parent"]["baseline"] = {"database_oid": 100, "comment": json.dumps(marker)}
    evidence_by_field = dict(catalog=captured, marker=marker, no_events=True, tracked=False)
    actor_by_field = {"database_oid": 100}

    async def actor(_session):
        return actor_by_field

    class Session:
        async def scalar(self, statement, parameters=None):
            sql = str(statement)
            if "SELECT NOT EXISTS" in sql:
                return evidence_by_field["no_events"]
            if sql.startswith("SELECT obj_description"):
                assert parameters == {"oid": 201}
                return json.dumps(evidence_by_field["marker"])
            if "SELECT source_revision_tracked" in sql:
                return evidence_by_field["tracked"]
            return evidence_by_field["catalog"]

    monkeypatch.setattr(control, "_source_transaction", actor)
    return Session(), actor_by_field, source_authority, captured, evidence_by_field


@pytest.mark.asyncio
async def test_initial_baseline_allows_real_untracked_zero_without_inventing_origin(initial_baseline_case):
    session, actor, source_authority, captured, _evidence = initial_baseline_case
    await control._require_initial_baseline(session, actor, source_authority, captured)
    assert source_authority["local_generation"] == 0
    assert source_authority["serving_generation"] is source_authority["relation_oids"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["event", "source", "native_hash", "database", "tracking"])
async def test_initial_baseline_refuses_other_or_consumed_authority(initial_baseline_case, change):
    session, actor, source_authority, captured, evidence_by_field = initial_baseline_case
    if change == "event":
        evidence_by_field["no_events"] = False
    elif change == "source":
        changed_authority_by_field = dict(source_authority)
        changed_authority_by_field["local_generation"] = 1
        source_authority = changed_authority_by_field
    else:
        marker = evidence_by_field["marker"]
        if change == "native_hash":
            marker["protected_native_sha256"] = "a" * 64
        elif change == "database":
            marker["database_oid"] += 1
        else:
            evidence_by_field["tracked"] = True
        marker["baseline_sha256"] = control.receipt_digest(
            {key: field_value for key, field_value in marker.items() if key != "baseline_sha256"}
        )
    with pytest.raises((control.NuccProtectedControlError, RuntimeError)):
        await control._require_initial_baseline(session, actor, source_authority, captured)


@pytest.mark.asyncio
async def test_initial_baseline_excludes_only_actual_native_comment_from_its_digest(initial_baseline_case):
    session, actor, source_authority, captured, evidence_by_field = initial_baseline_case
    assert captured["parent"]["baseline"]["database_oid"] == 100
    assert isinstance(captured["parent"]["baseline"]["comment"], str)
    await control._require_initial_baseline(session, actor, source_authority, captured)
    captured["parent"]["acl"].append([104, 101, "SELECT", False])
    with pytest.raises(control.NuccProtectedControlError, match="initial Owner baseline differs"):
        await control._require_initial_baseline(session, actor, source_authority, captured)
