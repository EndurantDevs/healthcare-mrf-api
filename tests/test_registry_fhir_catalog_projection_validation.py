# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure trust and memory contracts for closed-candidate projection replay."""

import json
from contextlib import asynccontextmanager
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import registry_fhir_catalog_projection_validation as validation
from process import registry_fhir_catalog_validation as catalog_validation
from tests.test_registry_fhir_catalog_grant import snapshot, standing_grant


@pytest.mark.asyncio
@pytest.mark.parametrize("in_transaction,matching_grant", [(False, True), (True, False)])
async def test_public_verifier_rejects_wrong_transaction_or_grant_before_any_work(in_transaction, matching_grant):
    grant = standing_grant()
    selected = snapshot(grant)
    if not matching_grant:
        selected["standing_grant_sha256"] = "f" * 64
    connection = SimpleNamespace(
        is_in_transaction=Mock(return_value=in_transaction),
        transaction=Mock(side_effect=AssertionError("rejected verifier must not open a savepoint")),
        execute=AsyncMock(),
        fetch=AsyncMock(),
    )
    with pytest.raises(ValueError, match="registry_fhir_catalog_observed_projection_changed"):
        await validation.validate_observed_projection(connection, selected, standing_grant=grant, prior_snapshot=None)
    connection.transaction.assert_not_called()
    connection.execute.assert_not_awaited()
    connection.fetch.assert_not_awaited()


def _replay():
    grant = standing_grant()
    return validation._ProjectionReplay(SimpleNamespace(execute=AsyncMock()), snapshot(grant), grant, None)


def _network_row(replay, network_id, name="Example Network"):
    return (replay.snapshot["metadata_generation"], network_id, name, '["Example alias"]', False, 1, None, name.lower())


@pytest.mark.asyncio
@pytest.mark.parametrize("row_limit,byte_limit", [(2, 4096), (10, 400)], ids=["row-limit", "byte-limit"])
async def test_replay_flushes_bounded_batches_without_losing_row_order(monkeypatch, row_limit, byte_limit):
    monkeypatch.setattr(validation, "_PAGE_ROWS", row_limit)
    monkeypatch.setattr(validation, "_COPY_BYTES", byte_limit)
    replay = _replay()
    await replay.write("network", (_network_row(replay, network_id) for network_id in range(1, 6)))
    writes = replay.connection.execute.await_args_list
    assert len(writes) > 1
    copied_ids = []
    for write in writes:
        query, payload = write.args
        assert query.startswith("INSERT INTO pg_temp.")
        documents = json.loads(payload)
        assert 0 < len(documents) <= row_limit
        assert len(payload.encode()) <= byte_limit
        assert all(document["aliases"] == ["Example alias"] for document in documents)
        copied_ids.extend(document["network_id"] for document in documents)
    assert copied_ids == [1, 2, 3, 4, 5]


@pytest.mark.asyncio
async def test_replay_rejects_oversized_utf8_row_before_flushing_pending_batch(monkeypatch):
    monkeypatch.setattr(validation, "_PAGE_ROWS", 10)
    monkeypatch.setattr(validation, "_COPY_BYTES", 400)
    replay = _replay()
    rows = (_network_row(replay, 1), _network_row(replay, 2, "界" * 128))
    with pytest.raises(ValueError, match="registry_fhir_catalog_observed_projection_changed"):
        await replay.write("network", rows)
    replay.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed",
    [
        {"actor": "other"},
        {"original_actor": "other"},
        {"database_oid": 23},
        {"backend": 10},
        {"isolation": "read committed"},
        {"readonly": "off"},
    ],
)
async def test_registry_pin_rejects_wrong_identity_or_transaction(changed):
    writer_by_field = {"actor": "sealer", "original_actor": "sealer", "database_oid": 22, "backend": 10}
    pinned_by_field = {**writer_by_field, "backend": 11, "isolation": "repeatable read", "readonly": "on", **changed}
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=writer_by_field))
    reader = SimpleNamespace(is_in_transaction=lambda: True, fetchrow=AsyncMock(return_value=pinned_by_field))
    grant = standing_grant()
    with pytest.raises(ValueError, match="observed_projection_changed"):
        await validation._require_registry_reader(connection, reader, snapshot(grant), grant)


@pytest.mark.asyncio
async def test_attached_or_retained_projection_requires_independent_pinned_reader():
    grant = standing_grant()
    connection = SimpleNamespace(fetchrow=AsyncMock())
    for selected, policy in (
        (snapshot(grant), grant),
        (snapshot(grant, network_generation=None, manifest_sha256=None), SimpleNamespace(retained_sources=(object(),))),
    ):
        with pytest.raises(ValueError, match="observed_projection_changed"):
            await validation._require_registry_reader(connection, None, selected, policy)
    with pytest.raises(ValueError, match="observed_projection_changed"):
        await validation._require_registry_reader(connection, connection, snapshot(grant), grant)
    connection.fetchrow.assert_not_awaited()


def _sealing_actor(name, events):
    @asynccontextmanager
    async def transaction(**options):
        events.append((name, "begin", options))
        try:
            yield
        except BaseException:
            events.append((name, "rollback"))
            raise
        else:
            events.append((name, "commit"))

    return SimpleNamespace(transaction=transaction, execute=AsyncMock())


@pytest.mark.asyncio
@pytest.mark.parametrize("fails", [False, True])
async def test_directory_pin_outlives_candidate_sealing_commit_or_failure(monkeypatch, fails):
    from process import registry_fhir_catalog_sync as sync

    events = []
    reader, sealer = _sealing_actor("reader", events), _sealing_actor("sealer", events)
    actors = iter((reader, sealer))

    @asynccontextmanager
    async def acquire():
        yield next(actors)

    workspace_token = object()

    @asynccontextmanager
    async def workspace(connection):
        assert connection is reader
        events.append(("reader", "workspace"))
        try:
            yield workspace_token
        finally:
            events.append(("reader", "workspace-drop"))

    monkeypatch.setattr(sync, "fhir_catalog_reference_workspace", workspace)

    grant = standing_grant()
    config = SimpleNamespace(
        grant=grant,
        consumer=SimpleNamespace(physical_owner_role="catalog_owner"),
        loader_role="catalog_loader",
        reader_roles=("catalog_reader",),
    )
    build = Mock(return_value=object())
    monkeypatch.setattr(sync, "build_fhir_catalog_observation_verifier", build)

    async def freeze(*args, **options):
        assert events == [
            ("reader", "workspace"),
            ("reader", "begin", {"isolation": "repeatable_read", "readonly": True}),
            ("sealer", "begin", {}),
        ]
        assert args[0] is sealer
        if fails:
            raise RuntimeError("synthetic sealing failure")
        return {"validation": {"tables": {}}}

    monkeypatch.setattr(sync, "freeze_fhir_catalog_snapshot", freeze)
    operation = sync._seal_catalog_snapshot(
        config, object(), snapshot(grant), SimpleNamespace(acquire=acquire), source_pool=None
    )
    if fails:
        with pytest.raises(RuntimeError, match="synthetic sealing failure"):
            await operation
    else:
        await operation
    build.assert_called_once_with(grant, None, registry_reader=reader, registry_workspace=workspace_token)
    outcome = "rollback" if fails else "commit"
    assert events[-3:] == [("sealer", outcome), ("reader", outcome), ("reader", "workspace-drop")]


def projected_documents():
    from process import registry_fhir_catalog_projection as projection
    from tests import test_registry_fhir_catalog_projection as fixture

    batch = fixture.project()
    documents_by_table = {}
    for name, columns, entries in (
        ("network", projection.NETWORK_COLUMNS, batch.network_rows),
        ("relationship", projection.RELATIONSHIP_COLUMNS, batch.relationship_rows),
        ("binding", projection.BINDING_COLUMNS, batch.binding_rows),
    ):
        documents = []
        for entry in entries:
            document_by_field = json.loads(json.dumps(dict(zip(columns, entry, strict=True)), default=str))
            for field in ("aliases", "document_json"):
                if field in document_by_field:
                    document_by_field[field] = json.loads(document_by_field[field])
            documents.append(document_by_field)
        documents_by_table[name] = documents
    return documents_by_table


def test_real_projection_rows_roundtrip_through_closed_candidate_document_validation():
    from tests.test_registry_fhir_catalog_enrichment import source_batch

    for table, documents in projected_documents().items():
        assert documents
        for document in documents:
            catalog_validation._validate_document("registry_fhir_catalog_" + table, document)
    batch = source_batch()
    for kind, resource_type, observation in (
        ("organization", "Organization", batch.organizations[0]),
        ("edge", "InsurancePlan", batch.edges[0]),
    ):
        catalog_validation._validate_document(
            "registry_fhir_catalog_observation",
            {
                "observation_kind": kind,
                "resource_type": resource_type,
                "resource_id": observation.resource_id if kind == "organization" else observation.owner_resource_id,
                "ordinal": 0,
                "document_json": json.loads(json.dumps(asdict(observation))),
            },
        )


@pytest.mark.parametrize(
    "table,field,value",
    [
        ("network", "search_name", "wrong name"),
        ("relationship", "company_id", "22222222-2222-4222-8222-222222222222"),
        ("relationship", "plan_search", "UPPERCASE"),
        ("binding", "source_id", "other-source"),
    ],
)
def test_candidate_validation_refuses_projection_link_or_search_index_tampering(table, field, value):
    document_by_field = projected_documents()[table][0]
    document_by_field[field] = value
    with pytest.raises(ValueError, match="registry_fhir_catalog_"):
        catalog_validation._validate_document("registry_fhir_catalog_" + table, document_by_field)


def empty_validation_receipt():
    import hashlib

    selected = snapshot(standing_grant(), network_generation=None, manifest_sha256=None)
    reports_by_table = {
        "registry_fhir_catalog_" + table: {
            "rows": 0,
            "sha256": hashlib.sha256(b"").hexdigest(),
            "batches": 0,
            "bytes": 0,
        }
        for table in ("source", "observation", "network", "relationship", "binding")
    }
    selected["accounting_json"] = json.dumps(reports_by_table)
    return selected, {
        "metadata_generation": selected["metadata_generation"],
        "inputs": catalog_validation.snapshot_inputs(selected),
        "tables": reports_by_table,
        "content_sha256": catalog_validation._content_digest(reports_by_table),
    }


def test_empty_catalog_validation_receipt_preserves_unattached_directory_and_serialized_accounting():
    selected, receipt = empty_validation_receipt()
    catalog_validation.verify_fhir_catalog_validation(selected, receipt)
    assert receipt["inputs"]["network_generation"] is receipt["inputs"]["manifest_sha256"] is None
    assert all(report["rows"] == report["batches"] == report["bytes"] == 0 for report in receipt["tables"].values())


@pytest.mark.parametrize("tamper", ["generation", "inputs", "tables", "counts", "batches", "content", "accounting"])
def test_stored_validation_receipt_refuses_changed_inputs_accounting_or_content(tamper):
    selected, receipt = empty_validation_receipt()
    report_by_field = receipt["tables"]["registry_fhir_catalog_network"]
    match tamper:
        case "generation":
            receipt["metadata_generation"] = "f" * 64
        case "inputs":
            receipt["inputs"]["metadata_approved_revision"] += 1
        case "tables":
            receipt["tables"].pop("registry_fhir_catalog_source")
        case "counts":
            report_by_field["rows"] = True
        case "batches":
            report_by_field.update(rows=1, batches=0)
        case "content":
            receipt["content_sha256"] = "f" * 64
        case "accounting":
            selected["accounting_json"] = {}
    with pytest.raises(ValueError, match="registry_fhir_catalog_"):
        catalog_validation.verify_fhir_catalog_validation(selected, receipt)
