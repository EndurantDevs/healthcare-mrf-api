# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Unit protocol boundaries; native profile/COPY/publication qualify separately."""

import asyncio
import time
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import geo_source_snapshot as snapshot
from process import geo_source_storage as storage
from process import reference_family_archive as archive
from process import reference_family_result_generation as generation
from process import reference_source_generation as source


def test_copy_accounting_bounds_native_framing_and_unicode():
    assert snapshot.record_bytes((None, 1, "é")) >= 64 + 3 * 32 + 2
    assert snapshot.MAX_ROWS == 2000 and snapshot.MAX_BYTES == 16 * 1024 * 1024


@pytest.mark.asyncio
async def test_logical_old_batch_numbers_survive_copy_chunking(monkeypatch):
    copied_batches = []

    async def copy(_session, _name, columns, records, _deadline):
        copied_batches.append((columns, list(records)))

    monkeypatch.setattr(snapshot, "_copy", copy)
    candidate = SimpleNamespace(raw="private", deadline=float("inf"))
    row = dict.fromkeys(snapshot.COLUMNS)
    row["zip_code"] = "10001"
    assert await snapshot._load(object(), candidate, [row] * 2001) == 2001
    assert [len(records) for _columns, records in copied_batches] == [2000, 1]
    assert copied_batches[0][1][-1][-2:] == (1999, 0)
    assert copied_batches[1][1][0][-2:] == (2000, 1)


@pytest.mark.asyncio
async def test_individual_oversized_row_refuses_before_copy(monkeypatch):
    copy = AsyncMock()
    monkeypatch.setattr(snapshot, "_copy", copy)
    row = dict.fromkeys(snapshot.COLUMNS)
    row["city"] = "x" * snapshot.MAX_BYTES
    with pytest.raises(RuntimeError, match="copy_row_oversized"):
        await snapshot._load(object(), SimpleNamespace(deadline=float("inf"), raw="private"), [row])
    copy.assert_not_awaited()


@pytest.mark.asyncio
async def test_duplicate_set_check_precedes_reduction_copy(monkeypatch):
    session = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock(return_value=True))
    copy = AsyncMock()
    monkeypatch.setattr(snapshot, "_copy", copy)
    with pytest.raises(RuntimeError, match="duplicate_zip_in_batch"):
        await snapshot._reduce(session, SimpleNamespace(schema="mrf", raw="geo_raw_example"))
    assert "GROUP BY source_batch,zip_code HAVING count(*)>1" in str(session.scalar.await_args.args[0])
    assert session.execute.await_count == 2
    copy.assert_not_awaited()


@pytest.mark.asyncio
async def test_reduction_pages_last_row_per_zip_in_native_order(monkeypatch):
    rows = [tuple(["10001", *([None] * 10)]), tuple(["10002", *([None] * 10)])]
    result = SimpleNamespace(all=lambda: rows)
    empty = SimpleNamespace(all=lambda: [])
    session = SimpleNamespace(
        execute=AsyncMock(side_effect=[None, None, result, empty]), scalar=AsyncMock(return_value=False)
    )
    copy = AsyncMock()
    monkeypatch.setattr(snapshot, "_copy", copy)
    candidate = SimpleNamespace(schema="mrf", raw="geo_raw_example", stage="geo_candidate_example", deadline=1)
    await snapshot._reduce(session, candidate)
    assert copy.await_args.args[3] == rows
    assert session.execute.await_args.args[1] == {"last": "10002"}
    assert "ORDER BY zip_code,source_ordinal DESC LIMIT 2000" in str(session.execute.await_args.args[0])


@pytest.mark.asyncio
async def test_copy_count_and_deadline_are_exact(monkeypatch):
    driver = SimpleNamespace(copy_records_to_table=AsyncMock(return_value="COPY 2"))
    publisher = SimpleNamespace(connection=driver, policy=SimpleNamespace(schema="mrf"))
    monkeypatch.setattr(snapshot, "bound_publisher", lambda: publisher)
    with pytest.raises(RuntimeError, match="copy_count_changed"):
        await snapshot._copy(object(), "private", ("zip_code",), [("10001",)], time.monotonic() + 1)
    with pytest.raises(RuntimeError, match="deadline_exceeded"):
        await snapshot._copy(object(), "private", ("zip_code",), [("10001",)], time.monotonic() - 1)
    assert driver.copy_records_to_table.await_count == 1


@pytest.mark.asyncio
async def test_interrupted_copy_drains_and_invalidates_original_owner(monkeypatch):
    driver = SimpleNamespace(copy_records_to_table=AsyncMock(side_effect=asyncio.CancelledError))
    publisher = SimpleNamespace(connection=driver, policy=SimpleNamespace(schema="mrf"))
    monkeypatch.setattr(snapshot, "bound_publisher", lambda: publisher)
    drain, invalidate = AsyncMock(), AsyncMock()
    monkeypatch.setattr(snapshot, "close_interrupted_copy", drain)
    monkeypatch.setattr(snapshot, "invalidate_copy_owner", invalidate)
    session = object()
    with pytest.raises(asyncio.CancelledError):
        await snapshot._copy(session, "private", ("zip_code",), [("10001",)], time.monotonic() + 1)
    drain.assert_awaited_once_with(driver)
    invalidate.assert_awaited_once_with(session, driver)


@pytest.mark.asyncio
async def test_geo_cannot_reach_legacy_guard_installer():
    with pytest.raises(RuntimeError, match="geo_source_native_publication_required"):
        await generation.publish_local_reference_family_generation(None, importer_id="geo", schema_name="mrf")
    with pytest.raises(RuntimeError, match="geo_source_native_publication_required"):
        await source.install_reference_revision_guards(None, importer_id="geo", schema_name="mrf")


@pytest.mark.parametrize("tables", [[], [{}, {}]])
def test_immutable_geo_archive_requires_exact_one_table(tables):
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="supported sealed family"):
        archive._validate_source_capture_contract(
            dict(
                importer_id="geo",
                tables=tables,
                publication_authority="tracked-generation",
                source_capture_contract=archive.IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT,
            )
        )


def test_immutable_geo_archive_accepts_only_fixed_contract():
    archive._validate_source_capture_contract(
        dict(
            importer_id="geo",
            tables=[{}],
            publication_authority="tracked-generation",
            source_capture_contract=archive.IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT,
        )
    )
    assert storage.ROOT == "geo_zip_lookup"


@pytest.mark.asyncio
async def test_test_mode_cannot_reuse_a_base_database_factory(monkeypatch):
    from process.ext import utils

    monkeypatch.setattr(utils, "TEST_DATABASE_SUFFIX", "_test")
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", "application")
    session = SimpleNamespace(scalar=AsyncMock(return_value="application"))
    with pytest.raises(RuntimeError, match="test_database_changed"):
        await snapshot._require_test_target(session, True)
    assert str(session.scalar.await_args.args[0]) == "SELECT current_database()"
    session.scalar.return_value = "application_test"
    await snapshot._require_test_target(session, True)


@pytest.mark.asyncio
async def test_test_target_keeps_existing_no_suffix_semantics(monkeypatch):
    from process.ext import utils

    monkeypatch.setattr(utils, "TEST_DATABASE_SUFFIX", None)
    session = SimpleNamespace(scalar=AsyncMock())
    await snapshot._require_test_target(session, True)
    await snapshot._require_test_target(session, False)
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
async def test_generation_cas_binds_old_and_new_native_oids(monkeypatch):
    from datetime import datetime, timezone
    from uuid import UUID

    current_by_field = dict(
        importer_id="geo",
        local_lineage_id=UUID(int=1),
        local_generation=1,
        origin_lineage_id=UUID(int=1),
        origin_generation=1,
        published_at=datetime.now(timezone.utc),
        relation_oids=[555],
    )
    incumbent = generation.validate_reference_family_result_generation_authority(current_by_field)
    successor_by_field = {**current_by_field, "local_generation": 2, "origin_generation": 2, "relation_oids": [777]}
    first = AsyncMock(return_value=successor_by_field)
    monkeypatch.setattr(storage, "_writer", AsyncMock())
    monkeypatch.setattr(storage, "require_geo_storage", AsyncMock(return_value=777))
    monkeypatch.setattr(
        generation, "read_reference_family_result_generation_authority", AsyncMock(return_value=incumbent)
    )
    monkeypatch.setattr(generation, "_first", first)
    result = await storage.publish_geo_generation(None, schema_name="mrf", expected_authority=incumbent)
    assert result.relation_oids == (777,) and result.local_generation == 2
    assert first.await_args.kwargs["previous"] == [555]
    assert first.await_args.kwargs["oids"] == [777]
    assert "relation_oids IS NOT DISTINCT FROM CAST(:previous AS bigint[])" in str(first.await_args.args[1])
    assert "source_revision_tracked=FALSE" in str(first.await_args.args[1])


@pytest.mark.asyncio
async def test_stale_generation_refuses_before_any_ledger_update(monkeypatch):
    incumbent = SimpleNamespace(as_dict=lambda: {"native": "new"})
    expected = SimpleNamespace(as_dict=lambda: {"native": "old"})
    first = AsyncMock()
    monkeypatch.setattr(storage, "_writer", AsyncMock())
    monkeypatch.setattr(storage, "require_geo_storage", AsyncMock(return_value=777))
    monkeypatch.setattr(
        generation, "read_reference_family_result_generation_authority", AsyncMock(return_value=incumbent)
    )
    monkeypatch.setattr(generation, "_first", first)
    with pytest.raises(RuntimeError, match="generation_changed"):
        await storage.publish_geo_generation(None, schema_name="mrf", expected_authority=expected)
    first.assert_not_awaited()


def _cut_case(monkeypatch):
    events = []
    authority = SimpleNamespace(as_dict=lambda: {"generation": 1})
    candidate = snapshot.GeoCandidate(
        "mrf", "geo_raw_example", "geo_candidate_example", 11, authority, time.monotonic() + 10
    )
    publisher = SimpleNamespace(
        connection=object(), policy=SimpleNamespace(schema="mrf", runtime_roles=("reader_a", "reader_b"))
    )
    monkeypatch.setattr(snapshot, "bound_publisher", lambda: publisher)

    async def shape(_session, **options):
        assert options.get("is_incumbent", False) == ("expected_oid" in options)
        events.append("shape")
        return 11 if "expected_oid" in options else 22

    async def ledger(*args, **kwargs):
        assert kwargs["lock"] is True
        events.append("ledger")
        return authority

    close = AsyncMock(side_effect=lambda *args, **kwargs: events.append("readable"))
    replay = AsyncMock(side_effect=lambda *args: events.append("replay"))
    monkeypatch.setattr(snapshot, "require_geo_storage", shape)
    monkeypatch.setattr(generation, "read_reference_family_result_generation_authority", ledger)
    monkeypatch.setattr(snapshot, "_close_grants", close)
    monkeypatch.setattr(snapshot, "replay_geo_reads", replay)
    monkeypatch.setattr(snapshot, "_rename_indexes", AsyncMock())
    monkeypatch.setattr(snapshot, "_canonical_indexes", AsyncMock())
    monkeypatch.setattr(
        snapshot, "publish_geo_generation", AsyncMock(side_effect=lambda *args, **kwargs: events.append("generation"))
    )
    session = SimpleNamespace(execute=AsyncMock(side_effect=lambda statement: events.append(str(statement))))
    return SimpleNamespace(
        events=events, candidate=candidate, publisher=publisher, close=close, replay=replay, session=session
    )


@pytest.mark.asyncio
async def test_reader_grant_precedes_preserved_select_and_locked_geo_swap(monkeypatch):
    cut = _cut_case(monkeypatch)
    await snapshot._publish(cut.session, cut.candidate)
    assert "ACCESS EXCLUSIVE MODE NOWAIT" in cut.events[0]
    assert cut.events.index("ledger") < cut.events.index("readable") < cut.events.index("replay")
    first_swap = next(index for index, event in enumerate(cut.events) if event.startswith("ALTER TABLE"))
    assert cut.events.index("replay") < first_swap < cut.events.index("generation")
    cut.close.assert_awaited_once_with(
        cut.publisher.connection, cut.publisher.policy, cut.candidate.stage, readable=True
    )
    cut.replay.assert_awaited_once_with(cut.session, 11, '"mrf"."geo_candidate_example"')


@pytest.mark.asyncio
async def test_loading_raw_and_final_heaps_never_receive_reader_access(monkeypatch):
    publisher = SimpleNamespace(connection=object(), policy=SimpleNamespace(schema="mrf"))
    monkeypatch.setattr(snapshot, "bound_publisher", lambda: publisher)
    monkeypatch.setattr(snapshot, "require_geo_storage", AsyncMock(return_value=11))
    monkeypatch.setattr(
        generation,
        "read_reference_family_result_generation_authority",
        AsyncMock(return_value=SimpleNamespace(relation_oids=(11,))),
    )
    close = AsyncMock()
    monkeypatch.setattr(snapshot, "_close_grants", close)
    candidate = await snapshot._prepare(SimpleNamespace(execute=AsyncMock()), "mrf", time.monotonic() + 10)
    assert [call.args for call in close.await_args_list] == [
        (publisher.connection, publisher.policy, candidate.raw),
        (publisher.connection, publisher.policy, candidate.stage),
    ]
    assert all(call.kwargs == {} for call in close.await_args_list)
    snapshot.require_geo_storage.assert_awaited_once_with(
        snapshot.require_geo_storage.await_args.args[0], schema_name="mrf", is_incumbent=True
    )


@pytest.mark.asyncio
async def test_geo_candidate_rejects_historical_indexes(monkeypatch):
    monkeypatch.setattr(storage, "_storage_owner", AsyncMock(return_value=7))
    custody = AsyncMock()
    monkeypatch.setattr(storage, "_heap_custody", custody)
    session = SimpleNamespace(scalar=AsyncMock(return_value=17))
    with pytest.raises(RuntimeError, match="incumbent_scope_changed"):
        await storage.require_geo_storage(
            session, schema_name="mrf", relation_name="geo_candidate_example", is_incumbent=True
        )
    custody.assert_not_awaited()


@pytest.mark.asyncio
async def test_reader_grant_failure_is_inside_original_cut_savepoint(monkeypatch):
    cut = _cut_case(monkeypatch)

    @asynccontextmanager
    async def transaction(*args, **kwargs):
        yield cut.session

    @asynccontextmanager
    async def savepoint():
        cut.events.append("savepoint")
        try:
            yield
        except BaseException:
            cut.events.append("rollback")
            raise

    cut.session.begin_nested = savepoint
    monkeypatch.setattr(snapshot, "source_transaction", transaction)
    monkeypatch.setattr(snapshot, "_prepare", AsyncMock(return_value=cut.candidate))
    monkeypatch.setattr(snapshot, "_load", AsyncMock(return_value=1))
    monkeypatch.setattr(snapshot, "_reduce", AsyncMock())
    monkeypatch.setattr(snapshot, "_finish", AsyncMock(side_effect=lambda *args: cut.events.append("finished")))
    cut.replay.side_effect = RuntimeError("prior_acl_changed")
    with pytest.raises(RuntimeError, match="prior_acl_changed"):
        await snapshot.publish_geo_rows(object(), [], 1, deadline=cut.candidate.deadline)
    assert cut.events.index("savepoint") < cut.events.index("finished") < cut.events.index("readable")
    assert cut.events[-1] == "rollback" and not any(event.startswith("ALTER TABLE") for event in cut.events)
    snapshot.publish_geo_generation.assert_not_awaited()
