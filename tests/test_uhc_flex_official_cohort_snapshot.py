# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Component checks for bounded cohort preparation and fail-closed publication."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import uhc_flex_official_cohort_snapshot as snapshot
from process import uhc_flex_official_cohort_store as store
from process.uhc_flex_official_cohort_contract import UHCFlexOfficialCohortError, build_uhc_flex_official_cohort


def cohort(count=2):
    return build_uhc_flex_official_cohort(
        official_endpoint_id="e" * 64,
        official_dataset_id="dataset",
        official_acquisition_root_run_id="r" * 64,
        official_dataset_hash="d" * 64,
        official_content_proof_sha256="c" * 64,
        practitioner_resource_count=count,
        npi_count=count,
    )


class Stream:
    def __init__(self, pages):
        self.pages, self.closed = pages, False

    async def partitions(self, limit):
        assert limit == 4096
        for page in self.pages:
            yield page

    async def close(self):
        self.closed = True


class Pin:
    def __init__(self, pages):
        self.stream = Stream(pages)
        self.reader = SimpleNamespace(_session=SimpleNamespace(stream=AsyncMock(return_value=self.stream)))
        self.checks = 0

    def source_relation(self, directory):
        self.checks += 1
        return '"sample"."source_child"'


class Driver:
    def __init__(self, *, wrong_status=False, missing=False, accounting=None):
        self.statements, self.copies = [], []
        self.wrong_status, self.missing = wrong_status, missing
        self.accounting_by_field = accounting or {}

    async def execute(self, statement):
        self.statements.append(statement)
        return "OK"

    async def copy_records_to_table(self, relation, **kwargs):
        self.copies.append((relation, kwargs))
        return "COPY 0" if self.wrong_status else f"COPY {len(kwargs['records'])}"

    async def fetchrow(self, statement, *args):
        self.statements.append(statement)
        return {
            "count": 2,
            "expected_count": 2,
            "wrong_scope": False,
            "missing": self.missing,
            **self.accounting_by_field,
        }


@pytest.mark.parametrize("value", [None, "pdufc_" + "a" * 47, "pdufc_" + "A" * 48, "x';DROP TABLE x"])
def test_child_name_rejects_noncanonical_identity(value):
    with pytest.raises(UHCFlexOfficialCohortError):
        snapshot.child_name(value)


def test_child_name_binds_full_cohort_identity():
    value = cohort()
    assert snapshot.child_name(value.cohort_id) == "pdufcm_" + value.cohort_id[6:]
    assert len(snapshot.child_name(value.cohort_id) + "_scope") <= 63


@pytest.mark.asyncio
async def test_copy_is_bounded_and_rechecks_pin_for_every_page():
    pin = Pin([[(number,) for number in range(4096)], [(4096,), (4097,)]])
    driver = Driver()
    await snapshot._copy_members(
        driver, SimpleNamespace(schema="sample"), "private_child", pin, object(), cohort(4098), "SELECT npi"
    )
    assert [len(copy[1]["records"]) for copy in driver.copies] == [4096, 2]
    assert all(copy[1]["columns"] == ("cohort_id", "npi") for copy in driver.copies)
    assert pin.checks == 3 and pin.stream.closed
    assert pin.reader._session.stream.call_args.kwargs["execution_options"] == {"yield_per": 4096}


@pytest.mark.asyncio
@pytest.mark.parametrize("wrong_status,rows", [(True, 2), (False, 1)])
async def test_copy_refuses_incomplete_status_or_total_and_closes_cursor(wrong_status, rows):
    pin = Pin([[(number,) for number in range(rows)]])
    with pytest.raises(UHCFlexOfficialCohortError):
        await snapshot._copy_members(
            Driver(wrong_status=wrong_status),
            SimpleNamespace(schema="sample"),
            "private_child",
            pin,
            object(),
            cohort(),
            "SELECT npi",
        )
    assert pin.stream.closed


@pytest.mark.asyncio
async def test_cancel_drains_copy_owner_and_closes_source_cursor(monkeypatch):
    pin, driver = Pin([[(1,), (2,)]]), Driver()
    driver.copy_records_to_table = AsyncMock(side_effect=asyncio.CancelledError)
    close, invalidate = AsyncMock(), AsyncMock()
    owner = object()
    monkeypatch.setattr(snapshot, "source_copy_session", lambda: owner)
    monkeypatch.setattr(snapshot, "close_interrupted_copy", close)
    monkeypatch.setattr(snapshot, "invalidate_copy_owner", invalidate)
    with pytest.raises(asyncio.CancelledError):
        await snapshot._copy_members(
            driver, SimpleNamespace(schema="sample"), "private_child", pin, object(), cohort(), "SELECT npi"
        )
    close.assert_awaited_once_with(driver)
    invalidate.assert_awaited_once_with(owner, driver)
    assert pin.stream.closed


@pytest.mark.asyncio
async def test_finish_indexes_precede_complete_parity_and_seal(monkeypatch):
    driver = Driver()
    capture = AsyncMock(return_value={"private_child": {"oid": 71}})
    monkeypatch.setattr(snapshot, "_capture", capture)
    proof = await snapshot._finish(
        driver,
        SimpleNamespace(schema="sample"),
        "private_child",
        cohort(),
        "SELECT npi FROM source WHERE dataset_id=:dataset_id AND resource_type=:resource_type",
    )
    sql = "\n".join(driver.statements)
    assert sql.index("PRIMARY KEY") < sql.index("ANALYZE") < sql.index("WITH expected") < sql.index("CHECK(cohort_id=")
    assert "expected_count" in sql and "WHERE NOT EXISTS" in sql
    assert "m.cohort_id=$3 AND m.npi=e.npi" in sql
    assert "FOREIGN KEY" not in sql and "CREATE FUNCTION" not in sql and "CREATE TRIGGER" not in sql
    assert proof == {"oid": 71}


@pytest.mark.asyncio
@pytest.mark.parametrize("accounting", [{"missing": True}, {"count": 3}, {"expected_count": 1}, {"wrong_scope": True}])
async def test_inexact_membership_or_accounting_denies_seal(monkeypatch, accounting):
    driver = Driver(accounting=accounting)
    capture = AsyncMock()
    monkeypatch.setattr(snapshot, "_capture", capture)
    with pytest.raises(UHCFlexOfficialCohortError):
        await snapshot._finish(driver, SimpleNamespace(schema="sample"), "private_child", cohort(), "SELECT npi")
    capture.assert_not_awaited()
    assert not any("CHECK(cohort_id=" in sql for sql in driver.statements)


@pytest.mark.asyncio
async def test_replay_requires_issued_task_identity_before_database_io():
    class NoIO:
        def _transaction_binding(self):
            raise AssertionError("forged replay reached database")

    with pytest.raises(UHCFlexOfficialCohortError):
        await snapshot.replay_cohort(NoIO(), cohort())


@pytest.mark.asyncio
async def test_replay_cannot_cross_task_even_with_equal_cohort():
    task = asyncio.current_task()
    snapshot._ISSUED[task] = {cohort().cohort_id: object()}
    try:

        async def inherited():
            with pytest.raises(UHCFlexOfficialCohortError):
                await snapshot.replay_cohort(object(), cohort())

        await asyncio.create_task(inherited())
    finally:
        snapshot._ISSUED.pop(task)


@pytest.mark.asyncio
async def test_store_routes_fresh_and_expected_replay_without_legacy_insert(monkeypatch):
    value, database = cohort(), object()
    fresh, replay = AsyncMock(return_value="fresh"), AsyncMock(return_value="replay")
    monkeypatch.setattr(snapshot, "prepare_cohort", fresh)
    monkeypatch.setattr(snapshot, "replay_cohort", replay)
    assert await store.sync_uhc_flex_official_cohort(database=database) == "fresh"
    assert await store.sync_uhc_flex_official_cohort(database=database, expected_cohort=value) == "replay"
    fresh.assert_awaited_once_with(database)
    replay.assert_awaited_once_with(database, value)
    assert not hasattr(store, "_insert_members")


def test_projection_preserves_complete_source_semantics_without_custom_npi_hook():
    sql = store._member_projection_sql('"sample"."source_child"')
    assert 'FROM "sample"."source_child"' in sql
    assert "SELECT DISTINCT" in sql and "jsonb_array_elements" in sql
    assert "jsonb_typeof(identifier -> 'value') = 'string'" in sql
    assert "public_evidence_npi_valid" not in sql
    assert "INSERT" not in sql and "ORDER BY npi" in sql


@pytest.mark.asyncio
async def test_fresh_cohort_checks_directory_identity_when_cli_alias_is_installed(monkeypatch):
    import importlib

    from click import Command

    import process

    directory = importlib.import_module("process.provider_directory_fhir")
    database = SimpleNamespace(_transaction_binding=lambda: object())
    monkeypatch.setattr(process, "provider_directory_fhir", Command("directory"))
    monkeypatch.setattr(directory, "db", database)
    monkeypatch.setattr(snapshot, "source_transaction_available", lambda: None)
    with pytest.raises(UHCFlexOfficialCohortError, match="state"):
        await snapshot.prepare_cohort(database)


@pytest.fixture
def publication(monkeypatch):
    value = cohort()
    driver = Driver()
    driver.fetchval = AsyncMock(return_value=True)
    policy = SimpleNamespace(schema="sample", control_runtime_roles=("builder",))
    directory = SimpleNamespace(_schema=lambda: "sample")
    publisher = SimpleNamespace(connection=driver, policy=policy)
    source = SimpleNamespace(dataset_id="dataset")
    prepared_by_field = {"oid": 31}
    pin = SimpleNamespace(scope=SimpleNamespace(fence_identity=("published",)))
    monkeypatch.setattr(snapshot, "bound_publisher", lambda: publisher)
    monkeypatch.setattr(snapshot, "_load_dataset_scope", AsyncMock(return_value=pin.scope))
    monkeypatch.setattr(snapshot, "_request", lambda _source: object())
    monkeypatch.setattr(snapshot, "_custody", AsyncMock(return_value={"qualified": "unit-scope-only"}))
    monkeypatch.setattr(snapshot, "_capture", AsyncMock(return_value={"loading": prepared_by_field}))
    monkeypatch.setattr(snapshot, "_close_grants", AsyncMock())
    monkeypatch.setattr(snapshot, "_issue", AsyncMock())
    monkeypatch.setattr(store, "_current_official_snapshot", AsyncMock(return_value=source))
    monkeypatch.setattr(store, "_stored_cohort_for_dataset", AsyncMock(side_effect=[None, value]))
    monkeypatch.setattr(store, "_insert_header", AsyncMock())
    return SimpleNamespace(
        driver=driver,
        directory=directory,
        pin=pin,
        source=source,
        cohort=value,
        proof=prepared_by_field,
        database=object(),
    )


async def publish(case):
    return await snapshot._publish(
        case.database, case.pin, case.directory, case.source, case.cohort, "loading", case.proof
    )


@pytest.mark.asyncio
async def test_publication_attaches_exact_child_and_renames_finished_indexes(publication):
    result = await publish(publication)
    sql = "\n".join(publication.driver.statements)
    name = snapshot.child_name(publication.cohort.cohort_id)
    assert result.created and result.cohort == publication.cohort
    assert 'RENAME CONSTRAINT "loading_pk" TO "' + name + '_pk"' in sql
    assert 'ALTER INDEX "sample"."loading_npi" RENAME TO "' + name + '_npi"' in sql
    assert sql.index("RENAME CONSTRAINT") < sql.index("LOCK TABLE") < sql.index("ATTACH PARTITION")
    assert "INSERT INTO" not in sql
    assert f'GRANT SELECT ON TABLE "sample"."{name}" TO "builder"' in sql
    snapshot._close_grants.assert_awaited_once_with(publication.driver, snapshot.bound_publisher().policy, name)
    store._insert_header.assert_awaited_once_with(publication.database, publication.cohort)
    snapshot._issue.assert_awaited_once()


@pytest.mark.asyncio
async def test_changed_complete_source_fence_denies_before_any_cut_ddl(publication, monkeypatch):
    monkeypatch.setattr(
        snapshot, "_load_dataset_scope", AsyncMock(return_value=SimpleNamespace(fence_identity=("changed",)))
    )
    with pytest.raises(UHCFlexOfficialCohortError):
        await publish(publication)
    assert publication.driver.statements == []
    store._insert_header.assert_not_awaited()
    snapshot._issue.assert_not_awaited()


@pytest.mark.asyncio
async def test_final_cohort_fence_busy_denies_before_source_or_table_work(publication):
    publication.driver.fetchval.return_value = False
    with pytest.raises(UHCFlexOfficialCohortError) as error:
        await publish(publication)
    assert error.value.code == "busy"
    store._current_official_snapshot.assert_not_awaited()
    assert publication.driver.statements == []


@pytest.mark.asyncio
async def test_compatible_late_winner_discards_only_own_private_heap(publication):
    store._stored_cohort_for_dataset.side_effect = None
    store._stored_cohort_for_dataset.return_value = publication.cohort
    result = await publish(publication)
    assert result.created is False
    assert publication.driver.statements == ['DROP TABLE "sample"."loading"']
    store._insert_header.assert_not_awaited()


@pytest.mark.asyncio
async def test_late_incompatible_header_refuses_without_repair(publication):
    store._stored_cohort_for_dataset.side_effect = None
    store._stored_cohort_for_dataset.return_value = cohort(3)
    with pytest.raises(UHCFlexOfficialCohortError):
        await publish(publication)
    assert publication.driver.statements == []
    store._insert_header.assert_not_awaited()


@pytest.mark.asyncio
async def test_late_header_failure_escapes_to_original_owner_rollback(publication):
    store._insert_header.side_effect = RuntimeError("late header refusal")
    with pytest.raises(RuntimeError, match="late header refusal"):
        await publish(publication)
    assert any("ATTACH PARTITION" in sql for sql in publication.driver.statements)
    snapshot._issue.assert_not_awaited()


@pytest.mark.asyncio
async def test_issued_replay_detaches_namespace_and_native_proof(monkeypatch):
    custody_by_field = {"namespace_roles": ("migration", "namespace_owner"), "selected": {"oid": 12}}
    monkeypatch.setattr(snapshot, "_custody", AsyncMock(return_value=custody_by_field))
    driver = SimpleNamespace(fetchval=AsyncMock(return_value=41))
    publisher = SimpleNamespace(policy=object())
    value = cohort()
    task = asyncio.current_task()
    try:
        await snapshot._issue(driver, publisher, value, object())
        custody_by_field["selected"]["oid"] = 99
        issued = snapshot._ISSUED[task][value.cohort_id]
        assert issued.custody["selected"]["oid"] == 12
        assert issued.custody["namespace_roles"] == ("migration", "namespace_owner")
        assert issued.database_oid == 41
    finally:
        snapshot._ISSUED.pop(task, None)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [None, "xid", "backend", "binding"])
async def test_builder_replay_uses_only_issued_namespace_and_original_transaction(monkeypatch, changed):
    from process import provider_directory_source_acquisition_custody as custody

    expected_cohort = cohort()
    current = SimpleNamespace(dataset_id=expected_cohort.official_dataset_id)
    policy = object()
    custody_by_field = {"namespace_roles": ("migration", "namespace_owner")}
    issued = snapshot._Replay(expected_cohort, current, policy, custody_by_field, 41)
    task = asyncio.current_task()
    snapshot._ISSUED[task] = {expected_cohort.cohort_id: issued}
    binding, driver = object(), object()
    actor_by_field = {"backend": 7, "xid": "23"}
    final_actor_by_field = {**actor_by_field, **({changed: "changed"} if changed in {"xid", "backend"} else {})}
    origins = [
        (binding, driver, actor_by_field),
        (object() if changed == "binding" else binding, driver, final_actor_by_field),
    ]
    monkeypatch.setattr(snapshot, "_replay_origin", AsyncMock(side_effect=origins))
    raw_capture = AsyncMock(return_value=custody_by_field)
    monkeypatch.setattr(custody, "capture_cohort_snapshot_custody", raw_capture)
    fresh_capture = AsyncMock(side_effect=AssertionError("Builder replay cannot bind Publisher"))
    monkeypatch.setattr(snapshot, "_custody", fresh_capture)
    monkeypatch.setattr(store, "_current_official_snapshot", AsyncMock(return_value=current))
    monkeypatch.setattr(store, "_stored_cohort_for_dataset", AsyncMock(return_value=expected_cohort))
    try:
        if changed is None:
            assert (await snapshot.replay_cohort(object(), expected_cohort)).cohort == expected_cohort
        else:
            with pytest.raises(UHCFlexOfficialCohortError):
                await snapshot.replay_cohort(object(), expected_cohort)
        raw_capture.assert_awaited_once_with(
            driver,
            policy,
            migration_role="migration",
            namespace_owner_role="namespace_owner",
            cohort_id=expected_cohort.cohort_id,
        )
        fresh_capture.assert_not_awaited()
    finally:
        snapshot._ISSUED.pop(task, None)
