import importlib

import pytest


openaddresses = importlib.import_module("process.openaddresses")


def _publication_names(import_id):
    stage_table = f"openaddresses_geocode_{import_id}"
    recovery_table = f"openaddresses_zip_recovery_{import_id}"
    return (
        stage_table,
        recovery_table,
        openaddresses._published_zip_recovery_table_name(recovery_table),
    )


def _pending_marker_fixture(state):
    import_id = "retry20260814"
    stage_table, recovery_table, marker_table = _publication_names(import_id)
    relation_names = {openaddresses.OPENADDRESSES_TABLE, marker_table}
    generation_by_field = {"table_name": marker_table, "import_id": import_id, "live_oid": 41}
    generation_records = [generation_by_field]
    if state == "ambiguous":
        generation_records.append({**generation_by_field, "table_name": "another_marker"})
    if state == "wrong_generation":
        generation_records[0] = {**generation_by_field, "import_id": "another20260814"}
    if state == "stale":
        generation_records[0] = {**generation_by_field, "live_oid": 40}
    if state == "simultaneous_stage":
        relation_names.add(stage_table)
    if state == "unbound_marker":
        generation_records.clear()
    return import_id, stage_table, recovery_table, marker_table, relation_names, generation_records


def _completed_state_fixture(state):
    import_id = "retry20260814"
    stage_table, recovery_table, marker_table = _publication_names(import_id)
    relation_names = {openaddresses.OPENADDRESSES_TABLE}
    marker_oid = 40 if state == "stale" else 41
    generation_records = []
    if state == "simultaneous_stage":
        relation_names.add(stage_table)
    elif state == "pending_and_completed":
        relation_names.add(marker_table)
        generation_records.append(
            {"table_name": marker_table, "import_id": import_id, "live_oid": 41}
        )
    return import_id, stage_table, recovery_table, marker_table, relation_names, marker_oid, generation_records


class _RetryTransaction:
    def __init__(self, transaction_events):
        self.transaction_events = transaction_events

    async def __aenter__(self):
        self.transaction_events.append("begin")

    async def __aexit__(self, *_exc):
        self.transaction_events.append("commit")


class _PendingRetryHarness:
    def __init__(self, import_id):
        self.import_id = import_id
        self.stage_table, self.recovery_table, self.marker_table = _publication_names(import_id)
        self.live_table = openaddresses.OPENADDRESSES_TABLE
        self.relation_names = {self.live_table, self.marker_table}
        self.prepare_calls = []
        self.backfill_calls = []
        self.live_comment = None
        self.transaction_events = []

    async def ensure_database(self, _test_mode):
        return None

    async def is_table_present(self, _schema, table_name):
        return table_name in self.relation_names

    async def published_generations(self, _schema):
        if self.marker_table not in self.relation_names:
            return []
        return [{"table_name": self.marker_table, "import_id": self.import_id, "live_oid": 41}]

    async def relation_oid(self, _schema, table_name):
        return 41 if table_name == self.live_table else None

    async def reject_prepare(self, *_args, **_kwargs):
        self.prepare_calls.append("destructive reload")
        raise AssertionError("post-publish retry must not rebuild stage tables")

    async def backfill(self, **_kwargs):
        self.backfill_calls.append("backfill")
        return openaddresses.OpenAddressesBackfillStats(0, 0, 0)

    async def scalar(self, statement, **_params):
        return self.live_comment if "obj_description" in statement else 8

    async def status(self, statement, **_params):
        if statement.startswith("LOCK TABLE"):
            self.transaction_events.append("lock")
        elif statement.startswith("COMMENT ON TABLE"):
            self.live_comment = openaddresses._completed_generation_marker_comment(self.import_id, 41)
            self.transaction_events.append("comment")
        elif statement.startswith("DROP TABLE"):
            assert self.marker_table in statement
            self.relation_names.remove(self.marker_table)
            self.transaction_events.append("drop")

    def transaction(self):
        return _RetryTransaction(self.transaction_events)


@pytest.mark.asyncio
async def test_openaddresses_control_retry_resumes_owned_postpublish_marker_before_reload(
    monkeypatch,
):
    """Retry process_data before shutdown without rebuilding a published generation."""
    harness = _PendingRetryHarness("retry20260814")
    monkeypatch.setattr(openaddresses, "ensure_database", harness.ensure_database)
    monkeypatch.setattr(openaddresses, "_is_table_present", harness.is_table_present)
    monkeypatch.setattr(openaddresses, "_published_recovery_generations", harness.published_generations)
    monkeypatch.setattr(openaddresses, "_relation_oid", harness.relation_oid)
    monkeypatch.setattr(openaddresses, "_prepare_stage_table", harness.reject_prepare)
    monkeypatch.setattr(openaddresses, "_prepare_zip_recovery_table", harness.reject_prepare)
    monkeypatch.setattr(openaddresses, "refresh_archive_geocodes_from_openaddresses_sharded", harness.backfill)
    monkeypatch.setattr(openaddresses, "print_time_info", lambda _started_at: None)
    monkeypatch.setattr(openaddresses, "db", harness)
    retry_context_by_name = {"context": {"run": 0}, "import_date": "startup"}

    await openaddresses.process_data(retry_context_by_name, {"import_id": harness.import_id})
    await openaddresses.shutdown(retry_context_by_name)

    assert harness.prepare_calls == []
    assert harness.backfill_calls == ["backfill"]
    assert harness.marker_table not in harness.relation_names
    assert harness.live_comment == openaddresses._completed_generation_marker_comment(harness.import_id, 41)
    assert harness.transaction_events == ["begin", "lock", "comment", "drop", "commit"]


@pytest.mark.asyncio
async def test_openaddresses_control_retry_keeps_completed_generation_without_reload(
    monkeypatch,
):
    """Survive a crash after marker cleanup but before control success."""
    prepare_calls = []

    async def ensure_database(_test_mode):
        return None

    async def no_pending_generations(_schema):
        return []

    async def is_table_present(_schema, table_name):
        return table_name == openaddresses.OPENADDRESSES_TABLE

    async def relation_comment(_schema, _table_name):
        return openaddresses._completed_generation_marker_comment("retry20260814", 41)

    async def relation_oid(_schema, _table_name):
        return 41

    async def reject_prepare(*_args, **_kwargs):
        prepare_calls.append("destructive reload")
        raise AssertionError("completed generation retry must not rebuild stage tables")

    monkeypatch.setattr(openaddresses, "ensure_database", ensure_database)
    monkeypatch.setattr(openaddresses, "_published_recovery_generations", no_pending_generations)
    monkeypatch.setattr(openaddresses, "_is_table_present", is_table_present)
    monkeypatch.setattr(openaddresses, "_relation_comment", relation_comment)
    monkeypatch.setattr(openaddresses, "_relation_oid", relation_oid)
    monkeypatch.setattr(openaddresses, "_prepare_stage_table", reject_prepare)
    retry_context_by_name = {"context": {"run": 0}, "import_date": "startup"}

    await openaddresses.process_data(retry_context_by_name, {"import_id": "retry20260814"})
    await openaddresses.shutdown(retry_context_by_name)

    assert prepare_calls == []


@pytest.mark.asyncio
async def test_openaddresses_fresh_control_worker_reuses_run_id_as_import_identity(
    monkeypatch,
):
    prepared_tables = []

    async def ensure_database(_test_mode):
        return None

    async def no_generations(_schema):
        return []

    async def no_comment(_schema, _table_name):
        return None

    async def is_table_present(_schema, _table_name):
        return False

    async def prepare(table_class, _schema, **_kwargs):
        prepared_tables.append(table_class.__tablename__)

    async def load(*_args, **_kwargs):
        return {"processed_files": 0, "processed_rows": 0, "accepted_rows": 0}

    monkeypatch.setattr(openaddresses, "ensure_database", ensure_database)
    monkeypatch.setattr(openaddresses, "_published_recovery_generations", no_generations)
    monkeypatch.setattr(openaddresses, "_relation_comment", no_comment)
    monkeypatch.setattr(openaddresses, "_is_table_present", is_table_present)
    monkeypatch.setattr(openaddresses, "_prepare_stage_table", prepare)
    monkeypatch.setattr(openaddresses, "_prepare_zip_recovery_table", prepare)
    monkeypatch.setattr(openaddresses, "_load_openaddresses_data", load)
    context_by_name = {"context": {"run": 0}, "import_date": "startup202608140001"}

    await openaddresses.process_data(context_by_name, {"run_id": "run-retry-20260814"})

    assert context_by_name["context"]["import_date"] == "runretry20260814"
    assert prepared_tables == [
        "openaddresses_geocode_runretry20260814",
        "openaddresses_zip_recovery_runretry20260814",
    ]


@pytest.mark.asyncio
async def test_openaddresses_explicit_import_id_remains_authoritative_over_run_id():
    context_by_name = {"context": {}, "import_date": "startup"}

    await openaddresses.process_data(
        context_by_name,
        {"publish_only": True, "run_id": "retry-run", "import_id": "owned-generation"},
    )

    assert context_by_name["context"]["import_date"] == "ownedgeneration"


@pytest.mark.asyncio
async def test_openaddresses_direct_enqueue_stamps_one_import_identity(monkeypatch):
    enqueued_payloads = []

    class FakeRedis:
        async def enqueue_job(self, _function, payload, **_kwargs):
            enqueued_payloads.append(payload)

    async def create_pool(*_args, **_kwargs):
        return FakeRedis()

    monkeypatch.setattr(openaddresses, "create_pool", create_pool)

    await openaddresses.main(test_mode=True, import_id="direct-20260814")

    assert enqueued_payloads == [
        {
            "test_mode": True,
            "backfill_only": False,
            "import_id": "direct20260814",
        }
    ]


@pytest.mark.asyncio
async def test_openaddresses_default_direct_enqueues_use_distinct_import_ids(monkeypatch):
    enqueued_payloads = []

    class FakeRedis:
        async def enqueue_job(self, _function, payload, **_kwargs):
            enqueued_payloads.append(payload)

    class FixedDatetime(openaddresses.datetime.datetime):
        @classmethod
        def utcnow(cls):
            return cls(2026, 8, 14, 0, 1, 1)

    class GeneratedId:
        def __init__(self, value):
            self.hex = value

    async def create_pool(*_args, **_kwargs):
        return FakeRedis()

    generated_ids = iter(("a" * 32, "b" * 32))
    monkeypatch.setattr(openaddresses, "create_pool", create_pool)
    monkeypatch.setattr(openaddresses.datetime, "datetime", FixedDatetime)
    monkeypatch.setattr(
        openaddresses.uuid, "uuid4", lambda: GeneratedId(next(generated_ids))
    )
    monkeypatch.delenv("HLTHPRT_IMPORT_ID_OVERRIDE", raising=False)

    await openaddresses.main()
    await openaddresses.main()

    assert [job_payload["import_id"] for job_payload in enqueued_payloads] == [
        "a" * 32,
        "b" * 32,
    ]


@pytest.mark.asyncio
async def test_openaddresses_direct_enqueue_preserves_environment_import_identity(
    monkeypatch,
):
    enqueued_payloads = []

    class FakeRedis:
        async def enqueue_job(self, _function, payload, **_kwargs):
            enqueued_payloads.append(payload)

    async def create_pool(*_args, **_kwargs):
        return FakeRedis()

    monkeypatch.setattr(openaddresses, "create_pool", create_pool)
    monkeypatch.setenv("HLTHPRT_IMPORT_ID_OVERRIDE", "environment-20260814")

    await openaddresses.main()

    assert enqueued_payloads[0]["import_id"] == "environment20260814"


@pytest.mark.parametrize(
    ("state", "error"),
    [
        ("ambiguous", "marker state is ambiguous"),
        ("wrong_generation", "Another OpenAddresses published generation"),
        ("stale", "marker is stale"),
        ("simultaneous_stage", "conflicts with staging state"),
        ("unbound_marker", "marker state is ambiguous"),
    ],
)
@pytest.mark.asyncio
async def test_openaddresses_postpublish_marker_state_fails_closed(
    monkeypatch, state, error
):
    fixture = _pending_marker_fixture(state)
    import_id, stage_table, recovery_table, marker_table, relation_names, generation_records = fixture

    async def published_generations(_schema):
        return generation_records

    async def is_table_present(_schema, table_name):
        return table_name in relation_names

    async def live_oid(*_args):
        return 41

    monkeypatch.setattr(openaddresses, "_published_recovery_generations", published_generations)
    monkeypatch.setattr(openaddresses, "_is_table_present", is_table_present)
    monkeypatch.setattr(openaddresses, "_relation_oid", live_oid)

    with pytest.raises(RuntimeError, match=error):
        await openaddresses._is_owned_postpublish_generation_ready(
            schema="mrf", import_id=import_id, stage_table=stage_table,
            recovery_table=recovery_table, marker_table=marker_table,
            live_table=openaddresses.OPENADDRESSES_TABLE,
        )


@pytest.mark.parametrize(
    ("state", "error"),
    [
        ("stale", "completed-generation marker is stale"),
        ("simultaneous_stage", "completed generation conflicts with staging state"),
        ("pending_and_completed", "pending and completed evidence coexist"),
    ],
)
@pytest.mark.asyncio
async def test_openaddresses_completed_generation_state_fails_closed(
    monkeypatch, state, error
):
    fixture = _completed_state_fixture(state)
    import_id, stage_table, recovery_table, marker_table, relation_names, marker_oid, generation_records = fixture

    async def relation_comment(_schema, _table_name):
        return openaddresses._completed_generation_marker_comment(import_id, marker_oid)

    async def relation_oid(_schema, _table_name):
        return 41

    async def published_generations(_schema):
        return generation_records

    async def is_table_present(_schema, table_name):
        return table_name in relation_names

    monkeypatch.setattr(openaddresses, "_relation_comment", relation_comment)
    monkeypatch.setattr(openaddresses, "_relation_oid", relation_oid)
    monkeypatch.setattr(openaddresses, "_published_recovery_generations", published_generations)
    monkeypatch.setattr(openaddresses, "_is_table_present", is_table_present)

    with pytest.raises(RuntimeError, match=error):
        await openaddresses._publication_retry_state(
            schema="mrf", import_id=import_id, stage_table=stage_table,
            recovery_table=recovery_table, marker_table=marker_table,
            live_table=openaddresses.OPENADDRESSES_TABLE,
        )


@pytest.mark.asyncio
async def test_openaddresses_different_import_proceeds_past_completed_generation(
    monkeypatch,
):
    previous_import_id = "previous20260813"
    import_id = "current20260814"
    prepared_tables = []

    async def ensure_database(_test_mode):
        return None

    async def relation_comment(_schema, _table_name):
        return openaddresses._completed_generation_marker_comment(previous_import_id, 41)

    async def relation_oid(_schema, _table_name):
        return 41

    async def no_generations(_schema):
        return []

    async def is_table_present(_schema, _table_name):
        return False

    async def prepare(table_class, _schema, **_kwargs):
        prepared_tables.append(table_class.__tablename__)

    async def load(*_args, **_kwargs):
        return {"processed_files": 0, "processed_rows": 0, "accepted_rows": 0}

    monkeypatch.setattr(openaddresses, "ensure_database", ensure_database)
    monkeypatch.setattr(openaddresses, "_relation_comment", relation_comment)
    monkeypatch.setattr(openaddresses, "_relation_oid", relation_oid)
    monkeypatch.setattr(openaddresses, "_published_recovery_generations", no_generations)
    monkeypatch.setattr(openaddresses, "_is_table_present", is_table_present)
    monkeypatch.setattr(openaddresses, "_prepare_stage_table", prepare)
    monkeypatch.setattr(openaddresses, "_prepare_zip_recovery_table", prepare)
    monkeypatch.setattr(openaddresses, "_load_openaddresses_data", load)
    context_by_name = {"context": {"run": 0}, "import_date": "startup"}

    await openaddresses.process_data(context_by_name, {"import_id": import_id})

    assert prepared_tables == [
        f"openaddresses_geocode_{import_id}",
        f"openaddresses_zip_recovery_{import_id}",
    ]
    assert context_by_name["context"]["run"] == 1


@pytest.mark.asyncio
async def test_openaddresses_completion_preserves_foreign_relation_comment(monkeypatch):
    import_id = "retry20260814"
    stage_table, recovery_table, marker_table = _publication_names(import_id)
    status_calls = []

    async def is_ready(**_kwargs):
        return True

    async def relation_comment(_schema, _table_name):
        return "external relation contract"

    class FakeTransaction:
        async def __aenter__(self):
            return None

        async def __aexit__(self, *_exc):
            return False

    class FakeDb:
        def transaction(self):
            return FakeTransaction()

        async def status(self, statement, **_params):
            status_calls.append(statement)

    monkeypatch.setattr(openaddresses, "_is_owned_postpublish_generation_ready", is_ready)
    monkeypatch.setattr(openaddresses, "_relation_comment", relation_comment)
    monkeypatch.setattr(openaddresses, "db", FakeDb())

    with pytest.raises(RuntimeError, match="preserving it instead of overwriting"):
        await openaddresses._complete_published_generation(
            schema="mrf", import_id=import_id, stage_table=stage_table,
            recovery_table=recovery_table, marker_table=marker_table,
            live_table=openaddresses.OPENADDRESSES_TABLE,
        )

    assert len(status_calls) == 1
    assert status_calls[0].startswith("LOCK TABLE")
