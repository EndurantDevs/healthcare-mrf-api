# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Segment coordinates and successor authority retain the original acquisition."""

from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_compact_payload as compact
from process import provider_directory_compact_source_completion as compact_completion
from process import provider_directory_compact_source_migration as migration
from process import provider_directory_generic_candidate as candidate_rows
from process import provider_directory_generic_source_storage as storage
from process.provider_directory_source_storage_binding import named_source_document
from tests.test_provider_directory_compact_payload import resource
from tests.test_provider_directory_generic_source_transition import ROOT, _transition
from tests.test_provider_directory_generic_source_transition import case as case
from tests.test_provider_directory_named_source_transition import named_case as named_case


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "missing", "duplicate", "unexpected", "payload", "hash", "acquired", "read"])
async def test_segment_verification_bounds_stored_charges_and_requires_all_rows_before_progress(monkeypatch, damage):
    observations = [{**resource(str(index)), "batch_ordinal": 7, "row_ordinal": index} for index in range(5)]
    keys = [{"batch_ordinal": 7, "row_ordinal": index, "bytes": 1} for index in range(5)]
    stored_keys = [{**key, "read_bytes": 100} for key in keys]
    binding_by_field = {
        "identity": {"dataset_id": "dataset"},
        migration.KEY: {"compact_tables": {}, "rows": 0, "after": [0, 0]},
    }
    before = deepcopy(binding_by_field)
    monkeypatch.setattr(candidate_rows, "COPY_MAX_BYTES", candidate_rows.COPY_FRAME_BYTES + 200)
    pages = []

    async def cursor(*_args, **_kwargs):
        for key in stored_keys:
            yield key

    async def reconstruct(connection, tables, dataset_id, selected):
        assert dataset_id == "dataset" and sum(key["read_bytes"] for key in selected) <= 200
        pages.append(list(selected))
        decoded_rows = [deepcopy(observations[key["row_ordinal"]]) for key in selected]
        if len(pages) != 3:
            return decoded_rows
        if damage == "read":
            raise TimeoutError("verification interrupted")
        if damage == "missing":
            return []
        if damage == "duplicate":
            return [deepcopy(observations[0])]
        if damage == "unexpected":
            decoded_rows[0]["row_ordinal"] = 99
        elif damage == "payload":
            decoded_rows[0]["payload_json"]["network_refs"] = ["Organization/changed"]
        elif damage == "hash":
            decoded_rows[0]["payload_hash"] = "c" * 64
        elif damage == "acquired":
            decoded_rows[0]["acquired_resource_sha256"] = None
        return decoded_rows

    connection, policy = SimpleNamespace(cursor=cursor), object()
    monkeypatch.setattr(migration, "_read_segment", AsyncMock(return_value=observations))
    monkeypatch.setattr(
        migration.layout, "payload_tables", lambda *_args: SimpleNamespace(relation=lambda _root: "aliases")
    )
    monkeypatch.setattr(migration, "validate_conversion", lambda _binding: None)
    monkeypatch.setattr(compact, "append_compact_payload_segment", AsyncMock())
    monkeypatch.setattr(compact, "read_compact_payload_keys", reconstruct)
    stored = AsyncMock()
    monkeypatch.setattr(storage, "_store", stored)
    if damage:
        error = TimeoutError if damage == "read" else ValueError
        with pytest.raises(error, match="verification interrupted" if damage == "read" else "conversion_data_changed"):
            await migration._copy_segment(connection, policy, binding_by_field, keys)
        stored.assert_not_awaited()
    else:
        actual = await migration._copy_segment(connection, policy, binding_by_field, keys)
        assert actual[migration.KEY]["rows"] == 5 and actual[migration.KEY]["after"] == [7, 4]
        stored.assert_awaited_once_with(connection, policy, binding_by_field["identity"], binding_by_field, actual)
    assert pages == [stored_keys[:2], stored_keys[2:4], stored_keys[4:]]
    assert binding_by_field == before


def test_segments_preserve_original_observation_coordinates_and_disjoint_blocks():
    rows = [resource("same", value=index) for index in range(70)]
    first = compact.prepare_compact_payload_batch(
        rows[:3], dataset_id="dataset", batch_ordinal=19, format_id=compact.FORMAT, row_start=0
    )
    second = compact.prepare_compact_payload_batch(
        rows[3:], dataset_id="dataset", batch_ordinal=19, format_id=compact.FORMAT, row_start=3
    )
    assert [alias[1:3] for part in (first, second) for alias in part[0]] == [(19, index) for index in range(70)]
    assert not {block[2] for block in first[1]} & {block[2] for block in second[1]}
    assert all(alias[5:7] == ("a" * 64, "b" * 64) for part in (first, second) for alias in part[0])
    for row_start in (-1, True, 2**31):
        with pytest.raises(ValueError, match="segment_ordinal"):
            compact.prepare_compact_payload_batch(
                [resource()],
                dataset_id="dataset",
                batch_ordinal=19,
                format_id=compact.FORMAT,
                row_start=row_start,
            )


def compact_candidate(named_case):
    binding = named_case.header["candidate_storage_json"]
    prefix = "pdgs_" + "c" * 32 + "_"
    tables_by_root = {root: {"name": prefix + suffix} for root, suffix in storage._COMPACT_SUFFIXES.items()}
    binding.update(contract=storage.NAMED_COMPACT_CONTRACT, payload_format=compact.FORMAT, tables=tables_by_root)
    named_case.allowed_tables.clear()
    named_case.allowed_tables.update(table["name"] for table in tables_by_root.values())
    # This fixture begins with legacy physical inventory, as the earlier named tests do.
    binding["storage"] = deepcopy(named_case.old)
    binding["resets"] = {"PractitionerRole": 1}
    for row in named_case.input_rows:
        row["invalid"] = 0
        if row["resource_type"] == "PractitionerRole":
            row["total"] += 2
    return binding


@pytest.mark.asyncio
async def test_full_successor_preserves_compact_format_tables_and_reset_retired_census(named_case, monkeypatch):
    binding = compact_candidate(named_case)
    before = deepcopy(binding)
    checkpoints = deepcopy(named_case.checkpoints)
    monkeypatch.setattr(compact_completion, "validate_storage", AsyncMock())
    result = await _transition(named_case, named_case.counts, stopped_run_id=ROOT)
    actual = named_case.header["candidate_storage_json"]
    assert result["rows"] == sum(named_case.counts.values())
    assert actual == named_source_document({**before, "storage": named_case.new})
    assert named_case.checkpoints == checkpoints
    assert (await _transition(named_case, named_case.counts, stopped_run_id=ROOT))["already_applied"]


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["census", "future", "producer"])
async def test_compact_successor_refuses_changed_census_future_rows_or_producer(named_case, monkeypatch, damage):
    compact_candidate(named_case)
    monkeypatch.setattr(compact_completion, "validate_storage", AsyncMock())
    if damage == "census":
        named_case.input_rows[0]["active"] += 1
    elif damage == "future":
        named_case.input_rows[0]["invalid"] = 1
    else:
        named_case.old["producer"] = ["d" * 40, "sha256:" + "f" * 64]
    before = deepcopy(named_case.header)
    with pytest.raises(ValueError):
        await _transition(named_case, named_case.counts, stopped_run_id=ROOT)
    assert named_case.header == before and not named_case.writes


def test_conversion_record_refuses_coordinate_and_inventory_substitution(named_case):
    binding = compact_candidate(named_case)
    legacy_tables_by_root = {
        root: {"name": "pdgs_" + "a" * 32 + "_" + suffix}
        for root, suffix in (("resource_input", "ri"), ("witness_input", "wi"))
    }
    binding[migration.KEY] = {
        "format": compact.FORMAT,
        "state": "retained",
        "legacy_tables": legacy_tables_by_root,
        "compact_tables": deepcopy(binding["tables"]),
        "ordinal": binding["ordinal"],
        "resets": binding["resets"],
        "after": [10, 0],
        "rows": 12,
        "total_rows": 12,
        "sequence": [13, True],
    }
    migration.validate_conversion(binding)
    for field, value in (("after", [11, 0]), ("rows", 11), ("state", "copying"), ("sequence", [9, True])):
        changed = deepcopy(binding)
        changed[migration.KEY][field] = value
        with pytest.raises(ValueError):
            migration.validate_conversion(changed)
    changed = deepcopy(binding)
    changed[migration.KEY]["legacy_tables"]["resource_input"]["name"] = binding["tables"]["resource_input"]["name"]
    with pytest.raises(ValueError):
        migration.validate_conversion(changed)


@pytest.mark.asyncio
async def test_sequence_quotes_logical_schema_names():
    from types import SimpleNamespace

    connection = SimpleNamespace(
        fetchval=AsyncMock(return_value=["NamedStorage", "candidate_sequence"]),
        fetchrow=AsyncMock(return_value={"last_value": 14, "is_called": True}),
    )
    name, sequence = await migration._sequence(connection, "NamedStorage", '"NamedStorage"."candidate"')
    assert name == '"NamedStorage"."candidate_sequence"' and sequence == [14, True]
    connection.fetchrow.assert_awaited_once_with('SELECT last_value,is_called FROM "NamedStorage"."candidate_sequence"')


@pytest.mark.asyncio
@pytest.mark.parametrize("partitions", [(), ("partition",)])
@pytest.mark.parametrize("returns_candidate", [False, True])
async def test_initializer_preserves_existing_overloads(partitions, returns_candidate):
    from types import SimpleNamespace

    class Candidate:
        """Synthetic candidate type for the existing initializer fallback."""

    candidate = Candidate()
    initialized = Candidate() if returns_candidate else None
    directory = SimpleNamespace(
        EndpointDatasetCandidate=Candidate, _ensure_endpoint_dataset_candidate=AsyncMock(return_value=initialized)
    )
    returned = await migration.initialize_source_candidate(directory, candidate, partitions)
    assert returned is (initialized if returns_candidate else candidate)
    expected = (candidate, partitions) if partitions else (candidate,)
    directory._ensure_endpoint_dataset_candidate.assert_awaited_once_with(*expected)


@pytest.mark.asyncio
async def test_preparation_preserves_cancellation_callback(monkeypatch):
    from types import SimpleNamespace

    migrate = AsyncMock(return_value="prepared")
    monkeypatch.setattr(migration, "migrate_source_candidate", migrate)
    directory = SimpleNamespace(db=object())
    cancellation = SimpleNamespace(check=AsyncMock())
    candidate = object()
    assert await migration.prepare_retained_prefix(directory, candidate, cancellation) == "prepared"
    migrate.assert_awaited_once_with(
        directory.db, directory, candidate, check_cancelled=cancellation.check, payload_format=None
    )


@pytest.mark.asyncio
async def test_release_without_conversion_has_no_io():
    connection = AsyncMock()
    binding_by_field = {"phase": "prepared"}
    assert await migration.release_conversion_inputs(connection, object(), binding_by_field) is binding_by_field
    assert not connection.mock_calls


@pytest.mark.asyncio
async def test_non_named_publisher_exits_without_reading_or_converting_candidate(monkeypatch):
    database, candidate = object(), object()
    identity_by_field = {"metadata": {"source_ids": ["registered-source"]}}
    directory = SimpleNamespace(
        anthem=SimpleNamespace(seed_rows=lambda: ["seed"]), _stable_source_id=lambda _seed: "registered-source"
    )
    transaction_events = []

    @asynccontextmanager
    async def transaction(actual_database):
        assert actual_database is database
        transaction_events.append("entered")
        try:
            yield
        finally:
            transaction_events.append("exited")

    require_candidate, locked, begin = Mock(), AsyncMock(), AsyncMock()
    check_cancelled = AsyncMock()
    monkeypatch.setattr(storage, "candidate_identity", lambda *_args: identity_by_field)
    monkeypatch.setattr(migration.policy_scope, "source_transaction", transaction)
    monkeypatch.setattr(migration.policy_scope, "bound_publisher", lambda: SimpleNamespace(storage_binding=object()))
    monkeypatch.setattr(storage, "_require_candidate_storage", require_candidate)
    monkeypatch.setattr(storage, "_locked", locked)
    monkeypatch.setattr(migration, "_begin", begin)
    assert (
        await migration.migrate_source_candidate(database, directory, candidate, check_cancelled=check_cancelled)
        is None
    )
    assert transaction_events == ["entered", "exited"]
    check_cancelled.assert_awaited_once_with()
    require_candidate.assert_not_called()
    locked.assert_not_awaited()
    begin.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("candidate_present", [False, True])
async def test_unselected_candidate_does_not_start_conversion_transaction(monkeypatch, candidate_present):
    identity_by_field = {"metadata": {"source_ids": ["another-source"]}}
    directory = SimpleNamespace(
        anthem=SimpleNamespace(seed_rows=lambda: ["seed"]), _stable_source_id=lambda _seed: "registered-source"
    )
    transaction = Mock(side_effect=AssertionError("unexpected conversion transaction"))
    get_identity = Mock(return_value=identity_by_field)
    monkeypatch.setattr(storage, "candidate_identity", get_identity)
    monkeypatch.setattr(migration.policy_scope, "source_transaction", transaction)
    assert (
        await migration.migrate_source_candidate(object(), directory, object() if candidate_present else None) is None
    )
    assert get_identity.call_count == int(candidate_present)
    transaction.assert_not_called()


@pytest.mark.asyncio
async def test_conversion_refuses_outer_publisher_transaction_before_io(monkeypatch):
    directory = SimpleNamespace(
        anthem=SimpleNamespace(seed_rows=lambda: ["seed"]), _stable_source_id=lambda _seed: "registered-source"
    )
    monkeypatch.setattr(
        storage, "candidate_identity", lambda *_args: {"metadata": {"source_ids": ["registered-source"]}}
    )
    transaction = Mock(side_effect=AssertionError("unexpected nested conversion transaction"))
    monkeypatch.setattr(migration.policy_scope, "source_transaction", transaction)
    check_cancelled = AsyncMock()
    token = migration.policy_scope._PUBLISHER.set(object())
    try:
        with pytest.raises(ValueError, match="payload_conversion_outer_transaction"):
            await migration.migrate_source_candidate(object(), directory, object(), check_cancelled=check_cancelled)
    finally:
        migration.policy_scope._PUBLISHER.reset(token)
    transaction.assert_not_called()
    check_cancelled.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["copying", "retained"])
async def test_conversion_disposal_keeps_the_other_payload_inventory(phase):
    compact_tables_by_root = {
        root: {"name": "compact_" + suffix}
        for root, suffix in {"resource_input": "view", "resource_aliases": "aliases", "body_blocks": "blocks"}.items()
    }
    legacy_tables_by_root = {"resource_input": {"name": "legacy_rows"}, "witness_input": {"name": "legacy_witness"}}
    binding_by_field = {
        migration.KEY: {
            "state": phase,
            "compact_tables": compact_tables_by_root,
            "legacy_tables": legacy_tables_by_root,
        }
    }
    connection = SimpleNamespace(execute=AsyncMock())
    await migration.discard_conversion_tables(connection, SimpleNamespace(schema="candidate"), binding_by_field)
    statements = [call.args[0] for call in connection.execute.await_args_list]
    if phase == "copying":
        assert statements == [
            'DROP VIEW "candidate"."compact_view"',
            'DROP TABLE "candidate"."compact_aliases"',
            'DROP TABLE "candidate"."compact_blocks"',
        ]
    else:
        assert statements == ['DROP TABLE "candidate"."legacy_rows"', 'DROP TABLE "candidate"."legacy_witness"']
