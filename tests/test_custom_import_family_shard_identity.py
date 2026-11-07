# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Stable global identities preserve scope, exact bytes and observed ID order."""

import asyncio
from dataclasses import replace
from types import SimpleNamespace

import pytest
from sqlalchemy.dialects import postgresql

from process.custom_import import family_block_store as store
from process.custom_import import family_shard_identity as identity
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.runner_codec import digest_text, root_key_contract_hash, root_key_document
from process.custom_import.runner_graph import entity_value_digest
from tests.test_custom_import_snowflake_bundle import _definition


def _key(suffix="", entity="1003000126"):
    text = root_key_document(_definition(), {"npi": entity}) + suffix
    return identity.GlobalIdentityKey(text, digest_text("root-key", text), entity)


def _row(ordinal, root, entity, *, valid=True):
    return dict(ordinality=ordinal, root_record_id=root, entity_binding_id=entity, valid=valid)


def _intern(keys, session=None, *, maximum_rows=10, maximum_bytes=10000):
    return asyncio.run(
        identity.intern_global_identities(
            session, _definition(), 17, keys, maximum_rows=maximum_rows, maximum_bytes=maximum_bytes
        )
    )


def _session(monkeypatch, rows, *, fail_at=None):
    events = []

    async def budget(session):
        events.append("budget")

    async def schema(session):
        return postgresql.dialect().identifier_preparer, 'synthetic"scope'

    class Session:
        async def execute(self, statement, parameters):
            events.append((str(statement), parameters))
            count = sum(type(event) is tuple for event in events)
            if count == fail_at:
                raise RuntimeError("synthetic statement refusal")
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: rows))

    monkeypatch.setattr(store, "_prepare_statement", budget)
    monkeypatch.setattr(identity, "_control_schema", schema)
    return Session(), events


def test_existing_ids_gaps_duplicates_and_exact_lexical_keys_survive(monkeypatch):
    canonical, lexical = _key(), _key(" ")
    keys = (lexical, canonical, lexical)
    session, events = _session(monkeypatch, [_row(1, 9001, 41), _row(2, 41, 41), _row(3, 9001, 41)])
    assert _intern(keys, session) == (
        identity.GlobalIdentity(9001, 41),
        identity.GlobalIdentity(41, 41),
        identity.GlobalIdentity(9001, 41),
    )
    assert len(events) == 6 and events[::2] == ["budget"] * 3
    queries = [event[0] for event in events[1::2]]
    parameters = events[1][1]
    assert all(event[1] is parameters for event in events[1::2])
    assert parameters == {
        "dataset_id": 17,
        "key_contract": root_key_contract_hash(_definition()),
        "root_keys": [key.root_key_text for key in keys],
        "root_hashes": [key.root_key_sha256 for key in keys],
        "entity_values": ["1003000126"] * 3,
        "entity_hashes": [entity_value_digest("1003000126")] * 3,
    }
    assert '"synthetic""scope".custom_import_root_record' in queries[0]
    assert "ON CONFLICT(dataset_id,key_contract_sha256,logical_key_sha256) DO NOTHING" in queries[0]
    assert "ON CONFLICT(dataset_id,adapter_id,canonical_value) DO NOTHING" in queries[1]
    assert "ORDER BY i.root_hash,convert_to(i.root_key,'UTF8')" in queries[0]
    assert "ORDER BY convert_to(i.entity_value,'UTF8'),i.entity_hash" in queries[1]
    for guard in (
        "r.dataset_id=:dataset_id",
        "r.key_contract_sha256=:key_contract",
        "r.logical_key_sha256=i.root_hash",
        "b.dataset_id=:dataset_id",
        "b.adapter_id='npi'",
        "b.value_sha256=i.entity_hash",
        "convert_to(r.canonical_logical_key,'UTF8')=convert_to(i.root_key,'UTF8')",
        "convert_to(b.canonical_value,'UTF8')=convert_to(i.entity_value,'UTF8')",
        "ORDER BY i.ordinality",
    ):
        assert guard in queries[2]
    assert all("WITH ORDINALITY" in query and "unnest(" in query for query in queries)
    assert not any(word in " ".join(queries) for word in ("UPDATE", "ALTER", "jsonb", "nextval"))


@pytest.mark.parametrize(
    "rows",
    [
        [_row(1, 41, 91)],
        [_row(1, 41, 91), _row(2, 41, 91), _row(3, 41, 91)],
        [_row(2, 41, 91), _row(1, 41, 91)],
        [_row(1, 0, 91), _row(2, 41, 91)],
        [_row(1, True, 91), _row(2, 41, 91)],
        [_row(1, 41, 2**63), _row(2, 41, 91)],
        [_row(1, 41, 91, valid=False), _row(2, 41, 91)],
        [_row(1, 41, 91, valid=None), _row(2, 41, 91)],
    ],
)
def test_missing_colliding_or_unscoped_results_never_return_ids(rows):
    keys = (_key(), _key())
    with pytest.raises(FamilyBlockUnavailable):
        identity._checked_identities(keys, rows)


@pytest.mark.parametrize("field", ["root_record_id", "entity_binding_id"])
def test_repeated_keys_cannot_observe_different_ids(field):
    rows = [_row(1, 41, 91), _row(2, 41, 91)]
    rows[1][field] += 100
    with pytest.raises(FamilyBlockUnavailable, match="repeated key"):
        identity._checked_identities((_key(), _key()), rows)


@pytest.mark.parametrize(
    "key",
    [
        None,
        replace(_key(), root_key_text=""),
        replace(_key(), root_key_text="nul\x00key"),
        replace(_key(), root_key_text="\ud800"),
        replace(_key(), root_key_sha256=b"short"),
        replace(_key(), entity_value="1003000127"),
        replace(_key(), entity_value=True),
    ],
)
def test_malformed_evidence_is_refused_before_database_access(key):
    with pytest.raises(ValueError):
        _intern((key,))


def test_wrong_hash_and_simulated_batch_collision_are_refused_before_writes(monkeypatch):
    with pytest.raises(FamilyBlockUnavailable, match="digest differs"):
        _intern((replace(_key(), root_key_sha256=b"h" * 32),))
    monkeypatch.setattr(identity, "digest_text", lambda *args: b"h" * 32)
    keys = tuple(replace(key, root_key_sha256=b"h" * 32) for key in (_key(), _key(" ")))
    with pytest.raises(FamilyBlockUnavailable, match="collision"):
        _intern(keys)


def test_both_budgets_are_checked_before_a_session_is_used():
    assert _intern(()) == ()
    with pytest.raises(ValueError):
        _intern([])
    for option in (dict(maximum_rows=True), dict(maximum_bytes=0)):
        with pytest.raises(ValueError):
            _intern((_key(),), **option)
    with pytest.raises(FamilyBlockResourceLimit, match="row budget"):
        _intern((_key(), _key()), maximum_rows=1)
    key = _key()
    size = len(key.root_key_text.encode("utf-8")) + len(key.entity_value.encode("utf-8")) + 80
    with pytest.raises(FamilyBlockResourceLimit, match="byte budget"):
        _intern((key,), maximum_bytes=size - 1)
    assert identity._parameters(_definition(), 17, (key,), 1, size)["root_keys"] == [key.root_key_text]
    for dataset in (True, 0, 2**63):
        with pytest.raises(ValueError, match="dataset"):
            asyncio.run(identity.intern_global_identities(None, None, dataset, (), maximum_rows=1, maximum_bytes=1))


def test_statement_failure_propagates_without_reading_or_committing_partial_ids(monkeypatch):
    session, events = _session(monkeypatch, [], fail_at=2)
    with pytest.raises(RuntimeError, match="statement refusal"):
        _intern((_key(),), session)
    assert len(events) == 4 and events[::2] == ["budget", "budget"]


def test_final_collision_refusal_is_propagated_for_owner_transaction_rollback(monkeypatch):
    session, events = _session(monkeypatch, [_row(1, 41, 91, valid=False)])
    with pytest.raises(FamilyBlockUnavailable):
        _intern((_key(),), session)
    assert len(events) == 6


def test_source_root_dictionary_preserves_orphan_text_without_an_entity_binding(monkeypatch):
    canonical = "exact orphan parent key"
    key = canonical, digest_text("root-key", canonical)
    session, events = _session(
        monkeypatch,
        [
            {"ordinality": 1, "root_record_id": 9001, "valid": True},
            {"ordinality": 2, "root_record_id": 9001, "valid": True},
        ],
    )
    result = asyncio.run(
        identity.intern_root_records(session, _definition(), 17, (key, key), maximum_rows=2, maximum_bytes=4096)
    )
    assert result == (9001, 9001) and events[::2] == ["budget"] * 2
    queries = [event[0] for event in events[1::2]]
    assert "ORDER BY min(ordinality)" in queries[0]
    assert all("entity_binding" not in query for query in queries)
    assert "convert_to(r.canonical_logical_key,'UTF8')=convert_to(i.root_key,'UTF8')" in queries[1]


@pytest.mark.parametrize("changed", ["count", "scope", "order", "duplicate"])
def test_source_root_dictionary_refuses_incomplete_or_rebound_result(monkeypatch, changed):
    canonical = "orphan key"
    key = canonical, digest_text("root-key", canonical)
    rows = [{"ordinality": index, "root_record_id": 47, "valid": True} for index in (1, 2)]
    if changed == "count":
        rows.pop()
    elif changed == "scope":
        rows[0]["valid"] = False
    elif changed == "order":
        rows.reverse()
    else:
        rows[1]["root_record_id"] = 79
    session, _ = _session(monkeypatch, rows)
    with pytest.raises(FamilyBlockUnavailable):
        asyncio.run(
            identity.intern_root_records(session, _definition(), 17, (key, key), maximum_rows=2, maximum_bytes=4096)
        )
