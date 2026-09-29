# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import reference_source_generation as source_generation


def _session():
    return SimpleNamespace(
        in_transaction=lambda: True,
        scalar=AsyncMock(return_value="read committed"),
        execute=AsyncMock(),
    )


@pytest.mark.parametrize("transaction_check", [None, lambda: False])
async def test_source_observation_requires_a_caller_transaction(transaction_check):
    session = _session()
    session.in_transaction = transaction_check
    with pytest.raises(ValueError, match="requires a caller transaction"):
        await source_generation.require_reference_source_generation(
            session, importer_id="geo", schema_name="mrf", expected_relation_oids=(17,)
        )
    session.scalar.assert_not_awaited()
    session.execute.assert_not_awaited()


async def test_source_bootstrap_cannot_replace_the_label_publication_ledger():
    session = _session()
    with pytest.raises(ValueError, match="native publication ledger"):
        await source_generation.bootstrap_reference_source_generation(
            session, importer_id="label", schema_name="mrf", expected_relation_oids=(17,)
        )
    session.execute.assert_not_awaited()


async def test_source_observation_rejects_an_oid_changed_before_the_fence(monkeypatch):
    session = _session()
    current = AsyncMock(return_value=(18,))
    monkeypatch.setattr(source_generation.generation, "current_reference_family_relation_oids", current)
    with pytest.raises(RuntimeError, match="relation identity changed"):
        await source_generation.require_reference_source_generation(
            session, importer_id="geo", schema_name="mrf", expected_relation_oids=(17,)
        )
    assert [str(call.args[0]) for call in session.execute.await_args_list] == [
        'LOCK TABLE "mrf"."geo_zip_lookup" IN SHARE MODE NOWAIT',
        'LOCK TABLE "mrf"."reference_family_result_generation" IN ROW SHARE MODE NOWAIT',
    ]
    statement, bindings = session.scalar.await_args.args
    assert str(statement).endswith("WHERE importer_id=:importer FOR SHARE NOWAIT")
    assert bindings == {"importer": "geo"}
    current.assert_awaited_once_with(session, importer_id="geo", schema_name="mrf")


async def test_source_observation_rejects_a_drifted_serving_ledger(monkeypatch):
    session = _session()
    monkeypatch.setattr(
        source_generation.generation, "current_reference_family_relation_oids", AsyncMock(return_value=(17,))
    )
    monkeypatch.setattr(
        source_generation.generation, "_first", AsyncMock(return_value={"source_revision_tracked": True})
    )
    monkeypatch.setattr(source_generation, "_guarded_oids", AsyncMock(return_value=(17,)))
    authority = SimpleNamespace(serving_generation=object(), relation_oids=(18,))
    reader = AsyncMock(return_value=authority)
    monkeypatch.setattr(source_generation.generation, "read_reference_family_result_generation_authority", reader)
    with pytest.raises(RuntimeError, match="serving generation is unavailable or drifted"):
        await source_generation.require_reference_source_generation(
            session, importer_id="geo", schema_name="mrf", expected_relation_oids=(17,)
        )
    reader.assert_awaited_once_with(session, importer_id="geo", schema_name="mrf")


@pytest.mark.parametrize("bootstrap", [False, True])
async def test_source_fence_rejects_missing_family_authority(monkeypatch, bootstrap):
    session = _session()
    session.scalar.side_effect = ["read committed", None]
    current = AsyncMock()
    monkeypatch.setattr(source_generation.generation, "current_reference_family_relation_oids", current)
    with pytest.raises(RuntimeError, match="generation authority is unavailable"):
        await source_generation._lock_source(
            session, importer_id="geo", schema_name="mrf", expected_relation_oids=(17,), bootstrap=bootstrap
        )
    statement, bindings = session.scalar.await_args.args
    row_lock = "FOR UPDATE" if bootstrap else "FOR SHARE"
    assert str(statement).endswith(f"WHERE importer_id=:importer {row_lock} NOWAIT")
    assert bindings == {"importer": "geo"}
    current.assert_not_awaited()


async def test_guard_installation_requires_a_matching_post_install_catalog(monkeypatch):
    database = SimpleNamespace(status=AsyncMock())
    monkeypatch.setattr(
        source_generation.generation, "current_reference_family_relation_oids", AsyncMock(return_value=(17,))
    )
    guarded = AsyncMock(side_effect=[(), (18,)])
    monkeypatch.setattr(source_generation, "_guarded_oids", guarded)
    with pytest.raises(RuntimeError, match="revision guards are unavailable"):
        await source_generation.install_reference_revision_guards(database, importer_id="geo", schema_name="mrf")
    assert guarded.await_count == 2
    statements = [call.args[0] for call in database.status.await_args_list]
    assert len(statements) == 5
    assert statements[-1] == (
        'ALTER TABLE "mrf"."geo_zip_lookup" ENABLE ALWAYS TRIGGER "reference_source_generation_revision_guard"'
    )
