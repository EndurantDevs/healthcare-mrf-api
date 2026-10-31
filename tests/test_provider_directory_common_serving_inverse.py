# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded inverse SQL restores removed rows without replacing native serving heaps."""

import json
import sqlite3
from contextlib import nullcontext
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_common_serving_inverse as inverse
from process import provider_directory_common_serving_recovery_document as documents
from tests.test_provider_directory_common_serving_recovery_document import actor, retained, state


def payload_and_database():
    payload = retained()
    database = sqlite3.connect(":memory:")
    database.row_factory = sqlite3.Row
    database.execute("ATTACH DATABASE ':memory:' AS unit")
    columns = ["id", "npi", "location_key", "source_id", "review_status", "reviewed_by", "retired_at"]
    previous = ("original", 1, "gone", "changed", "approved", "reviewer", "old-history")
    unrelated = ("unrelated", 2, "other", "other", "rejected", "another-reviewer", None)
    replacement = ("new-only", 1, "new", "changed", "needs_review", None, None)
    for entry in payload["families"]:
        entry["columns"] = columns
        for table in [entry["source"], entry["name"]]:
            database.execute(
                f"CREATE TABLE unit.{table}(id TEXT PRIMARY KEY,npi BIGINT,location_key TEXT,source_id TEXT,review_status TEXT,reviewed_by TEXT,retired_at TEXT)"
            )
        database.executemany(f"INSERT INTO unit.{entry['source']} VALUES(?,?,?,?,?,?,?)", [replacement, unrelated])
        database.execute(f"INSERT INTO unit.{entry['name']} VALUES(?,?,?,?,?,?,?)", previous)
    for entry in payload["scopes"]:
        if entry["family"] == "npi_scope":
            entry["columns"] = ["npi"]
            database.execute(f"CREATE TABLE unit.{entry['name']}(npi BIGINT PRIMARY KEY)")
            database.execute(f"INSERT INTO unit.{entry['name']} VALUES(1)")
        else:
            database.execute(f"CREATE TABLE unit.{entry['name']}(location_key TEXT)")
            database.executemany(f"INSERT INTO unit.{entry['name']} VALUES(?)", [("gone",), ("new",)])
    database.commit()
    return payload, database, previous, unrelated


def snapshot(database):
    return {
        root: [tuple(row) for row in database.execute(f"SELECT * FROM unit.{root} ORDER BY id")]
        for root in documents.FAMILIES.values()
    }


def apply_restore(database, restore):
    statement = restore.delete_sql.replace(
        "=ANY(CAST(:changed_source_ids AS varchar[]))", " IN (SELECT value FROM json_each(:changed_source_ids))"
    )
    database.execute(statement, {key: json.dumps(value) for key, value in restore.parameters})
    database.execute(restore.insert_sql)


def test_ten_family_inverse_restores_removed_keys_and_manual_review_history():
    payload, database, previous, unrelated = payload_and_database()
    before_native_by_root = {
        root: database.execute(f"SELECT rootpage FROM unit.sqlite_master WHERE name=?", (root,)).fetchone()[0]
        for root in documents.FAMILIES.values()
    }
    restores = inverse.restore_rows_sql("unit", payload, ["changed"])
    assert {restore.family for restore in restores} == set(documents.FAMILIES)
    for restore in restores:
        apply_restore(database, restore)
    assert all(rows == [previous, unrelated] for rows in snapshot(database).values())
    assert before_native_by_root == {
        root: database.execute(f"SELECT rootpage FROM unit.sqlite_master WHERE name=?", (root,)).fetchone()[0]
        for root in documents.FAMILIES.values()
    }
    database.close()


def test_failure_during_inverse_leaves_all_ten_forward_families_intact():
    payload, database, _previous, _unrelated = payload_and_database()
    forward = snapshot(database)
    database.execute("BEGIN")
    for restore in inverse.restore_rows_sql("unit", payload, ["changed"])[:5]:
        apply_restore(database, restore)
    database.rollback()
    assert snapshot(database) == forward
    database.close()


def initial_baseline_and_payload():
    from process.provider_directory_profile_initial_contract import TARGET_CONTRACT

    baseline, retained_by_field = state(), retained()
    baseline["metadata"]["profile"] = None
    baseline["initial_targets"] = dict(
        contract_id=TARGET_CONTRACT,
        resolution="empty",
        serving_singleton_absent=True,
        initial_commit_receipt_absent=True,
        evidence_target_oid=baseline["roots"]["provider_directory_profile_evidence"]["oid"],
        profile_target_oid=baseline["roots"]["provider_directory_profile"]["oid"],
        evidence_target_storage_fingerprint="a" * 64,
        profile_target_storage_fingerprint="b" * 64,
        evidence_target_bytes=0,
        profile_target_bytes=0,
        evidence_rows=0,
        profile_rows=0,
        historical_publication=None,
    )
    for entry in retained_by_field["families"]:
        if entry["family"] not in {"profile", "evidence"}:
            continue
        entry.update(
            name=inverse.receipts.retained_name(entry["source_oid"]),
            oid=entry["source_oid"],
            columns=[],
            rows=0,
            data_sha256=None,
            copied_bytes=0,
            reused_initial=True,
        )
    return baseline, retained_by_field


def test_initial_inverse_accepts_only_original_oid_derived_retained_heaps():
    baseline, payload = initial_baseline_and_payload()
    baseline["roots"]["provider_directory_profile"]["indexes"] = [
        dict(oid=700, name="original_profile_key", valid=True, ready=True, live=True)
    ]
    plan = inverse.initial_restore_plan(baseline, payload)
    assert {entry.family for entry in plan} == {"profile", "evidence"}
    assert next(entry for entry in plan if entry.family == "profile").retained_indexes == (
        ("pdsrv_i_2bc", "original_profile_key"),
    )
    assert all(entry.retained_relation == inverse.receipts.retained_name(entry.original_oid) for entry in plan)
    assert len(inverse.restore_rows_sql("unit", payload, ["changed"])) == 8
    payload["families"][0]["name"] = "arbitrary_retained_table"
    with pytest.raises(ValueError, match="retention_invalid"):
        inverse.initial_restore_plan(baseline, payload)


def test_initial_codec_bounds_pre_and_postcut_names():
    _baseline, payload = initial_baseline_and_payload()
    assert documents.validate_retention(payload) == payload
    payload["families"][0]["name"] = payload["families"][0]["source"]
    assert documents.validate_retention(payload) == payload
    payload["families"][0]["name"] = "pdsrv_r_ffff"
    with pytest.raises(ValueError, match="retention_invalid"):
        documents.validate_retention(payload)


@pytest.mark.asyncio
async def test_retained_recheck_requires_existing_owner_session(monkeypatch):
    session = object()
    monkeypatch.setattr(inverse.receipts, "ordinary", AsyncMock(return_value=(object(), object(), actor())))
    with pytest.raises(RuntimeError, match="inverse_invalid"):
        await inverse.assert_retained_rows(session, "unit", retained())


@pytest.mark.asyncio
async def test_retained_recheck_detects_corrupted_copy_before_any_restore(monkeypatch):
    from process import entity_address_snapshot_receipt as snapshots
    from process import provider_directory_ordinary_retention_capacity as capacity
    from process import provider_directory_ordinary_row_retention as retention

    payload = retained()
    session, driver = object(), object()
    by_oid = {entry["oid"]: entry for entry in (*payload["scopes"], *payload["families"])}
    monkeypatch.setattr(inverse.receipts, "ordinary", AsyncMock(return_value=(session, driver, actor())))
    monkeypatch.setattr(inverse.receipts, "_require_freeze", AsyncMock())
    monkeypatch.setattr(capacity, "_custody", AsyncMock(return_value=dict(persistence="p", owner_oid=actor()["owner"])))
    monkeypatch.setattr(
        retention, "_columns", AsyncMock(side_effect=lambda _driver, oid: tuple(by_oid[oid]["columns"]))
    )
    monkeypatch.setattr(capacity, "retained_scope_layout_sha256", AsyncMock(return_value="a" * 64))
    monkeypatch.setattr(capacity, "retention_layout_sha256", AsyncMock(return_value="a" * 64))
    monkeypatch.setattr(inverse.receipts, "normalized_receipt_scope", lambda *_args: nullcontext())
    row_identity = AsyncMock(return_value=(1, "b" * 64))
    monkeypatch.setattr(snapshots, "_row_identity", row_identity)
    assert await inverse.assert_retained_rows(session, "unit", payload) == ()
    row_identity.return_value = (2, "b" * 64)
    with pytest.raises(RuntimeError, match="inverse_invalid"):
        await inverse.assert_retained_rows(session, "unit", payload)
