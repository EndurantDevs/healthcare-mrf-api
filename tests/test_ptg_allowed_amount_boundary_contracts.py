# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Behavior boundaries for allowed-amount artifacts used beside frozen rates."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from process.ptg_parts import allowed_amounts
from process.ptg_parts.domain import PTG2FileProcessResult


def test_allowed_amount_facade_and_network_fallback(monkeypatch):
    monkeypatch.setitem(allowed_amounts.sys.modules, "process.ptg", None)
    with pytest.raises(RuntimeError, match="facade"):
        allowed_amounts._ptg_facade()

    assert (
        allowed_amounts._normalize_allowed_amount_network_status("unknown")
        == allowed_amounts.ALLOWED_AMOUNT_NETWORK_STATUS_NOT_CONFIRMED
    )


def test_allowed_plan_rows_include_metadata_skip_empty_and_dedupe(
    monkeypatch,
):
    monkeypatch.setattr(
        allowed_amounts,
        "_normalize_plan_payload",
        lambda plan: plan,
    )
    monkeypatch.setattr(
        allowed_amounts,
        "_derive_plan_fields",
        lambda _meta, _plans: {
            "plan_id": "metadata-plan",
            "plan_id_type": "ein",
            "plan_market_type": "group",
        },
    )

    plan_rows = allowed_amounts._allowed_amount_plan_rows(
        snapshot_id="snapshot-a",
        source_file_version_id="version-a",
        file_id=7,
        meta={},
        plan_info=[
            {"plan_id": ""},
            {
                "plan_id": "plan-a",
                "plan_id_type": "ein",
                "plan_market_type": "group",
            },
            {
                "plan_id": "PLAN-A",
                "plan_id_type": "ein",
                "plan_market_type": "group",
            },
        ],
    )

    assert [plan_row["plan_id"] for plan_row in plan_rows] == [
        "PLAN-A",
        "METADATA-PLAN",
    ]


@pytest.mark.asyncio
async def test_allowed_block_and_item_limits(monkeypatch):
    state = allowed_amounts._new_allowed_amount_parse_state(
        snapshot_id="snapshot-a",
        source_file_version_id=None,
        file_id=1,
        meta={},
        plan_info=None,
        plan_count=0,
    )
    await allowed_amounts._append_allowed_amount_block(
        state,
        item_hash="item-a",
        allowed_amount={"tin": {}, "payments": []},
    )
    assert state.unique_tins == set()
    assert allowed_amounts._has_reached_allowed_item_limit(
        2,
        test_mode=False,
        max_items=2,
    )

    monkeypatch.setattr(
        allowed_amounts,
        "_ptg_facade",
        lambda: SimpleNamespace(TEST_ALLOWED_ITEMS=1),
    )
    assert allowed_amounts._has_reached_allowed_item_limit(
        1,
        test_mode=True,
        max_items=None,
    )


@pytest.mark.asyncio
async def test_allowed_parser_stops_at_explicit_limit(monkeypatch):
    appended_items = []

    async def append_item(_state, item):
        appended_items.append(item)

    monkeypatch.setattr(allowed_amounts, "_append_allowed_item", append_item)
    monkeypatch.setattr(
        allowed_amounts,
        "open_json_artifact_stream",
        lambda _path: SimpleNamespace(
            __enter__=lambda self: self,
            __exit__=lambda *_args: None,
        ),
    )
    monkeypatch.setattr(
        allowed_amounts.ijson,
        "items",
        lambda *_args, **_kwargs: iter([{"name": "one"}, {"name": "two"}]),
    )
    monkeypatch.setattr(
        allowed_amounts,
        "open_json_artifact_stream",
        lambda _path: pytest.MonkeyPatch.context(),
    )

    state = SimpleNamespace()
    await allowed_amounts._parse_allowed_items(
        "ignored",
        state,
        test_mode=False,
        max_items=1,
    )

    assert appended_items == [{"name": "one"}]


@pytest.mark.asyncio
async def test_allowed_materialization_reuses_complete_pair(monkeypatch):
    raw_artifact = object()
    logical_artifact = object()

    assert await allowed_amounts._materialize_allowed_amount_artifacts(
        "https://rates.example.test/allowed.json.gz",
        "/tmp",
        reuse_raw_artifacts=True,
        max_bytes=None,
        keep_partial_artifacts=False,
        raw_artifact=raw_artifact,
        logical_artifact=logical_artifact,
    ) == (raw_artifact, logical_artifact)

    async def materialize(*_args, **_kwargs):
        return raw_artifact, logical_artifact

    monkeypatch.setattr(
        allowed_amounts,
        "_ptg_facade",
        lambda: SimpleNamespace(materialize_json_source=materialize),
    )
    assert await allowed_amounts._materialize_allowed_amount_artifacts(
        "https://rates.example.test/allowed.json.gz",
        "/tmp",
        reuse_raw_artifacts=True,
        max_bytes=10,
        keep_partial_artifacts=False,
        raw_artifact=None,
        logical_artifact=None,
    ) == (raw_artifact, logical_artifact)


def _source_version():
    return SimpleNamespace(
        source_identity_hash="identity-a",
        source_file_version_id="version-a",
        canonical_url="https://rates.example.test/allowed.json.gz",
        raw_sha256="a" * 64,
        logical_sha256="b" * 64,
        logical_hash_deferred=False,
        content_length=100,
        raw_byte_count=100,
        etag='"etag"',
        last_modified="Mon, 27 Jul 2026 10:00:00 GMT",
        verification_mode="downloaded",
    )


@pytest.mark.asyncio
async def test_allowed_import_records_file_source_and_parser_metrics(
    monkeypatch,
):
    source_version = _source_version()

    async def extract_metadata(_path):
        return {"reporting_entity_name": "Synthetic"}

    async def record_source_version(**_kwargs):
        return source_version

    async def push_rows(*_args, **_kwargs):
        return None

    async def parse_amounts(*_args, **_kwargs):
        return {"allowed_amount_payments": 1}

    monkeypatch.setattr(
        allowed_amounts,
        "_ptg_facade",
        lambda: SimpleNamespace(
            _extract_metadata_fields=extract_metadata,
            _record_source_version=record_source_version,
        ),
    )
    monkeypatch.setattr(
        allowed_amounts,
        "_build_file_row",
        lambda *_args, **_kwargs: {"file_id": 9},
    )
    monkeypatch.setattr(
        allowed_amounts,
        "_push_ptg2_objects_from_facade",
        push_rows,
    )
    monkeypatch.setattr(
        allowed_amounts,
        "_parse_allowed_amounts",
        parse_amounts,
    )

    outcome = await allowed_amounts._import_allowed_amount_artifacts(
        {"url": "https://rates.example.test/allowed.json.gz"},
        {"PTGFile": object, "ImportLog": object},
        False,
        "snapshot-a",
        "run-a",
        None,
        object(),
        SimpleNamespace(logical_path="/tmp/logical.json"),
    )

    assert outcome.file_id == 9
    assert outcome.source_version is source_version
    assert outcome.metrics_by_name == {"allowed_amount_payments": 1}


def test_allowed_summary_carries_source_proof_and_empty_evidence():
    outcome = allowed_amounts._AllowedAmountImportOutcome(
        file_id=1,
        source_version=_source_version(),
        metrics_by_name={
            "allowed_amount_payments": 0,
            "allowed_amount_provider_payments": 0,
        },
    )

    summary = allowed_amounts._allowed_amount_file_summary(outcome)

    assert summary["engine_source_file_version_id"] == "version-a"
    assert summary["allowed_amount_evidence"] is False
    no_source = allowed_amounts._allowed_amount_file_summary(
        allowed_amounts._AllowedAmountImportOutcome(
            file_id=1,
            source_version=None,
            metrics_by_name={"allowed_amount_payments": 1},
        )
    )
    assert no_source == {
        "allowed_amount_payments": 1,
        "allowed_amount_evidence": True,
    }


@pytest.mark.asyncio
async def test_allowed_process_rejects_missing_snapshot_and_shapes_download_error(
    monkeypatch,
    tmp_path,
):
    job_by_field = {"url": "https://rates.example.test/allowed.json.gz"}
    with pytest.raises(ValueError, match="snapshot_id"):
        await allowed_amounts._process_allowed_amounts_file(
            job_by_field,
            {},
            False,
        )

    async def fail_materialize(*_args, **_kwargs):
        raise RuntimeError("download unavailable")

    monkeypatch.setattr(
        allowed_amounts,
        "_ptg_facade",
        lambda: SimpleNamespace(ptg2_temp_parent=lambda: str(tmp_path)),
    )
    monkeypatch.setattr(
        allowed_amounts,
        "_materialize_allowed_amount_artifacts",
        fail_materialize,
    )

    process_result = await allowed_amounts._process_allowed_amounts_file(
        job_by_field,
        {},
        False,
        snapshot_id="snapshot-a",
    )

    assert process_result == PTG2FileProcessResult(
        "allowed_amounts",
        job_by_field["url"],
        False,
        error="download unavailable",
    )


@pytest.mark.asyncio
async def test_allowed_process_success_shapes_file_result(monkeypatch, tmp_path):
    job_by_field = {"url": "https://rates.example.test/allowed.json.gz"}
    artifacts = object(), object()
    outcome = allowed_amounts._AllowedAmountImportOutcome(
        file_id=9,
        source_version=None,
        metrics_by_name={"allowed_amount_provider_payments": 2},
    )

    async def materialize(*_args, **_kwargs):
        return artifacts

    async def import_artifacts(*_args, **_kwargs):
        return outcome

    monkeypatch.setattr(
        allowed_amounts,
        "_ptg_facade",
        lambda: SimpleNamespace(ptg2_temp_parent=lambda: str(tmp_path)),
    )
    monkeypatch.setattr(
        allowed_amounts,
        "_materialize_allowed_amount_artifacts",
        materialize,
    )
    monkeypatch.setattr(
        allowed_amounts,
        "_import_allowed_amount_artifacts",
        import_artifacts,
    )

    process_result = await allowed_amounts._process_allowed_amounts_file(
        job_by_field,
        {},
        False,
        snapshot_id="snapshot-a",
    )

    assert process_result.success is True
    assert process_result.file_id == 9
    assert process_result.summary["allowed_amount_evidence"] is True
