# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_shared_price as price


class _FalseyProgress:
    def __bool__(self) -> bool:
        return False

    def __call__(self, _metric: str, _amount: int) -> None:
        return None


def test_price_progress_reports_only_positive_amounts() -> None:
    progress_events: list[tuple[str, int]] = []

    with price.observe_shared_price_progress(
        lambda metric, amount: progress_events.append((metric, amount))
    ):
        price._report_price_progress("rows", 0)
        price._report_price_progress("rows", 2)
    price._report_price_progress("rows", 3)

    assert progress_events == [("rows", 2)]


def test_row_value_supports_mapping_dict_and_position() -> None:
    assert price._row_value(SimpleNamespace(_mapping={"field": 1}), "field", 0) == 1
    assert price._row_value({"field": 2}, "field", 0) == 2
    assert price._row_value((3,), "field", 0) == 3


@pytest.mark.parametrize(
    ("value", "expected"),
    (
        ("", None),
        ("0", None),
        ("none", None),
        ("off", None),
        ("false", None),
        ("64MB", "64MB"),
        ("unsafe;drop", price._DEFAULT_COPY_WORK_MEM),
    ),
)
def test_copy_work_memory_accepts_only_safe_postgres_literals(
    monkeypatch: pytest.MonkeyPatch,
    value: str,
    expected: str | None,
) -> None:
    monkeypatch.setenv(price._COPY_WORK_MEM_ENV, value)
    assert price._copy_work_mem() == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    (
        (None, None),
        ("", None),
        ("0", None),
        ("1", 1),
        (
            str(price._MAX_COPY_PARALLEL_WORKERS_PER_GATHER),
            price._MAX_COPY_PARALLEL_WORKERS_PER_GATHER,
        ),
        ("-1", None),
        ("not-an-int", None),
        (str(price._MAX_COPY_PARALLEL_WORKERS_PER_GATHER + 1), None),
    ),
)
def test_copy_parallelism_is_bounded_and_zero_disables_it(
    monkeypatch: pytest.MonkeyPatch,
    value: str | None,
    expected: int | None,
) -> None:
    if value is None:
        monkeypatch.delenv(price._COPY_PARALLEL_WORKERS_ENV, raising=False)
    else:
        monkeypatch.setenv(price._COPY_PARALLEL_WORKERS_ENV, value)
    assert price._copy_parallel_workers_per_gather() == expected


@pytest.mark.asyncio
async def test_copy_settings_apply_only_requested_limits() -> None:
    driver = SimpleNamespace(execute=AsyncMock())

    await price._set_copy_local_settings(
        driver,
        work_mem="16MB",
        parallel_workers=2,
    )

    assert [call.args[0] for call in driver.execute.await_args_list] == [
        "SET LOCAL work_mem TO '16MB'",
        "SET LOCAL max_parallel_workers_per_gather TO 2",
    ]


@pytest.mark.asyncio
async def test_copy_settings_skip_absent_limits() -> None:
    driver = SimpleNamespace(execute=AsyncMock())

    await price._set_copy_local_settings(
        driver,
        work_mem=None,
        parallel_workers=None,
    )

    driver.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_price_encoder_prerequisite_guards(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    chunks = price._encoder_stdout_chunks(
        SimpleNamespace(stdout=None), metrics={}, started_at=0.0
    )
    with pytest.raises(RuntimeError, match="stdout is closed"):
        await anext(chunks)

    monkeypatch.setattr(price, "_ptg2_rust_scanner_binary", lambda: None)
    with pytest.raises(RuntimeError, match="requires the Rust scanner"):
        await price._stream_shared_price_copy(
            kind="price_atoms",
            sql="SELECT 1",
            schema_name="mrf",
            target_table="stage",
            atom_key_bits=32,
        )


@pytest.mark.parametrize(
    ("stderr", "message"),
    (
        (b"diagnostic only", "did not emit a summary"),
        (
            b"PTG2_SERVING_BINARY_COPY\t{bad-json}\n",
            "invalid summary JSON",
        ),
        (
            b"PTG2_SERVING_BINARY_COPY\t[]\n",
            "did not emit a summary",
        ),
    ),
)
def test_price_stream_summary_fails_closed_without_one_object(
    stderr: bytes,
    message: str,
) -> None:
    with pytest.raises(RuntimeError, match=message):
        price._parse_shared_price_stream_summary(stderr)


def test_price_stream_summary_uses_last_authenticated_frame() -> None:
    stderr = (
        b'PTG2_SERVING_BINARY_COPY\t{"row_count":1}\n'
        b"noise\n"
        b'PTG2_SERVING_BINARY_COPY\t{"row_count":2}\n'
    )
    assert price._parse_shared_price_stream_summary(stderr) == {"row_count": 2}


class _BrokenWriter:
    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        raise BrokenPipeError


@pytest.mark.asyncio
async def test_encoder_stdin_helpers_fail_closed_and_tolerate_peer_exit() -> None:
    with pytest.raises(RuntimeError, match="stdin is closed"):
        await price._feed_encoder_stdin(
            SimpleNamespace(stdin=None),
            b"row",
            metrics={},
            started_at=0.0,
        )

    await price._close_encoder_stdin(SimpleNamespace(stdin=None))
    await price._close_encoder_stdin(SimpleNamespace(stdin=_BrokenWriter()))


class _StoppingProcess:
    def __init__(self, *, lookup_race: bool) -> None:
        self.returncode = None
        self.stdin = None
        self.lookup_race = lookup_race
        self.killed = False
        self.waited = False

    def kill(self) -> None:
        if self.lookup_race:
            raise ProcessLookupError
        self.killed = True

    async def wait(self) -> None:
        self.waited = True


@pytest.mark.asyncio
@pytest.mark.parametrize("lookup_race", (False, True))
async def test_stop_encoder_handles_process_exit_race(lookup_race: bool) -> None:
    process = _StoppingProcess(lookup_race=lookup_race)
    stderr_task = asyncio.create_task(asyncio.sleep(0, result=b"diagnostic"))

    stderr = await price._stop_price_encoder(process, stderr_task)

    assert stderr == b"diagnostic"
    assert process.waited
    assert process.killed is (not lookup_race)


def _membership_summary(**updates: object) -> dict[str, object]:
    summary_by_field: dict[str, object] = {
        "artifact_kind": price._PRICE_MEMBERSHIP_ARTIFACT_KIND,
        "row_count": 2,
        "atom_reference_count": 2,
        "price_set_count": 1,
        "maximum_price_key": 0,
        "atom_key_bits": 32,
        "atom_key_bytes": 4,
    }
    summary_by_field.update(updates)
    return summary_by_field


@pytest.mark.parametrize(
    ("updates", "message"),
    (
        ({"artifact_kind": "wrong"}, "kind mismatch"),
        ({"row_count": "bad"}, "invalid row_count"),
        ({"row_count": None}, "missing row_count"),
        ({"atom_reference_count": 1}, "reference count mismatch"),
        ({"price_set_count": 2}, "price-set count mismatch"),
        ({"maximum_price_key": 1}, "dense price-key bounds mismatch"),
        (
            {"row_count": 0, "atom_reference_count": 0},
            "omitted a membership",
        ),
        ({"atom_key_bits": None, "atom_key_bytes": None}, "missing atom-key width"),
        ({"atom_key_bits": 64}, "atom-key width mismatch"),
        ({"atom_key_bytes": 8}, "atom-key byte width mismatch"),
    ),
)
def test_price_membership_summary_rejects_each_integrity_drift(
    updates: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(RuntimeError, match=message):
        price._v3_membership_stats_from_summary(
            _membership_summary(**updates),
            price_set_count=1,
            atom_count=1,
            atom_key_bits=32,
        )


def test_price_membership_summary_rejects_references_to_empty_atom_map() -> None:
    with pytest.raises(RuntimeError, match="references an empty atom map"):
        price._v3_membership_stats_from_summary(
            _membership_summary(),
            price_set_count=1,
            atom_count=0,
            atom_key_bits=32,
        )


def _atom_summary(**updates: object) -> dict[str, object]:
    summary_by_field: dict[str, object] = {
        "artifact_kind": price._PRICE_ATOM_ARTIFACT_KIND,
        "atom_count": 1,
        "attribute_count": len(price._V3_ATTRIBUTE_KEY_COLUMNS),
        "atom_key_bits": 32,
        "atom_key_bytes": 4,
    }
    summary_by_field.update(updates)
    return summary_by_field


@pytest.mark.parametrize(
    ("updates", "message"),
    (
        ({"artifact_kind": "wrong"}, "kind mismatch"),
        ({"atom_count": 2}, "row count mismatch"),
        ({"attribute_count": 0}, "attribute width mismatch"),
        ({"atom_key_bits": None, "atom_key_bytes": None}, "missing atom-key width"),
        ({"atom_key_bits": 64}, "atom-key width mismatch"),
        ({"atom_key_bytes": 8}, "atom-key byte width mismatch"),
    ),
)
def test_price_atom_summary_rejects_each_integrity_drift(
    updates: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(RuntimeError, match=message):
        price._validate_v3_atom_summary(
            _atom_summary(**updates),
            atom_count=1,
            atom_key_bits=32,
        )


@pytest.mark.parametrize(
    ("summary", "field", "expected", "message"),
    (
        ({"count": None}, "count", None, None),
        ({"count": "4"}, "count", 4, None),
        ({}, "count", None, "missing count"),
        ({"count": "bad"}, "count", None, "invalid count"),
    ),
)
def test_optional_summary_integer_distinguishes_missing_null_and_invalid(
    summary: dict[str, object],
    field: str,
    expected: int | None,
    message: str | None,
) -> None:
    if message is None:
        assert price._optional_summary_integer(summary, "artifact", field) == expected
    else:
        with pytest.raises(RuntimeError, match=message):
            price._optional_summary_integer(summary, "artifact", field)


def test_price_atom_sql_supports_constants_and_dictionary_columns() -> None:
    sql = price._v3_price_atom_sql(
        qualified_price_atom_table='"mrf"."atoms"',
        qualified_atom_key_map='"mrf"."keys"',
        constant_key_by_column={
            price._V3_ATTRIBUTE_KEY_COLUMNS[0]: 7,
        },
    )

    assert "7::bigint AS" in sql
    assert f"price_atom.{price._V3_ATTRIBUTE_KEY_COLUMNS[1]}::bigint" in sql


@pytest.mark.asyncio
async def test_publish_price_artifacts_rejects_identity_before_db_io() -> None:
    prepared = SimpleNamespace(schema_name="other", price_set_count=1)
    with pytest.raises(RuntimeError, match="unsupported price-key order"):
        await price.publish_shared_price_artifacts(
            schema_name="mrf",
            manifest_stage_table="manifest",
            snapshot_key=1,
            build_token="build",
            expected_price_set_count=1,
            expected_price_key_order="wrong",
            prepared=prepared,
        )
    with pytest.raises(RuntimeError, match="disagrees"):
        await price.publish_shared_price_artifacts(
            schema_name="mrf",
            manifest_stage_table="manifest",
            snapshot_key=1,
            build_token="build",
            expected_price_set_count=1,
            expected_price_key_order=price.PTG2_V3_PRICE_KEY_ORDER,
            prepared=prepared,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("stage_fails", (False, True))
async def test_publish_price_artifacts_always_drops_its_stage(
    monkeypatch: pytest.MonkeyPatch,
    stage_fails: bool,
) -> None:
    prepared = price.PreparedSharedPriceArtifacts(
        schema_name="mrf",
        price_atom_table="atoms",
        price_set_atom_table="memberships",
        price_attr_dictionary_table="attributes",
        price_key_map="price_keys",
        atom_key_map="atom_keys",
        price_set_count=1,
        atom_count=1,
        atom_key_bits=32,
        lean_manifest={},
        stage_metrics={},
    )
    staged = price._StagedSharedPriceBlocks(
        _membership_summary(),
        _atom_summary(),
        {},
        {},
        None,
    )
    publication = object()
    stage_blocks = AsyncMock(
        side_effect=RuntimeError("stream failed") if stage_fails else None,
        return_value=staged,
    )
    publish_blocks = AsyncMock(return_value=publication)
    status = AsyncMock()
    monkeypatch.setattr(price, "_stage_shared_price_blocks", stage_blocks)
    monkeypatch.setattr(price, "_publish_staged_price_blocks", publish_blocks)
    monkeypatch.setattr(price.db, "status", status)

    call = price.publish_shared_price_artifacts(
        schema_name="mrf",
        manifest_stage_table="manifest",
        snapshot_key=7,
        build_token="build",
        expected_price_set_count=1,
        expected_price_key_order=price.PTG2_V3_PRICE_KEY_ORDER,
        prepared=prepared,
    )
    if stage_fails:
        with pytest.raises(RuntimeError, match="stream failed"):
            await call
        publish_blocks.assert_not_awaited()
    else:
        assert await call is publication
        publish_blocks.assert_awaited_once()
    status.assert_awaited_once_with(
        'DROP TABLE IF EXISTS "mrf"."ptg2_v3_block_stage_price7" CASCADE;'
    )


@pytest.mark.asyncio
async def test_staged_publish_keeps_one_layout_snapshot_and_falsey_callback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = price.PreparedSharedPriceArtifacts(
        "mrf",
        "atoms",
        "memberships",
        "attributes",
        "price_keys",
        "atom_keys",
        1,
        1,
        32,
        {"price_atom_constant_values": {"setting": "mutated"}},
        {},
    )
    staged = price._StagedSharedPriceBlocks(
        _membership_summary(),
        _atom_summary(),
        {"setting_key": 7},
        {"setting": "outpatient"},
        "frozen_dictionary",
    )
    block_result = SimpleNamespace(
        object_kinds=("price",),
        mapping_count=1,
        unique_block_count=1,
        logical_byte_count=10,
        stored_byte_count=8,
    )
    publish_blocks = AsyncMock(return_value=block_result)
    publish_attributes = AsyncMock(return_value=(1, b"digest"))
    monkeypatch.setattr(price, "publish_shared_block_stage", publish_blocks)
    monkeypatch.setattr(price, "_publish_price_attributes", publish_attributes)
    callback = _FalseyProgress()

    with price.observe_shared_price_progress(callback):
        price_publication = await price._publish_staged_price_blocks(
            "mrf",
            "stage",
            7,
            "build",
            "generation",
            prepared,
            staged,
        )

    assert publish_blocks.await_args.kwargs["progress_callback"] is callback
    assert (
        publish_attributes.await_args.kwargs["dictionary_table"] == "frozen_dictionary"
    )
    assert publish_attributes.await_args.kwargs["constant_values"] == {
        "setting": "outpatient"
    }
    assert price_publication.price_atom_constant_keys == {"setting_key": 7}
    assert price_publication.price_atom_constant_values == {"setting": "outpatient"}
