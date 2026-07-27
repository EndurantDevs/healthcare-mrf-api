# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from types import SimpleNamespace

import pytest

from process.ptg_parts import ptg2_legacy_orphan_sweeper_cli as cli


def _conflicting_schema_environment(monkeypatch) -> None:
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "runtime_schema")
    monkeypatch.setenv("DB_SCHEMA", "legacy_schema")
    monkeypatch.setenv("HLTHPRT_PTG_CONTROL_SCHEMA", "control_plane")


@pytest.mark.asyncio
async def test_control_schema_is_explicitly_required(monkeypatch) -> None:
    monkeypatch.delenv("HLTHPRT_PTG_CONTROL_SCHEMA", raising=False)

    with pytest.raises(
        ValueError,
        match="control schema must be supplied",
    ):
        await cli._amain(("--schema", "mrf"))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "arguments",
    (
        (),
        (
            "--apply",
            "--expected-plan-digest",
            "a" * 64,
            "--actor",
            "cli-test",
        ),
    ),
)
async def test_implicit_schema_conflict_fails_before_database(
    monkeypatch,
    arguments: tuple[str, ...],
) -> None:
    _conflicting_schema_environment(monkeypatch)

    with pytest.raises(
        RuntimeError,
        match="DB_SCHEMA and HLTHPRT_DB_SCHEMA",
    ):
        await cli._amain(arguments)


@pytest.mark.asyncio
async def test_explicit_schema_intentionally_overrides_conflicting_env(
    monkeypatch,
    capsys,
) -> None:
    _conflicting_schema_environment(monkeypatch)
    observed_schema_names: list[str | None] = []

    async def build_plan(**parameters):
        observed_schema_names.append(parameters["schema_name"])
        return SimpleNamespace(
            plan_digest="a" * 64,
            authority_digest="b" * 64,
            catalog_digest="c" * 64,
            candidates=(),
            table_count=0,
            relation_count=0,
            total_bytes=0,
            snapshot_ids=(),
            eligible_suffix_count=0,
            remaining_eligible_suffix_count=0,
            blocked=(),
        )

    monkeypatch.setattr(cli, "build_legacy_orphan_sweep_plan", build_plan)

    await cli._amain(("--schema", "reviewed_schema"))

    assert observed_schema_names == ["reviewed_schema"]
    assert '"state": "dry_run"' in capsys.readouterr().out
