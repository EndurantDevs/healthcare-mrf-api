# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Force-full acquisition contracts for formulary synchronization."""

from __future__ import annotations

import ast
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

import process.formulary_fhir.synchronizer as sync_module
from process.formulary_fhir.repository_admission import TwinAdmissionError
from process.formulary_fhir.parser import parse_medication_knowledge
from process.formulary_fhir.repository_shared import medication_variant_hash
from process.formulary_fhir.repository_shared import membership_hash
from process.formulary_fhir.source import load_enabled_source
from tests.test_formulary_fhir_synchronizer import CUTOFF
from tests.test_formulary_fhir_synchronizer import _Client
from tests.test_formulary_fhir_synchronizer import _medication_resource
from tests.test_formulary_fhir_synchronizer import _published_snapshot
from tests.test_formulary_fhir_synchronizer import _Repository
from tests.test_formulary_fhir_synchronizer import _SourceDatabase


ROOT = Path(__file__).resolve().parents[1]
INVALID_FORCE_FULL_VALUES = (None, 0, 1, "false")


async def _sync_context(*, checkpoint_mode: str | None = None):
    """Build an unchanged-alias synchronization context."""
    medication = parse_medication_knowledge(_medication_resource())
    prior_membership_hash = membership_hash(
        {
            medication.upstream_medication_id: medication_variant_hash(
                medication
            )
        }
    )
    event_names: list[str] = []
    database = _SourceDatabase(event_names)
    binding = await load_enabled_source("source-alpha", database=database)
    repository = _Repository(
        event_names,
        checkpoint_mode=checkpoint_mode,
        current=_published_snapshot(
            membership_hash_value=prior_membership_hash,
        ),
    )
    client = _Client(binding.config, event_names)
    return binding, client, database, repository, event_names


async def _execute_sync(*, force_full: bool):
    """Execute one unchanged-alias synchronization with an exact mode."""
    binding, client, database, repository, event_names = await _sync_context()
    synchronization = await sync_module._run_verified_sync(
        binding=binding,
        client=client,
        repository=repository,
        database=database,
        run_id="synthetic-run-1",
        cutoff_at=CUTOFF,
        intent="none",
        force_full=force_full,
    )
    return synchronization, event_names


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "force_full",
        "expected_full_aliases",
        "expected_reused_aliases",
        "expected_write_event",
        "forbidden_write_event",
    ),
    (
        (False, 0, 1, "put-reuse", "put-full"),
        (True, 1, 0, "put-full", "put-reuse"),
    ),
    ids=("ordinary-reuse", "forced-full-write"),
)
async def test_force_full_controls_unchanged_alias_write(
    force_full,
    expected_full_aliases,
    expected_reused_aliases,
    expected_write_event,
    forbidden_write_event,
):
    """Keep ordinary reuse while forcing an actual full alias write."""
    synchronization, event_names = await _execute_sync(force_full=force_full)

    assert synchronization.full_aliases == expected_full_aliases
    assert synchronization.reused_aliases == expected_reused_aliases
    assert expected_write_event in event_names
    assert forbidden_write_event not in event_names


@pytest.mark.asyncio
async def test_force_full_rejects_completed_reuse_checkpoint():
    """Reject a forced-full replay that already completed as reuse."""
    context = await _sync_context(checkpoint_mode="reuse")
    binding, client, database, repository, event_names = context

    with pytest.raises(
        RuntimeError,
        match="full acquisition cannot resume reused content",
    ) as caught:
        await sync_module._run_verified_sync(
            binding=binding,
            client=client,
            repository=repository,
            database=database,
            run_id="synthetic-run-1",
            cutoff_at=CUTOFF,
            intent="none",
            force_full=True,
        )

    assert repository.failed_with is caught.value
    assert client.medication_calls == 0
    assert "fail" in event_names
    assert "put-full" not in event_names
    assert "put-reuse" not in event_names


@pytest.mark.asyncio
@pytest.mark.parametrize("checkpoint_error", [None, TwinAdmissionError("independence")])
async def test_force_full_verified_replay_requires_full_checkpoints(
    monkeypatch,
    checkpoint_error,
):
    """Revalidate full evidence before accepting a verified reviewed root."""
    binding, client, database, repository, _event_names = await _sync_context()
    repository.dataset_status = "verified"
    full_checkpoint_proof = AsyncMock(side_effect=checkpoint_error)
    monkeypatch.setattr(
        sync_module,
        "require_full_checkpoints",
        full_checkpoint_proof,
    )

    if checkpoint_error is None:
        synchronization = await sync_module._run_verified_sync(
            binding=binding,
            client=client,
            repository=repository,
            database=database,
            run_id="synthetic-run-1",
            cutoff_at=CUTOFF,
            intent="none",
            force_full=True,
        )
        assert synchronization.resumed_aliases == 1
    else:
        with pytest.raises(TwinAdmissionError) as caught:
            await sync_module._run_verified_sync(
                binding=binding,
                client=client,
                repository=repository,
                database=database,
                run_id="synthetic-run-1",
                cutoff_at=CUTOFF,
                intent="none",
                force_full=True,
            )
        assert caught.value is checkpoint_error
        assert repository.failed_with is checkpoint_error

    full_checkpoint_proof.assert_awaited_once_with(
        database,
        repository.dataset,
        repository.alias_count,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_force_full", INVALID_FORCE_FULL_VALUES)
async def test_run_verified_sync_requires_exact_bool(invalid_force_full):
    """Reject bool-like values before verified synchronization side effects."""
    with pytest.raises(RuntimeError, match="full-acquisition mode is invalid"):
        await sync_module._run_verified_sync(
            binding=object(),
            client=object(),
            repository=object(),
            database=object(),
            run_id="synthetic-run-1",
            cutoff_at=CUTOFF,
            intent="none",
            force_full=invalid_force_full,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_force_full", INVALID_FORCE_FULL_VALUES)
async def test_synchronize_alias_requires_exact_bool(invalid_force_full):
    """Reject bool-like values before alias synchronization side effects."""
    with pytest.raises(RuntimeError, match="full-acquisition mode is invalid"):
        await sync_module._synchronize_alias(
            binding=object(),
            client=object(),
            repository=object(),
            database=object(),
            dataset=object(),
            current=object(),
            work_item=object(),
            force_full=invalid_force_full,
        )


class _VerifiedSyncCallVisitor(ast.NodeVisitor):
    """Collect exact force-full flags with their enclosing functions."""

    def __init__(self) -> None:
        self.function_name: str | None = None
        self.call_bindings: list[tuple[str, bool]] = []

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        """Track the async function enclosing each verified-sync call."""
        previous_function_name = self.function_name
        self.function_name = node.name
        self.generic_visit(node)
        self.function_name = previous_function_name

    def visit_Call(self, node: ast.Call) -> None:
        """Record calls only when the force-full keyword is an exact bool."""
        called_name = getattr(node.func, "id", getattr(node.func, "attr", None))
        if called_name == "_run_verified_sync":
            force_full_keywords = [
                keyword
                for keyword in node.keywords
                if keyword.arg == "force_full"
            ]
            assert len(force_full_keywords) == 1
            keyword_value = force_full_keywords[0].value
            assert isinstance(keyword_value, ast.Constant)
            assert type(keyword_value.value) is bool
            assert self.function_name is not None
            self.call_bindings.append(
                (self.function_name, keyword_value.value)
            )
        self.generic_visit(node)


def test_verified_sync_has_one_force_full_caller():
    """Keep the reviewed twin as the sole true force-full caller."""
    observed_bindings_by_file: dict[str, list[tuple[str, bool]]] = {}
    for module_path in sorted(
        (ROOT / "process" / "formulary_fhir").glob("*.py")
    ):
        visitor = _VerifiedSyncCallVisitor()
        visitor.visit(ast.parse(module_path.read_text(encoding="utf-8")))
        if visitor.call_bindings:
            observed_bindings_by_file[module_path.name] = visitor.call_bindings

    assert observed_bindings_by_file == {
        "reviewed_source.py": [("_verify_registered_candidate", False)],
        "reviewed_twin.py": [("_synchronize_candidate", True)],
        "synchronizer.py": [("synchronize_verified_dataset", False)],
        "synthetic_canary.py": [("_verify_enabled_candidate", False)],
    }
