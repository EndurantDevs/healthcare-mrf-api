# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Required-mode NPPES evidence integration at the canonical NPI boundary."""

from __future__ import annotations

import importlib
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock

import pytest

from process.nppes_public_evidence_archive import archive_error
from process.nppes_public_evidence_import import (
    NPPES_RIGHTS_PROOF_SHA256,
    NppesEvidenceRuntimeConfig,
)
from tests.test_process_npi_import import (
    _build_nppes_zip,
    _chain_receipt,
    _prepared_chain,
)


@pytest.fixture
def npi_module():
    return importlib.import_module("process.npi")


def _patch_required_dependencies(monkeypatch, npi_module, prepared_chain):
    config = NppesEvidenceRuntimeConfig(
        "required",
        NPPES_RIGHTS_PROOF_SHA256,
    )
    monkeypatch.setattr(
        npi_module,
        "resolve_nppes_evidence_runtime_config",
        lambda: config,
    )
    prepare_chain = AsyncMock(return_value=prepared_chain)
    monkeypatch.setattr(npi_module, "prepare_nppes_release_chain", prepare_chain)
    scratch_root = prepared_chain.archives[0].retained.path.parent
    monkeypatch.setattr(
        npi_module,
        "resolve_nppes_scratch_root",
        lambda: scratch_root,
    )

    def assert_scratch_capacity(fixed_chain, fixed_root):
        assert fixed_chain is prepared_chain
        assert fixed_root == scratch_root

    monkeypatch.setattr(
        npi_module,
        "assert_nppes_scratch_capacity",
        assert_scratch_capacity,
    )
    monkeypatch.setattr(npi_module, "source_enabled", lambda _source: False)
    monkeypatch.setattr(npi_module, "ensure_database", AsyncMock())
    monkeypatch.setattr(npi_module, "_ensure_required_extensions", AsyncMock())
    monkeypatch.setattr(npi_module, "_npi_requires_nucc", lambda _context: False)
    monkeypatch.setattr(
        npi_module,
        "_load_nucc_taxonomy_int_code_map",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(npi_module, "_prepare_npi_staging", AsyncMock())
    monkeypatch.setattr(npi_module, "_acquire_npi_import_lease", AsyncMock())
    monkeypatch.setattr(npi_module, "_assert_npi_import_lease", AsyncMock())
    monkeypatch.setattr(npi_module, "_assert_nppes_postgres_runtime", AsyncMock())
    monkeypatch.setattr(npi_module, "_assert_nppes_storage_catalog", AsyncMock())
    release_lease = AsyncMock()
    monkeypatch.setattr(npi_module, "_release_npi_import_lease", release_lease)
    monkeypatch.setattr(npi_module, "save_npi_data", AsyncMock())
    canonical_parity = AsyncMock()
    monkeypatch.setattr(
        npi_module,
        "_assert_nppes_canonical_stage_parity",
        canonical_parity,
    )

    async def forbidden_legacy_io(*_args, **_kwargs):
        raise AssertionError("required mode entered legacy acquisition")

    monkeypatch.setattr(npi_module, "download_it", forbidden_legacy_io)
    monkeypatch.setattr(npi_module, "download_it_and_save", forbidden_legacy_io)
    monkeypatch.setattr(npi_module, "unzip", forbidden_legacy_io)
    return config, prepare_chain, release_lease, canonical_parity


def _worker_context() -> dict[str, object]:
    return {
        "context": {},
        "redis": SimpleNamespace(enqueue_job=AsyncMock()),
        "import_date": "20260331",
    }


@pytest.mark.asyncio
async def test_required_mode_admits_only_after_all_legacy_rows_stage(
    monkeypatch,
    tmp_path,
    npi_module,
) -> None:
    prepared_chain = _prepared_chain(tmp_path, _build_nppes_zip(tmp_path))
    expected_receipt = _chain_receipt(prepared_chain)
    config, prepare_chain, release_lease, canonical_parity = _patch_required_dependencies(
        monkeypatch,
        npi_module,
        prepared_chain,
    )
    evidence_import = AsyncMock(return_value=expected_receipt)
    monkeypatch.setattr(
        npi_module,
        "import_nppes_public_evidence_chain",
        evidence_import,
    )
    worker_context = _worker_context()

    await npi_module.process_data(worker_context, {})

    prepare_chain.assert_awaited_once_with(config, cancel_check=ANY)
    evidence_import.assert_awaited_once()
    call = evidence_import.await_args
    assert call.args == (prepared_chain, config)
    assert call.kwargs["expected_source_record_counts"] == (
        (prepared_chain.archives[0].archive_name, 1),
    )
    assert call.kwargs["schema"] == "mrf"
    assert callable(call.kwargs["cancel_check"])
    canonical_parity.assert_awaited_once_with(
        expected_receipt,
        "20260331",
        "mrf",
    )
    assert worker_context["context"]["run"] == 1
    assert worker_context["context"]["_nppes_public_evidence_chain_receipt"] == expected_receipt
    assert worker_context["context"]["nppes_public_evidence"]["status"] == "admitted"
    release_lease.assert_not_awaited()


@pytest.mark.asyncio
async def test_required_mode_writer_failure_cannot_publish_or_reuse_receipt(
    monkeypatch,
    tmp_path,
    npi_module,
) -> None:
    prepared_chain = _prepared_chain(tmp_path, _build_nppes_zip(tmp_path))
    _, _, release_lease, canonical_parity = _patch_required_dependencies(
        monkeypatch,
        npi_module,
        prepared_chain,
    )
    monkeypatch.setattr(
        npi_module,
        "import_nppes_public_evidence_chain",
        AsyncMock(side_effect=archive_error()),
    )
    worker_context = _worker_context()

    with pytest.raises(type(archive_error())):
        await npi_module.process_data(worker_context, {})

    assert worker_context["context"]["run"] == 0
    assert "_nppes_public_evidence_chain_receipt" not in worker_context["context"]
    assert "nppes_public_evidence" not in worker_context["context"]
    canonical_parity.assert_not_awaited()
    release_lease.assert_awaited_once_with(
        worker_context["context"],
        suppress_errors=True,
    )
