# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Small independent guard paths for frozen multipart admission."""

from __future__ import annotations

import importlib

import pytest

from process import ptg_frozen_control
from process.ptg_parts import frozen_rate_binding as binding
from process.ptg_parts import frozen_rate_binding_store as binding_store
from process.ptg_parts import frozen_rate_candidate as candidate
from process.ptg_parts import frozen_rate_runtime as runtime
from process.ptg_parts.frozen_rate_files import FrozenRateFileMismatchError
from tests.ptg_frozen_test_support import (
    frozen_artifacts,
    frozen_candidate_evidence,
    frozen_descriptor_by_ordinal,
    protected_control_payload,
)
from tests.ptg_singleton_direct_test_support import _direct_params

ptg = importlib.import_module("process.ptg")


def test_worker_argument_helpers_cover_scalar_and_list_shapes():
    assert ptg_frozen_control._normalized_string_list(None) is None
    assert ptg_frozen_control._normalized_string_list(" one ") == ["one"]
    assert ptg_frozen_control._normalized_string_list(" ") is None
    assert ptg_frozen_control._normalized_string_list([" one ", "", "two"]) == [
        "one",
        "two",
    ]
    assert ptg_frozen_control._normalized_string_list(7) is None
    assert ptg_frozen_control._normalized_optional_int("") is None
    assert ptg_frozen_control._normalized_optional_int("2") == 2
    assert ptg_frozen_control.frozen_rate_main_kwargs({}) == {}
    assert binding.frozen_rate_binding_from_params({}) is None
    direct_params = _direct_params()
    assert ptg_frozen_control.protected_rate_main_kwargs(
        direct_params
    ) == ptg_frozen_control.singleton_direct_main_kwargs(direct_params)
    assert set(
        ptg_frozen_control.frozen_rate_main_kwargs(
            protected_control_payload()["params"]
        )
    ) == {
        "source_file_import_id",
        "frozen_rate_file_set_contract",
        "frozen_rate_files",
        "frozen_rate_file_set_sha256",
        "frozen_rate_file_count",
    }
    params_by_name = protected_control_payload()["params"]
    params_by_name["invalid_price_exclusion_policy"] = {"private": True}
    assert ptg_frozen_control.frozen_rate_main_kwargs(params_by_name)[
        "invalid_price_exclusion_policy"
    ] == {"private": True}


@pytest.mark.asyncio
async def test_binding_storage_requires_exactly_one_measurement_row():
    class EmptyConnection:
        async def all(self, *_args, **_kwargs):
            return []

    with pytest.raises(
        binding.FrozenRateFileBindingMismatchError,
        match="measurement",
    ):
        await binding_store.measure_frozen_binding_storage(EmptyConnection())


@pytest.mark.asyncio
async def test_scope_result_and_store_last_guards(monkeypatch):
    assert runtime.bind_frozen_rate_set_to_scope("a" * 64, "b" * 64, 2)
    descriptor = frozen_descriptor_by_ordinal(1)
    with pytest.raises(FrozenRateFileMismatchError, match="canonical_url"):
        runtime._frozen_result_proof(
            descriptor,
            {
                "source_type": "in_network",
                "summary": {"canonical_url": "wrong"},
            },
        )
    assert binding_store._binding_options_from_row(
        {"binding_payload": "{"}
    ) == {binding.FROZEN_RATE_FILE_BINDING_OPTION: None}

    monkeypatch.setattr(
        binding_store,
        "frozen_rate_binding_from_params",
        lambda _params: {"source_file_import_id": "x"},
    )
    monkeypatch.setattr(
        binding_store,
        "source_file_import_id_from_params",
        lambda _params: None,
    )
    with pytest.raises(binding.FrozenRateFileBindingMismatchError):
        await binding_store.insert_or_compare_frozen_binding(
            None,
            protected_control_payload()["params"],
        )


def test_candidate_rejects_ambiguous_source_version_hash():
    params = protected_control_payload()["params"]
    frozen_binding = binding.frozen_rate_binding_from_params(params)
    manifest, sources = frozen_candidate_evidence(params, frozen_binding)
    manifest["source_file_versions"][1]["raw_sha256"] = (
        manifest["source_file_versions"][0]["raw_sha256"]
    )

    with pytest.raises(FrozenRateFileMismatchError, match="ambiguous"):
        candidate.validate_frozen_candidate_evidence(
            manifest,
            candidate_run_id="ptg2:source-file-import-001",
            database_binding=frozen_binding,
            database_sources=sources,
        )


@pytest.mark.asyncio
async def test_preacquired_artifact_and_provenance_paths_are_reused(tmp_path):
    descriptor = frozen_descriptor_by_ordinal(1)
    raw_artifact, logical_artifact = frozen_artifacts(descriptor, tmp_path)
    provenance_by_field = {"file_row": {"file_id": 1}}
    context = ptg._InNetworkFileContext(
        job={"url": descriptor["canonical_url"]},
        classes={},
        test_mode=True,
        raw_artifact=raw_artifact,
        logical_artifact=logical_artifact,
        recorded_provenance=provenance_by_field,
    )

    assert await ptg._in_network_artifacts(context, str(tmp_path)) == (
        raw_artifact,
        logical_artifact,
    )
    assert await ptg._in_network_provenance(
        context,
        raw_artifact=raw_artifact,
        logical_artifact=logical_artifact,
    ) == provenance_by_field


def test_shared_identity_and_quarantine_evidence_fail_closed():
    assert ptg._shared_v3_identity_traces([{}]) == []
    with pytest.raises(RuntimeError, match="identity/trace"):
        ptg._shared_v3_identity_traces([{"summary": {"manifest": {}}}])
    with pytest.raises(RuntimeError, match="quarantine evidence"):
        ptg._shared_v3_provider_identifier_quarantine([])
    with pytest.raises(RuntimeError, match="quarantine evidence"):
        ptg._shared_v3_provider_identifier_quarantine(
            [{"summary": {"scanner": {"summary": {}}}}]
        )
    with pytest.raises(RuntimeError, match="no empty-NPI"):
        ptg._sum_v4_tin_only_audits([{"skipped": True}])
    with pytest.raises(RuntimeError, match="source-set seal"):
        ptg._shared_v3_source_set_metadata(
            [{"raw_container_sha256": "a" * 64}],
            expected_source_count=2,
        )
