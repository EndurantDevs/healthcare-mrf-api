

"""Residual fail-closed branch coverage for exact-wave controller contracts."""


from __future__ import annotations


import asyncio


import runpy


import types


from unittest.mock import AsyncMock, Mock


import pytest


from sanic.exceptions import BadRequest, NotFound, SanicException


from api import control as control_api


from api import control_import_wave_attestation as attestation


from api import control_imports


from api import control_wave_routes as routes


from api import control_workers


from api import mrf_discovery_catalog_manifest as catalog_manifest


from api import provider_specialty_filters


from api import ptg2_candidate_audit_reverse as reverse_scope


from api import ptg2_candidate_audit_v4 as v4_scope


from api.mrf_discovery_catalog_paging import bounded_file_windows


from api.provider_profile_display import display_value


from process import ptg_control


from process import ptg_wave_barrier as barrier


from process import ptg_wave_controller_isolation as isolation


from process import ptg_wave_controller_receipts as receipts


from process import ptg_wave_outcome_contract as outcome_contract


from process import ptg_wave_outcome_terminal_validation as terminal_validation


from process import ptg_wave_receipt_projection as receipt_projection


from process import ptg_wave_worker as wave_worker


from process.ptg_parts import frozen_rate_binding_store as bindings


from process.ptg_parts import ptg_wave_admission_fence as fence


from process.ptg_wave_terminal_state import derive_terminal_state


from tests.test_ptg_wave_failure_controller_edges import (
    _CONFIG_DIGEST,
    _IMAGE,
    _JOBS_DIGEST,
    _LINKAGE_KEY,
    _MANIFEST_DIGEST,
    _MANIFEST_IDENTITY,
    _RUNTIME_IDENTITY,
    _WAVE_DIGEST,
    _Request,
    _claim,
    _intent,
    _outcome,
    _wave,
)


def _identity(**overrides):
    values_by_field = {
        "wave_digest": _WAVE_DIGEST,
        "queue": barrier.queue_for_wave(_WAVE_DIGEST),
        "worker_class": "process.PTGSmall",
        "slot_index": 0,
        "pod_uid": "pod-synthetic",
        "manifest_digest": _MANIFEST_DIGEST,
        "jobs_digest": _JOBS_DIGEST,
        "job_count": 2,
        "config_identity": _CONFIG_DIGEST,
        "manifest_identity": _MANIFEST_IDENTITY,
        "image_identity": _IMAGE,
        "runtime_image_identity": _RUNTIME_IDENTITY,
    }
    values_by_field.update(overrides)
    return barrier.PTGWaveWorkerIdentity(**values_by_field)


def _identity_environment(**overrides):
    values_by_field = {
        "HLTHPRT_PTG_WAVE_DIGEST": _WAVE_DIGEST,
        "HLTHPRT_ACTIVE_WORKER_QUEUE": barrier.queue_for_wave(_WAVE_DIGEST),
        "HLTHPRT_ACTIVE_WORKER_CLASS": "process.PTGSmall",
        "HLTHPRT_PTG_WAVE_SLOT_INDEX": "0",
        "HLTHPRT_PTG_WAVE_POD_UID": "pod-synthetic",
        "HLTHPRT_PTG_WAVE_REDIS_MANIFEST_DIGEST": _MANIFEST_DIGEST,
        "HLTHPRT_PTG_WAVE_JOBS_DIGEST": _JOBS_DIGEST,
        "HLTHPRT_PTG_WAVE_JOB_COUNT": "2",
        "HLTHPRT_PTG_WAVE_CONFIG_IDENTITY": _CONFIG_DIGEST,
        "HLTHPRT_PTG_WAVE_MANIFEST_IDENTITY": _MANIFEST_IDENTITY,
        "HLTHPRT_PTG_WAVE_IMAGE_IDENTITY": _IMAGE,
        "HLTHPRT_PTG_WAVE_RUNTIME_IMAGE_IDENTITY": _RUNTIME_IDENTITY,
    }
    values_by_field.update(overrides)
    return values_by_field


def _terminal_kubernetes_receipt(wave, ready_slots):
    expected = terminal_validation._expected_kubernetes_receipt(wave, ready_slots)
    return {
        **expected,
        "attestation_digest": terminal_validation.sha256_digest(
            terminal_validation.canonical_json(expected)
        ),
    }


def _terminal_redis_receipt(wave):
    wave.redis_release_attestation = {"release_digest": "a" * 64}
    unsigned_by_field = {
        "schema_version": 1,
        "wave_id": wave.wave_digest,
        "queue_name": wave.release_queue,
        "manifest_digest": wave.manifest_digest,
        "jobs_digest": wave.jobs_digest,
        "job_count": wave.intent_count,
        "image_identity": wave.pinned_image_reference,
        "release_digest": "a" * 64,
        "target_key_count": 4 + (4 * wave.intent_count),
        "queue_entry_count": 0,
        "job_payload_count": 0,
        "result_count": 0,
        "retry_count": 0,
        "in_progress_count": 0,
        "health_check_count": 0,
        "result_presence_digest": "b" * 64,
    }
    return {
        **unsigned_by_field,
        "attestation_digest": terminal_validation.sha256_digest(
            terminal_validation.canonical_json(unsigned_by_field)
        ),
    }


class _Acquire:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, *_args):
        return False


class _Column:
    def in_(self, _values):
        return self

    def __eq__(self, _other):
        return self


class _Query:
    def where(self, *_args):
        return self

    def limit(self, *_args):
        return self

    def join(self, *_args):
        return self

    def order_by(self, *_args):
        return self


def _isolation_controller(rows, generic_jobs):
    result = types.SimpleNamespace(all=lambda: rows)
    return types.SimpleNamespace(
        exists=lambda _query: False,
        select=lambda *_args: _Query(),
        PTGImportWaveIntent=types.SimpleNamespace(
            run_id=_Column(),
            wave_id=_Column(),
            ordinal=_Column(),
        ),
        ImportRun=types.SimpleNamespace(
            run_id=_Column(),
            importer=_Column(),
            status=_Column(),
        ),
        PTG_WAVE_FENCED_IMPORTERS=("ptg",),
        PTG_ACTIVE_RUN_STATES=("queued", "running"),
        db=types.SimpleNamespace(execute=AsyncMock(return_value=result)),
        PTGWaveControllerHold=RuntimeError,
        PTGWaveStateConflict=ValueError,
        list_generic_ptg_jobs=lambda: generic_jobs,
        _generic_job_nonterminal=isolation.is_generic_job_nonterminal,
    )


def _synthetic_catalog_metadata() -> dict[str, object]:
    accumulator = catalog_manifest._SourceManifestAccumulator.create(
        "synthetic-source"
    )
    accumulator.add_file("synthetic-file", 0)
    return {
        "discovery_run_id": "synthetic-run",
        catalog_manifest.CATALOG_PAGING_MANIFEST_METADATA_KEY: accumulator.manifest(
            "synthetic-run"
        ),
    }
