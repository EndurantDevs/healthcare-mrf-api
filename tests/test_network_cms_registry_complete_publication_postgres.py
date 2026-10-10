# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete native publication proof using test-only authority enrollment."""

import datetime
import hashlib
import json
import subprocess
from copy import deepcopy
from dataclasses import replace
from functools import partial
from uuid import uuid4

import pytest
from sqlalchemy import text

from process import provider_directory_cms_capacity_contract as cms_contract
from process import provider_directory_cms_npd as cms
from process import provider_directory_cms_serving as serving
from process import provider_directory_profile_capacity_preflight_contract as preflight
from process import provider_directory_profile_capacity_runtime as capacity_runtime
from tests import cms_npd_admission_postgres_support as source
from tests import cms_registry_complete_publication_postgres_support as native
from tests import provider_directory_cms_capacity_test_support as cms_signing
from tests import provider_directory_profile_capacity_signing_guard_test_support as signing
from tests.provider_directory_profile_capacity_trust_fixtures import capacity_trust_from_envelope
from tests.test_provider_directory_cms_resource_batch_postgres import cms_resource_template

pytestmark = pytest.mark.usefixtures(cms_resource_template.__name__)


async def _native_profile_lease(monkeypatch, execution):
    """Sign actual native preflight output with the enrolled synthetic authority."""
    now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
    timing = signing._GuardTiming(
        now - datetime.timedelta(seconds=2),
        now - datetime.timedelta(seconds=1),
        now + datetime.timedelta(seconds=630),
        now + datetime.timedelta(seconds=610),
        hashlib.sha256(f"profile-{execution.generation}".encode()).hexdigest(),
    )
    limits = signing._limits_payload(max_build_seconds=600, temp_file_limit_bytes=64 * 1024 * 1024)
    for cap in limits["relation_byte_caps"]:
        for field in cap:
            if field.endswith("_bytes") and cap[field]:
                cap[field] = 512 * 1024 * 1024
        cap["max_temp_bytes"] = limits["temp_file_limit_bytes"]
    monkeypatch.setenv(capacity_runtime.CAPACITY_LIMITS_ENV, json.dumps(limits))
    control_request, control, request, validated, _, followup = native.profile_preflight_inputs(
        execution, limits, signing._storage_observation(timing), timing
    )
    receipt = await source.fhir.provider_directory_profile_capacity_preflight(
        preflight.validated_capacity_preflight_request(request)
    )
    if execution.generation == 1:
        assert receipt["serving_generation_preflight"]["serving_singleton_absent"]
    guard = signing._guard_payload(control_request, control, request, validated, receipt, followup)
    profile_lease = native.sign_native_receipt(
        guard, receipt, timing, reservation=f"synthetic-profile-{execution.generation}"
    )
    monkeypatch.setattr(
        capacity_runtime, "configured_capacity_lease_trust", lambda: capacity_trust_from_envelope(profile_lease)
    )
    return guard, profile_lease, timing


async def _paired_native_execution(monkeypatch, execution):
    """Produce separately signed CMS geometry around the original Profile lease."""
    guard, profile_lease, timing = await _native_profile_lease(monkeypatch, execution)
    cms_guard = deepcopy(guard)
    admission_by_field = {
        "contract_id": cms_contract.CMS_ADMISSION_REQUEST_CONTRACT,
        "admission_purpose": "cms_nonprofile",
        "paired_profile_lease": profile_lease,
        "limits": {
            "contract_id": cms_contract.CMS_LIMITS_CONTRACT,
            "batch_size": 100_000,
            "worker_count": 1,
            "temp_file_limit_bytes_per_backend": 64 * 1024 * 1024,
            "minimum_remaining_bytes": 1024 * 1024,
            "required_build_seconds": 300,
            "max_data_bytes": 2 * 1024**3,
            "max_temp_bytes": 2 * 1024**3,
            "max_wal_bytes": 8 * 1024**3,
        },
    }
    for document, contract in (
        (cms_guard["control_plane_request"], cms_contract.CMS_CONTROL_REQUEST_CONTRACT),
        (cms_guard["healthcare_request"], cms_contract.CMS_PREFLIGHT_REQUEST_CONTRACT),
    ):
        document.update(contract_id=contract, cms_nonprofile_admission=deepcopy(admission_by_field))
        document.pop("profile_materialization", None)
    cms_guard["control_plane_receipt"].update(
        contract_id=cms_contract.CMS_CONTROL_CONTRACT,
        request_contract_id=cms_contract.CMS_CONTROL_REQUEST_CONTRACT,
    )
    cms_guard["control_plane_receipt"].pop("profile_materialization", None)
    nonce = hashlib.sha256(f"cms-{execution.generation}".encode()).hexdigest()
    cms_guard["control_plane_request"]["signing_intent"]["request_nonce"] = nonce
    cms_guard["control_plane_receipt"]["request_nonce"] = nonce
    cms_guard["healthcare_request"]["signing_guard"]["request_nonce"] = nonce
    cms_signing.rehash_guard(cms_guard)
    cms_request = preflight.validated_capacity_preflight_request(cms_guard["healthcare_request"])
    cms_receipt = await source.fhir.provider_directory_profile_capacity_preflight(cms_request)
    cms_guard["healthcare_receipt"] = cms_receipt
    cms_signing.rehash_guard(cms_guard)
    cms_lease = native.sign_native_receipt(
        cms_guard, cms_receipt, timing, reservation=f"synthetic-cms-{execution.generation}"
    )
    execution = replace(execution, capacity_attestation=profile_lease, cms_nonprofile_capacity_attestation=cms_lease)
    monkeypatch.setattr(serving, "configured_capacity_lease_trust", lambda: capacity_trust_from_envelope(profile_lease))
    monkeypatch.setattr(serving, "configured_storage_continuation", lambda: native.storage_authority(cms_lease))
    return execution


async def test_complete_publication_and_retained_pair(monkeypatch, tmp_path):
    """Exercise real publication before accepting its receipt and retained pair."""
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_UNLOGGED_STAGE", "true")
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_SUPPORT_CODE_LOCATION_INDEXES", "true")
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_MIN_ROWS", "2")
    with pytest.raises(RuntimeError, match="stage row count 0 below minimum 2"):
        native.entity_address_unified._validate_publish_row_count(
            stage_rows=0, previous_rows=0, test_mode=False, min_rows_required=2
        )
    monkeypatch.setattr(source, "_resource_rows", partial(native.office_resources, source._resource_rows))
    directory, acquired = source.retained_release(tmp_path, include_missing_network=False)
    async with source.admission_database(monkeypatch) as database:
        with source.release_probe_client(directory) as client:
            initial = await cms._run_acquired(
                {"context": {}}, {}, "complete-pair-acquisition", directory, acquired, client
            )
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        await native.install_native_inputs(database, monkeypatch, tmp_path, commit)
        execution = await native.attested_execution(database, initial, monkeypatch)
        run_id = "run_" + uuid4().hex
        async with database.session_factory() as session, session.begin():
            await session.execute(
                text(
                    "INSERT INTO mrf.import_run(run_id,engine,importer,status,params,metrics) "
                    "VALUES(:run,'test-engine','provider-directory-fhir','running','{}','{}')"
                ),
                {"run": run_id},
            )
        execution = await _paired_native_execution(monkeypatch, execution)
        native.observe_geometry(monkeypatch, database)
        verified = native.verify_resume_geometry(database, monkeypatch)
        await serving.publish_current_attested_profile(
            source.fhir, execution=execution, run_id=run_id, control_run_id=run_id, metrics={}
        )
        proof, receipt_payload = await native.committed_publication_proof(database, initial)
        assert verified == [True]
        await native.assert_native_offices(database, receipt_payload)
        async with native.retained_pair(database, proof) as pair:
            assert pair.proof == proof
            assert json.loads(pair.cms_receipt_payload) == receipt_payload
