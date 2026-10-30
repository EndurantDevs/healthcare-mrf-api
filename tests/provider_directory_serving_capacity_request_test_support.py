# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine synthetic signatures for ordinary initial and delta request parsing."""

from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace

from process import provider_directory_profile_capacity as capacity
from process import provider_directory_profile_initial_contract as initial
from process import provider_directory_profile_selection as selection
from process import provider_directory_profile_selection_snapshot as snapshot
from process import provider_directory_serving_capacity_contract as serving
from tests.provider_directory_cms_capacity_test_support import _profile_guard, rehash_guard, sign_guard
from tests.provider_directory_profile_initial_test_support import signed_initial_envelope
from tests.test_provider_directory_profile_bounded_capacity import _bounded_geometry
from tests.test_provider_directory_profile_capacity_attestation import VALIDATION_TIME
from tests.test_provider_directory_profile_initial import _geometry as initial_geometry
from tests.test_provider_directory_profile_initial import _target
from tests.test_provider_directory_profile_selection_desired import _date_only, _date_only_rows


def ordinary_execution(*, day="2026-07-30", include_cms=False, empty=False):
    """Compute complete selection identity_by_field with the same node as the signed fixture."""
    catalog, sources, rows = _date_only_rows(include_cms=include_cms)
    if empty:
        computed = snapshot._computed_selection_from_rows(
            catalog, node_id="dev-node", source_rows=sources, dataset_rows=[]
        )
    else:
        computed = snapshot._computed_desired_selection_from_rows(
            catalog,
            node_id="dev-node",
            source_rows=sources,
            dataset_rows=rows,
            desired_dataset_row=None,
            desired_selection=_date_only(day),
        )
    payload = {**computed.identity_payload, "authority_revision": 7}
    payload["proof_id"] = selection._proof_id(payload)
    return selection.ProviderDirectoryProfileExecution(
        selection.validated_profile_selection_attestation(payload), 11, capacity_attestation={}
    )


def ordinary_scope(*, initial_mode=False):
    """Supply synthetic captured identity_by_field; these values alone are not runtime authority."""
    identity_by_field = {
        "source_vector_sha256": "ab" * 32,
        "predecessor_sha256": "bc" * 32,
        "affected_scope_sha256": "cd" * 32,
        "retention_plan_sha256": "de" * 32,
        "native_address_input_hash": "ef" * 32,
        "owner_oid": 16400,
        "schema_oid": 16401,
        "retention_kind": "snapshot" if initial_mode else "delta_preimage",
    }
    return {
        "contract_id": serving.SCOPE,
        **identity_by_field,
        "input_sha256": serving.input_sha256(identity_by_field),
        "max_retention_data_bytes": 8192,
        "resident_retention_data_bytes": 0,
        "requested_source_ids": [],
        "max_retention_wal_bytes": 16384,
        "max_commit_wal_bytes": 8192,
    }


def ordinary_limits():
    """Reuse the closed nonprofile limit fields with their distinct purpose contract."""
    return {
        "contract_id": serving.LIMITS_CONTRACT,
        "batch_size": 8,
        "worker_count": 2,
        "temp_file_limit_bytes_per_backend": 1024,
        "minimum_remaining_bytes": 4096,
        "required_build_seconds": 60,
        "max_data_bytes": 1048576,
        "max_temp_bytes": 1048576,
        "max_wal_bytes": 1048576,
    }


def _profile_geometry(execution, *, initial_mode):
    geometry = initial_geometry() if initial_mode else _bounded_geometry()[0]
    value = replace(
        geometry,
        database_name="healthporta_test",
        selection_proof_id=execution.attestation.proof_id,
        profile_input_digest=execution.attestation.profile_input_digest,
        profile_schema_version=execution.attestation.profile_schema_version,
        profile_strategy_version=execution.attestation.profile_strategy_version,
        profile_as_of=execution.attestation.desired_profile_as_of or geometry.profile_as_of,
    )
    if initial_mode:
        value = replace(value, initial_target_state_sha256=initial.target_state_sha256(_target()))
    return capacity.revalidate_capacity_geometry(value)


def ordinary_pair(execution, *, initial_mode=False):
    """Sign the actual initial or delta receipt contracts without runtime admission claims."""
    geometry = _profile_geometry(execution, initial_mode=initial_mode)
    if initial_mode:
        database = SimpleNamespace(
            **vars(geometry), temp_tablespace_oid=geometry.tablespace_oid, temp_tablespace_name=geometry.tablespace_name
        )
        return signed_initial_envelope(geometry, database, _target(), execution, VALIDATION_TIME)[0]
    guard = _profile_guard(execution)
    receipt = guard["healthcare_receipt"]
    receipt["capacity_geometry"] = capacity.capacity_geometry_payload(geometry)
    receipt["capacity_geometry_hash"] = capacity.capacity_geometry_hash(geometry)
    rehash_guard(guard)
    return sign_guard(guard)


def ordinary_request(*, initial_mode=False, projection=False, execution=None):
    execution = execution or ordinary_execution()
    pair = ordinary_pair(execution, initial_mode=initial_mode)
    request = deepcopy(pair["lease"]["signing_preflight_guard"]["healthcare_request"])
    request.pop("profile_materialization", None)
    request["contract_id"] = serving.PROJECTION_REQUEST if projection else serving.PREFLIGHT_REQUEST
    if projection:
        request.pop("signing_guard")
    scope = ordinary_scope(initial_mode=initial_mode)
    scope["source_vector_sha256"] = serving.source_vector_sha256(execution.attestation.payload["pairs"])
    scope["input_sha256"] = serving.input_sha256({name: scope[name] for name in serving.INPUT_FIELDS})
    request[serving.ADMISSION_FIELD] = {
        "contract_id": serving.ADMISSION_CONTRACT,
        "admission_purpose": serving.PURPOSE,
        "paired_profile_lease": pair,
        "limits": ordinary_limits(),
        "ordinary_scope": scope,
    }
    return request
