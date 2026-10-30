# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic standalone inverse wire values; no native admission authority."""

from process import provider_directory_serving_inverse_capacity_contract as inverse
from process.provider_directory_common_serving_recovery_document import FAMILIES


def execution():
    """Bind a real-shaped old event independently from a future inverse worker."""
    return dict(
        contract_id=inverse.EXECUTION,
        node_id="dev-node",
        importer="provider-directory-fhir",
        operation="recover",
        generation="00000000-0000-4000-8000-000000000001",
        published_event_sha256="1" * 64,
        source_ids=["source-a"],
    )


def limits():
    """Keep finite test ceilings; these values never claim physical reservation."""
    return dict(
        contract_id=inverse.LIMITS,
        families=[
            dict(
                family=family,
                max_delete_rows=10,
                max_delete_logical_bytes=1024,
                max_insert_rows=10,
                max_insert_logical_bytes=1024,
            )
            for family in inverse.FAMILIES
        ],
        max_data_bytes=16 * 1024**2,
        max_temp_bytes=16 * 1024**2,
        max_wal_bytes=16 * 1024**2,
        max_preparation_wal_bytes=2 * 1024**2,
        max_cutover_wal_bytes=4 * 1024**2,
        max_commit_wal_bytes=1024**2,
        temp_file_limit_bytes_per_backend=1024**2,
        minimum_remaining_bytes=1024**2,
        max_prepare_seconds=60,
        max_cutover_seconds=2,
        statement_timeout_ms=1000,
        lock_timeout_ms=500,
    )


def _relation(name, oid):
    return dict(
        schema="unit",
        name=name,
        oid=oid,
        native_sha256="a" * 64,
        baseline_total_bytes=8192,
        allowed_names=[name],
    )


def _family_rows():
    relations, families = [], []
    for index, family in enumerate(inverse.FAMILIES):
        root = FAMILIES[family]
        retained_name = "pd_" + family + "_before_" + "a" * 20
        for name, oid in [(root, 100 + index), (retained_name, 200 + index)]:
            relations.append(_relation(name, oid))
        families.append(
            dict(
                family=family,
                root_oid=100 + index,
                native_sha256="a" * 64,
                retained_name=retained_name,
                retained_oid=200 + index,
                retained_layout_sha256="b" * 64,
                retained_rows=1,
                retained_data_sha256="c" * 64,
                live_rows=1,
                live_logical_bytes=128,
                insert_rows=1,
                insert_logical_bytes=128,
                expected_postimage=dict(rows=1, data_sha256="d" * 64),
                restore_mode="rows",
            )
        )
    return families, relations


def _scope_rows():
    scopes, relations = [], []
    for index, family in enumerate(["group_scope", "location_scope", "npi_scope"]):
        name = "pd_" + family + "_before_" + "b" * 20
        scopes.append(
            dict(family=family, name=name, oid=300 + index, layout_sha256="b" * 64, rows=1, data_sha256="c" * 64)
        )
        relations.append(_relation(name, 300 + index))
    return scopes, relations


def scope():
    """Supply exact closed schema shapes without pretending their native observations exist."""
    operation = execution()
    profile_by_field = dict(
        generation_id="pdprofile_" + "2" * 32,
        control_generation=3,
        selection_proof_id="3" * 64,
        selection_fingerprint="4" * 64,
        profile_input_digest="5" * 64,
        source_vector_hash="6" * 64,
        source_context_vector_hash="7" * 64,
        profile_as_of="2026-07-01",
        profile_schema_version=1,
        profile_strategy_version="example-strategy",
    )
    families, relations = _family_rows()
    scopes, scope_relations = _scope_rows()
    relations.extend(scope_relations)
    scope_by_field = dict(
        contract_id=inverse.SCOPE,
        operation_sha256=inverse.operation_sha256(operation),
        generation=operation["generation"],
        published_event_sha256=operation["published_event_sha256"],
        node_id=operation["node_id"],
        owner_oid=20,
        schema_oid=30,
        database_system_identifier="12345",
        database_oid=40,
        database_name="example",
        forward_state_sha256="e" * 64,
        retention_payload_sha256="f" * 64,
        qualified_source_ids=["source-a"],
        initial_receipt=None,
        profile_provenance=profile_by_field,
        families=families,
        scopes=scopes,
        relations=sorted(relations, key=lambda row: row["oid"]),
    )
    scope_by_field["input_sha256"] = inverse.digest(inverse.INPUT_DOMAIN, scope_by_field)
    return scope_by_field


def geometry():
    """Use signed-ceiling semantics rather than a fabricated index forecast."""
    scope_by_field, limits_by_field = scope(), limits()
    return dict(
        contract_id=inverse.GEOMETRY,
        admission_purpose=inverse.PURPOSE,
        input_sha256=scope_by_field["input_sha256"],
        scope=scope_by_field,
        limits_sha256=inverse.limits_sha256(limits_by_field),
        reservation_bytes=[[kind, limits_by_field["max_" + kind + "_bytes"]] for kind in ("data", "temp", "wal")],
        minimum_remaining_bytes=limits_by_field["minimum_remaining_bytes"],
        required_build_seconds=limits_by_field["max_prepare_seconds"],
        temp_file_limit_bytes_per_backend=limits_by_field["temp_file_limit_bytes_per_backend"],
        preparation_wal_upper_bound_bytes=limits_by_field["max_preparation_wal_bytes"],
        cutover_wal_upper_bound_bytes=limits_by_field["max_cutover_wal_bytes"],
        commit_wal_upper_bound_bytes=limits_by_field["max_commit_wal_bytes"],
        postgres_block_size_bytes=8192,
        wal_segment_size_bytes=16 * 1024**2,
        data_tablespace_oid=1663,
        data_tablespace_name="pg_default",
        temp_tablespace_oid=1663,
        temp_tablespace_name="pg_default",
        worker_count=1,
    )


def _control_preflight(operation, ceiling, storage, nonce):
    from process.provider_directory_profile_capacity_signing_guard_contract import (
        CONTROL_PLANE_SIGNING_INTENT_CONTRACT_ID,
        CONTROL_PLANE_STORAGE_OBSERVATION_DIGEST_DOMAIN,
    )

    control_request_by_field = dict(
        contract_id=inverse.CONTROL_REQUEST,
        recovery_execution=operation,
        limits=ceiling,
        storage_observation=storage,
        signing_intent=dict(contract_id=CONTROL_PLANE_SIGNING_INTENT_CONTRACT_ID, request_nonce=nonce),
    )
    quiescence_by_field = dict(
        contract_id=inverse.CONTROL_QUIESCENCE,
        operation_sha256=inverse.operation_sha256(operation),
        active_profile_run_count=0,
        active_inverse_run_count=0,
        active_held_dispatch_count=0,
    )
    control_receipt_by_field = dict(
        contract_id=inverse.CONTROL_RECEIPT,
        request_contract_id=inverse.CONTROL_REQUEST,
        request_sha256=inverse.digest(inverse.CONTROL_REQUEST, control_request_by_field),
        request_nonce=nonce,
        recovery_execution_identity=operation,
        operation_sha256=inverse.operation_sha256(operation),
        capacity_limits_sha256=inverse.limits_sha256(ceiling),
        storage_observation_sha256=inverse.digest(CONTROL_PLANE_STORAGE_OBSERVATION_DIGEST_DOMAIN, storage),
        quiescence=quiescence_by_field,
        quiescence_sha256=inverse.digest(inverse.CONTROL_QUIESCENCE, quiescence_by_field),
        **{key: storage[key] for key in ("issued_at", "expires_at", "max_build_deadline")},
    )
    control_receipt_by_field["receipt_sha256"] = inverse.digest(inverse.CONTROL_RECEIPT, control_receipt_by_field)
    return control_request_by_field, control_receipt_by_field


def _healthcare_request(operation, ceiling, control_receipt):
    challenge_by_field = dict(
        contract_id="healthporta.provider-directory-profile-capacity-signing-guard-request.v1",
        request_nonce=control_receipt["request_nonce"],
        control_plane_receipt_sha256=control_receipt["receipt_sha256"],
        expires_at=control_receipt["expires_at"],
    )
    return dict(
        contract_id=inverse.HEALTHCARE_REQUEST,
        recovery_execution=operation,
        limits=ceiling,
        signing_guard=challenge_by_field,
    )


def _healthcare_receipt(request, control_receipt, plan, previous):
    from copy import deepcopy

    from process import provider_directory_serving_inverse_capacity_signing as signing

    engine_quiescence = deepcopy(previous["healthcare_receipt"]["quiescence"])
    engine_quiescence.update(contract_id=inverse.HEALTHCARE_QUIESCENCE, active_inverse_run_count=0)
    receipt_by_field = dict(
        contract_id=inverse.HEALTHCARE_RECEIPT,
        request_contract_id=inverse.HEALTHCARE_REQUEST,
        request_sha256=inverse.digest(inverse.HEALTHCARE_REQUEST, request),
        request_nonce=control_receipt["request_nonce"],
        control_plane_receipt_sha256=control_receipt["receipt_sha256"],
        issued_at=control_receipt["issued_at"],
        expires_at=control_receipt["expires_at"],
        recovery_execution_identity=request["recovery_execution"],
        capacity_limits=request["limits"],
        capacity_limits_sha256=inverse.limits_sha256(request["limits"]),
        capacity_geometry=plan,
        capacity_geometry_hash=signing.geometry_sha256(plan),
        required_reservation_bytes_by_storage_class=dict(plan["reservation_bytes"]),
        runtime_observation=deepcopy(previous["healthcare_receipt"]["runtime_observation"]),
        quiescence=engine_quiescence,
        quiescence_sha256=inverse.digest(inverse.HEALTHCARE_QUIESCENCE, engine_quiescence),
        preflight_receipt_storage=deepcopy(previous["healthcare_receipt"]["preflight_receipt_storage"]),
    )
    receipt_by_field["receipt_sha256"] = inverse.digest(inverse.HEALTHCARE_RECEIPT, receipt_by_field)
    return receipt_by_field


def signing_guard():
    """Compose genuine synthetic replay bytes around the standalone operation schema."""
    from copy import deepcopy

    from process.provider_directory_profile_capacity_signing_guard_contract import (
        CAPACITY_BUNDLE_VALIDATION_PATH,
        CONTROL_PLANE_PREFLIGHT_PATH,
        HEALTHCARE_PREFLIGHT_PATH,
    )
    from tests.test_provider_directory_profile_capacity_attestation import _signed_envelope

    base = _signed_envelope()["lease"]
    previous = base["signing_preflight_guard"]
    storage = deepcopy(previous["control_plane_request"]["storage_observation"])
    operation, ceiling, plan = execution(), limits(), geometry()
    for key in ("database_system_identifier", "database_oid", "database_name"):
        plan["scope"][key] = base[key]
    plan["scope"]["input_sha256"] = inverse.digest(
        inverse.INPUT_DOMAIN, {key: scope_value for key, scope_value in plan["scope"].items() if key != "input_sha256"}
    )
    plan["input_sha256"] = plan["scope"]["input_sha256"]
    control_request_by_field, control_receipt_by_field = _control_preflight(operation, ceiling, storage, "8" * 64)
    request_by_field = _healthcare_request(operation, ceiling, control_receipt_by_field)
    receipt_by_field = _healthcare_receipt(request_by_field, control_receipt_by_field, plan, previous)
    return dict(
        contract_id=inverse.SIGNING_GUARD,
        bundle_validation_path=CAPACITY_BUNDLE_VALIDATION_PATH,
        control_plane_preflight_path=CONTROL_PLANE_PREFLIGHT_PATH,
        healthcare_preflight_path=HEALTHCARE_PREFLIGHT_PATH,
        control_plane_request=control_request_by_field,
        control_plane_request_sha256=control_receipt_by_field["request_sha256"],
        control_plane_receipt=control_receipt_by_field,
        control_plane_receipt_sha256=control_receipt_by_field["receipt_sha256"],
        healthcare_request=request_by_field,
        healthcare_request_sha256=receipt_by_field["request_sha256"],
        healthcare_receipt=receipt_by_field,
        healthcare_receipt_sha256=receipt_by_field["receipt_sha256"],
        capacity_limits_sha256=receipt_by_field["capacity_limits_sha256"],
        storage_observation_sha256=control_receipt_by_field["storage_observation_sha256"],
        operation_preimage_sha256=inverse.operation_sha256(operation),
    )


def signed_envelope(guard=None):
    """Use the existing real Ed25519 fixture signer; no native acceptance is implied."""
    from copy import deepcopy

    from tests.test_provider_directory_profile_capacity_attestation import _signed_envelope

    guard = signing_guard() if guard is None else guard

    def update(body):
        receipt = guard["healthcare_receipt"]
        body.update(
            signing_preflight_guard=deepcopy(guard),
            signing_preflight_guard_sha256=inverse.digest(inverse.SIGNING_GUARD, guard),
            nonce=receipt["receipt_sha256"],
            capacity_geometry_hash=receipt["capacity_geometry_hash"],
            reservation_id="synthetic-inverse",
        )
        for volume in body["volumes"]:
            volume["reserved_bytes"] = receipt["required_reservation_bytes_by_storage_class"][volume["volume_class"]]

    return _signed_envelope(body_mutator=update)
