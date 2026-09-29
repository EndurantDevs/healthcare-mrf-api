# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One immutable receipt for a CMS source, Profile, and native address publication."""

import json
import re

from sqlalchemy import text

_TABLE = "provider_directory_cms_serving_receipt"
_PIN_FIELDS = {"source_id", "endpoint_id", "dataset_id", "dataset_hash", "acquisition_root_run_id"}
_AUTHORITY_FIELDS = {
    "local_lineage_id",
    "local_generation",
    "origin_lineage_id",
    "origin_generation",
    "published_at",
    "relation_oids",
}
_PROFILE_FIELDS = {
    "status",
    "operation",
    "control_generation",
    "generation_id",
    "selection_proof_id",
    "authority_revision",
    "profile_schema_version",
    "profile_strategy_version",
    "source_vector_hash",
    "source_context_vector_hash",
    "executable_plan_hash",
    "profile_as_of",
    "evidence_target_oid",
    "profile_target_oid",
    "evidence_rows",
    "profile_rows",
}
_PAYLOAD_FIELDS = {
    "contract_version",
    "predecessor_receipt_id",
    "expected_incumbent",
    "cms",
    "desired_datasets",
    "selection",
    "profile",
    "address",
    "doctors",
    "alias_generation",
    "overlay_oid",
}
_ARCHIVE_FIELDS = {"target_oid", "from_revision", "to_revision", "native_input_hash", "delta_rows", "delta_sha256"}
_NATIVE_RELATIONS = (
    "doctor_clinician_address",
    "cms_doctor_education",
    "cms_doctor_group_site",
    "provider_directory_profile",
    "provider_directory_profile_evidence",
    "provider_directory_address_overlay",
    "entity_address_unified",
    "entity_address_evidence",
    "entity_address_plan_bridge",
    "entity_address_network_bridge",
    "entity_address_procedure_bridge",
    "entity_address_medication_bridge",
    "facility_anchor_npi_candidate",
)


def _schema(schema):
    if not isinstance(schema, str) or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is None:
        raise ValueError("cms_serving_schema_invalid")
    return f'"{schema}"'


def _fields(value, expected):
    if not isinstance(value, dict) or set(value) != expected:
        raise ValueError("cms_serving_receipt_fields_invalid")


def validate_receipt_payload(payload):
    """Require fixed fields and a unique ordered vector; database guards verify their authority."""
    _fields(payload, _PAYLOAD_FIELDS | ({"archive"} if isinstance(payload, dict) and "archive" in payload else set()))
    if "archive" in payload:
        _validate_archive(payload["archive"])
    if type(payload["contract_version"]) is not int or payload["contract_version"] != 1:
        raise ValueError("cms_serving_receipt_contract_invalid")
    _fields(payload["cms"], _PIN_FIELDS | {"release_id", "proof_version"})
    _fields(payload["selection"], {"proof_id", "fingerprint", "catalog_digest"})
    _fields(payload["profile"], _PROFILE_FIELDS)
    _fields(payload["address"], _AUTHORITY_FIELDS)
    _fields(payload["doctors"], _AUTHORITY_FIELDS | {"importer_id"})
    if payload["expected_incumbent"] is not None:
        _fields(payload["expected_incumbent"], _PIN_FIELDS)
    pins = payload["desired_datasets"]
    if not isinstance(pins, list) or len(pins) > 256:
        raise ValueError("cms_serving_receipt_vector_invalid")
    for pin in pins:
        _fields(pin, _PIN_FIELDS)
        if any(not isinstance(value, str) or not value or len(value) > 256 for value in pin.values()):
            raise ValueError("cms_serving_receipt_pin_invalid")
    source_ids = [pin["source_id"] for pin in pins]
    if source_ids != sorted(set(source_ids)) or payload["cms"]["source_id"] != "cms-npd":
        raise ValueError("cms_serving_receipt_vector_invalid")
    return payload


def _validate_archive(archive):
    """Keep the optional immutable merge result closed and bounded."""
    _fields(archive, _ARCHIVE_FIELDS)
    if (
        any(
            type(archive[name]) is not int or not 0 <= archive[name] < 2**63
            for name in ("from_revision", "to_revision", "delta_rows")
        )
        or type(archive["target_oid"]) is not int
        or not 0 < archive["target_oid"] < 2**32
        or archive["to_revision"] != archive["from_revision"] + 2
        or any(
            not isinstance(archive[name], str) or re.fullmatch(r"[0-9a-f]{64}", archive[name]) is None
            for name in ("native_input_hash", "delta_sha256")
        )
    ):
        raise ValueError("cms_serving_archive_result_invalid")


async def read_current_receipt(session, schema):
    """Resolve the incumbent native pair; dependency or alias drift still requires a fresh build."""
    return await _read_receipt(session, schema, "cms_serving_current_receipt_matches")


async def read_serving_receipt(session, schema):
    """Read accepted native serving identity without granting new publication or withdrawal authority."""
    return await _read_receipt(session, schema, "cms_serving_snapshot_receipt_matches")


async def _read_receipt(session, schema, predicate):
    schema = _schema(schema)
    result = await session.execute(
        text(f"""SELECT r.receipt_id,r.payload
        FROM {schema}.{_TABLE} r JOIN {schema}.entity_address_result_generation a
          ON r.address_lineage_id=a.local_lineage_id AND r.address_generation=a.local_generation
        JOIN {schema}.provider_directory_profile_serving_generation p
          ON r.profile_generation_id=p.generation_id AND p.singleton_key='global'
        WHERE a.singleton AND {schema}.{predicate}(r.payload)""")
    )
    row = result.mappings().one_or_none()
    return dict(row) if row is not None else None


async def _lock_native_dependencies(session, schema):
    """Protect relation names before ledger rows, failing promptly on competing native cutovers."""
    if not session.in_transaction():
        raise ValueError("cms_serving_receipt_requires_transaction")
    await session.execute(text("SET LOCAL lock_timeout='5s'"))
    locked = await session.scalar(text("SELECT pg_try_advisory_xact_lock(hashtext('address_numeric_grid_alias_v1'))"))
    if not locked:
        raise RuntimeError("cms_serving_native_dependencies_busy")
    relations = ",".join(f"{schema}.{name}" for name in _NATIVE_RELATIONS)
    await session.execute(text(f"LOCK TABLE {relations} IN ACCESS SHARE MODE NOWAIT"))
    for table, predicate in (
        ("reference_family_result_generation", "importer_id='cms-doctors'"),
        ("entity_address_result_generation", "singleton"),
        ("provider_directory_profile_serving_generation", "singleton_key='global'"),
        ("address_alias_state_v1", "singleton"),
        ("address_alias_artifact_state_v1", "artifact_name='provider_directory_address_overlay'"),
    ):
        await session.execute(text(f"SELECT 1 FROM {schema}.{table} WHERE {predicate} FOR SHARE NOWAIT"))


async def capture_native_dependencies(session, schema, *, lock=False):
    """Capture exact native fields; generation-zero Doctors tables confer no acceptance."""
    schema = _schema(schema)
    if lock:
        await _lock_native_dependencies(session, schema)
    snapshot = await session.scalar(text(f"SELECT {schema}.cms_serving_native_snapshot()"))
    doctors = snapshot.get("doctors") if snapshot else None
    if not doctors or doctors.get("local_generation", 0) <= 0 or doctors.get("origin_generation") is None:
        raise RuntimeError("cms_serving_doctors_authority_unavailable")
    if not await session.scalar(
        text(f"SELECT {schema}.cms_serving_native_matches(CAST(:snapshot AS jsonb))"),
        {"snapshot": json.dumps(snapshot)},
    ):
        raise RuntimeError("cms_serving_native_authority_drift")
    return snapshot


async def assert_native_dependencies(session, schema, expected):
    """Keep the pre-build dependency snapshot pinned through caller-owned cutover."""
    if await capture_native_dependencies(session, schema, lock=True) != expected:
        raise RuntimeError("cms_serving_native_dependencies_changed")


async def append_serving_receipt(session, schema, payload):
    """Append the exact result inside the owner's transaction; deferred guards fence its commit."""
    if not session.in_transaction():
        raise ValueError("cms_serving_receipt_requires_transaction")
    schema = _schema(schema)
    payload = validate_receipt_payload(payload)
    return await session.scalar(
        text(f"""INSERT INTO {schema}.{_TABLE} (receipt_id,payload)
        SELECT encode(sha256(convert_to(p::text,'UTF8')),'hex'),p
        FROM (SELECT CAST(:payload AS jsonb) p) value RETURNING receipt_id"""),
        {"payload": json.dumps(payload)},
    )


async def verify_historical_receipt(session, schema, receipt_id, payload):
    """Prove a committed result even after a later native generation supersedes it."""
    schema = _schema(schema)
    validate_receipt_payload(payload)
    return bool(
        await session.scalar(
            text(f"""SELECT EXISTS (SELECT 1 FROM {schema}.{_TABLE}
        WHERE receipt_id=:receipt_id AND payload=CAST(:payload AS jsonb)
          AND publication_xid IS DISTINCT FROM pg_current_xact_id_if_assigned())"""),
            {"receipt_id": receipt_id, "payload": json.dumps(payload)},
        )
    )
