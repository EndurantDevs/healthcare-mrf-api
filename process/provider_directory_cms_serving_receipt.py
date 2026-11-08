# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One immutable receipt for a CMS source, Profile, and native address publication."""

import json
import re
from uuid import UUID

from sqlalchemy import text

from db.cms_registry_source_pair_slot import COLUMN, MAX_BYTES

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


def registry_source_pair_document(prepared_pair):
    """Bind the existing closed preparation to its canonical nonzero capture UUID."""
    from process.network_registry_cms_prepared_pair import PreparedRegistryCMSSourcePair

    if type(prepared_pair) is not PreparedRegistryCMSSourcePair:
        raise ValueError("cms_serving_registry_pair_invalid")
    slot_by_field = {
        "protocol": 1,
        "capture_id": str(prepared_pair.recipe.source_pin.retained_epoch.epoch_id),
        "prepared_pair": prepared_pair.as_dict(),
    }
    slot_by_field = json.loads(json.dumps(slot_by_field, allow_nan=False))
    decode_registry_source_pair_document(slot_by_field)
    return slot_by_field


def decode_registry_source_pair_document(document):
    """Decode the bounded immutable slot; this grants no native custody or authority."""
    from process.network_registry_cms_prepared_cleanup import decode_prepared_registry_cms_source_pair
    from process.network_registry_cms_prepared_pair import _PREPARED_COMMENT, _PREPARED_COMMENT_V2

    _fields(document, {"protocol", "capture_id", "prepared_pair"})
    try:
        capture_id = UUID(document["capture_id"])
        if (
            type(document["protocol"]) is not int
            or document["protocol"] != 1
            or not capture_id.int
            or str(capture_id) != document["capture_id"]
            or len(json.dumps(document, ensure_ascii=False, allow_nan=False).encode()) > MAX_BYTES
        ):
            raise ValueError
        prefix = _PREPARED_COMMENT_V2 if "source_attempt" in document["prepared_pair"] else _PREPARED_COMMENT
        prepared = decode_prepared_registry_cms_source_pair(prefix + json.dumps(document["prepared_pair"]))
        if prepared.recipe.source_pin.retained_epoch.epoch_id != capture_id:
            raise ValueError
        return prepared
    except ValueError, TypeError, KeyError, AttributeError, UnicodeError, RecursionError:
        raise ValueError("cms_serving_registry_pair_invalid") from None


async def require_registry_receipt_owner(session, schema):
    """Require the actual original same-task Source owner, not a retained publisher."""
    from process.network_cms_registry_address_capture import native_driver
    from process.provider_directory_generic_source_policy import bound_publisher, source_copy_session

    publisher = bound_publisher()
    if source_copy_session() is not session or schema != publisher.policy.schema:
        raise ValueError("cms_serving_registry_original_session_required")
    connection = await native_driver(session)
    if connection is not publisher.connection or not connection.is_in_transaction():
        raise ValueError("cms_serving_registry_original_session_required")
    actor = await connection.fetchrow("SELECT session_user::text AS session,current_user::text AS current")
    if actor is None or dict(actor) != {
        "session": publisher.policy.publisher_role,
        "current": publisher.policy.owner_role,
    }:
        raise ValueError("cms_serving_registry_original_session_required")
    return connection


async def append_serving_receipt(session, schema, payload, *, registry_source_pair=None):
    """Append the exact result inside the owner's transaction; deferred guards fence its commit."""
    if not session.in_transaction():
        raise ValueError("cms_serving_receipt_requires_transaction")
    payload = validate_receipt_payload(payload)
    document = None
    if registry_source_pair is not None:
        document = registry_source_pair_document(registry_source_pair)
        await require_registry_receipt_owner(session, schema)
    schema = _schema(schema)
    columns = "receipt_id,payload" + (f",{COLUMN}" if document is not None else "")
    selection = "encode(sha256(convert_to(p::text,'UTF8')),'hex'),p"
    parameters_by_name = {"payload": json.dumps(payload)}
    if document is not None:
        selection += ",CAST(:registry_source_pair AS jsonb)"
        parameters_by_name["registry_source_pair"] = json.dumps(document, allow_nan=False)
    return await session.scalar(
        text(f"""INSERT INTO {schema}.{_TABLE} ({columns})
        SELECT {selection}
        FROM (SELECT CAST(:payload AS jsonb) p) value RETURNING receipt_id"""),
        parameters_by_name,
    )


async def read_registry_source_pair_receipt(session, schema, capture_id):
    """Read the unique committed capture slot; an own-XID row is never recovery proof."""
    if type(capture_id) is not UUID or not capture_id.int:
        raise ValueError("cms_serving_registry_pair_invalid")
    namespace = _schema(schema)
    result = await session.execute(
        text(f"""SELECT receipt_id,payload,publication_xid::text AS publication_xid,profile_generation_id,
        CASE WHEN octet_length({COLUMN}::text)<={MAX_BYTES} THEN {COLUMN} END AS {COLUMN}
        FROM {namespace}.{_TABLE} WHERE {COLUMN} IS NOT NULL AND {COLUMN}->>'capture_id'=:capture
        AND publication_xid IS DISTINCT FROM pg_current_xact_id_if_assigned()"""),
        {"capture": str(capture_id)},
    )
    row = result.mappings().one_or_none()
    if row is not None:
        decode_registry_source_pair_document(row[COLUMN])
    return dict(row) if row is not None else None


async def has_registry_source_pair_receipt(session, schema, capture_id):
    """Deny deletion for any visible slot, including an unfinished own-XID insert."""
    if type(capture_id) is not UUID or not capture_id.int:
        raise ValueError("cms_serving_registry_pair_invalid")
    return bool(
        await session.scalar(
            text(
                f"SELECT EXISTS(SELECT FROM {_schema(schema)}.{_TABLE} WHERE {COLUMN} IS NOT NULL "
                f"AND {COLUMN}->>'capture_id'=:capture)"
            ),
            {"capture": str(capture_id)},
        )
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
