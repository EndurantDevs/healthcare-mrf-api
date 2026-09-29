# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read durable capacity evidence for reconciliation with the signing authority.

This is an observation, not an admission or a remaining-capacity calculation.
Original signed ceilings are never added to Control's reservation subtotals.
Signatures still require the authority's trusted verification before accounting.
"""

from __future__ import annotations

import datetime
import json
from collections.abc import Mapping
from typing import Any

from sqlalchemy import text

from process import provider_directory_profile_capacity_attestation as lease_contract
from process.provider_directory_cms_capacity_contract import CMS_ADMISSION_FIELD, CMS_PREFLIGHT_CONTRACT
from process.provider_directory_profile_capacity_attestation_contract import _SIGNED_BODY_FIELDS
from process.provider_directory_profile_capacity_preflight_contract import (
    CAPACITY_PREFLIGHT_CONTRACT_ID,
    preflight_domain_sha256,
)
from process.provider_directory_profile_selection_contract import CMS_CAPACITY_EXECUTION_PARAM

CONTRACT_ID = "provider-directory-healthcare-capacity-reservation-snapshot.v1"
PROFILE_CAPACITY_PARAM = "provider_directory_profile_capacity_attestation"
ACTIVE_RUN_STATUSES = ("queued", "starting", "running", "finalizing", "canceling")
_DATABASE_FIELDS = ("database_system_identifier", "database_oid", "database_name")
_LEGACY_CONTRACTS = {
    "provider-directory-database-capacity-lease-v1",
    lease_contract.LEGACY_CAPACITY_LEASE_V2_CONTRACT_ID,
}


def _fail(reason: str) -> None:
    raise RuntimeError("provider_directory_capacity_snapshot_" + reason)


def _json_object(raw: Any) -> dict:
    if isinstance(raw, str):
        raw = json.loads(raw)
    if not isinstance(raw, Mapping):
        _fail("object_invalid")
    return dict(raw)


def _timestamp(value: Any) -> datetime.datetime:
    if not isinstance(value, datetime.datetime) or value.tzinfo is None or value.utcoffset() is None:
        _fail("timestamp_invalid")
    return value.astimezone(datetime.timezone.utc)


def _json_values(raw: Any) -> Any:
    """Keep database values exactly, encoding timestamp columns for transport."""
    if isinstance(raw, datetime.datetime):
        return raw.isoformat()
    if isinstance(raw, Mapping):
        return {name: _json_values(value) for name, value in raw.items()}
    if isinstance(raw, (tuple, list)):
        return [_json_values(value) for value in raw]
    return raw


def _envelope_identity(raw: Any) -> dict:
    """Check current envelope structure and hashes without claiming trust verification."""
    envelope = _json_object(raw)
    if set(envelope) != {"lease", "signature"}:
        _fail("envelope_invalid")
    body = _json_object(envelope["lease"])
    if set(body) != _SIGNED_BODY_FIELDS:
        _fail("envelope_contract_unsupported")
    parsed = lease_contract._parse_capacity_lease_fields(body)
    lease_contract._decode_signature(envelope["signature"])
    lease_contract._assert_attestation_id(body)
    lease_contract._assert_tablespace_volume_binding(parsed["tablespaces"], parsed["volumes"])
    lease_contract._assert_colocated_volume_accounting(parsed["volumes"])
    lease_contract._parse_capacity_signing_guard_fields(body, parsed)
    envelope = json.loads(lease_contract.canonical_capacity_lease_json(envelope))
    purpose = (
        "cms_nonprofile" if CMS_ADMISSION_FIELD in body["signing_preflight_guard"]["healthcare_request"] else "profile"
    )
    return {
        "reservation_id": body["reservation_id"],
        "attestation_id": body["attestation_id"],
        "lease_digest": lease_contract._domain_hash(lease_contract.CAPACITY_LEASE_DIGEST_DOMAIN, envelope),
        "preflight_receipt_sha256": body["nonce"],
        "admission_purpose": purpose,
        "envelope": envelope,
        "database_binding": {name: body[name] for name in _DATABASE_FIELDS},
        "tablespace_identity_hash": lease_contract._identity_hash(
            lease_contract.CAPACITY_TABLESPACE_IDENTITY_DOMAIN, body["tablespaces"]
        ),
        "volume_identity_hash": lease_contract._identity_hash(
            lease_contract.CAPACITY_VOLUME_IDENTITY_DOMAIN, body["volumes"]
        ),
        "signature_verification": "required_by_authority",
        "opaque_legacy": False,
    }


def _legacy_consumption_identity(row: dict, envelope: dict) -> dict:
    """Retain historical ledger assertions without applying v3 schemas or hash domains."""
    return {
        **{
            name: row[name]
            for name in (
                "reservation_id",
                "attestation_id",
                "lease_digest",
                "admission_purpose",
                "tablespace_identity_hash",
                "volume_identity_hash",
            )
        },
        "envelope": envelope,
        "database_binding": {name: row[name] for name in _DATABASE_FIELDS},
        # A legacy nonce is not evidence of a current signing-preflight receipt.
        "preflight_receipt_sha256": None,
        "signature_verification": "unresolved_legacy_contract",
        "opaque_legacy": True,
    }


def _add_reservation(reservations: dict, raw: Any, *, consumption: dict | None = None) -> dict:
    candidate = (
        _legacy_consumption_identity(consumption, raw)
        if consumption is not None and consumption["contract_id"] in _LEGACY_CONTRACTS
        else _envelope_identity(raw)
    )
    identity = candidate["reservation_id"]
    prior = reservations.setdefault(identity, {**candidate, "observations": []})
    if any(prior[name] != value for name, value in candidate.items()):
        _fail("reservation_identity_conflict")
    for other_id, other in reservations.items():
        if other_id != identity and any(
            candidate[field] is not None and other[field] == candidate[field]
            for field in ("attestation_id", "lease_digest", "preflight_receipt_sha256")
        ):
            _fail("reservation_identity_conflict")
    if not candidate["opaque_legacy"] and candidate["admission_purpose"] == "cms_nonprofile":
        pair = candidate["envelope"]["lease"]["signing_preflight_guard"]["healthcare_request"][CMS_ADMISSION_FIELD][
            "paired_profile_lease"
        ]
        paired = _add_reservation(reservations, pair)
        observation_by_field = {"kind": "paired_profile", "cms_reservation_id": identity}
        if observation_by_field not in paired["observations"]:
            paired["observations"].append(observation_by_field)
    return prior


def _assert_current_consumption(consumption_by_field: dict, reservation: dict) -> None:
    body = reservation["envelope"]["lease"]
    expected_by_field = {
        **{
            name: reservation[name]
            for name in (
                "reservation_id",
                "attestation_id",
                "lease_digest",
                "admission_purpose",
                "tablespace_identity_hash",
                "volume_identity_hash",
            )
        },
        **{
            name: body[name]
            for name in (
                *_DATABASE_FIELDS,
                "contract_id",
                "capacity_geometry_hash",
                "key_id",
                "environment_id",
                "attestor_id",
                "attestor_release_digest",
            )
        },
        "canonical_lease_json": lease_contract.canonical_capacity_lease_json(body),
        "signature": reservation["envelope"]["signature"],
    }
    if any(consumption_by_field.get(name) != expected_value for name, expected_value in expected_by_field.items()):
        _fail("consumption_identity_changed")
    accepted_at = _timestamp(consumption_by_field["accepted_at"])
    if accepted_at != _timestamp(consumption_by_field["recorded_at"]) or not (
        _timestamp(consumption_by_field["issued_at"])
        <= accepted_at
        < min(_timestamp(consumption_by_field["expires_at"]), _timestamp(consumption_by_field["max_build_deadline"]))
    ):
        _fail("consumption_time_changed")
    for name in ("observed_at", "issued_at", "expires_at", "max_build_deadline"):
        if _timestamp(consumption_by_field[name]) != lease_contract._timestamp(body[name], field=name):
            _fail("consumption_time_changed")


def _consumption_observation(row: dict, reservation: dict, observed_at: datetime.datetime) -> dict:
    if not reservation["opaque_legacy"]:
        _assert_current_consumption(row, reservation)
    observation_by_field = {
        "kind": "consumption",
        "run_id": row["run_id"],
        "record": _json_values(row),
        "admission_unexpired": _timestamp(row["expires_at"]) > observed_at,
        "build_deadline_passed": _timestamp(row["max_build_deadline"]) <= observed_at,
        "release_status": "unproven",
    }
    if reservation["opaque_legacy"]:
        observation_by_field["recorded_expiry_in_future"] = observation_by_field["admission_unexpired"]
        observation_by_field["admission_unexpired"] = None
    return observation_by_field


def _run_reservation(run_by_field: dict, params: dict, field: str, reservations: dict) -> dict:
    purpose = "profile" if field == PROFILE_CAPACITY_PARAM else "cms_nonprofile"
    legacy_reservations = [
        reservation_by_field
        for reservation_by_field in reservations.values()
        if reservation_by_field["opaque_legacy"]
        and reservation_by_field["admission_purpose"] == purpose
        and any(
            observation["kind"] == "consumption" and observation["run_id"] == run_by_field["run_id"]
            for observation in reservation_by_field["observations"]
        )
    ]
    if legacy_reservations:
        if len(legacy_reservations) != 1 or legacy_reservations[0]["envelope"] != params[field]:
            _fail("legacy_run_envelope_changed")
        return legacy_reservations[0]
    reservation = _add_reservation(reservations, params[field])
    if reservation["admission_purpose"] != purpose:
        _fail("run_purpose_changed")
    request = reservation["envelope"]["lease"]["signing_preflight_guard"]["healthcare_request"]
    execution = request["profile_execution"]
    if any(params.get(name) != expected for name, expected in execution.items() if name != PROFILE_CAPACITY_PARAM):
        _fail("run_execution_changed")
    if run_by_field["node_id"] != execution["provider_directory_profile_selection_attestation"]["node_id"]:
        _fail("run_node_changed")
    if (
        field == CMS_CAPACITY_EXECUTION_PARAM
        and params.get(PROFILE_CAPACITY_PARAM) != request[CMS_ADMISSION_FIELD]["paired_profile_lease"]
    ):
        _fail("run_profile_pair_changed")
    return reservation


def _run_observations(rows: list[dict], reservations: dict) -> list[dict]:
    owners = []
    for row in rows:
        params = _json_object(row.get("params") or {})
        owner = _json_values({name: value for name, value in row.items() if name != "params"})
        owner["active"] = row["status"] in ACTIVE_RUN_STATUSES
        owner["reservation_ids"] = []
        for field in (PROFILE_CAPACITY_PARAM, CMS_CAPACITY_EXECUTION_PARAM):
            if field not in params:
                continue
            reservation = _run_reservation(row, params, field, reservations)
            owner["reservation_ids"].append(reservation["reservation_id"])
            observation_by_field = {
                "kind": "import_run",
                "run_id": row["run_id"],
                "status": row["status"],
                "active": owner["active"],
            }
            if reservation["opaque_legacy"]:
                observation_by_field["execution_verification"] = "unresolved_legacy_contract"
            if observation_by_field not in reservation["observations"]:
                reservation["observations"].append(observation_by_field)
        owner["reservation_ids"].sort()
        owner["capacity_envelope_missing"] = owner["active"] and not owner["reservation_ids"]
        owners.append(owner)
    return owners


def _preflight_observations(
    preflight_rows: list[dict], reservations: dict, observed_at: datetime.datetime
) -> list[dict]:
    by_receipt = {
        candidate_reservation["preflight_receipt_sha256"]: candidate_reservation
        for candidate_reservation in reservations.values()
        if not candidate_reservation["opaque_legacy"]
    }
    receipts = []
    for preflight_by_field in preflight_rows:
        receipt = _json_object(preflight_by_field["receipt_json"])
        contract = receipt.get("contract_id")
        if contract not in {CAPACITY_PREFLIGHT_CONTRACT_ID, CMS_PREFLIGHT_CONTRACT}:
            _fail("preflight_contract_unsupported")
        digest = preflight_domain_sha256(
            contract, {name: receipt_value for name, receipt_value in receipt.items() if name != "receipt_sha256"}
        )
        if digest != preflight_by_field["receipt_sha256"] or receipt.get("receipt_sha256") != digest:
            _fail("preflight_digest_changed")
        for field in (
            "contract_id",
            "request_contract_id",
            "request_sha256",
            "request_nonce",
            "control_plane_receipt_sha256",
            "capacity_geometry_hash",
        ):
            if preflight_by_field.get(field) != receipt.get(field):
                _fail("preflight_metadata_changed")
        for field in ("issued_at", "expires_at"):
            if _timestamp(preflight_by_field[field]) != lease_contract._timestamp(receipt[field], field=field):
                _fail("preflight_metadata_changed")
        reservation = by_receipt.get(digest)
        if (
            reservation is not None
            and receipt != reservation["envelope"]["lease"]["signing_preflight_guard"]["healthcare_receipt"]
        ):
            _fail("preflight_envelope_changed")
        receipts.append(
            {
                "record": _json_values({**preflight_by_field, "receipt_json": receipt}),
                "pending": preflight_by_field.get("consumed_at") is None
                and _timestamp(preflight_by_field["expires_at"]) > observed_at,
                "reservation_id": reservation["reservation_id"] if reservation else None,
                "original_envelope_missing": reservation is None,
            }
        )
    return receipts


def reservation_projection(
    metadata: dict, consumptions: list[dict], runs: list[dict], preflights: list[dict], stages: list[dict]
) -> dict:
    """Deduplicate original leases; observed lifecycle states never release bytes."""
    observed_at = _timestamp(metadata["observed_at"])
    reservations_by_id: dict[str, dict] = {}
    for record_by_field in consumptions:
        envelope_by_field = {
            "lease": _json_object(record_by_field["canonical_lease_json"]),
            "signature": record_by_field["signature"],
        }
        reservation = _add_reservation(reservations_by_id, envelope_by_field, consumption=record_by_field)
        observation = _consumption_observation(record_by_field, reservation, observed_at)
        if any(
            recorded_observation["kind"] == "consumption" and recorded_observation != observation
            for recorded_observation in reservation["observations"]
        ):
            _fail("consumption_owner_conflict")
        if observation not in reservation["observations"]:
            reservation["observations"].append(observation)
    owners = _run_observations(runs, reservations_by_id)
    receipts = _preflight_observations(preflights, reservations_by_id, observed_at)
    receipt_ids = {record_by_field["receipt_sha256"] for record_by_field in preflights}
    for reservation in reservations_by_id.values():
        reservation["durable_preflight_present"] = reservation["preflight_receipt_sha256"] in receipt_ids
        active_runs = {
            recorded_observation["run_id"]
            for recorded_observation in reservation["observations"]
            if recorded_observation["kind"] == "import_run" and recorded_observation["active"]
        }
        consumed_runs = {
            recorded_observation["run_id"]
            for recorded_observation in reservation["observations"]
            if recorded_observation["kind"] == "consumption"
        }
        if len(active_runs) > 1 or (active_runs and consumed_runs and active_runs != consumed_runs):
            _fail("reservation_owner_conflict")
    return {
        "contract_id": CONTRACT_ID,
        "scope": "healthcare_consumptions_preflights_and_owners",
        "capacity_complete": False,
        "control_authority_required": True,
        "signature_verification_required": True,
        "release_proof_available": False,
        "observed_at": observed_at.isoformat(),
        "database_snapshot": metadata["database_snapshot"],
        "database_binding": {name: metadata[name] for name in _DATABASE_FIELDS},
        "reservations": [reservations_by_id[identity] for identity in sorted(reservations_by_id)],
        "owners": owners,
        "preflight_receipts": receipts,
        "profile_stage_observations": _json_values(stages),
        "limitations": [
            "original_signed_bytes_are_not_remaining_reservations",
            "pending_preflight_rows_do_not_store_original_requests_or_envelopes",
            "expiry_and_terminal_run_status_do_not_prove_physical_release",
            "profile_stage_catalog_presence_is_not_a_complete_cleanup_receipt",
            "cms_scratch_ownership_is_not_durable_in_the_capacity_ledger",
            "legacy_consumptions_are_opaque_unverified_ledger_observations",
        ],
    }


async def _snapshot_run_rows(session: Any, run_table: str, consumed: str) -> list[dict]:
    """Read the unchanged owner eligibility through the caller's pinned transaction."""
    return [
        dict(run_by_field)
        for run_by_field in (
            await session.execute(
                text(f"""
            SELECT run_id,node_id,importer,status,created_at,started_at,finished_at,heartbeat_at,params
              FROM {run_table} run
             WHERE EXISTS (SELECT 1 FROM {consumed} consumed WHERE consumed.run_id=run.run_id)
                OR (importer='provider-directory-fhir' AND status=ANY(CAST(:statuses AS text[])) AND (
                    params::jsonb ? :profile_param OR params::jsonb ? :cms_param
                    OR params::jsonb ? 'provider_directory_profile_contract_id'))
             ORDER BY run_id
        """),
                {
                    "statuses": list(ACTIVE_RUN_STATUSES),
                    "profile_param": PROFILE_CAPACITY_PARAM,
                    "cms_param": CMS_CAPACITY_EXECUTION_PARAM,
                },
            )
        ).mappings()
    ]


async def _snapshot_stage_rows(session: Any, checkpoint: str, schema: str) -> list[dict]:
    """Observe the same named and physical stages without acquiring another snapshot."""
    return [
        dict(stage_by_field)
        for stage_by_field in (
            await session.execute(
                text(f"""
            SELECT checkpoint.build_id, checkpoint.owner_run_id, checkpoint.state,
                   stage.kind, stage.name AS stage_name, stage.oid AS expected_oid,
                   named.oid::bigint AS current_name_oid,
                   owned.relname AS expected_oid_current_name, namespace.nspname AS expected_oid_current_schema,
                   owned.relkind::text AS relation_kind, owned.relpersistence::text AS persistence
              FROM {checkpoint} checkpoint
             CROSS JOIN LATERAL (VALUES
                 ('evidence', checkpoint.evidence_stage, checkpoint.evidence_stage_oid),
                 ('profile', checkpoint.profile_stage, checkpoint.profile_stage_oid),
                 ('affected_npi', checkpoint.affected_npi_stage, checkpoint.affected_npi_stage_oid)
             ) stage(kind,name,oid)
              LEFT JOIN pg_class owned ON owned.oid=stage.oid
              LEFT JOIN pg_namespace namespace ON namespace.oid=owned.relnamespace
              LEFT JOIN pg_namespace stage_namespace ON stage_namespace.nspname=:schema
              LEFT JOIN pg_class named ON named.relnamespace=stage_namespace.oid AND named.relname=stage.name
             WHERE stage.name IS NOT NULL
             ORDER BY checkpoint.build_id,stage.kind
        """),
                {"schema": schema},
            )
        ).mappings()
    ]


async def capacity_reservation_snapshot(fhir: Any) -> dict:
    """Own one fresh read-only MVCC snapshot; do not inherit a caller's transaction."""
    if fhir.db._transaction_binding() is not None:
        _fail("fresh_transaction_required")
    schema = fhir._schema()
    consumed = fhir._unscoped_qt(schema, "provider_directory_profile_capacity_lease_consumption")
    run_table = fhir._unscoped_qt(schema, "import_run")
    preflight_table = fhir._profile_capacity_preflight_receipt_ref(schema)
    checkpoint = fhir._provider_directory_profile_checkpoint_ref(schema)
    async with fhir.db.transaction() as session:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        for setting in (
            "lock_timeout='5s'",
            "statement_timeout='30s'",
            "temp_file_limit='64MB'",
            "max_parallel_workers_per_gather=0",
        ):
            await session.execute(text("SET LOCAL " + setting))
        metadata = dict(
            (
                await session.execute(
                    text("""
            SELECT transaction_timestamp() AS observed_at, pg_current_snapshot()::text AS database_snapshot,
                   (SELECT system_identifier::text FROM pg_control_system()) AS database_system_identifier,
                   oid::bigint AS database_oid, datname AS database_name
              FROM pg_database WHERE datname=current_database()
        """)
                )
            )
            .mappings()
            .one()
        )
        consumptions = [
            dict(consumption_by_field)
            for consumption_by_field in (
                await session.execute(text(f"SELECT * FROM {consumed} ORDER BY reservation_id"))
            ).mappings()
        ]
        runs = await _snapshot_run_rows(session, run_table, consumed)
        preflights = [
            dict(preflight_by_field)
            for preflight_by_field in (
                await session.execute(text(f"SELECT * FROM {preflight_table} ORDER BY receipt_sha256"))
            ).mappings()
        ]
        stages = await _snapshot_stage_rows(session, checkpoint, schema)
        return reservation_projection(metadata, consumptions, runs, preflights, stages)
