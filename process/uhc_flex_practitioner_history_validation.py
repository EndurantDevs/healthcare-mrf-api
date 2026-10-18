# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Adoption-only set validation of authenticated practitioner histories."""

import asyncio
import json
from dataclasses import fields
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

from db.plan_release_storage import storage_identifier, storage_relation
from process.fhir_request_failure_policy import FHIR_REQUEST_FAILURE_BUDGET_MULTIPLIER
from process.provider_directory_cms_callable_custody import _admin_identity
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.provider_directory_profile import valid_npi_sql
from process.uhc_canonical_proof import (
    UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY,
    UhcCanonicalProofError,
    validate_uhc_canonical_content_proof,
)
from process.uhc_flex_official_cohort_contract import UHC_FLEX_OFFICIAL_NPI_SYSTEM, UHCFlexOfficialNPICohort
from process.uhc_flex_practitioner_acquisition_contract import (
    UHC_FLEX_PRACTITIONER_ACQUISITION_MAX_ATTEMPTS,
    UHC_FLEX_PRACTITIONER_RETRY_EXHAUSTED_ERROR_CODE,
)
from process.uhc_flex_practitioner_contract import (
    UHC_FLEX_PRACTITIONER_QUERY_CONTRACT_ID,
    UHC_FLEX_PRACTITIONER_QUERY_COUNT,
)
from process.uhc_flex_practitioner_query import UHC_FLEX_PRACTITIONER_MAX_RESOURCE_JSON_BYTES
from process.uhc_flex_practitioner_store_contract import UHC_FLEX_PRACTITIONER_TERMINAL_RECORD_CONTRACT_ID

_PREFIX = "provider_directory_uhc_flex_"
_HEADER = _PREFIX + "practitioner_acquisition"
_WORK = _PREFIX + "practitioner_work"
_RESOURCE = _PREFIX + "practitioner_resource"
_FIXED = (
    _HEADER,
    _WORK,
    _RESOURCE,
    _WORK + "_legacy",
    _RESOURCE + "_legacy",
    _PREFIX + "npi_cohort",
    _PREFIX + "npi_member",
)
_TERMINAL_SET_CONTRACT = "healthporta.provider-directory.uhc-flex-practitioner-terminal-set.v1"
_SOURCE_HEADER = "provider_directory_endpoint_dataset"
_SOURCE_RESOURCE = "provider_directory_dataset_resource"
_SQL_PATH = Path(__file__).resolve().parents[1] / "db/sql/practitioner_history_validation.sql"
_CATALOG_SQL = """SELECT c.relname AS name,c.oid::bigint AS oid,c.relowner::bigint AS owner,
 i.inhparent::bigint AS parent_oid,EXISTS(SELECT FROM pg_catalog.pg_locks l
 WHERE l.pid=pg_catalog.pg_backend_pid() AND l.relation=c.oid AND l.granted
 AND l.mode IN ('ShareLock','ShareRowExclusiveLock','ExclusiveLock','AccessExclusiveLock')) AS locked,
 EXISTS(SELECT FROM pg_catalog.pg_locks l WHERE l.pid=pg_catalog.pg_backend_pid()
 AND l.relation=c.oid AND l.granted AND l.mode IN ('AccessShareLock','RowShareLock',
 'RowExclusiveLock','ShareUpdateExclusiveLock','ShareLock','ShareRowExclusiveLock',
 'ExclusiveLock','AccessExclusiveLock')) AS read_locked
 FROM pg_catalog.pg_class c
 LEFT JOIN pg_catalog.pg_inherits i ON i.inhrelid=c.oid
 WHERE c.relnamespace=pg_catalog.to_regnamespace($1::text) AND c.relname=ANY($2::text[]) ORDER BY c.oid"""
_COUNTS = ("acquisitions", "cohorts", "work_rows", "resource_rows", "selected_resource_rows")
_INVALID = (
    "invalid_work",
    "invalid_resources",
    "invalid_results",
    "invalid_headers",
    "invalid_resource_sets",
    "invalid_cohorts",
    "invalid_source",
    "invalid_members",
)
_PAYLOAD_ROWS = UHC_FLEX_PRACTITIONER_QUERY_COUNT
_PAYLOAD_BYTES = UHC_FLEX_PRACTITIONER_MAX_RESOURCE_JSON_BYTES


def _require(condition, code):
    if not condition:
        raise ValueError("practitioner_history_" + code)


def _cohort_record(cohort_fields, dataset_fields):
    """Authenticate the existing low-volume identity/proof, including old publications."""
    cohort = UHCFlexOfficialNPICohort(
        **{field.name: cohort_fields.get(field.name) for field in fields(UHCFlexOfficialNPICohort)}
    )
    _require(
        dataset_fields.get("dataset_id") == cohort.official_dataset_id
        and dataset_fields.get("endpoint_id") == cohort.official_endpoint_id
        and dataset_fields.get("acquisition_root_run_id") == cohort.official_acquisition_root_run_id
        and dataset_fields.get("dataset_hash") == cohort.official_dataset_hash
        and dataset_fields.get("status") in {"published", "superseded"},
        "cohort_dataset_changed",
    )
    metadata = dataset_fields.get("publication_metadata_json")
    if type(metadata) is str:
        metadata = json.loads(metadata)
    _require(type(metadata) is dict, "cohort_proof_missing")
    try:
        proof = validate_uhc_canonical_content_proof(
            metadata.get(UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY),
            dataset_id=cohort.official_dataset_id,
            endpoint_id=cohort.official_endpoint_id,
            acquisition_root_run_id=cohort.official_acquisition_root_run_id,
        )
    except (TypeError, ValueError, UhcCanonicalProofError) as error:
        raise ValueError("practitioner_history_cohort_proof_invalid") from error
    _require(
        proof["source_id"] == cohort.official_source_id
        and proof["dataset_hash"] == cohort.official_dataset_hash
        and proof["resource_count"] == dataset_fields.get("resource_count")
        and proof["resource_counts"].get("Practitioner") == cohort.practitioner_resource_count
        and proof["proof_sha256"] == cohort.official_content_proof_sha256,
        "cohort_proof_changed",
    )
    return cohort


def _scope(policy, family):
    """Select fixed roots and only previously captured deterministic candidates."""
    _require(type(policy) is GenericSourcePolicy and family["schema"] == policy.schema, "policy_changed")
    roots = family["roots"]
    _require(set(_FIXED) <= roots.keys(), "roots_missing")
    selected_by_name = {name: dict(roots[name]) for name in _FIXED}
    detached_by_root = {_WORK: [], _RESOURCE: []}
    loading, initialized = [], []
    for member in family["members"]:
        root, acquisition = member["root"], member["acquisition_id"]
        if root not in detached_by_root:
            continue
        prefix = "pd_uhc_flex_pw_" if root == _WORK else "pd_uhc_flex_pr_"
        _require(member["name"] == prefix + sha256(acquisition.encode("utf-8")).hexdigest()[:40], "member_changed")
        _require(acquisition in family["acquisitions"][root], "acquisition_changed")
        _require(member["name"] not in selected_by_name, "member_reused")
        selected_by_name[member["name"]] = dict(member)
        if member["parent_oid"] is None:
            _require(member["phase"] in {"loading", "indexed"}, "phase_changed")
            _require(family["acquisitions"][root][acquisition] == "building", "phase_changed")
            detached_by_root[root].append(member)
        else:
            _require(member["parent_oid"] == roots[root]["oid"], "parent_changed")
        if root == _WORK:
            (loading if member["phase"] == "loading" else initialized).append(acquisition)
    _require(len({entry["oid"] for entry in selected_by_name.values()}) == len(selected_by_name), "oid_reused")
    return selected_by_name, detached_by_root, sorted(loading), sorted(initialized)


def _source_scope(family):
    """Consume the coordinator's existing complete SOURCE custody receipt."""
    _require(type(family.get("source_inputs")) is dict, "source_inputs_required")
    inputs = family["source_inputs"]
    header = inputs["headers"][_SOURCE_HEADER]
    _require(header == inputs["header"], "source_header_alias_changed")
    _require(
        header["name"] == _SOURCE_HEADER and bool(header["columns"]) and bool(header["catalog"]),
        "source_header_changed",
    )
    payload_by_name = {entry["relname"]: entry for entry in inputs["family"]}
    _require(_SOURCE_RESOURCE in payload_by_name, "source_root_missing")
    parent = payload_by_name[_SOURCE_RESOURCE]
    _require(parent["relkind"] == "p" and parent["parents"] == [], "source_root_changed")
    selected_by_name = {_SOURCE_HEADER: dict(oid=header["oid"], parent_oid=None, source_input=True)}
    for entry in payload_by_name.values():
        if entry["relname"] == _SOURCE_RESOURCE or parent["oid"] in entry["parents"]:
            _require(entry["parents"] in ([], [parent["oid"]]), "source_parent_changed")
            _require(
                entry["direct_acl_closed"] is True
                and entry["readers_closed"] is True
                and entry["native_checks"] is True,
                "source_custody_changed",
            )
            selected_by_name[entry["relname"]] = dict(
                oid=entry["oid"],
                parent_oid=parent["oid"] if entry["parents"] else None,
                source_input=True,
            )
    return selected_by_name


def _relation_rows(schema, root, detached_by_root):
    parts = [f"SELECT r.*,NULL::text AS declared_acquisition FROM {storage_relation(schema, root)} r"]
    for member in detached_by_root:
        # Acquisition identity comes from the protected captured header, never a
        # SQL identifier or a caller-provided raw relation mapping.
        acquisition = member["acquisition_id"]
        _require(
            len(acquisition) == 55
            and acquisition.startswith("pdufpa_")
            and all(character in "0123456789abcdef" for character in acquisition[7:]),
            "acquisition_changed",
        )
        parts.append(
            f"SELECT r.*,'{acquisition}'::text AS declared_acquisition FROM {storage_relation(schema, member['name'])} r"
        )
    return " UNION ALL ".join(parts)


def _statement(policy, detached_by_root):
    return (
        _SQL_PATH.read_text()
        .replace('"__SCHEMA__"', storage_identifier(policy.schema))
        .replace("__WORK_ROWS__", _relation_rows(policy.schema, _WORK, detached_by_root[_WORK]))
        .replace("__RESOURCE_ROWS__", _relation_rows(policy.schema, _RESOURCE, detached_by_root[_RESOURCE]))
        .replace("__SOURCE_NPI_VALID__", valid_npi_sql("r.normalized_npi").replace("MOD(", "pg_catalog.mod("))
        .replace("__MEMBER_NPI_VALID__", valid_npi_sql("m.npi").replace("MOD(", "pg_catalog.mod("))
    )


async def _native_scope(connection, policy, selected_by_name):
    rows = [dict(row) for row in await connection.fetch(_CATALOG_SQL, policy.schema, sorted(selected_by_name))]
    owner = await connection.fetchval(
        "SELECT oid::bigint FROM pg_catalog.pg_roles WHERE rolname=$1::text", policy.owner_role
    )
    _require(len(rows) == len(selected_by_name), "relation_missing")
    for row in rows:
        expected = selected_by_name.get(row["name"])
        _require(
            expected is not None
            and row["oid"] == expected["oid"]
            and row["owner"] == owner
            and row["read_locked" if expected.get("source_input") else "locked"] is True
            and row["parent_oid"] == expected.get("parent_oid"),
            "relation_changed",
        )
    return rows


async def _cohort_metadata(connection, policy, expected_count):
    records = await connection.fetch(
        "SELECT pg_catalog.to_jsonb(h) AS cohort,pg_catalog.to_jsonb(d) AS dataset FROM "
        + storage_relation(policy.schema, _PREFIX + "npi_cohort")
        + " h LEFT JOIN "
        + storage_relation(policy.schema, _SOURCE_HEADER)
        + " d ON d.dataset_id=h.official_dataset_id"
    )
    _require(len(records) == expected_count, "cohort_count_changed")
    for record in records:
        cohort = json.loads(record["cohort"]) if type(record["cohort"]) is str else record["cohort"]
        dataset = json.loads(record["dataset"]) if type(record["dataset"]) is str else record["dataset"]
        _require(type(cohort) is dict and type(dataset) is dict, "cohort_dataset_missing")
        _cohort_record(cohort, dataset)


def _canonical_batch(rows):
    """Use the unchanged CPython codec once per bounded array, never per row."""
    _require(
        len(rows) <= _PAYLOAD_ROWS
        and all(
            type(row["payload_json_text"]) is str and type(row["bytes"]) is int and 2 <= row["bytes"] <= _PAYLOAD_BYTES
            for row in rows
        ),
        "payload_size_invalid",
    )
    original = "[" + ",".join(row["payload_json_text"] for row in rows) + "]"
    try:
        canonical = json.dumps(
            json.loads(original), allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        )
    except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError) as error:
        raise ValueError("practitioner_history_payload_invalid") from error
    _require(canonical == original, "payload_not_canonical")


async def _canonical_payloads(connection, policy, detached_by_root, expected_count):
    """Read the locked immutable payloads with one original-session native cursor."""
    name = storage_identifier("pd_history_" + uuid4().hex)
    source = _relation_rows(policy.schema, _RESOURCE, detached_by_root[_RESOURCE])
    await connection.execute(
        f"DECLARE {name} NO SCROLL CURSOR FOR SELECT payload_json_text,"
        f"pg_catalog.octet_length(payload_json_text)::bigint AS bytes FROM ({source}) r "
        'ORDER BY acquisition_id COLLATE pg_catalog."C",cohort_id COLLATE pg_catalog."C",'
        'npi,attempt,resource_id COLLATE pg_catalog."C"'
    )
    count, has_failed = 0, False
    try:
        while rows := await connection.fetch(f"FETCH FORWARD {_PAYLOAD_ROWS} FROM {name}"):
            _canonical_batch(rows)
            count += len(rows)
        _require(count == expected_count, "payload_count_changed")
    except BaseException:
        has_failed = True
        raise
    finally:
        try:
            await connection.execute(f"CLOSE {name}")
        except Exception:
            # A canceled native statement may abort the original transaction.
            # Its owning admin savepoint must roll back; never replace its error
            # or create a second transaction merely to close that dead portal.
            if not has_failed:
                raise


async def validate_practitioner_history(connection, policy, family):
    """Validate once inside the original custody cut, without changing rows or roles.

    The coordinator authenticates the full family layout, CHECKs, callable
    closure and write-blocking locks before this call. This routine accepts
    only that captured family; its receipt is an aggregate, not a capability.
    """
    _require(connection.is_in_transaction() is True, "transaction_required")
    selected_by_name, detached_by_root, loading, initialized = _scope(policy, family)
    selected_by_name.update(_source_scope(family))
    task, identity = asyncio.current_task(), await _admin_identity(connection, policy)
    native = await _native_scope(connection, policy, selected_by_name)
    statement = _statement(policy, detached_by_root)
    census_by_field = dict(
        await connection.fetchrow(
            statement,
            loading,
            initialized,
            UHC_FLEX_PRACTITIONER_QUERY_CONTRACT_ID,
            UHC_FLEX_PRACTITIONER_TERMINAL_RECORD_CONTRACT_ID,
            _TERMINAL_SET_CONTRACT,
            UHC_FLEX_PRACTITIONER_RETRY_EXHAUSTED_ERROR_CODE,
            UHC_FLEX_PRACTITIONER_ACQUISITION_MAX_ATTEMPTS,
            FHIR_REQUEST_FAILURE_BUDGET_MULTIPLIER,
            UHC_FLEX_OFFICIAL_NPI_SYSTEM,
        )
    )
    _require(set(census_by_field) == set((*_COUNTS, *_INVALID)), "result_changed")
    _require(all(type(count) is int and count >= 0 for count in census_by_field.values()), "result_changed")
    _require(not any(census_by_field[key] for key in _INVALID), "set_invalid")
    await _cohort_metadata(connection, policy, census_by_field["cohorts"])
    await _canonical_payloads(connection, policy, detached_by_root, census_by_field["resource_rows"])
    _require(
        asyncio.current_task() is task
        and connection.is_in_transaction() is True
        and await _admin_identity(connection, policy) == identity,
        "transaction_changed",
    )
    _require(await _native_scope(connection, policy, selected_by_name) == native, "relation_changed")
    return dict(
        protocol=1,
        contract="practitioner-history-validation-v1",
        **census_by_field,
        loading_acquisitions=len(loading),
        relations=native,
        sql_sha256=sha256(statement.encode()).hexdigest(),
    )
