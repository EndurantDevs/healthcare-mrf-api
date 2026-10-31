# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Application-owned generic SOURCE heaps bound to one existing dataset header.

This module never obtains credentials or substitutes an ambient connection. The
trusted caller supplies the publisher capability and the original transaction.
Only low-volume header controls are locked; payload intake is append-only COPY.
"""

import asyncio
import hashlib
import json
import re
import time
from contextlib import asynccontextmanager, nullcontext
from contextvars import ContextVar
from dataclasses import dataclass
from importlib import import_module
from uuid import uuid4

from db.plan_release_storage import storage_identifier, storage_relation
from process.native_copy_cleanup import close_interrupted_copy, invalidate_copy_owner
from process.plan_release_storage_catalog import CATALOG_SQL
from process.plan_release_storage_provision import _GRANTEES_SQL
from process.provider_directory_generic_candidate import (
    RESOURCE_COLUMNS,
    WITNESS_COLUMNS,
    GenericCandidateError,
    copy_record_batches,
)
from process.provider_directory_generic_source_policy import source_copy_session, source_owner_transaction
from process.provider_directory_source_storage_binding import require_storage_schema, storage_document

HEADER = "provider_directory_endpoint_dataset"
RESOURCE = "provider_directory_dataset_resource"
WITNESS = "provider_directory_cms_npd_resource_witness"
RELATIONSHIP = "provider_directory_cms_npd_relationship"
RELATIONS = (
    RESOURCE,
    "provider_directory_dataset_insurance_plan",
    "provider_directory_dataset_network_plan",
    "provider_directory_dataset_affiliation_organization",
    WITNESS,
    RELATIONSHIP,
)
ISOLATED_STORAGE_ROOTS = (HEADER, *RELATIONS)
HANDOFF_CONTROL_ROOTS = (HEADER, "provider_directory_dataset_proof_shard")
CMS_IDENTITY_ROOTS = (
    "provider_directory_entity_source_binding",
    "provider_directory_entity_release_evidence",
    "provider_directory_insurance_network_source_binding",
    "provider_directory_insurance_network_plan_evidence",
)
CMS_RESOURCE_IDENTITY_ROOT = "provider_directory_resource_identity"
AUXILIARY_ROOTS = (*CMS_IDENTITY_ROOTS, CMS_RESOURCE_IDENTITY_ROOT)
CONTRACT = "generic-source-candidate-v2"
ISOLATED_CONTRACT = "generic-source-detached-v1"
CMS_ISOLATED_CONTRACT = "generic-source-cms-detached-v1"
ISOLATED_CONTRACTS = (ISOLATED_CONTRACT, CMS_ISOLATED_CONTRACT)
# Autovacuum on a private heap competes with the bulk load and reduce for I/O; statistics come from
# explicit ANALYZE and attached heaps get the default back right before they join their root.
PRIVATE_HEAP_OPTIONS = " WITH (autovacuum_enabled = false)"
_NAME = re.compile(r"pdgs_[0-9a-f]{32}_(?:ri|wi|r|ip|np|ao|w|rl|eb|ee|nb|ne|re|h|p)")
_SUFFIXES = dict(zip((*RELATIONS, *AUXILIARY_ROOTS), ("r", "ip", "np", "ao", "w", "rl", "eb", "ee", "nb", "ne", "re")))
_SUFFIXES.update(zip(HANDOFF_CONTROL_ROOTS, ("h", "p")))
_BINDING_KEYS = {"contract", "identity", "phase", "ordinal", "resets", "tables", "auxiliary", "relationship_sha256"}
HANDOFF_HEADER_FIELDS = (
    "dataset_hash",
    "resource_count",
    "previous_dataset_id",
    "completion_proof_required_version",
    "completion_proof_sha256",
    "content_proof_admission_sha256",
)
_INPUT_SEQUENCES_SQL = """SELECT t.relname AS input_name,a.attname,a.attidentity::text,
 d.deptype::text,s.oid::bigint,s.relname,n.nspname,s.relowner::bigint,
 s.relkind::text,s.relpersistence::text,q.seqtypid::regtype::text,
 q.seqstart,q.seqincrement,q.seqmax,q.seqmin,q.seqcache,q.seqcycle,
 NOT EXISTS(SELECT FROM aclexplode(coalesce(s.relacl,acldefault('S',s.relowner))) grant_entry
  WHERE grant_entry.grantee<>s.relowner) AS acl_closed
 FROM pg_class t JOIN pg_depend d ON d.refclassid='pg_class'::regclass AND d.refobjid=t.oid
  AND d.classid='pg_class'::regclass AND d.deptype IN ('a','i')
 JOIN pg_class s ON s.oid=d.objid AND s.relkind='S'
 JOIN pg_namespace n ON n.oid=s.relnamespace JOIN pg_sequence q ON q.seqrelid=s.oid
 JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=d.refobjsubid
 WHERE t.oid=ANY(ARRAY(SELECT to_regclass(name)::oid FROM unnest($1::text[]) names(name)))"""


@dataclass(frozen=True)
class _StoredCandidate:
    """A native protected header binding, never an ambient caller's role claim."""

    identity: dict
    resource_hash_contract: str
    semantic_projection_as_of: str | None

    @property
    def dataset_id(self):
        """Return the exact dataset in the authenticated protected binding."""
        return self.identity["dataset_id"]

    @property
    def selected_resources(self):
        """Return the immutable declared resource-family scope."""
        return tuple(self.identity["metadata"]["selected_resources"])

    @property
    def source_release(self):
        """Return the immutable source vector without discovering another release."""
        return self.identity["metadata"].get("source_release")


def _require(condition, code):
    if not condition:
        raise GenericCandidateError("provider_directory_generic_source_" + code)


def _json(value):
    return json.loads(value) if isinstance(value, str) else value


def _encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def candidate_identity(directory, candidate):
    """Bind all existing immutable acquisition/hash/source/producer declarations."""
    if type(candidate) is _StoredCandidate:
        return candidate.identity
    _require(type(candidate.dataset_id) is str and 1 <= len(candidate.dataset_id) <= 96, "dataset_invalid")
    return dict(
        dataset_id=candidate.dataset_id,
        endpoint_id=candidate.endpoint_id,
        acquisition_root_run_id=candidate.acquisition_root_run_id,
        metadata=directory._endpoint_dataset_candidate_metadata(candidate),
    )


def _validate_binding(binding_by_field, identity, *, phases):
    """Authenticate the complete closed candidate document before native lookup."""
    _validate_binding_shape(binding_by_field, identity, phases=phases)
    _validate_binding_storage(binding_by_field, is_isolated=binding_by_field["contract"] in ISOLATED_CONTRACTS)
    _validate_binding_tables(binding_by_field, identity)


def _validate_binding_shape(binding_by_field, identity, *, phases):
    """Share the document checks across loading and protected snapshot readers."""
    is_isolated = type(binding_by_field) is dict and binding_by_field.get("contract") in ISOLATED_CONTRACTS
    keys = _BINDING_KEYS | {"storage", "admission"} if is_isolated else _BINDING_KEYS
    _require(
        type(binding_by_field) is dict
        and set(binding_by_field) == keys
        and (
            binding_by_field["contract"] in ISOLATED_CONTRACTS
            if is_isolated
            else binding_by_field["contract"] == CONTRACT
        )
        and type(binding_by_field["identity"]) is dict
        and set(binding_by_field["identity"]) == {"dataset_id", "endpoint_id", "acquisition_root_run_id", "metadata"}
        and type(binding_by_field["identity"]["metadata"]) is dict
        and binding_by_field["identity"] == identity
        and binding_by_field["phase"] in phases
        and (
            binding_by_field["relationship_sha256"] is None
            or type(binding_by_field["relationship_sha256"]) is str
            and re.fullmatch(r"[0-9a-f]{64}", binding_by_field["relationship_sha256"])
        )
        and type(binding_by_field["ordinal"]) is int
        and 0 <= binding_by_field["ordinal"] < 2**63 - 1
        and type(binding_by_field["resets"]) is dict
        and all(
            type(key) is str
            and 1 <= len(key) <= 64
            and type(cutoff) is int
            and 0 <= cutoff <= binding_by_field["ordinal"]
            for key, cutoff in binding_by_field["resets"].items()
        ),
        "binding_changed",
    )


def _validate_binding_storage(binding_by_field, *, is_isolated):
    """Keep detached evidence inseparable from the originally issued namespace."""
    if is_isolated:
        from process.provider_directory_generic_source_policy import bound_publisher

        _validate_isolated_binding_storage(binding_by_field, storage_document(bound_publisher()))
    else:
        _require(binding_by_field["phase"] != "ready_for_freeze", "binding_changed")


def _validate_isolated_binding_storage(binding_by_field, expected_storage):
    _require(binding_by_field["storage"] == expected_storage, "storage_binding_changed")
    _require(
        binding_by_field["phase"] != "published" and binding_by_field["auxiliary"] is None,
        "isolated_kind_unsupported",
    )
    if binding_by_field["contract"] == CMS_ISOLATED_CONTRACT:
        from process.provider_directory_cms_source_handoff import require_cms_handoff_identity

        require_cms_handoff_identity(binding_by_field["identity"], expected_storage["catalog_schema"])
        _require(binding_by_field["relationship_sha256"] is None, "isolated_cms_relationship_unexpected")
    else:
        metadata = binding_by_field["identity"]["metadata"]
        _require(
            metadata.get("source_release") is None
            and type(metadata.get("source_ids")) is list
            and "cms-npd" not in metadata["source_ids"],
            "isolated_kind_unsupported",
        )
    if binding_by_field["phase"] == "ready_for_freeze":
        validate_handoff_header(binding_by_field["admission"])
    else:
        _require(binding_by_field["admission"] is None, "admission_changed")


def validate_handoff_document(document, expected_storage):
    """Check data against issued coordinates; this grants no read or publication rights."""
    _require(type(document) is dict and document.get("contract") in ISOLATED_CONTRACTS, "isolated_storage_required")
    _validate_binding_shape(document, document.get("identity"), phases=("ready_for_freeze",))
    _validate_isolated_binding_storage(document, expected_storage)
    _validate_binding_tables(document, document["identity"])


def _validate_binding_tables(binding_by_field, identity):
    """Require the complete phase-specific native inventory, without aliases."""
    _require(type(binding_by_field["tables"]) is dict, "tables_changed")
    expected = (
        set()
        if binding_by_field["phase"] in {"published", "disposed", "ready_for_freeze"}
        else {"resource_input", "witness_input"}
    )
    if binding_by_field["phase"] in {"prepared", "published", "ready_for_freeze"}:
        expected |= set(RELATIONS)
    if binding_by_field["phase"] == "ready_for_freeze":
        expected |= set(HANDOFF_CONTROL_ROOTS)
    _require(set(binding_by_field["tables"]) == expected, "tables_changed")
    tables_by_kind = {**binding_by_field["tables"], **_auxiliary_tables(binding_by_field, identity)}
    names = []
    for root, table in tables_by_kind.items():
        _require(
            type(table) is dict
            and set(table) == {"name", "oid", "owner", "layout_sha256"}
            and type(table["name"]) is str
            and _NAME.fullmatch(table["name"]) is not None
            and type(table["oid"]) is int
            and table["oid"] > 0
            and type(table["owner"]) is int
            and table["owner"] > 0
            and type(table["layout_sha256"]) is str
            and re.fullmatch(r"[0-9a-f]{64}", table["layout_sha256"]),
            "table_binding_changed",
        )
        if root in HANDOFF_CONTROL_ROOTS or table["name"].endswith(("_h", "_p")):
            _require(
                binding_by_field["contract"] in ISOLATED_CONTRACTS
                and binding_by_field["phase"] == "ready_for_freeze"
                and root in HANDOFF_CONTROL_ROOTS
                and table["name"].endswith("_" + _SUFFIXES[root]),
                "table_binding_changed",
            )
        names.append(table["name"])
    _require(len(set(names)) == len(names), "table_binding_changed")


def validate_handoff_header(seal):
    """Keep complete content/completion evidence for revalidation after writer closure."""
    _require(type(seal) is dict and set(seal) == set(HANDOFF_HEADER_FIELDS), "admission_changed")
    from process.provider_directory_fhir_subset_canonical import SERVER_ISSUED_SUBSET_REQUIRED_VERSION

    completion_version, completion_hash = seal["completion_proof_required_version"], seal["completion_proof_sha256"]
    completion_valid = (
        completion_version is None
        and completion_hash is None
        or (
            type(completion_version) is int
            and completion_version == SERVER_ISSUED_SUBSET_REQUIRED_VERSION
            and type(completion_hash) is str
            and re.fullmatch(r"[0-9a-f]{64}", completion_hash)
        )
    )
    _require(
        all(
            type(seal[key]) is str and re.fullmatch(r"[0-9a-f]{64}", seal[key])
            for key in ("dataset_hash", "content_proof_admission_sha256")
        )
        and type(seal["resource_count"]) is int
        and 0 <= seal["resource_count"] < 2**63
        and completion_valid
        and (
            seal["previous_dataset_id"] is None
            or type(seal["previous_dataset_id"]) is str
            and 1 <= len(seal["previous_dataset_id"]) <= 96
        ),
        "admission_changed",
    )


def validate_handoff_completion(header):
    """Apply the two legacy callable CHECKs without installing them on private heaps."""
    from process.provider_directory_fhir_subset_canonical import validate_subset_completion_proof_pair

    validate_handoff_header({field: header[field] for field in HANDOFF_HEADER_FIELDS})
    proof = _json(header["completion_proof_json"])
    if header["completion_proof_required_version"] is None:
        _require(proof is None, "completion_proof_unexpected")
        return
    try:
        proof, _ = validate_subset_completion_proof_pair(proof, header["completion_proof_sha256"])
    except ValueError as error:
        raise GenericCandidateError("provider_directory_generic_source_completion_proof_invalid") from error
    _require(
        proof["dataset"]["hash"] == header["dataset_hash"] and proof["dataset"]["count"] == header["resource_count"],
        "completion_dataset_changed",
    )


def _require_candidate_storage(publisher, directory, identity):
    binding = require_storage_schema(publisher, directory._schema())
    if binding is not None:
        metadata = identity["metadata"]
        if metadata.get("source_ids") == ["cms-npd"]:
            from process.provider_directory_cms_source_handoff import require_cms_handoff_identity

            require_cms_handoff_identity(identity, binding.catalog_schema)
            return binding
        _require(
            metadata.get("source_release") is None
            and type(metadata.get("source_ids")) is list
            and bool(metadata["source_ids"])
            and all(type(source_id) is str and 1 <= len(source_id) <= 64 for source_id in metadata["source_ids"])
            and type(metadata.get("selected_resources")) is list
            and bool(metadata["selected_resources"])
            and all(type(resource) is str for resource in metadata["selected_resources"])
            and set(metadata["selected_resources"])
            <= {
                "Organization",
                "OrganizationAffiliation",
                "Practitioner",
                "PractitionerRole",
                "Location",
                "HealthcareService",
                "InsurancePlan",
                "Endpoint",
            },
            "isolated_kind_unsupported",
        )
    return binding


def _auxiliary_tables(binding_by_field, identity):
    """Require the complete producer-qualified auxiliary phase before table lookup."""
    auxiliary = binding_by_field["auxiliary"]
    if auxiliary is not None:
        _require(
            is_cms_identity(identity)
            and binding_by_field["phase"] in {"prepared", "published", "disposed"}
            and type(auxiliary) is dict
            and set(auxiliary) == {"phase", "tables", "sealed_sha256"}
            and auxiliary["phase"] in {"loading", "sealed", "published", "disposed"}
            and type(auxiliary["tables"]) is dict
            and set(auxiliary["tables"])
            == (set(AUXILIARY_ROOTS) if auxiliary["phase"] in {"loading", "sealed"} else set())
            and (
                auxiliary["sealed_sha256"] is None
                if auxiliary["phase"] == "loading"
                else type(auxiliary["sealed_sha256"]) is str
                and re.fullmatch(r"[0-9a-f]{64}", auxiliary["sealed_sha256"])
            )
            and (
                auxiliary["phase"] == binding_by_field["phase"]
                if binding_by_field["phase"] in {"published", "disposed"}
                else auxiliary["phase"] in {"loading", "sealed"}
            ),
            "auxiliary_binding_changed",
        )
        return auxiliary["tables"]
    else:
        _require(
            not is_cms_identity(identity)
            or binding_by_field["phase"] in {"loading", "disposed"}
            or binding_by_field["contract"] == CMS_ISOLATED_CONTRACT,
            "auxiliary_required",
        )
    return {}


def is_cms_identity(identity):
    """Select only the exact CMS producer/vector from immutable acquisition facts."""
    metadata = identity["metadata"]
    release = metadata.get("source_release")
    return (
        metadata.get("source_ids") == ["cms-npd"]
        and type(release) is dict
        and type(release.get("vector_sha256")) is str
        and re.fullmatch(r"[0-9a-f]{64}", release["vector_sha256"]) is not None
    )


async def _locked(connection, policy, identity, *, phases=("loading",), lock="UPDATE"):
    _require(lock in {"UPDATE", "KEY SHARE", "NO KEY UPDATE"}, "header_lock_invalid")
    row = await connection.fetchrow(
        f"SELECT endpoint_id,acquisition_root_run_id,status,is_current,publication_metadata_json,"
        f"candidate_storage_json FROM {storage_relation(policy.schema, HEADER)} WHERE dataset_id=$1 FOR {lock}",
        identity["dataset_id"],
    )
    _require(row is not None, "header_missing")
    metadata = _json(row["publication_metadata_json"])
    _require(
        row["endpoint_id"] == identity["endpoint_id"]
        and row["acquisition_root_run_id"] == identity["acquisition_root_run_id"]
        and type(metadata) is dict
        and all(metadata.get(key) == value for key, value in identity["metadata"].items()),
        "acquisition_changed",
    )
    if "published" not in phases and "ready_for_freeze" not in phases:
        _require(row["status"] == "acquiring" and row["is_current"] is False, "header_sealed")
    binding_by_field = _json(row["candidate_storage_json"])
    if binding_by_field is not None:
        _validate_binding(binding_by_field, identity, phases=phases)
        from process.provider_directory_generic_source_policy import current_source_storage_binding

        _require(
            (current_source_storage_binding() is not None) == (binding_by_field["contract"] in ISOLATED_CONTRACTS),
            "storage_contract_changed",
        )
    return binding_by_field


async def _store(connection, policy, identity, previous, binding_by_field):
    status = await connection.execute(
        f"UPDATE {storage_relation(policy.schema, HEADER)} SET candidate_storage_json=$2::jsonb "
        "WHERE dataset_id=$1 AND candidate_storage_json IS NOT DISTINCT FROM $3::jsonb",
        identity["dataset_id"],
        _encoded(binding_by_field),
        None if previous is None else _encoded(previous),
    )
    _require(status == "UPDATE 1", "header_compare_failed")


async def _close_grants(connection, policy, name, *, readable=False, sequence=False):
    """Remove default/column grants as well as PUBLIC before any COPY/read access."""
    relation = storage_relation(policy.schema, name)
    oid = await connection.fetchval("SELECT to_regclass($1)::oid::bigint", relation)
    for grant in await connection.fetch(_GRANTEES_SQL, oid):
        principal = "PUBLIC" if grant["grantee"] == 0 else storage_identifier(grant["rolname"])
        column = grant["column_name"]
        privileges = f"ALL PRIVILEGES ({storage_identifier(column)})" if column else "ALL PRIVILEGES"
        kind = "SEQUENCE" if sequence else "TABLE"
        await connection.execute(f"REVOKE {privileges} ON {kind} {relation} FROM {principal} CASCADE")
    if readable:
        readers = ",".join(storage_identifier(role) for role in policy.runtime_roles)
        await connection.execute(f"GRANT SELECT ON TABLE {relation} TO {readers}")


def _table_proof(row, *, private):
    columns, constraints, indexes = (_json(row[key]) for key in ("columns", "constraints", "indexes"))
    _require(
        row["relkind"] == "r"
        and row["relpersistence"] == "p"
        and not row["relrowsecurity"]
        and not row["relforcerowsecurity"]
        and row["inert"]
        and row["native_checks"]
        and row["direct_acl_closed"]
        and (private or row["readers_closed"])
        and (
            not row["parents"] and not row["relispartition"]
            if private
            else len(row["parents"]) == 1 and row["relispartition"]
        )
        and all(constraint["type"] in {"c", "p", "u"} and constraint["valid"] for constraint in constraints)
        and all(index["valid"] for index in indexes),
        "storage_custody_changed",
    )
    return _layout_proof(row)


def _layout_proof(row):
    """Hash the native layout only after the caller authenticates its exact storage shape."""
    layout_by_field = {key: _json(row[key]) for key in ("columns", "constraints", "indexes")}
    layout_by_field["parents"] = list(row["parents"])
    return dict(
        name=row["relname"],
        oid=row["oid"],
        owner=row["relowner"],
        layout_sha256=hashlib.sha256(_encoded(layout_by_field).encode()).hexdigest(),
    )


async def _capture(connection, policy, names, *, private=True):
    owner = await connection.fetchval("SELECT oid::bigint FROM pg_roles WHERE rolname=$1", policy.owner_role)
    readers = (
        []
        if private
        else list(
            await connection.fetchval(
                "SELECT array_agg(oid ORDER BY oid) FROM pg_roles WHERE rolname=ANY($1::text[])",
                list(policy.runtime_roles),
            )
        )
    )
    qualified_names = [storage_relation(policy.schema, name) for name in names]
    rows = await connection.fetch(CATALOG_SQL, qualified_names, owner, readers)
    _require(len(rows) == len(names) and {row["relname"] for row in rows} == set(names), "storage_missing")
    _require(all(row["relowner"] == owner and row["nspname"] == policy.schema for row in rows), "storage_owner_changed")
    proof_by_name = {row["relname"]: _table_proof(row, private=private) for row in rows}
    for name, sequence in (await _capture_input_sequences(connection, policy, names, owner)).items():
        proof_by_name[name]["layout_sha256"] = _input_layout_hash(proof_by_name[name]["layout_sha256"], sequence)
    return proof_by_name


_PRIVATE_READERS_CLOSED_SQL = (
    "SELECT NOT EXISTS(SELECT FROM unnest($1::oid[]) relations(oid),unnest($2::oid[]) readers(oid) "
    "WHERE has_table_privilege(readers.oid,relations.oid,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN') "
    "OR has_any_column_privilege(readers.oid,relations.oid,'SELECT,INSERT,UPDATE,REFERENCES'))"
)


async def require_private_source_readers_closed(connection, relation_oids, reader_oids):
    """Private loading storage grants no direct or inherited runtime access."""
    closed = await connection.fetchval(
        _PRIVATE_READERS_CLOSED_SQL,
        list(relation_oids),
        list(reader_oids),
    )
    _require(closed is True, "private_reader_access")


def _input_layout_hash(table_layout, sequence):
    return hashlib.sha256(_encoded({"table_layout": table_layout, "sequence": dict(sequence)}).encode()).hexdigest()


async def _capture_input_sequences(connection, policy, names, owner):
    """Bind each input ordinal sequence to its exact protected native identity."""
    inputs = [name for name in names if name.endswith(("_ri", "_wi"))]
    if not inputs:
        return {}
    sequences = await connection.fetch(_INPUT_SEQUENCES_SQL, [storage_relation(policy.schema, name) for name in inputs])
    resource_inputs = {name for name in inputs if name.endswith("_ri")}
    _require(
        len(sequences) == len(resource_inputs) and {row["input_name"] for row in sequences} == resource_inputs,
        "input_sequence_changed",
    )
    for row in sequences:
        _require_input_sequence(row, policy, owner)
    return {row["input_name"]: dict(row) for row in sequences}


def _require_input_sequence(row, policy, owner):
    _require(
        row["attname"] == "batch_ordinal"
        and row["attidentity"] == "d"
        and row["deptype"] == "i"
        and row["nspname"] == policy.schema
        and row["relowner"] == owner
        and row["relkind"] == "S"
        and row["relpersistence"] == "p"
        and row["seqtypid"] == "bigint"
        and row["seqstart"] == row["seqincrement"] == row["seqmin"] == row["seqcache"] == 1
        and row["seqmax"] == 2**63 - 1
        and row["seqcycle"] is False
        and row["acl_closed"] is True,
        "input_sequence_changed",
    )


async def _verify_tables(connection, policy, binding_by_field):
    proofs = binding_by_field["tables"]
    # Published attached children retain SELECT only; loading/prepared heaps are private.
    actual = await _capture(
        connection,
        policy,
        [table["name"] for table in proofs.values()],
        private=binding_by_field["phase"] != "published",
    )
    _require(all(actual[proof["name"]] == proof for proof in proofs.values()), "storage_binding_changed")
    await _verify_auxiliary_tables(connection, policy, binding_by_field["auxiliary"])


async def _verify_auxiliary_tables(connection, policy, auxiliary):
    if auxiliary is not None and auxiliary["tables"]:
        receipts = auxiliary["tables"]
        actual = await _capture(connection, policy, [proof["name"] for proof in receipts.values()])
        _require(
            all(actual[proof["name"]] == proof for proof in receipts.values()), "auxiliary_storage_binding_changed"
        )


async def refresh_statistics(connection, schema, names):
    """Refresh planner statistics on relations this transaction just filled.

    Set-based statements that follow a bulk load otherwise plan against stale or
    absent statistics. PostgreSQL only warns and skips a root the current
    role may not analyze, so no privilege probe is needed.
    """
    for name in names:
        await connection.execute(f"ANALYZE {storage_relation(schema, name)}")


_VERIFY_INTERVAL_SECONDS = 60.0
_VERIFIED_CUSTODY: ContextVar[tuple[str, float] | None] = ContextVar(
    "provider_directory_generic_source_verified_custody", default=None
)


async def _verify_tables_recently(connection, policy, binding_by_field):
    """Re-read native custody at most once per interval for an unchanged loading binding.

    Every batch still re-reads and compares the locked header binding; only the
    catalog capture behind it is bounded, because the private heaps are reachable
    solely through this owner's own transactions.
    """
    key = _encoded(
        {
            "schema": policy.schema,
            "owner": policy.owner_role,
            "phase": binding_by_field["phase"],
            "tables": binding_by_field["tables"],
            "auxiliary": binding_by_field["auxiliary"],
            "storage": binding_by_field.get("storage"),
        }
    )
    verified = _VERIFIED_CUSTODY.get()
    now = time.monotonic()
    if verified is not None and verified[0] == key and now - verified[1] < _VERIFY_INTERVAL_SECONDS:
        return
    await _verify_tables(connection, policy, binding_by_field)
    _VERIFIED_CUSTODY.set((key, now))


async def prepare_generic_source_candidate(database, directory, candidate):
    """Initialize two unindexed private input heaps, or authenticate exact resume."""
    identity = candidate_identity(directory, candidate)
    async with source_owner_transaction(database) as publisher:
        connection, policy = publisher.connection, publisher.policy
        isolated = _require_candidate_storage(publisher, directory, identity)
        binding_by_field = await _locked(connection, policy, identity)
        if binding_by_field is not None:
            await _verify_tables(connection, policy, binding_by_field)
            return binding_by_field
        old_payload = await connection.fetchval(
            f"SELECT EXISTS(SELECT FROM {storage_relation(policy.schema, RESOURCE)} WHERE dataset_id=$1)",
            candidate.dataset_id,
        )
        _require(old_payload is False and not candidate.reused_from_checkpoint, "legacy_acquisition_rerun_required")
        prefix = "pdgs_" + uuid4().hex
        names_by_kind = dict(resource_input=prefix + "_ri", witness_input=prefix + "_wi")
        for key, root in (("resource_input", RESOURCE), ("witness_input", WITNESS)):
            relation = storage_relation(policy.schema, names_by_kind[key])
            identity_clause = " GENERATED BY DEFAULT AS IDENTITY" if key == "resource_input" else ""
            await connection.execute(
                f"CREATE TABLE {relation} (LIKE {storage_relation(policy.schema, root)} "
                f"INCLUDING STORAGE, batch_ordinal bigint{identity_clause} NOT NULL, row_ordinal integer NOT NULL)"
                + PRIVATE_HEAP_OPTIONS
            )
            await _close_grants(connection, policy, names_by_kind[key])
            if key == "resource_input":
                sequence_name = await connection.fetchval(
                    "SELECT c.relname FROM pg_class c WHERE c.oid=pg_get_serial_sequence($1,'batch_ordinal')::regclass",
                    relation,
                )
                await _close_grants(connection, policy, sequence_name, sequence=True)
        captured = await _capture(connection, policy, list(names_by_kind.values()))
        binding_by_field = dict(
            contract=CONTRACT,
            identity=identity,
            phase="loading",
            ordinal=0,
            resets={},
            tables={key: captured[name] for key, name in names_by_kind.items()},
            auxiliary=None,
            relationship_sha256=None,
        )
        if isolated is not None:
            binding_by_field.update(
                contract=CMS_ISOLATED_CONTRACT if is_cms_identity(identity) else ISOLATED_CONTRACT,
                storage=storage_document(publisher),
                admission=None,
            )
        await _store(connection, policy, identity, None, binding_by_field)
        return binding_by_field


async def resolve_generic_source_candidate(
    database, directory, dataset_id, *, resource_hash_contract, semantic_projection_as_of
):
    """Resolve an already prepared native binding with the caller's exact contract."""
    candidate = await resolve_existing_source_candidate(database, directory, dataset_id)
    _require(
        candidate.resource_hash_contract == resource_hash_contract
        and candidate.semantic_projection_as_of == semantic_projection_as_of,
        "hash_contract_changed",
    )
    return candidate


async def resolve_existing_source_candidate(database, directory, dataset_id):
    """Read only the explicit existing protected binding for reset/witness controls."""
    _require(type(dataset_id) is str and 1 <= len(dataset_id) <= 96, "dataset_invalid")
    async with source_owner_transaction(database) as publisher:
        require_storage_schema(publisher, directory._schema())
        row = await publisher.connection.fetchrow(
            f"SELECT candidate_storage_json FROM {storage_relation(publisher.policy.schema, HEADER)} "
            "WHERE dataset_id=$1 FOR KEY SHARE",
            dataset_id,
        )
        binding_by_field = _json(row["candidate_storage_json"]) if row is not None else None
        _require(
            type(binding_by_field) is dict and type(binding_by_field.get("identity")) is dict, "preparation_required"
        )
        identity = binding_by_field["identity"]
        _require(identity.get("dataset_id") == dataset_id, "dataset_changed")
        await _locked(
            publisher.connection, publisher.policy, identity, phases=("loading", "prepared"), lock="KEY SHARE"
        )
        metadata = identity["metadata"]
        contract = metadata.get(directory.RESOURCE_HASH_CONTRACT_METADATA_KEY)
        _require(contract in directory.RESOURCE_HASH_CONTRACTS, "hash_contract_changed")
        return _StoredCandidate(identity, contract, metadata.get(directory.SEMANTIC_PROJECTION_AS_OF_METADATA_KEY))


def _validated_transition_identity(identity_by_field, expected_resource_counts):
    """Require one exact unpublished dataset and complete resource census."""
    _require(
        type(identity_by_field) is dict
        and set(identity_by_field) == {"dataset_id", "endpoint_id", "acquisition_root_run_id", "failed_run_id"}
        and type(identity_by_field["dataset_id"]) is str
        and 1 <= len(identity_by_field["dataset_id"]) <= 96
        and type(identity_by_field["endpoint_id"]) is str
        and bool(identity_by_field["endpoint_id"].strip())
        and all(
            type(identity_by_field[name]) is str and re.fullmatch(r"run_[0-9a-f]{32}", identity_by_field[name])
            for name in ("acquisition_root_run_id", "failed_run_id")
        )
        and type(expected_resource_counts) is dict
        and 1 <= len(expected_resource_counts) <= 32
        and all(
            type(name) is str and re.fullmatch(r"[A-Za-z][A-Za-z0-9]{0,63}", name) and type(count) is int and count >= 0
            for name, count in expected_resource_counts.items()
        ),
        "transition_identity_invalid",
    )


def _validated_transition_storage(policy, old_storage, new_storage):
    """Keep all native coordinates fixed except the issued producer seal."""
    from process.provider_directory_source_storage_binding import FHIRSourceStorageBinding

    binding_fields = set(FHIRSourceStorageBinding.__dataclass_fields__)
    _require(
        type(old_storage) is type(new_storage) is dict
        and set(old_storage) == set(new_storage) == binding_fields | {"producer"},
        "transition_storage_invalid",
    )
    old_binding = FHIRSourceStorageBinding(**{key: old_storage[key] for key in binding_fields})
    new_binding = FHIRSourceStorageBinding(**{key: new_storage[key] for key in binding_fields})
    _require(
        old_binding.storage_schema == new_binding.storage_schema == policy.schema
        and {key: old_storage[key] for key in binding_fields - {"enrollment_sha256"}}
        == {key: new_storage[key] for key in binding_fields - {"enrollment_sha256"}}
        and old_binding.enrollment_sha256 != new_binding.enrollment_sha256
        and all(
            type(storage_by_field["producer"]) is list
            and len(storage_by_field["producer"]) == 2
            and re.fullmatch(r"[0-9a-f]{40}", storage_by_field["producer"][0])
            and re.fullmatch(r"sha256:[0-9a-f]{64}", storage_by_field["producer"][1])
            for storage_by_field in (old_storage, new_storage)
        )
        and old_storage["producer"] != new_storage["producer"],
        "transition_storage_changed",
    )
    return old_binding


async def _locked_transition_candidate(
    connection, policy, identity_by_field, old_storage, new_storage, expected_counts
):
    """Lock only the private candidate and verify its complete native binding."""
    endpoint_id = identity_by_field["endpoint_id"]
    failed_run_id = identity_by_field["failed_run_id"]
    endpoint_lock_acquired = await connection.fetchval(
        "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))", endpoint_id
    )
    _require(endpoint_lock_acquired is True, "transition_endpoint_busy")
    worker_lock_acquired = await connection.fetchval(
        "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))",
        "control-run-worker-action:v1:" + failed_run_id,
    )
    _require(worker_lock_acquired is True, "transition_worker_action_busy")
    header = await connection.fetchrow(
        "SELECT dataset_id,endpoint_id,acquisition_root_run_id,import_run_id,status,is_current,published_at,"
        "publication_metadata_json,candidate_storage_json FROM "
        + storage_relation(policy.schema, HEADER)
        + " WHERE dataset_id=$1 FOR UPDATE NOWAIT",
        identity_by_field["dataset_id"],
    )
    _require(
        header is not None
        and all(
            header[field] == identity_by_field[identity_field]
            for field, identity_field in (
                ("dataset_id", "dataset_id"),
                ("endpoint_id", "endpoint_id"),
                ("acquisition_root_run_id", "acquisition_root_run_id"),
                ("import_run_id", "failed_run_id"),
            )
        )
        and header["status"] == "acquiring"
        and header["is_current"] is False
        and header["published_at"] is None,
        "transition_candidate_changed",
    )
    candidate_by_field = _json(header["candidate_storage_json"])
    stored_identity = candidate_by_field.get("identity") if type(candidate_by_field) is dict else None
    _validate_binding_shape(candidate_by_field, stored_identity, phases=("loading",))
    _validate_binding_tables(candidate_by_field, stored_identity)
    _require(
        candidate_by_field["contract"] == ISOLATED_CONTRACT
        and candidate_by_field["admission"] is None
        and candidate_by_field["auxiliary"] is None
        and all(
            stored_identity[field] == identity_by_field[field]
            for field in ("dataset_id", "endpoint_id", "acquisition_root_run_id")
        )
        and _json(header["publication_metadata_json"]) == stored_identity["metadata"]
        and set(stored_identity["metadata"].get("selected_resources", ())) == set(expected_counts)
        and set(stored_identity["metadata"].get("expected_resources", ())) == set(expected_counts)
        and candidate_by_field["storage"] in (old_storage, new_storage),
        "transition_candidate_changed",
    )
    await _verify_tables(connection, policy, candidate_by_field)
    return candidate_by_field


async def _verify_transition_run(connection, catalog_schema, identity_by_field, source_ids):
    """Require the exact latest failed child and no active same-root run."""
    root_run_id = identity_by_field["acquisition_root_run_id"]
    run_rows = await connection.fetch(
        "SELECT run_id,status,finished_at,retry_of_run_id,created_at,"
        "params::jsonb->>'provider_directory_pagination_root_run_id' AS root_run_id,"
        "params::jsonb->'source_ids' AS source_ids FROM "
        + storage_relation(catalog_schema, "import_run")
        + " WHERE importer='provider-directory-fhir' AND "
        "(run_id=$1 OR params::jsonb->>'provider_directory_pagination_root_run_id'=$1) "
        "ORDER BY created_at DESC,run_id DESC LIMIT 65",
        root_run_id,
    )
    run_ids = {run_record["run_id"] for run_record in run_rows}
    parents_by_id = {run_record["run_id"]: run_record["retry_of_run_id"] for run_record in run_rows}
    _require(
        1 <= len(run_rows) <= 64
        and run_rows[0]["run_id"] == identity_by_field["failed_run_id"]
        and run_rows[0]["status"] == "failed"
        and run_rows[0]["finished_at"] is not None
        and run_rows[0]["root_run_id"] == root_run_id
        and _json(run_rows[0]["source_ids"]) == source_ids
        and all(
            run_record["retry_of_run_id"] is None
            if run_record["run_id"] == root_run_id
            else run_record["retry_of_run_id"] in run_ids
            for run_record in run_rows
        )
        and all(
            _json(run_record["source_ids"]) == source_ids
            and (run_record["run_id"] == root_run_id or run_record["root_run_id"] == root_run_id)
            for run_record in run_rows
        )
        and all(
            run_record["status"] not in {"queued", "starting", "running", "finalizing", "canceling"}
            for run_record in run_rows
        ),
        "transition_run_changed",
    )
    lineage_cursor = identity_by_field["failed_run_id"]
    seen_run_ids = set()
    while lineage_cursor != root_run_id:
        _require(lineage_cursor in run_ids and lineage_cursor not in seen_run_ids, "transition_run_changed")
        seen_run_ids.add(lineage_cursor)
        lineage_cursor = parents_by_id[lineage_cursor]
    return sorted(run_ids)


def _is_resumable_transition_checkpoint(checkpoint):
    """Keep completed scans or an unchanged, same-origin retained continuation."""
    if checkpoint["state"] == "complete":
        return checkpoint["completed_at"] is not None
    cursor = checkpoint["next_url"]
    if (
        checkpoint["state"] != "active"
        or checkpoint["completed_at"] is not None
        or type(cursor) is not str
        or not cursor
        or any(character.isspace() for character in cursor)
        or hashlib.sha256(cursor.encode()).hexdigest() == checkpoint["start_url_hash"]
    ):
        return False
    directory = import_module("process.provider_directory_fhir")
    base = checkpoint["canonical_api_base"]
    try:
        return directory._is_trusted_fhir_pagination_url({"canonical_api_base": base}, base, cursor)
    except ValueError:
        return False


async def _verify_transition_inputs(connection, policy, candidate_by_field, identity_by_field, expected_counts):
    """Compare completed or resumable checkpoints with every active detached input row."""
    root_run_id = identity_by_field["acquisition_root_run_id"]
    dataset_id = identity_by_field["dataset_id"]
    checkpoint_rows = await connection.fetch(
        "SELECT resource_type,state,owner_run_id,dataset_id,rows_processed,pages_processed,completed_at,"
        "canonical_api_base,start_url_hash,next_url FROM "
        + storage_relation(policy.schema, "provider_directory_pagination_checkpoint")
        + " WHERE acquisition_root_run_id=$1 FOR SHARE NOWAIT",
        root_run_id,
    )
    _require(
        len(checkpoint_rows) == len(expected_counts)
        and {checkpoint_record["resource_type"] for checkpoint_record in checkpoint_rows} == set(expected_counts)
        and all(
            _is_resumable_transition_checkpoint(checkpoint_record)
            and checkpoint_record["owner_run_id"] == identity_by_field["failed_run_id"]
            and checkpoint_record["dataset_id"] == dataset_id
            and checkpoint_record["rows_processed"] == expected_counts[checkpoint_record["resource_type"]]
            and checkpoint_record["pages_processed"] > 0
            for checkpoint_record in checkpoint_rows
        ),
        "transition_checkpoint_changed",
    )
    resource_relation = storage_relation(policy.schema, candidate_by_field["tables"]["resource_input"]["name"])
    input_rows = await connection.fetch(
        "SELECT dataset_id,resource_type,count(*) AS total,"
        "count(*) FILTER (WHERE batch_ordinal > greatest($1::bigint,"
        "coalesce(($2::jsonb->>resource_type)::bigint,0)) AND batch_ordinal <= $3::bigint) AS active "
        "FROM " + resource_relation + " GROUP BY dataset_id,resource_type",
        candidate_by_field["resets"].get("*", 0),
        _encoded(candidate_by_field["resets"]),
        candidate_by_field["ordinal"],
    )
    input_counts_by_type = {input_record["resource_type"]: input_record["total"] for input_record in input_rows}
    _require(
        len(input_counts_by_type) == len(input_rows)
        and all(
            input_record["dataset_id"] == dataset_id
            and input_record["resource_type"] in expected_counts
            and input_record["total"] == input_record["active"] == expected_counts[input_record["resource_type"]]
            for input_record in input_rows
        )
        and all(
            input_counts_by_type.get(resource_type, 0) == count for resource_type, count in expected_counts.items()
        ),
        "transition_input_changed",
    )
    witness_relation = storage_relation(policy.schema, candidate_by_field["tables"]["witness_input"]["name"])
    _require(
        await connection.fetchval(
            "SELECT NOT EXISTS(SELECT FROM " + witness_relation + " WHERE dataset_id<>$1)", dataset_id
        )
        is True,
        "transition_witness_changed",
    )


async def _require_transition_administrator(connection, policy):
    """Bind the transition to the original independent admin session."""
    actor = await connection.fetchrow(
        "SELECT session_user::text AS session,current_user::text AS current,"
        "(SELECT rolsuper FROM pg_roles WHERE rolname=current_user) AS administrator"
    )
    _require(
        actor is not None
        and actor["session"] == actor["current"]
        and actor["administrator"] is True
        and actor["current"] not in {policy.owner_role, policy.publisher_role, *policy.runtime_roles},
        "transition_admin_required",
    )


async def transition_isolated_loading_candidate_storage(
    connection,
    *,
    policy,
    identity_by_field,
    old_storage,
    new_storage,
    expected_resource_counts,
):
    """Move one failed private intake to an admin-issued successor producer."""
    from process.provider_directory_generic_source_policy import GenericSourcePolicy
    from process.provider_directory_source_storage_binding import verify_isolated_source_storage_coordinates

    _require(type(policy) is GenericSourcePolicy and connection.is_in_transaction(), "transition_transaction_required")
    _require(await connection.fetchval("SHOW transaction_isolation") == "serializable", "transition_isolation_required")
    await _require_transition_administrator(connection, policy)
    _validated_transition_identity(identity_by_field, expected_resource_counts)
    old_binding = _validated_transition_storage(policy, old_storage, new_storage)
    await verify_isolated_source_storage_coordinates(connection, policy, old_binding)
    candidate_by_field = await _locked_transition_candidate(
        connection,
        policy,
        identity_by_field,
        old_storage,
        new_storage,
        expected_resource_counts,
    )
    lineage_run_ids = await _verify_transition_run(
        connection,
        old_binding.catalog_schema,
        identity_by_field,
        candidate_by_field["identity"]["metadata"]["source_ids"],
    )
    await _verify_transition_inputs(connection, policy, candidate_by_field, identity_by_field, expected_resource_counts)
    old_document_by_field = {**candidate_by_field, "storage": old_storage}
    successor_by_field = {**candidate_by_field, "storage": new_storage}
    receipt_by_field = {
        "dataset_id": identity_by_field["dataset_id"],
        "rows": sum(expected_resource_counts.values()),
        "lineage_run_ids": lineage_run_ids,
        "old_binding_sha256": hashlib.sha256(_encoded(old_document_by_field).encode()).hexdigest(),
        "new_binding_sha256": hashlib.sha256(_encoded(successor_by_field).encode()).hexdigest(),
    }
    if candidate_by_field["storage"] == new_storage:
        return {**receipt_by_field, "already_applied": True}
    updated = await connection.execute(
        "UPDATE "
        + storage_relation(policy.schema, HEADER)
        + " SET candidate_storage_json=$2::jsonb WHERE dataset_id=$1 "
        "AND candidate_storage_json IS NOT DISTINCT FROM $3::jsonb "
        "AND status='acquiring' AND NOT is_current AND published_at IS NULL",
        identity_by_field["dataset_id"],
        _encoded(successor_by_field),
        _encoded(candidate_by_field),
    )
    _require(updated == "UPDATE 1", "transition_header_compare_failed")
    return {**receipt_by_field, "already_applied": False}


def _records(rows, columns, ordinal):
    for index, row in enumerate(rows):
        _require(
            set(row) <= set(columns) and all(name in row for name in columns if name != "acquired_resource_sha256"),
            "copy_columns_changed",
        )
        yield tuple(_json_text(row.get(name)) if name.endswith("_json") else row.get(name) for name in columns) + (
            ordinal,
            index,
        )


def _json_text(value):
    """Staging workers hand over canonical JSON text already; encode only Python values."""
    return value if isinstance(value, str) else _encoded(value)


async def _copy(connection, policy, table, columns, records):
    count = 0
    for batch in copy_record_batches(records):
        try:
            status = await connection.copy_records_to_table(
                table, schema_name=policy.schema, columns=columns, records=batch
            )
        except asyncio.CancelledError, TimeoutError:
            await close_interrupted_copy(connection)
            await invalidate_copy_owner(source_copy_session(), connection)
            raise
        _require(status == f"COPY {len(batch)}", "copy_incomplete")
        count += len(batch)
    return count


async def _lock_resource_family(connection, policy, dataset_id, resource_type):
    """Serialize one family's observations and resets while sibling COPY stays independent."""
    await connection.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
        "provider-directory-source-family:" + _encoded([policy.schema, dataset_id, resource_type]),
    )


def _resource_family(resource_type):
    return resource_type.split(":", 2)[1] if resource_type.startswith("LU:") else resource_type


async def append_generic_source_batch(database, directory, candidate, resources, witnesses=()):
    """Commit payload observations and CMS raw facts in the same original savepoint."""
    identity = candidate_identity(directory, candidate)
    _require(len(resources) <= 4096 and len(witnesses) <= 4096 and bool(resources or witnesses), "batch_size_invalid")
    for batch_rows in (resources, witnesses):
        _require(
            all(resource_row.get("dataset_id") == candidate.dataset_id for resource_row in batch_rows),
            "dataset_changed",
        )
    async with source_owner_transaction(database) as publisher:
        connection, policy = publisher.connection, publisher.policy
        _require_candidate_storage(publisher, directory, identity)
        previous = await _locked(connection, policy, identity, lock="KEY SHARE")
        _require(previous is not None, "preparation_required")
        families = {_resource_family(resource_row["resource_type"]) for resource_row in (*resources, *witnesses)}
        for resource_type in sorted(families):
            await _lock_resource_family(connection, policy, candidate.dataset_id, resource_type)
        previous = await _locked(connection, policy, identity, lock="KEY SHARE")
        _require(previous is not None, "preparation_required")
        await _verify_tables_recently(connection, policy, previous)
        ordinal = await connection.fetchval(
            "SELECT nextval(pg_get_serial_sequence($1,'batch_ordinal'))",
            storage_relation(policy.schema, previous["tables"]["resource_input"]["name"]),
        )
        _require(type(ordinal) is int and previous["ordinal"] < ordinal < 2**63 - 1, "ordinal_exhausted")
        for key, columns, batch_rows in (
            ("resource_input", RESOURCE_COLUMNS, resources),
            ("witness_input", WITNESS_COLUMNS, witnesses),
        ):
            if batch_rows:
                await _copy(
                    connection,
                    policy,
                    previous["tables"][key]["name"],
                    (*columns, "batch_ordinal", "row_ordinal"),
                    _records(batch_rows, columns, ordinal),
                )
        current = await _locked(connection, policy, identity, lock="NO KEY UPDATE")
        _require(current is not None and current["tables"] == previous["tables"], "storage_binding_changed")
        binding_by_field = {**current, "ordinal": max(current["ordinal"], ordinal)}
        await _store(connection, policy, identity, current, binding_by_field)
    return len(resources)


async def reset_generic_source_family(database, directory, candidate, resource_type=None):
    """Retire old observations by a low-volume cutoff; never delete payload rows."""
    identity = candidate_identity(directory, candidate)
    _require(resource_type is None or resource_type in candidate.selected_resources, "reset_scope_changed")
    async with source_owner_transaction(database) as publisher:
        connection, policy = publisher.connection, publisher.policy
        _require_candidate_storage(publisher, directory, identity)
        if resource_type is not None:
            await _locked(connection, policy, identity, lock="KEY SHARE")
            await _lock_resource_family(connection, policy, candidate.dataset_id, resource_type)
        previous = await _locked(
            connection, policy, identity, lock="UPDATE" if resource_type is None else "NO KEY UPDATE"
        )
        _require(previous is not None, "preparation_required")
        await _verify_tables(connection, policy, previous)
        cutoff_by_family = dict(previous["resets"])
        cutoff_by_family[resource_type or "*"] = previous["ordinal"]
        binding_by_field = {**previous, "resets": cutoff_by_family}
        await _store(connection, policy, identity, previous, binding_by_field)


async def discard_generic_source_inputs(database, directory, candidate, release):
    """Reclaim only exact failed, unpublished, unattached heaps; never row DELETE."""
    async with source_owner_transaction(database) as publisher:
        connection, policy = publisher.connection, publisher.policy
        require_storage_schema(publisher, directory._schema())
        header_by_field = await connection.fetchrow(
            f"SELECT endpoint_id,acquisition_root_run_id,status,is_current,published_at,publication_metadata_json,"
            f"candidate_storage_json FROM {storage_relation(policy.schema, HEADER)} WHERE dataset_id=$1 FOR UPDATE",
            candidate.dataset_id,
        )
        _require(
            header_by_field is not None
            and header_by_field["status"] == "failed"
            and header_by_field["is_current"] is False
            and header_by_field["published_at"] is None
            and header_by_field["endpoint_id"] == candidate.endpoint_id
            and header_by_field["acquisition_root_run_id"] == candidate.acquisition_root_run_id
            and _json(header_by_field["publication_metadata_json"]).get("source_release") == release,
            "disposition_changed",
        )
        binding_by_field = _json(header_by_field["candidate_storage_json"])
        _require(
            type(binding_by_field) is dict and type(binding_by_field.get("identity")) is dict,
            "legacy_retirement_required",
        )
        _validate_binding(binding_by_field, binding_by_field["identity"], phases=("loading", "prepared", "disposed"))
        _require(
            binding_by_field["identity"]["dataset_id"] == candidate.dataset_id
            and binding_by_field["identity"]["endpoint_id"] == candidate.endpoint_id
            and binding_by_field["identity"]["acquisition_root_run_id"] == candidate.acquisition_root_run_id
            and binding_by_field["identity"]["metadata"].get("source_release") == release,
            "disposition_changed",
        )
        if binding_by_field["phase"] == "disposed":
            return
        await _verify_tables(connection, policy, binding_by_field)
        auxiliary = binding_by_field["auxiliary"]
        receipts = list(binding_by_field["tables"].values())
        if auxiliary is not None:
            receipts.extend(auxiliary["tables"].values())
        for table in receipts:
            await connection.execute(f"DROP TABLE {storage_relation(policy.schema, table['name'])}")
        disposed_auxiliary = (
            None
            if auxiliary is None
            else {
                "phase": "disposed",
                "tables": {},
                "sealed_sha256": auxiliary["sealed_sha256"] or hashlib.sha256(_encoded(auxiliary).encode()).hexdigest(),
            }
        )
        disposed_by_field = {**binding_by_field, "phase": "disposed", "tables": {}, "auxiliary": disposed_auxiliary}
        await _store(connection, policy, binding_by_field["identity"], binding_by_field, disposed_by_field)


@asynccontextmanager
async def generic_source_relation_scope(database, directory, candidate):
    """Read/write only the exact closed candidate group in the original owner tx."""
    identity = candidate_identity(directory, candidate)
    async with source_owner_transaction(database) as publisher:
        isolated = _require_candidate_storage(publisher, directory, identity)
        binding_by_field = await _locked(publisher.connection, publisher.policy, identity, phases=("prepared",))
        _require(binding_by_field is not None, "completion_required")
        await _verify_tables(publisher.connection, publisher.policy, binding_by_field)
        relations_by_root = {name: binding_by_field["tables"][name]["name"] for name in RELATIONS}
        auxiliary = binding_by_field["auxiliary"]
        if auxiliary is not None:
            relations_by_root.update({root: proof["name"] for root, proof in auxiliary["tables"].items()})
        from process.provider_directory_generic_source_auxiliary import identity_candidate_scope
        from process.provider_directory_source_storage_binding import isolated_source_relation_scope

        async with identity_candidate_scope(publisher, binding_by_field):
            isolated_scope = isolated_source_relation_scope(publisher, relations_by_root) if isolated else nullcontext()
            with isolated_scope, directory._provider_directory_artifact_relation_scope(relations_by_root):
                yield publisher, binding_by_field
