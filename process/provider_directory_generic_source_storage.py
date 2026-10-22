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
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
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
CMS_IDENTITY_ROOTS = (
    "provider_directory_entity_source_binding",
    "provider_directory_entity_release_evidence",
    "provider_directory_insurance_network_source_binding",
    "provider_directory_insurance_network_plan_evidence",
)
CMS_RESOURCE_IDENTITY_ROOT = "provider_directory_resource_identity"
AUXILIARY_ROOTS = (*CMS_IDENTITY_ROOTS, CMS_RESOURCE_IDENTITY_ROOT)
CONTRACT = "generic-source-candidate-v2"
_NAME = re.compile(r"pdgs_[0-9a-f]{32}_(?:ri|wi|r|ip|np|ao|w|rl|eb|ee|nb|ne|re)")
_SUFFIXES = dict(zip((*RELATIONS, *AUXILIARY_ROOTS), ("r", "ip", "np", "ao", "w", "rl", "eb", "ee", "nb", "ne", "re")))
_BINDING_KEYS = {"contract", "identity", "phase", "ordinal", "resets", "tables", "auxiliary", "relationship_sha256"}
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
    _require(
        type(binding_by_field) is dict
        and set(binding_by_field) == _BINDING_KEYS
        and binding_by_field["contract"] == CONTRACT
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
    _require(type(binding_by_field["tables"]) is dict, "tables_changed")
    expected = set() if binding_by_field["phase"] in {"published", "disposed"} else {"resource_input", "witness_input"}
    if binding_by_field["phase"] in {"prepared", "published"}:
        expected |= set(RELATIONS)
    _require(set(binding_by_field["tables"]) == expected, "tables_changed")
    tables_by_kind = {**binding_by_field["tables"], **_auxiliary_tables(binding_by_field, identity)}
    names = []
    for table in tables_by_kind.values():
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
        names.append(table["name"])
    _require(len(set(names)) == len(names), "table_binding_changed")


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
            not is_cms_identity(identity) or binding_by_field["phase"] in {"loading", "disposed"}, "auxiliary_required"
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
    if "published" not in phases:
        _require(row["status"] == "acquiring" and row["is_current"] is False, "header_sealed")
    binding_by_field = _json(row["candidate_storage_json"])
    if binding_by_field is not None:
        _validate_binding(binding_by_field, identity, phases=phases)
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
    """Refresh planner statistics on global roots this transaction just filled.

    Set-based replay comparisons that follow a bulk INSERT otherwise plan against
    stale or absent statistics. PostgreSQL only warns and skips a root the current
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
        _require(policy.schema == directory._schema(), "schema_changed")
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
        _require(publisher.policy.schema == directory._schema(), "schema_changed")
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


def _records(rows, columns, ordinal):
    for index, row in enumerate(rows):
        _require(
            set(row) <= set(columns) and all(name in row for name in columns if name != "acquired_resource_sha256"),
            "copy_columns_changed",
        )
        yield tuple(_encoded(row.get(name)) if name.endswith("_json") else row.get(name) for name in columns) + (
            ordinal,
            index,
        )


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
        _require(policy.schema == directory._schema(), "schema_changed")
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
        _require(policy.schema == directory._schema(), "schema_changed")
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
        _require(policy.schema == directory._schema(), "schema_changed")
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
        _require(publisher.policy.schema == directory._schema(), "schema_changed")
        binding_by_field = await _locked(publisher.connection, publisher.policy, identity, phases=("prepared",))
        _require(binding_by_field is not None, "completion_required")
        await _verify_tables(publisher.connection, publisher.policy, binding_by_field)
        relations_by_root = {name: binding_by_field["tables"][name]["name"] for name in RELATIONS}
        auxiliary = binding_by_field["auxiliary"]
        if auxiliary is not None:
            relations_by_root.update({root: proof["name"] for root, proof in auxiliary["tables"].items()})
        from process.provider_directory_generic_source_auxiliary import identity_candidate_scope

        async with identity_candidate_scope(publisher, binding_by_field):
            with directory._provider_directory_artifact_relation_scope(relations_by_root):
                yield publisher, binding_by_field
