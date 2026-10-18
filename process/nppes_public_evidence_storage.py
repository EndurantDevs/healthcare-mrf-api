# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected native NPPES families; payload rows never enter shared history heaps."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import date, datetime
from uuid import uuid4

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_generic_source_policy as source_policy
from process.native_copy_cleanup import close_interrupted_copy, invalidate_copy_owner
from process.nppes_public_evidence_prepared_chunks import chunk_payloads
from process.nppes_public_evidence_rows import (
    ADMISSION_COLUMNS,
    COMMON_COLUMNS,
    MEMBER_COLUMNS,
    NPI_ENUMERATION_COLUMNS,
    SOURCE_IDENTITY_COLUMNS,
    SOURCE_LINK_COLUMNS,
    SOURCE_RECORD_COLUMNS,
    SOURCE_RELEASE_COLUMNS,
    admission_values,
    source_identity_values,
    source_release_values,
)
from process.nppes_public_evidence_writer_contract import writer_error
from process.plan_release_storage_catalog import CATALOG_SQL
from process.plan_release_storage_custody import storage_roles
from process.plan_release_storage_provision import _GRANTEES_SQL
from process.provider_directory_generic_candidate import COPY_FRAME_BYTES, COPY_MAX_BYTES, COPY_MAX_ROWS

HEADER = "public_evidence_nppes_registry_storage"
CONTRACT = "nppes-evidence-candidate-v2"
ROOTS = {
    "source_record": ("public_evidence_source_record", SOURCE_RECORD_COLUMNS, "source_records", "sr"),
    "member": ("public_evidence_nppes_registry_member", MEMBER_COLUMNS, "members", "m"),
    "common": ("public_evidence_record", COMMON_COLUMNS, "common_rows", "c"),
    "source_link": ("public_evidence_record_source_link", SOURCE_LINK_COLUMNS, "source_links", "l"),
    "typed": ("public_evidence_npi_enumeration", NPI_ENUMERATION_COLUMNS, "typed_rows", "t"),
}
_NAME = re.compile(r"npec_[0-9a-f]{32}_(?:sr|m|c|l|t)")
_CHUNK_NAME = re.compile(r"npec_[0-9a-f]{32}_p")
_HASH = re.compile(r"[0-9a-f]{64}")
_KEYS = {
    "contract",
    "identity",
    "producer_identity",
    "policy",
    "phase",
    "ordinal",
    "counts",
    "last_batch",
    "tables",
    "chunks",
    "validation_sha256",
}
_CHUNK_COLUMNS = (
    "chunk_ordinal",
    "start_ordinal",
    "end_ordinal",
    "batch_sha256",
    "counts_json",
    *(column for root in ROOTS for column in (root + "_payload", root + "_sha256")),
)
_INDEX_KEYS = {
    "source_record": (("source_record_ref",),),
    "member": (
        ("admission_ref", "source_row_ordinal"),
        ("source_release_ref", "npi"),
        ("source_release_ref", "source_record_ref"),
        ("source_release_ref", "evidence_ref"),
    ),
    "common": (("evidence_ref",),),
    "source_link": (("evidence_ref",),),
    "typed": (("evidence_ref",),),
}
_NULLABLE = {
    "source_record": {"nppes_admission_ref"},
    "member": {
        "entity_type_code",
        "provider_enumeration_date",
        "last_update_date",
        "npi_deactivation_date",
        "npi_reactivation_date",
        "exclusion_reason",
        "evidence_ref",
    },
    "common": {"effective_end_at"},
    "source_link": set(),
    "typed": set(),
}
_NATIVE_LAYOUT_SQL = """SELECT NOT EXISTS(
 SELECT FROM pg_attribute a JOIN pg_type t ON t.oid=a.atttypid
 WHERE a.attrelid=ANY($1::oid[]) AND a.attnum>0 AND NOT a.attisdropped
 AND t.typnamespace<>'pg_catalog'::regnamespace
 UNION ALL SELECT FROM pg_index i CROSS JOIN LATERAL unnest(i.indclass::oid[]) k(oid)
 JOIN pg_opclass op ON op.oid=k.oid
 WHERE i.indrelid=ANY($1::oid[]) AND op.opcnamespace<>'pg_catalog'::regnamespace)"""


@dataclass(frozen=True, slots=True, repr=False)
class NppesEvidenceCandidate:
    """A replay cursor, never a capability to choose physical relations."""

    identity: dict
    producer_identity: object
    ordinal: int
    counts: dict
    phase: str = "loading"

    @property
    def source_record_count(self):
        """Return the committed source-row count used by exact-prefix replay."""
        return self.ordinal

    def __repr__(self):
        return "<nppes-evidence-candidate>"


def _require(condition):
    if not condition:
        raise writer_error()


def _json(value):
    return json.loads(value) if type(value) is str else value


def _encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _fingerprint(value):
    return hashlib.sha256(_encoded(value).encode()).hexdigest()


def _identity(prepared):
    row = prepared.admission_row
    # The public writer rebuilds the prepared replay before entering this boundary.
    admission_values(row)
    source_release_values(prepared.manifest.release)
    _require(
        row.source_release_ref == prepared.manifest.release.source_release_ref
        and row.manifest_sha256 == prepared.manifest.manifest_sha256
    )
    return {
        "admission_ref": row.admission_ref,
        "admission_contract_sha256": row.contract_sha256,
        "source_release_ref": row.source_release_ref,
        "source_release_contract_sha256": row.source_release_contract_sha256,
        "source_kind": row.source_kind,
        "artifact_sha256": row.artifact_sha256,
        "manifest_sha256": row.manifest_sha256,
    }


def _policy_fields(policy):
    return dict(
        schema=policy.schema,
        owner_role=policy.owner_role,
        publisher_role=policy.publisher_role,
        runtime_roles=list(policy.runtime_roles),
        control_runtime_roles=list(policy.control_runtime_roles),
    )


def _stored_policy(binding):
    fields = binding["policy"]
    _require(
        type(fields) is dict
        and set(fields) == {"schema", "owner_role", "publisher_role", "runtime_roles", "control_runtime_roles"}
    )
    return source_policy.GenericSourcePolicy(
        fields["schema"],
        fields["owner_role"],
        fields["publisher_role"],
        tuple(fields["runtime_roles"]),
        tuple(fields["control_runtime_roles"]),
    )


def _candidate(binding):
    return NppesEvidenceCandidate(
        binding["identity"], binding["producer_identity"], binding["ordinal"], binding["counts"], binding["phase"]
    )


def _chunk_seed(binding):
    return _fingerprint(
        {
            "contract": CONTRACT,
            "identity": binding["identity"],
            "producer_identity": binding["producer_identity"],
            "policy": binding["policy"],
            "tables": {
                root: {key: proof[key] for key in ("name", "oid", "owner")} for root, proof in binding["tables"].items()
            },
        }
    )


def _validation_fingerprint(binding):
    return _fingerprint(
        {
            "identity": binding["identity"],
            "tables": binding["tables"],
            "counts": binding["counts"],
            "prepared_chunks": {key: binding["chunks"][key] for key in ("count", "sha256")},
        }
    )


def _assert_chunk_binding(binding, names):
    """Keep prepared-byte custody and its ordered manifest inside the family scope."""
    chunks = binding["chunks"]
    _require(
        type(chunks) is dict
        and set(chunks) == {"table", "count", "sha256"}
        and type(chunks["count"]) is int
        and 0 <= chunks["count"] <= binding["ordinal"]
        and (chunks["count"] == 0) == (binding["ordinal"] == 0)
        and type(chunks["sha256"]) is str
        and _HASH.fullmatch(chunks["sha256"]) is not None
        and (chunks["count"] != 0 or chunks["sha256"] == _chunk_seed(binding))
    )
    if binding["phase"] == "published":
        _require(chunks["table"] is None)
    else:
        proof = chunks["table"]
        _require(
            type(proof) is dict
            and set(proof) == {"name", "oid", "owner", "layout_sha256"}
            and type(proof["name"]) is str
            and _CHUNK_NAME.fullmatch(proof["name"]) is not None
            and type(proof["oid"]) is int
            and proof["oid"] > 0
            and type(proof["owner"]) is int
            and proof["owner"] > 0
            and type(proof["layout_sha256"]) is str
            and _HASH.fullmatch(proof["layout_sha256"]) is not None
            and proof["name"] not in names
            and proof["oid"] not in {fact_proof["oid"] for fact_proof in binding["tables"].values()}
        )


def _binding(binding_document, identity=None, *, phases=("loading", "prepared", "published")):
    binding = _json(binding_document)
    _require(
        type(binding) is dict
        and set(binding) == _KEYS
        and binding["contract"] == CONTRACT
        and type(binding["identity"]) is dict
        and (identity is None or binding["identity"] == identity)
        and binding["phase"] in phases
        and type(binding["ordinal"]) is int
        and 0 <= binding["ordinal"] < 2**63
        and type(binding["counts"]) is dict
        and set(binding["counts"]) == set(ROOTS)
        and all(type(count) is int and 0 <= count < 2**63 for count in binding["counts"].values())
        and binding["counts"]["source_record"] == binding["counts"]["member"] == binding["ordinal"]
        and binding["counts"]["common"] == binding["counts"]["source_link"] == binding["counts"]["typed"]
        and type(binding["tables"]) is dict
        and set(binding["tables"]) == set(ROOTS)
    )
    names = []
    for proof in binding["tables"].values():
        _require(
            type(proof) is dict
            and set(proof) == {"name", "oid", "owner", "layout_sha256"}
            and type(proof["name"]) is str
            and _NAME.fullmatch(proof["name"]) is not None
            and type(proof["oid"]) is int
            and proof["oid"] > 0
            and type(proof["owner"]) is int
            and proof["owner"] > 0
            and type(proof["layout_sha256"]) is str
            and _HASH.fullmatch(proof["layout_sha256"]) is not None
        )
        names.append(proof["name"])
    _require(len(set(names)) == len(ROOTS))
    _assert_chunk_binding(binding, names)
    last = binding["last_batch"]
    if binding["ordinal"] == 0:
        _require(last is None)
    else:
        _require(
            type(last) is dict
            and set(last) == {"start", "end", "sha256", "counts"}
            and type(last["start"]) is int
            and 1 <= last["start"] <= last["end"] == binding["ordinal"]
            and type(last["sha256"]) is str
            and _HASH.fullmatch(last["sha256"]) is not None
            and type(last["counts"]) is dict
            and set(last["counts"]) == set(ROOTS)
            and all(
                type(count) is int and 0 <= count <= binding["counts"][root] for root, count in last["counts"].items()
            )
            and last["counts"]["source_record"] == last["counts"]["member"] == last["end"] - last["start"] + 1
        )
    if binding["phase"] != "loading":
        _require(binding["validation_sha256"] == _validation_fingerprint(binding))
    return binding


async def _invoke(callback):
    if callback is not None:
        result = callback()
        if inspect.isawaitable(result):
            await result


@asynccontextmanager
async def _transaction(database):
    async with source_policy.source_transaction(database):
        publisher = source_policy.bound_publisher()
        await storage_roles(publisher.connection, publisher.policy.schema, **publisher.policy.role_arguments())
        yield publisher.connection, publisher.policy


async def _header_custody(connection, policy):
    owner, readers = await storage_roles(connection, policy.schema, **policy.role_arguments())
    rows = await connection.fetch(CATALOG_SQL, [storage_relation(policy.schema, HEADER)], owner, readers)
    _require(len(rows) == 1)
    row = rows[0]
    _require(
        row["relowner"] == owner
        and row["relkind"] == "r"
        and row["relpersistence"] == "p"
        and not row["parents"]
        and row["inert"]
        and row["native_checks"]
        and not row["relrowsecurity"]
        and not row["relforcerowsecurity"]
        and row["direct_acl_closed"]
        and row["readers_closed"]
        and tuple(column[0] for column in _json(row["columns"]))
        == ("admission_ref", "source_release_ref", "candidate_storage_json", "created_at", "updated_at")
    )


async def _locked(connection, policy, candidate, *, phases=("loading", "prepared", "published")):
    _require(type(candidate) is NppesEvidenceCandidate)
    await _header_custody(connection, policy)
    row = await connection.fetchrow(
        f"SELECT source_release_ref,candidate_storage_json FROM {storage_relation(policy.schema, HEADER)} "
        "WHERE admission_ref=$1 FOR UPDATE",
        candidate.identity["admission_ref"],
    )
    _require(row is not None and row["source_release_ref"] == candidate.identity["source_release_ref"])
    binding = _binding(row["candidate_storage_json"], candidate.identity, phases=phases)
    _require(binding["policy"] == _policy_fields(policy))
    if binding["phase"] != "published":
        producer = source_policy.source_producer_identity()
        _require(binding["producer_identity"] == candidate.producer_identity == _json(_encoded(producer)))
    return binding


async def _store(connection, schema, previous, updated):
    status = await connection.execute(
        f"UPDATE {storage_relation(schema, HEADER)} SET candidate_storage_json=$2::jsonb,"
        "updated_at=transaction_timestamp() WHERE admission_ref=$1 AND candidate_storage_json=$3::jsonb",
        previous["identity"]["admission_ref"],
        _encoded(updated),
        _encoded(previous),
    )
    _require(status == "UPDATE 1")


async def _close_grants(connection, policy, name, *, readable=False):
    relation = storage_relation(policy.schema, name)
    oid = await connection.fetchval("SELECT to_regclass($1)::oid::bigint", relation)
    for grant in await connection.fetch(_GRANTEES_SQL, oid):
        principal = "PUBLIC" if grant["grantee"] == 0 else storage_identifier(grant["rolname"])
        column = grant["column_name"]
        privileges = f"ALL PRIVILEGES ({storage_identifier(column)})" if column else "ALL PRIVILEGES"
        await connection.execute(f"REVOKE {privileges} ON TABLE {relation} FROM {principal} CASCADE")
    if readable:
        readers = ",".join(storage_identifier(role) for role in policy.runtime_roles)
        await connection.execute(f"GRANT SELECT ON TABLE {relation} TO {readers}")


def _require_native_table(catalog_row, policy, owner, root, readable):
    columns, constraints, indexes = (_json(catalog_row[key]) for key in ("columns", "constraints", "indexes"))
    expected = (
        (*ROOTS[root][1], "created_at")
        if root == "member"
        else (*ROOTS[root][1][:-1], "created_at", ROOTS[root][1][-1])
    )
    _require(
        catalog_row["nspname"] == policy.schema
        and catalog_row["relowner"] == owner
        and catalog_row["relkind"] == "r"
        and catalog_row["relpersistence"] == "p"
        and not catalog_row["parents"]
        and not catalog_row["relispartition"]
        and not catalog_row["relrowsecurity"]
        and not catalog_row["relforcerowsecurity"]
        and catalog_row["inert"]
        and catalog_row["native_checks"]
        and catalog_row["direct_acl_closed"]
        and (not readable or catalog_row["readers_closed"])
        and tuple(column[0] for column in columns) == expected
        and all(column[2] is (column[0] not in _NULLABLE[root]) for column in columns)
        and all(column[3] == ("transaction_timestamp()" if column[0] == "created_at" else None) for column in columns)
        and all(column_item["type"] in {"p", "u"} and column_item["valid"] for column_item in constraints)
    )
    return columns, constraints, indexes


async def _capture(connection, policy, tables, *, indexed, readable=False):
    owner, readers = await storage_roles(connection, policy.schema, **policy.role_arguments())
    catalog_rows = await connection.fetch(
        CATALOG_SQL,
        [storage_relation(policy.schema, name) for name in tables.values()],
        owner,
        readers if readable else [],
    )
    by_name = {catalog_row["relname"]: catalog_row for catalog_row in catalog_rows}
    _require(len(catalog_rows) == len(ROOTS) and set(by_name) == set(tables.values()))
    _require(
        await connection.fetchval(_NATIVE_LAYOUT_SQL, [catalog_row["oid"] for catalog_row in catalog_rows]) is True
    )
    proofs_by_root = {}
    for root, name in tables.items():
        catalog_row = by_name[name]
        columns, constraints, indexes = _require_native_table(catalog_row, policy, owner, root, readable)
        expected_keys = _INDEX_KEYS[root] if indexed else ()
        _require(
            len(indexes) == len(expected_keys)
            and {tuple(index["keys"]) for index in indexes} == set(expected_keys)
            and all(
                index["valid"]
                and index["unique"]
                and index["method"] == "btree"
                and index["predicate"] is None
                and index["expression"] is None
                for index in indexes
            )
            and sum(index["primary"] for index in indexes) == int(indexed)
        )
        layout_by_field = {"columns": columns, "constraints": constraints, "indexes": indexes, "parents": []}
        proofs_by_root[root] = dict(
            name=name, oid=catalog_row["oid"], owner=owner, layout_sha256=_fingerprint(layout_by_field)
        )
    return proofs_by_root


async def _verify(connection, policy, binding):
    tables_by_root = {root: proof["name"] for root, proof in binding["tables"].items()}
    actual = await _capture(
        connection,
        policy,
        tables_by_root,
        indexed=binding["phase"] != "loading",
        readable=binding["phase"] == "published",
    )
    _require(actual == binding["tables"])
    if binding["phase"] != "published":
        _require(
            await _capture_chunks(connection, policy, binding["chunks"]["table"]["name"]) == binding["chunks"]["table"]
        )
    return tables_by_root


async def _capture_chunks(connection, policy, name):
    owner, _ = await storage_roles(connection, policy.schema, **policy.role_arguments())
    relations = await connection.fetch(CATALOG_SQL, [storage_relation(policy.schema, name)], owner, [])
    _require(len(relations) == 1)
    chunk_relation = relations[0]
    columns, constraints, indexes = (_json(chunk_relation[key]) for key in ("columns", "constraints", "indexes"))
    _require(
        chunk_relation["relname"] == name
        and chunk_relation["nspname"] == policy.schema
        and chunk_relation["relowner"] == owner
        and chunk_relation["relkind"] == "r"
        and chunk_relation["relpersistence"] == "p"
        and not chunk_relation["parents"]
        and not chunk_relation["relispartition"]
        and not chunk_relation["relrowsecurity"]
        and not chunk_relation["relforcerowsecurity"]
        and chunk_relation["inert"]
        and chunk_relation["native_checks"]
        and chunk_relation["direct_acl_closed"]
        and chunk_relation["readers_closed"]
        and tuple(column[0] for column in columns) == _CHUNK_COLUMNS
        and tuple(column[1] for column in columns)
        == ("bigint", "bigint", "bigint", "text", "jsonb", *(kind for _root in ROOTS for kind in ("bytea", "text")))
        and all(column[2] and column[3] is None for column in columns)
        and len(constraints) == 1
        and constraints[0]["type"] == "p"
        and constraints[0]["valid"]
        and len(indexes) == 1
        and indexes[0]["keys"] == ["chunk_ordinal"]
        and indexes[0]["primary"]
        and indexes[0]["unique"]
        and indexes[0]["valid"]
        and indexes[0]["method"] == "btree"
        and indexes[0]["predicate"] is None
        and indexes[0]["expression"] is None
    )
    _require(await connection.fetchval(_NATIVE_LAYOUT_SQL, [chunk_relation["oid"]]) is True)
    layout_by_field = {"columns": columns, "constraints": constraints, "indexes": indexes, "parents": []}
    return dict(name=name, oid=chunk_relation["oid"], owner=owner, layout_sha256=_fingerprint(layout_by_field))


async def _create_chunks(connection, policy, name):
    fields = [
        "chunk_ordinal bigint PRIMARY KEY",
        "start_ordinal bigint NOT NULL",
        "end_ordinal bigint NOT NULL",
        "batch_sha256 text NOT NULL",
        "counts_json jsonb NOT NULL",
    ]
    fields.extend(
        column + " " + ("bytea" if column.endswith("_payload") else "text") + " NOT NULL"
        for column in _CHUNK_COLUMNS[5:]
    )
    await connection.execute(f"CREATE TABLE {storage_relation(policy.schema, name)} ({','.join(fields)})")
    await _close_grants(connection, policy, name)
    return await _capture_chunks(connection, policy, name)


async def _create_candidate_binding(connection, policy, identity, producer_identity):
    prefix = "npec_" + uuid4().hex
    tables_by_root = {root: prefix + "_" + fields[3] for root, fields in ROOTS.items()}
    for root, name in tables_by_root.items():
        await connection.execute(
            f"CREATE TABLE {storage_relation(policy.schema, name)} (LIKE "
            f"{storage_relation(policy.schema, ROOTS[root][0])} INCLUDING DEFAULTS INCLUDING STORAGE)"
        )
        await _close_grants(connection, policy, name)
    binding_by_field = dict(
        contract=CONTRACT,
        identity=identity,
        producer_identity=producer_identity,
        policy=_policy_fields(policy),
        phase="loading",
        ordinal=0,
        counts=dict.fromkeys(ROOTS, 0),
        last_batch=None,
        tables=await _capture(connection, policy, tables_by_root, indexed=False),
        chunks=dict(table=await _create_chunks(connection, policy, prefix + "_p"), count=0, sha256=""),
        validation_sha256=None,
    )
    binding_by_field["chunks"]["sha256"] = _chunk_seed(binding_by_field)
    status = await connection.execute(
        f"INSERT INTO {storage_relation(policy.schema, HEADER)} (admission_ref,source_release_ref,candidate_storage_json) "
        "VALUES ($1,$2,$3::jsonb)",
        identity["admission_ref"],
        identity["source_release_ref"],
        _encoded(binding_by_field),
    )
    _require(status == "INSERT 0 1")
    return _candidate(binding_by_field)


async def prepare_nppes_evidence_candidate(database, prepared, *, producer_identity):
    """Resolve an exact published binding or create a producer-bound isolated candidate."""
    identity = _identity(prepared)
    producer_identity = _json(_encoded(producer_identity))
    async with _transaction(database) as (connection, policy):
        _require(producer_identity == _json(_encoded(source_policy.source_producer_identity())))
        await _header_custody(connection, policy)
        select = (
            f"SELECT admission_ref,source_release_ref,candidate_storage_json FROM {storage_relation(policy.schema, HEADER)} "
            "WHERE source_release_ref=$1"
        )
        header_row = await connection.fetchrow(select, identity["source_release_ref"])
        if header_row is not None:
            binding_by_field = _binding(header_row["candidate_storage_json"], identity)
            _require(
                header_row["admission_ref"] == identity["admission_ref"]
                and header_row["source_release_ref"] == identity["source_release_ref"]
                and binding_by_field["policy"] == _policy_fields(policy)
            )
            if binding_by_field["phase"] == "published":
                await _verify(connection, policy, binding_by_field)
                await _assert_control_rows(connection, policy.schema, prepared)
                await _assert_seal(connection, policy.schema, binding_by_field)
                return _candidate(binding_by_field)
        await connection.execute("SELECT pg_advisory_xact_lock(hashtextextended($1,0))", identity["source_release_ref"])
        header_row = await connection.fetchrow(
            select + " FOR UPDATE",
            identity["source_release_ref"],
        )
        if header_row is not None:
            _require(header_row["admission_ref"] == identity["admission_ref"])
            binding_by_field = _binding(header_row["candidate_storage_json"], identity)
            _require(
                binding_by_field["policy"] == _policy_fields(policy)
                and (
                    binding_by_field["phase"] == "published"
                    or binding_by_field["producer_identity"] == producer_identity
                )
            )
            await _verify(connection, policy, binding_by_field)
            if binding_by_field["phase"] == "published":
                await _assert_control_rows(connection, policy.schema, prepared)
                await _assert_seal(connection, policy.schema, binding_by_field)
            return _candidate(binding_by_field)
        return await _create_candidate_binding(connection, policy, identity, producer_identity)


def _record_bytes(record):
    _require(type(record) is tuple)
    size = 2
    for value in record:
        _require(value is None or type(value) in {str, bytes, int, bool, date, datetime})
        size += 4 + (
            0
            if value is None
            else len(value.encode())
            if type(value) is str
            else len(value)
            if type(value) is bytes
            else 8
        )
    _require(size + COPY_FRAME_BYTES <= COPY_MAX_BYTES)
    return size


def _batch(buffers):
    prepared_by_root = {root: list(getattr(buffers, fields[2])) for root, fields in ROOTS.items()}
    payloads = chunk_payloads(prepared_by_root)
    rows_by_root = {
        root: [prepared_row.values for prepared_row in prepared_records]
        for root, prepared_records in prepared_by_root.items()
    }
    counts_by_root = {root: len(prepared_records) for root, prepared_records in rows_by_root.items()}
    _require(
        1 <= counts_by_root["source_record"] == counts_by_root["member"] <= COPY_MAX_ROWS
        and counts_by_root["common"]
        == counts_by_root["source_link"]
        == counts_by_root["typed"]
        <= counts_by_root["member"]
    )
    size = COPY_FRAME_BYTES + 2 * len(ROOTS)
    digest = hashlib.sha256()
    for root, prepared_records in prepared_by_root.items():
        digest.update(root.encode() + b"\0")
        digest.update(len(payloads[root]).to_bytes(8, "big") + payloads[root])
        for prepared_record in prepared_records:
            size += max(_record_bytes(prepared_record.values), len(prepared_record.payload) + 1)
    _require(size <= COPY_MAX_BYTES)
    member_ordinal = MEMBER_COLUMNS.index("source_row_ordinal")
    ordinals = [prepared_record[member_ordinal] for prepared_record in rows_by_root["member"]]
    _require(
        all(type(ordinal_value) is int for ordinal_value in ordinals)
        and ordinals == list(range(ordinals[0], ordinals[0] + len(ordinals)))
    )
    return (
        rows_by_root,
        counts_by_root,
        dict(start=ordinals[0], end=ordinals[-1], sha256=digest.hexdigest(), counts=counts_by_root),
    )


async def _append_chunk(connection, policy, previous, payloads, batch):
    payload_sha256_by_root = {root: hashlib.sha256(payload).hexdigest() for root, payload in payloads.items()}
    metadata = dict(chunk_ordinal=previous["chunks"]["count"] + 1, **batch, payload_sha256=payload_sha256_by_root)
    values = [metadata["chunk_ordinal"], batch["start"], batch["end"], batch["sha256"], _encoded(batch["counts"])]
    values.extend(value for root in ROOTS for value in (payloads[root], payload_sha256_by_root[root]))
    placeholders = ",".join(f"${index}" + ("::jsonb" if index == 5 else "") for index in range(1, len(values) + 1))
    columns = ",".join(storage_identifier(column) for column in _CHUNK_COLUMNS)
    status = await connection.execute(
        f"INSERT INTO {storage_relation(policy.schema, previous['chunks']['table']['name'])} ({columns}) VALUES ({placeholders})",
        *values,
    )
    _require(status == "INSERT 0 1")
    return {
        **previous["chunks"],
        "count": metadata["chunk_ordinal"],
        "sha256": _fingerprint({"previous": previous["chunks"]["sha256"], "chunk": metadata}),
    }


async def _verify_chunk_manifest(connection, policy, binding):
    relation = storage_relation(policy.schema, binding["chunks"]["table"]["name"])
    digest_checks = ",".join(
        f"{root}_sha256,{root}_sha256=pg_catalog.encode(pg_catalog.sha256({root}_payload),'hex') AS {root}_valid"
        for root in ROOTS
    )
    size = "+".join(f"octet_length({root}_payload)" for root in ROOTS)
    query = (
        f"SELECT chunk_ordinal,start_ordinal,end_ordinal,batch_sha256,counts_json,{digest_checks},"
        f"{size}<=$1::bigint AS bounded FROM {relation} ORDER BY chunk_ordinal"
    )
    chain, ordinal, chunk_count = _chunk_seed(binding), 0, 0
    count_by_root, last_batch_by_field = dict.fromkeys(ROOTS, 0), None
    async for chunk_record in connection.cursor(query, COPY_MAX_BYTES, prefetch=64):
        counts = _json(chunk_record["counts_json"])
        _require(
            type(counts) is dict
            and set(counts) == set(ROOTS)
            and all(type(count) is int and 0 <= count <= COPY_MAX_ROWS for count in counts.values())
        )
        _require(
            chunk_record["bounded"] is True
            and chunk_record["chunk_ordinal"] == chunk_count + 1
            and chunk_record["start_ordinal"] == ordinal + 1
            and chunk_record["end_ordinal"] <= binding["ordinal"]
            and counts["source_record"] == counts["member"] == chunk_record["end_ordinal"] - ordinal > 0
            and counts["common"] == counts["source_link"] == counts["typed"] <= counts["member"]
            and type(chunk_record["batch_sha256"]) is str
            and _HASH.fullmatch(chunk_record["batch_sha256"]) is not None
            and all(chunk_record[root + "_valid"] is True for root in ROOTS)
        )
        last_batch_by_field = dict(
            start=chunk_record["start_ordinal"],
            end=chunk_record["end_ordinal"],
            sha256=chunk_record["batch_sha256"],
            counts=counts,
        )
        metadata = dict(
            chunk_ordinal=chunk_record["chunk_ordinal"],
            **last_batch_by_field,
            payload_sha256={root: chunk_record[root + "_sha256"] for root in ROOTS},
        )
        chain = _fingerprint({"previous": chain, "chunk": metadata})
        count_by_root = {root: count_by_root[root] + counts[root] for root in ROOTS}
        ordinal, chunk_count = chunk_record["end_ordinal"], chunk_record["chunk_ordinal"]
    _require(
        count_by_root == binding["counts"]
        and ordinal == binding["ordinal"]
        and chunk_count == binding["chunks"]["count"]
        and chain == binding["chunks"]["sha256"]
        and last_batch_by_field == binding["last_batch"]
    )


async def append_nppes_evidence_batch(database, candidate, buffers, *, cancel_check=None):
    """Append one bounded ordinal batch under the existing candidate write fence."""
    rows_by_root, counts, batch = _batch(buffers)
    payloads = chunk_payloads({root: getattr(buffers, fields[2]) for root, fields in ROOTS.items()})
    async with _transaction(database) as (connection, policy):
        await _invoke(cancel_check)
        previous = await _locked(connection, policy, candidate, phases=("loading",))
        tables = await _verify(connection, policy, previous)
        await _invoke(cancel_check)
        if batch == previous["last_batch"]:
            return _candidate(previous)
        _require(candidate.ordinal == previous["ordinal"] and batch["start"] == previous["ordinal"] + 1)
        for root, prepared_records in rows_by_root.items():
            if prepared_records:
                try:
                    status = await connection.copy_records_to_table(
                        tables[root], schema_name=policy.schema, columns=ROOTS[root][1], records=prepared_records
                    )
                except asyncio.CancelledError, TimeoutError:
                    await close_interrupted_copy(connection)
                    await invalidate_copy_owner(source_policy.source_copy_session(), connection)
                    raise
                _require(status == f"COPY {len(prepared_records)}")
        await _invoke(cancel_check)
        chunks = await _append_chunk(connection, policy, previous, payloads, batch)
        updated_by_field = {
            **previous,
            "ordinal": batch["end"],
            "last_batch": batch,
            "counts": {root: previous["counts"][root] + counts[root] for root in ROOTS},
            "chunks": chunks,
        }
        await _store(connection, policy.schema, previous, updated_by_field)
        return _candidate(updated_by_field)


async def prepare_nppes_evidence_publication(database, candidate, prepared, *, cancel_check=None):
    """Close candidate writes and validate the complete canonical family before publication."""
    from process.nppes_public_evidence_validation import validate_nppes_evidence_family

    _require(candidate.identity == _identity(prepared))
    async with _transaction(database) as (connection, policy):
        await _invoke(cancel_check)
        previous = await _locked(connection, policy, candidate)
        tables = await _verify(connection, policy, previous)
        await _invoke(cancel_check)
        if previous["phase"] != "loading":
            return _candidate(previous)
        expected_counts_by_root = {
            root: (
                prepared.admission_row.source_record_count
                if root in {"source_record", "member"}
                else prepared.admission_row.projected_record_count
            )
            for root in ROOTS
        }
        _require(previous["counts"] == expected_counts_by_root)
        for root, name in tables.items():
            await _close_grants(connection, policy, name)
            for ordinal, key in enumerate(_INDEX_KEYS[root]):
                columns = ",".join(storage_identifier(column) for column in key)
                kind = "PRIMARY KEY" if ordinal == 0 else "UNIQUE"
                await connection.execute(f"ALTER TABLE {storage_relation(policy.schema, name)} ADD {kind} ({columns})")
            await connection.execute(f"ANALYZE {storage_relation(policy.schema, name)}")
        proofs = await _capture(connection, policy, tables, indexed=True)
        await connection.execute(
            "LOCK TABLE "
            + ",".join(
                storage_relation(policy.schema, name)
                for name in (*tables.values(), previous["chunks"]["table"]["name"])
            )
            + " IN SHARE MODE"
        )
        await _verify_chunk_manifest(connection, policy, previous)
        await validate_nppes_evidence_family(
            connection,
            policy.schema,
            tables,
            prepared,
            prepared_chunks=previous["chunks"]["table"]["name"],
            chunk_count=previous["chunks"]["count"],
        )
        await _invoke(cancel_check)
        updated_by_field = {
            **previous,
            "phase": "prepared",
            "tables": proofs,
        }
        updated_by_field["validation_sha256"] = _validation_fingerprint(updated_by_field)
        await _store(connection, policy.schema, previous, updated_by_field)
        return _candidate(updated_by_field)


async def publish_nppes_evidence_candidate(database, candidate, prepared, *, cancel_check=None):
    """Atomically publish verified candidate controls while retaining exact replay identity."""
    from process.nppes_public_evidence_writer import _insert_single

    _require(candidate.identity == _identity(prepared))
    async with _transaction(database) as (connection, policy):
        await _invoke(cancel_check)
        previous = await _locked(connection, policy, candidate, phases=("prepared", "published"))
        tables = await _verify(connection, policy, previous)
        await _invoke(cancel_check)
        if previous["phase"] == "published":
            await _assert_control_rows(connection, policy.schema, prepared)
            await _assert_seal(connection, policy.schema, previous)
            return "already_present"
        _require(previous["validation_sha256"] == _validation_fingerprint(previous))
        release = prepared.manifest.release
        existing_admission = await connection.fetchval(
            f"SELECT EXISTS(SELECT FROM {storage_relation(policy.schema, 'public_evidence_nppes_registry_admission')} "
            "WHERE source_release_ref=$1)",
            candidate.identity["source_release_ref"],
        )
        _require(type(existing_admission) is bool)
        if existing_admission:
            await _assert_control_rows(connection, policy.schema, prepared)
            await _assert_seal(connection, policy.schema, previous)
        for table, columns, control_values, identical in (
            ("public_evidence_source_identity", SOURCE_IDENTITY_COLUMNS, source_identity_values(release), True),
            (
                "public_evidence_source_release",
                SOURCE_RELEASE_COLUMNS,
                source_release_values(release),
                existing_admission,
            ),
            (
                "public_evidence_nppes_registry_admission",
                ADMISSION_COLUMNS,
                admission_values(prepared.admission_row),
                existing_admission,
            ),
            (
                "public_evidence_nppes_registry_admission_seal",
                ("admission_ref",),
                (candidate.identity["admission_ref"],),
                existing_admission,
            ),
        ):
            await _insert_single(connection, policy.schema, table, columns, control_values, allow_identical=identical)
        for name in tables.values():
            await _close_grants(connection, policy, name, readable=True)
        await connection.execute(f"DROP TABLE {storage_relation(policy.schema, previous['chunks']['table']['name'])}")
        updated_by_field = {**previous, "phase": "published", "chunks": {**previous["chunks"], "table": None}}
        await _verify(connection, policy, updated_by_field)
        await _invoke(cancel_check)
        await _store(connection, policy.schema, previous, updated_by_field)
        return "already_present" if existing_admission else "inserted"


async def _assert_control_rows(connection, schema, prepared):
    release = prepared.manifest.release
    for table, columns, values in (
        ("public_evidence_source_identity", SOURCE_IDENTITY_COLUMNS, source_identity_values(release)),
        ("public_evidence_source_release", SOURCE_RELEASE_COLUMNS, source_release_values(release)),
        ("public_evidence_nppes_registry_admission", ADMISSION_COLUMNS, admission_values(prepared.admission_row)),
    ):
        selected = ",".join(storage_identifier(column) for column in columns)
        row = await connection.fetchrow(
            f"SELECT {selected} FROM {storage_relation(schema, table)} WHERE {storage_identifier(columns[0])}=$1",
            values[0],
        )
        _require(row is not None and tuple(row[column] for column in columns) == values)


async def _assert_seal(connection, schema, binding):
    identity = binding["identity"]
    sealed = await connection.fetchval(
        f"SELECT EXISTS(SELECT FROM {storage_relation(schema, 'public_evidence_nppes_registry_admission')} a "
        f"JOIN {storage_relation(schema, 'public_evidence_nppes_registry_admission_seal')} s USING(admission_ref) "
        "WHERE a.admission_ref=$1 AND a.source_release_ref=$2 AND a.contract_sha256=$3 "
        "AND a.source_release_contract_sha256=$4 AND a.source_kind=$5 AND a.manifest_sha256=$6 AND a.artifact_sha256=$7)",
        identity["admission_ref"],
        identity["source_release_ref"],
        bytes.fromhex(identity["admission_contract_sha256"]),
        bytes.fromhex(identity["source_release_contract_sha256"]),
        identity["source_kind"],
        bytes.fromhex(identity["manifest_sha256"]),
        bytes.fromhex(identity["artifact_sha256"]),
    )
    _require(sealed is True)


async def resolve_nppes_evidence_family(connection, schema, admission_ref):
    """Verify the sealed native family and resolve its fixed physical table identities."""
    _require(connection.is_in_transaction())
    row = await connection.fetchrow(
        f"SELECT source_release_ref,candidate_storage_json FROM {storage_relation(schema, HEADER)} WHERE admission_ref=$1",
        admission_ref,
    )
    _require(row is not None)
    binding = _binding(row["candidate_storage_json"], phases=("published",))
    _require(
        binding["identity"]["admission_ref"] == admission_ref
        and binding["identity"]["source_release_ref"] == row["source_release_ref"]
    )
    policy = _stored_policy(binding)
    _require(policy.schema == schema)
    await _header_custody(connection, policy)
    await _verify(connection, policy, binding)
    await _assert_seal(connection, schema, binding)
    return binding["tables"]


async def resolve_nppes_chain_families(connection, schema, chain_ref):
    """Resolve each exact sealed archive family on the caller-owned chain snapshot."""
    _require(connection.is_in_transaction())
    rows = await connection.fetch(
        f"SELECT a.archive_ordinal,a.archive_count,a.admission_ref FROM "
        f"{storage_relation(schema, 'public_evidence_nppes_registry_chain_archive')} a "
        f"JOIN {storage_relation(schema, 'public_evidence_nppes_registry_chain_admission_seal')} s USING(chain_ref) "
        "WHERE a.chain_ref=$1 ORDER BY a.archive_ordinal",
        chain_ref,
    )
    _require(
        bool(rows)
        and len(rows) == rows[0]["archive_count"]
        and all(row["archive_ordinal"] == index and row["archive_count"] == len(rows) for index, row in enumerate(rows))
    )
    return tuple([await resolve_nppes_evidence_family(connection, schema, row["admission_ref"]) for row in rows])
