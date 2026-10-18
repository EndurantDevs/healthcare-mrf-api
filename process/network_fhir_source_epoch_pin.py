# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""An original Reader pins a locally published CMS candidate before retention."""

import asyncio
import copy
import sys
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from weakref import WeakKeyDictionary

from db.connection import Database
from db.plan_release_storage import storage_relation
from process import network_fhir_source_custody as custody
from process import network_fhir_source_epoch as epoch
from process import provider_directory_generic_source_storage as storage
from process.provider_directory_dataset_rehydrate_pin import _close_reader, _require_reader, _same_database
from process.provider_directory_generic_source_policy import (
    bound_publisher,
    source_transaction,
    source_transaction_available,
)
from process.provider_directory_source_cms_epoch_control import (
    capture_source_cms_epoch_controls,
    cms_epoch_control_receipts,
)
from process.provider_directory_source_coverage_control import capture_source_coverage_control

_INPUTS = WeakKeyDictionary()


def _require(condition):
    if not condition:
        epoch._fail()


class _CopyConnections:
    """The retained savepoint owns import; the original Reader owns export."""

    def __init__(self, source, target):
        self.source, self.target = source, target

    def transaction(self):
        """Keep every import in the retained writer's original savepoint."""
        return self.target.transaction()

    async def copy_from_query(self, *args, **kwargs):
        """Export only on the original active SOURCE Reader."""
        await self.source.verify_transaction()
        return await self.source.connection.copy_from_query(*args, **kwargs)

    async def copy_to_table(self, *args, **kwargs):
        """Import only on the retained target connection."""
        await self.source.verify_transaction()
        return await self.target.copy_to_table(*args, **kwargs)


@dataclass(eq=False, frozen=True)
class PinnedCMSFHIRSourceEpochInput:
    """Only the issuing context, original task and open Reader may use this input."""

    source_pin: object
    admission_sha256: str
    metadata_sha256: str
    connection: object

    def _receipt(self):
        receipt = _INPUTS.get(self)
        _require(
            receipt is not None
            and receipt["task"] is asyncio.current_task()
            and receipt["connection"] is self.connection
            and self.connection.is_in_transaction()
            and receipt["identity"] == (self.source_pin, self.admission_sha256, self.metadata_sha256)
        )
        return receipt

    def require_active(self):
        """Reject forged, expired, cross-task or changed native input handles."""
        self._receipt()

    async def verify_transaction(self):
        """A committed and reopened Reader cannot reuse the original physical pins."""
        receipt = self._receipt()
        _require(await _virtual_xid(self.connection) == receipt["virtual_xid"])

    def relation(self, table):
        """Return only a fixed root's authenticated pinned native relation."""
        return self._receipt()["relations"][table]

    def query(self, query):
        """Substitute closed children in the fixed eleven-family projection."""
        receipt = self._receipt()
        return epoch._source_query(query, epoch._identifier(self.source_pin.schema_name), receipt["relations"])

    def columns(self, table):
        """Return admitted built-in column DDL without defaults or hooks."""
        return self._receipt()["columns"][table]

    def copy_connections(self, target):
        """Pair the existing bounded spool with its distinct retained writer."""
        _require(target is not self.connection)
        self.require_active()
        return _CopyConnections(self, target)

    async def require_target(self, connection, owner_role):
        """Reject SourceOwner reachability and a different physical database."""
        receipt = self._receipt()
        await self.verify_transaction()
        _require(owner_role != receipt["policy"].owner_role and connection is not self.connection)
        actor = await connection.fetchrow(
            "SELECT session_user::text AS login,current_user::text AS actor,"
            "pg_has_role(session_user,$1,'USAGE') OR pg_has_role(session_user,$1,'SET') AS source_owner",
            receipt["policy"].owner_role,
        )
        _require(actor["login"] == actor["actor"] and not actor["source_owner"])
        await _same_database(self.connection, connection)


async def _virtual_xid(connection):
    value = await connection.fetchval(
        "SELECT virtualxid FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='virtualxid' AND granted"
    )
    _require(type(value) is str and bool(value))
    return value


async def _header(connection, pin, *, lock=False):
    header = storage_relation(pin.schema_name, storage.HEADER)
    record = await connection.fetchrow(
        f"SELECT endpoint_id,acquisition_root_run_id,dataset_hash,status,is_current,candidate_storage_json,"
        "content_proof_admission_sha256,publication_metadata_sha256,"
        "octet_length(to_jsonb(head)::text)::bigint AS native_bytes,"
        "encode(sha256(convert_to(to_jsonb(head)::text,'UTF8')),'hex') AS row_sha256 "
        f"FROM {header} head WHERE dataset_id=$1 AND octet_length(to_jsonb(head)::text)<=$2"
        + (" FOR UPDATE" if lock else ""),
        pin.dataset_id,
        epoch.MAX_ROW_BYTES,
    )
    _require(
        record is not None
        and (record["endpoint_id"], record["dataset_hash"]) == (pin.endpoint_id, pin.dataset_sha256)
        and record["status"] in {"validated", "published", "superseded"}
        and record["native_bytes"] <= epoch.MAX_ROW_BYTES
    )
    return dict(record)


async def _control_custody(connection, policy):
    """Use the actual fixed admin-adopted controls, without a legacy owner fallback."""
    coverage = await capture_source_coverage_control(connection, policy)
    controls = await capture_source_cms_epoch_controls(connection, policy)
    return epoch._hash((coverage, cms_epoch_control_receipts(controls)))


async def _control_rows(connection, pin):
    """Only native hashes/counts leave the four bounded checkpoint families."""
    receipts = []
    for root in (epoch._TABLES[0], *epoch._TABLES[8:]):
        relation = storage_relation(pin.schema_name, root)
        record = await connection.fetchrow(
            "WITH rows AS MATERIALIZED (SELECT to_jsonb(head)::text AS body "
            f"FROM {relation} head WHERE dataset_id=$1 LIMIT 17) "
            "SELECT count(*)::bigint AS count,COALESCE(sum(octet_length(body)),0)::bigint AS bytes,"
            "encode(sha256(convert_to(COALESCE(string_agg(body,'' ORDER BY body COLLATE \"C\"),''),'UTF8')),'hex') AS sha "
            "FROM rows",
            pin.dataset_id,
        )
        _require(record["count"] <= 16 and record["bytes"] <= epoch.MAX_ROW_BYTES)
        receipts.append((root, dict(record)))
    return tuple(receipts)


async def _columns(connection, relations):
    """Copy native built-in column types and nullability, never source defaults/hooks."""
    ddl_by_root = {}
    for root, relation in relations.items():
        rows = await connection.fetch(
            "SELECT a.attname,pg_catalog.format_type(a.atttypid,a.atttypmod) AS type,a.attnotnull,"
            "a.attgenerated::text AS attgenerated,a.attidentity::text AS attidentity,"
            "n.nspname,a.attcollation=t.typcollation AS native_collation "
            "FROM pg_attribute a JOIN pg_type t ON t.oid=a.atttypid "
            "JOIN pg_namespace n ON n.oid=t.typnamespace "
            "WHERE a.attrelid=to_regclass($1) AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum",
            relation,
        )
        _require(
            rows
            and all(
                row["nspname"] == "pg_catalog"
                and not row["attgenerated"]
                and not row["attidentity"]
                and row["native_collation"]
                for row in rows
            )
        )
        ddl_by_root[root] = ",".join(
            epoch._identifier(row["attname"]) + " " + row["type"] + (" NOT NULL" if row["attnotnull"] else "")
            for row in rows
        )
    return ddl_by_root


async def _capture(publisher, pin, admission_sha256, metadata_sha256):
    connection, policy = publisher.connection, publisher.policy
    _require(policy.schema == pin.schema_name)
    header = await _header(connection, pin, lock=True)
    _require(
        (header["content_proof_admission_sha256"], header["publication_metadata_sha256"])
        == (admission_sha256, metadata_sha256)
    )
    native = await custody._generic_partitioned_custody(
        connection, pin, policy.owner_role, policy.runtime_roles, source_policy=policy
    )
    binding = native["binding"]
    _require(binding["identity"]["acquisition_root_run_id"] == header["acquisition_root_run_id"])
    relations_by_root = {root: storage_relation(policy.schema, root) for root in epoch._TABLES}
    relations_by_root.update(
        {
            root: storage_relation(policy.schema, binding["tables"][root]["name"])
            for root in (storage.RESOURCE, storage.WITNESS, storage.RELATIONSHIP)
        }
    )
    return policy, header, copy.deepcopy(native), relations_by_root, await _control_custody(connection, policy)


async def _pin(driver, policy, native, relations):
    locks = sorted(
        set(relations.values())
        | {storage_relation(policy.schema, proof["name"]) for proof in native["binding"]["tables"].values()}
    )
    await driver.execute("LOCK TABLE " + ",".join(locks) + " IN ACCESS SHARE MODE NOWAIT")
    await storage._verify_tables(driver, policy, native["binding"])
    from process.provider_directory_generic_source_provision import verify_generic_source_family

    captured_by_name = {record["relname"]: record for record in native["family"]}
    observed_by_name = {
        record["relname"]: dict(record) for record in await verify_generic_source_family(driver, policy)
    }
    _require(all(observed_by_name[root] == captured_by_name[root] for root in storage.CMS_IDENTITY_ROOTS))
    oids = await driver.fetch(
        "SELECT name,to_regclass(name)::oid::bigint AS oid FROM unnest($1::text[]) n(name)", locks
    )
    _require(
        await driver.fetchval(
            "SELECT count(*)=$2 FROM pg_locks WHERE pid=pg_backend_pid() AND granted "
            "AND mode='AccessShareLock' AND relation=ANY($1::oid[])",
            [row["oid"] for row in oids],
            len(oids),
        )
    )


@asynccontextmanager
async def pinned_cms_source_epoch(directory, source_pin, *, expected_admission_sha256, expected_metadata_sha256):
    """Release SourceOwner before any retained transaction or native payload copy."""
    epoch._origin(source_pin)
    _require(
        directory is sys.modules.get("process.provider_directory_fhir")
        and isinstance(directory.db, Database)
        and directory._schema() == source_pin.schema_name
        and directory.db._transaction_binding() is None
        and not directory.db.has_reader_session()
        and source_transaction_available() is None
        and epoch._sha(expected_admission_sha256)
        and epoch._sha(expected_metadata_sha256)
    )
    async with AsyncExitStack() as cleanup:
        async with source_transaction(directory.db, isolation="repeatable read"):
            publisher = bound_publisher()
            policy, header, native, relations, controls = await _capture(
                publisher, source_pin, expected_admission_sha256, expected_metadata_sha256
            )
            reader_database = await directory.db._connect_reader()
            session = reader_database.session_factory()
            cleanup.push_async_callback(_close_reader, session)
            driver = await _require_reader(session, reader_database, policy)
            await _same_database(publisher.connection, driver)
            await _pin(driver, policy, native, relations)
            _require(await _header(driver, source_pin) == header)
            _require(await _control_custody(driver, policy) == controls)
            _require(await _control_rows(driver, source_pin) == await _control_rows(publisher.connection, source_pin))
        proof = await epoch._require_admitted_candidate(driver, source_pin, relations=relations)
        _require(
            (proof["admission_sha256"], proof["metadata_sha256"])
            == (expected_admission_sha256, expected_metadata_sha256)
        )
        for table in epoch._TABLES:
            await epoch._require_epoch_copy_key(driver, source_pin, table, relation=relations[table])
        columns = await _columns(driver, relations)
        issued = PinnedCMSFHIRSourceEpochInput(source_pin, expected_admission_sha256, expected_metadata_sha256, driver)
        _INPUTS[issued] = dict(
            task=asyncio.current_task(),
            connection=driver,
            policy=policy,
            relations=relations,
            columns=columns,
            virtual_xid=await _virtual_xid(driver),
            identity=(source_pin, expected_admission_sha256, expected_metadata_sha256),
        )
        cleanup.callback(_INPUTS.pop, issued)
        yield issued
