# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pin the exact admitted address stages on the original SOURCE Reader."""

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import dataclass
from weakref import WeakKeyDictionary, ref

from sqlalchemy import text

from db.plan_release_storage import storage_relation
from process.entity_address_snapshot_source import _legacy_source_indexes
from process.network_cms_registry_address_capture import NativeReceiptSession, native_driver
from process.network_cms_registry_address_equivalence import (
    _native_capture,
    capture_registry_cms_address_copy_source,
    validate_registry_cms_address_copy,
)
from process.network_fhir_source_epoch_pin import PinnedCMSFHIRSourceEpochInput, _CopyConnections, _virtual_xid
from process.provider_directory_generic_source_policy import bound_publisher, source_copy_session, source_transaction

_INPUTS = WeakKeyDictionary()


def _require(condition):
    if not condition:
        raise ValueError("registry_cms_prepared_address_pin_invalid")


async def prepared_index_recipes(session, stages):
    """Capture qualified native headers without rewriting index expressions."""
    await session.execute(text("SET LOCAL search_path=pg_catalog"))
    return {logical: await _legacy_source_indexes(session, {"relation_oid": oid}) for logical, _, oid in stages}


def _column_recipes(native, layouts):
    columns_by_logical = {
        logical: table["columns"] for logical, table in json.loads(native[1])["logical_catalog"].items()
    }
    for logical, layout in layouts.items():
        attributes_by_name = {
            attribute["attname"]: attribute
            for attribute in json.loads(layout.catalog_json)["attributes"]
            if attribute["relation_oid"] == layout.oid
        }
        for column in columns_by_logical[logical]:
            column["storage"] = attributes_by_name[column["attname"]]["attstorage"]
            column["compression"] = attributes_by_name[column["attname"]]["attcompression"]
    return columns_by_logical


@dataclass(frozen=True, eq=False)
class PinnedRegistryCMSAddressInput:
    """Only the issuing task and still-open original Reader may export this family."""

    prepared: object
    source_input: object
    source_capture: object
    stages: tuple
    layouts: tuple
    policy_json: str
    indexes_json: str
    columns_json: str

    @property
    def connection(self):
        """Keep native exports on the already configured SOURCE Reader."""
        return self.source_input.connection

    def require_active(self):
        """Refuse forged, expired, changed-admission or cross-task handles."""
        original = self._issued()
        _require(original["active"])
        self.source_input.require_active()
        _require(not self.prepared.address.committed and tuple(sorted(self.prepared.address.stage_oids)) == self.stages)
        self.prepared.nonprofile_admission._assert_lease()

    def _issued(self):
        original = _INPUTS.get(self)
        _require(original is not None and original["task"]() is asyncio.current_task())
        _require(
            self.prepared is original["prepared"]()
            and self.source_input is original["source_input"]
            and self.prepared.nonprofile_admission is original["admission"]()
        )
        return original

    async def verify_transaction(self):
        """A committed and reopened Reader cannot reuse its physical pins."""
        self.require_active()
        await self.source_input.verify_transaction()

    async def require_target(self, connection, owner_role):
        """Retain the existing distinct-database/actor and no-SourceOwner checks."""
        self.require_active()
        await self.source_input.require_target(connection, owner_role)
        original = self._issued()
        target = (connection, await _virtual_xid(connection))
        _require(original["target"] is None or original["target"] == target)
        original["target"] = target
        owners = await connection.fetchrow(
            "SELECT current_user::regrole::oid::bigint AS creator,"
            "(SELECT oid::bigint FROM pg_roles WHERE rolname=$1) AS retained",
            owner_role,
        )
        _require(all(type(owners[field]) is int for field in ("creator", "retained")))
        original["owners"] = (owners["creator"], owners["retained"])

    async def register_relation(self, annotation, admission):
        """Bind one newly created heap to this actual Reader and original Ret transaction."""
        self.require_active()
        original = self._issued()
        _require(admission is original["admission"]() and original["target"] is not None)
        await self._require_original_target()
        _require(
            annotation.source_input is self
            and annotation.phase == "loading"
            and annotation.source_layout == dict(self.layouts).get(annotation.relation)
            and annotation.policy_json == self.policy_json
            and (annotation.schema, annotation.relation) not in original["relations"]
        )
        original["relations"][(annotation.schema, annotation.relation)] = ref(annotation)

    async def _require_original_target(self):
        original = self._issued()
        _require(original["target"] is not None)
        binding = self.prepared.fhir.db._transaction_binding()
        _require(binding is not None)
        connection, transaction = original["target"]
        _require(await native_driver(binding.session) is connection)
        _require(await _virtual_xid(connection) == transaction)

    async def advance_relation(self, previous, annotation):
        """Advance only the original registered heap, never replace its physical identity."""
        self.require_annotation(previous)
        await self.verify_transaction()
        await self._require_original_target()
        original = self._issued()
        _require((previous.phase, annotation.phase) in {("loading", "indexing"), ("indexing", "finished")})
        _require(
            annotation.source_input is self
            and (
                annotation.schema,
                annotation.relation,
                annotation.oid,
                annotation.policy_json,
                annotation.source_layout,
            )
            == (previous.schema, previous.relation, previous.oid, previous.policy_json, previous.source_layout)
        )
        original["relations"][(annotation.schema, annotation.relation)] = ref(annotation)

    def require_annotation(self, annotation, *, final=False):
        """A constructor or equal-valued annotation cannot grant native SOURCE authority."""
        original = self._issued()
        registered = original["relations"].get((annotation.schema, annotation.relation))
        _require(registered is not None and registered() is annotation)
        _require(annotation.source_input is self and annotation.phase in {"loading", "indexing", "finished"})
        if final or not original["active"]:
            _require(original["complete"] and annotation.phase == "finished")
        else:
            self.require_active()

    def require_target_owner(self, owner):
        """Require the original creator or the exact retained owner selected before COPY."""
        _require(owner in self._issued()["owners"])

    async def finish_copy(self, session, ownership, receipt):
        """Keep the immutable source proof only after all seven cloned contents pass."""
        self.require_active()
        await self._require_original_target()
        original = self._issued()
        annotations = tuple(reference() for reference in original["relations"].values())
        _require(
            len(annotations) == 7
            and all(entry is not None for entry in annotations)
            and {entry.relation for entry in annotations} == {name for name, _, _ in self.stages}
            and all(entry.phase == "finished" for entry in annotations)
        )
        witness = await validate_registry_cms_address_copy(
            session, source_capture=self.source_capture, clone_ownership=ownership
        )
        _require(witness.clone_receipt == receipt)
        _require(dict(ownership.relation_oids) == {entry.relation: entry.oid for entry in annotations})
        original["complete"] = True

    def require_complete(self):
        """Reject an incomplete family before the retained owner's outer commit."""
        _require(self._issued()["complete"])

    def copy_connections(self, target):
        """Reuse the bounded native spool with separate export and import drivers."""
        self.require_active()
        _require(target is not self.connection)
        return _CopyConnections(self, target)


async def _custody(connection, policy, stages, reader):
    """Require inert Source-owned heaps and the trusted Reader's existing read rights."""
    catalog_entries = await connection.fetch(
        """SELECT c.oid::bigint,n.nspname,c.relname,c.relowner::bigint,c.relkind::text,
        c.relpersistence::text,c.relrowsecurity,c.relforcerowsecurity,
        EXISTS(SELECT FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid) AS inherited,
        EXISTS(SELECT FROM pg_trigger WHERE tgrelid=c.oid) AS hooks,
        EXISTS(SELECT FROM pg_rewrite WHERE ev_class=c.oid) AS rules,
        has_table_privilege($4::name,c.oid,'SELECT') AS reader_select,
        NOT EXISTS(SELECT FROM aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a
          WHERE a.grantee<>c.relowner AND (a.grantee<>ALL($3::oid[])
            OR a.privilege_type<>'SELECT' OR a.is_grantable))
        AND NOT EXISTS(SELECT FROM pg_attribute col,LATERAL aclexplode(col.attacl) a
          WHERE col.attrelid=c.oid AND a.grantee<>c.relowner
            AND (a.grantee<>ALL($3::oid[]) OR a.privilege_type<>'SELECT' OR a.is_grantable)) AS closed
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE c.oid=ANY($1::oid[]) AND n.nspname=$2 ORDER BY c.oid""",
        [oid for _, _, oid in stages],
        policy.schema,
        [await connection.fetchval("SELECT oid FROM pg_roles WHERE rolname=$1", role) for role in policy.runtime_roles],
        reader,
    )
    owner = await connection.fetchval("SELECT oid FROM pg_roles WHERE rolname=$1", policy.owner_role)
    expected_identities = {(name, oid) for _, name, oid in stages}
    _require(
        {(catalog_entry["relname"], catalog_entry["oid"]) for catalog_entry in catalog_entries} == expected_identities
    )
    _require(
        all(
            catalog_entry["relowner"] == owner
            and catalog_entry["relkind"] == "r"
            and catalog_entry["relpersistence"] == "p"
            and catalog_entry["reader_select"]
            and catalog_entry["closed"]
            and not any(
                catalog_entry[field]
                for field in ("relrowsecurity", "relforcerowsecurity", "inherited", "hooks", "rules")
            )
            for catalog_entry in catalog_entries
        )
    )


async def _pin_reader(source_input, policy, stages, native):
    driver = source_input.connection
    await source_input.verify_transaction()
    await driver.execute(
        "LOCK TABLE "
        + ",".join(storage_relation(policy.schema, name) for _, name, _ in stages)
        + " IN ACCESS SHARE MODE NOWAIT"
    )
    locked = await driver.fetch(
        "SELECT relation::bigint FROM pg_locks WHERE pid=pg_backend_pid() AND granted "
        "AND locktype='relation' AND mode='AccessShareLock' AND relation=ANY($1::oid[])",
        [oid for _, _, oid in stages],
    )
    _require({row["relation"] for row in locked} == {oid for _, _, oid in stages})
    captured = await _native_capture(
        NativeReceiptSession(driver),
        policy.schema,
        tuple((logical, oid) for logical, _, oid in stages),
        verify_content=False,
        stage_table_names={logical: name for logical, name, _ in stages},
    )
    _require(captured == native)


async def _capture(prepared, source_input):
    from process import network_registry_cms_prepared_address as address
    from process.provider_directory_cms_preparation import _ACTIVE, NonprofileAdmission

    admission = prepared.nonprofile_admission
    _require(type(admission) is NonprofileAdmission and _ACTIVE.get() is admission and admission._started)
    admission._assert_lease()
    publisher = bound_publisher()
    policy = source_input._receipt()["policy"]
    from process.entity_address_source_preparation import require_source_generation

    finalized = require_source_generation(prepared.address, admission, policy=policy)
    _require(publisher.policy is policy and prepared.address.db_schema == policy.schema)
    session = source_copy_session()
    stages = await address.lock_prepared_address(session, prepared.address)
    layouts, policy_json = await address._prepared_native_sources(prepared, stages)
    reader = await source_input.connection.fetchval("SELECT current_user::text")
    _require(reader in policy.runtime_roles and reader not in policy.control_runtime_roles)
    await _custody(publisher.connection, policy, stages, reader)
    from process.mrf_address_publication import require_native_read_catalog

    await require_native_read_catalog(
        session, tuple(oid for _, _, oid in stages), executable_roles=(policy.owner_role, policy.publisher_role)
    )
    native = await _native_capture(
        session,
        policy.schema,
        tuple((logical, oid) for logical, _, oid in stages),
        verify_content=False,
        stage_table_names={logical: name for logical, name, _ in stages},
    )
    _require(
        native[1:] == (finalized.catalog_json, finalized.logical_sha256, finalized.ordinals, finalized.sequence_json)
    )
    indexes_by_logical = await prepared_index_recipes(session, stages)
    await _pin_reader(source_input, policy, stages, native)
    await _custody(source_input.connection, policy, stages, reader)
    return stages, tuple(sorted(layouts.items())), policy_json, indexes_by_logical, _column_recipes(native, layouts)


@asynccontextmanager
async def pinned_prepared_registry_address(prepared, source_input):
    """Authenticate before RetPub starts, then scan only the existing original Reader."""
    _require(type(source_input) is PinnedCMSFHIRSourceEpochInput)
    await source_input.verify_transaction()
    _require(prepared.fhir.db._transaction_binding() is None)
    async with source_transaction(prepared.fhir.db, isolation="repeatable read"):
        stages, layouts, policy_json, indexes, columns = await _capture(prepared, source_input)
    source_capture = await capture_registry_cms_address_copy_source(
        NativeReceiptSession(source_input.connection),
        source_schema=prepared.address.db_schema,
        expected_relation_oids=tuple((logical, oid) for logical, _, oid in stages),
        stage_table_names={logical: name for logical, name, _ in stages},
    )
    from process.entity_address_source_preparation import require_source_generation

    _require(source_capture == require_source_generation(prepared.address, prepared.nonprofile_admission))
    issued = PinnedRegistryCMSAddressInput(
        prepared,
        source_input,
        source_capture,
        stages,
        layouts,
        policy_json,
        json.dumps(indexes, sort_keys=True),
        json.dumps(columns, sort_keys=True),
    )
    _INPUTS[issued] = dict(
        task=ref(asyncio.current_task()),
        prepared=ref(prepared),
        source_input=source_input,
        admission=ref(prepared.nonprofile_admission),
        active=True,
        complete=False,
        target=None,
        relations={},
    )
    try:
        yield issued
        issued.require_complete()
    except BaseException:
        del _INPUTS[issued]
        raise
    finally:
        if issued in _INPUTS:
            _INPUTS[issued]["active"] = False
            _INPUTS[issued]["target"] = None
