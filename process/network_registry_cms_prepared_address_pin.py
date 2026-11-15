# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pin the exact admitted address stages on the original SOURCE Reader."""

import json
from contextlib import asynccontextmanager
from dataclasses import dataclass
from weakref import WeakKeyDictionary

from sqlalchemy import text

from db.plan_release_storage import storage_relation
from process.entity_address_snapshot_source import _legacy_source_indexes
from process.network_cms_registry_address_capture import NativeReceiptSession, native_driver
from process.network_cms_registry_address_equivalence import (
    _native_capture,
    capture_registry_cms_address_copy_source,
    validate_registry_cms_address_copy,
)
from process.network_fhir_source_epoch_pin import PinnedCMSFHIRSourceEpochInput, _CopyConnections
from process.provider_directory_generic_source_policy import bound_publisher, source_copy_session, source_transaction

_INPUTS = WeakKeyDictionary()


def _require(condition):
    if not condition:
        raise ValueError("registry_cms_prepared_address_pin_invalid")


async def prepared_index_recipes(session, stages, *, schema):
    """Resolve current index DDL at finite names, discarding local catalog handles."""
    await session.execute(text("SET LOCAL search_path=pg_catalog"))
    recipes_by_logical = {}
    for logical, name in stages:
        local_handle = await session.scalar(
            text("SELECT to_regclass(:relation)::oid"),
            {"relation": storage_relation(schema, name)},
        )
        indexes = await _legacy_source_indexes(session, {"relation_oid": local_handle})
        recipes_by_logical[logical] = [
            {key: value for key, value in index.items() if key != "index_oid"} for index in indexes
        ]
    return recipes_by_logical


def _column_recipes(native, layouts):
    columns_by_logical = {
        logical: table["columns"] for logical, table in json.loads(native[1])["logical_catalog"].items()
    }
    for logical, layout in layouts.items():
        attributes_by_name = {
            attribute["attname"]: attribute
            for attribute in json.loads(layout.catalog_json)["attributes"]
            if attribute["relation_kind"] == "main"
        }
        for column in columns_by_logical[logical]:
            column["storage"] = attributes_by_name[column["attname"]]["attstorage"]
            column["compression"] = attributes_by_name[column["attname"]]["attcompression"]
    return columns_by_logical


@dataclass(frozen=True, eq=False)
class PinnedRegistryCMSAddressInput:
    """Leased named source inputs and bounded COPY progress for one prepared family."""

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
        """Use the configured source reader for native exports."""
        return self.source_input.connection

    def _issued(self):
        progress = _INPUTS.get(self)
        _require(progress is not None)
        return progress

    def require_active(self):
        """Retain the input lease and complete declared prepared-name selection."""
        progress = self._issued()
        _require(progress["active"])
        self.source_input.require_active()
        _require(
            not self.prepared.address.committed and tuple(sorted(self.prepared.address.stage_relations)) == self.stages
        )
        self.prepared.nonprofile_admission._assert_lease()

    async def verify_transaction(self):
        """Use the current native source transaction without a connection identity token."""
        self.require_active()
        await self.source_input.verify_transaction()

    async def _require_target_transaction(self):
        """Use actual native target access without a cached owner-authorization flag."""
        binding = self.prepared.fhir.db._transaction_binding()
        _require(binding is not None)
        connection = await native_driver(binding.session)
        _require(connection.is_in_transaction())

    async def register_relation(self, annotation, admission):
        """Register finite target names and the existing complete layout/lease contract."""
        self.require_active()
        await self._require_target_transaction()
        _require(admission is self.prepared.nonprofile_admission)
        key = (annotation.schema, annotation.relation)
        _require(
            annotation.phase == "loading"
            and annotation.source_layout == dict(self.layouts).get(annotation.relation)
            and annotation.policy_json == self.policy_json
            and key not in self._issued()["relations"]
        )
        self._issued()["relations"][key] = annotation

    async def advance_relation(self, previous, annotation):
        """Advance logical phase only after preserving complete names, layout and lease."""
        self.require_annotation(previous)
        await self.verify_transaction()
        await self._require_target_transaction()
        _require((previous.phase, annotation.phase) in {("loading", "indexing"), ("indexing", "finished")})
        _require(_annotation_frame(previous) == _annotation_frame(annotation))
        self._issued()["relations"][(annotation.schema, annotation.relation)] = annotation

    def require_annotation(self, annotation, *, final=False):
        """Require registered logical progress; constructed equal records carry no extra authority."""
        progress = self._issued()
        registered = progress["relations"].get((annotation.schema, annotation.relation))
        _require(
            registered is not None
            and _annotation_frame(registered) == _annotation_frame(annotation)
            and registered.phase == annotation.phase
            and annotation.phase in {"loading", "indexing", "finished"}
        )
        if final or not progress["active"]:
            _require(progress["complete"] and annotation.phase == "finished")
        else:
            self.require_active()

    async def finish_copy(self, session, ownership, receipt):
        """Complete only after all seven named clones pass full content/layout validation."""
        self.require_active()
        await self._require_target_transaction()
        progress = self._issued()
        annotations = tuple(progress["relations"].values())
        _require(
            len(annotations) == 7
            and {entry.relation for entry in annotations} == {name for name, _stage in self.stages}
            and all(entry.phase == "finished" and entry.schema == ownership.schema_name for entry in annotations)
            and set(ownership.relation_names) == {entry.relation for entry in annotations}
        )
        witness = await validate_registry_cms_address_copy(
            session, source_capture=self.source_capture, clone_ownership=ownership
        )
        _require(witness.clone_receipt == receipt)
        progress["complete"] = True

    def require_complete(self):
        """Refuse commit orchestration with an incomplete cloned family."""
        _require(self._issued()["complete"])

    def copy_connections(self, target):
        """Reuse the native bounded spool and configured actual drivers."""
        self.require_active()
        return _CopyConnections(self, target)


async def _require_named_source(connection, policy, stages):
    """Use real native SELECT and complete structural layout at the selected names."""
    from process.network_cms_registry_address_equivalence import _require_native_layout

    session = NativeReceiptSession(connection)
    for _logical, name in stages:
        await connection.execute("SELECT 1 FROM " + storage_relation(policy.schema, name) + " LIMIT 0")
        await _require_native_layout(session, policy.schema, name)


async def _pin_reader(source_input, policy, stages, native):
    driver = source_input.connection
    await source_input.verify_transaction()
    await driver.execute(
        "LOCK TABLE "
        + ",".join(storage_relation(policy.schema, name) for _logical, name in stages)
        + " IN ACCESS SHARE MODE NOWAIT"
    )
    captured = await _native_capture(
        NativeReceiptSession(driver),
        policy.schema,
        tuple(logical for logical, _name in stages),
        verify_content=False,
        stage_table_names=dict(stages),
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
    _require(publisher.policy.schema == prepared.address.db_schema == policy.schema)
    session = source_copy_session()
    stages = await address.lock_prepared_address(session, prepared.address)
    layouts, policy_json = await address._prepared_native_sources(prepared, stages)
    await _require_named_source(publisher.connection, policy, stages)
    native = await _native_capture(
        session,
        policy.schema,
        tuple(logical for logical, _stage in stages),
        verify_content=False,
        stage_table_names=dict(stages),
    )
    _require(
        native[1:] == (finalized.catalog_json, finalized.logical_sha256, finalized.ordinals, finalized.sequence_json)
    )
    indexes_by_logical = await prepared_index_recipes(session, stages, schema=policy.schema)
    await _pin_reader(source_input, policy, stages, native)
    await _require_named_source(source_input.connection, policy, stages)
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
        expected_relation_names=tuple(logical for logical, _stage in stages),
        stage_table_names=dict(stages),
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
    _INPUTS[issued] = dict(active=True, complete=False, relations={})
    try:
        yield issued
        issued.require_complete()
    except BaseException:
        del _INPUTS[issued]
        raise
    finally:
        if issued in _INPUTS:
            _INPUTS[issued]["active"] = False


def _annotation_frame(annotation):
    return annotation.schema, annotation.relation, annotation.policy_json, annotation.source_layout
