# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named retained metadata preserves logical scope without probing source rows."""

import asyncio
import copy
from dataclasses import replace

import pytest

from api import provider_directory_retained_fhir_metadata as metadata
from process.network_fhir_retained_capture_contract import (
    TABLES,
    RetainedFHIRCaptureError,
    canonical,
    capture_digest,
    validate_capture,
)
from tests.test_network_fhir_retained_capture import pin, retained


def _relation_columns():
    """Describe every native column in the synthetic retained metadata catalog."""
    return {
        TABLES[0]: [("dataset_id", "character varying(96)", True), ("publication_metadata_json", "json", False)],
        TABLES[1]: [
            ("dataset_id", "character varying(96)", True),
            ("resource_type", "character varying(64)", True),
            ("resource_id", "character varying(256)", True),
            ("payload_hash", "character varying(64)", True),
            ("payload_json", "json", True),
        ],
        TABLES[2]: [
            ("source_id", "character varying(64)", True),
            ("resource_type", "character varying(16)", True),
            ("resource_id", "character varying(256)", True),
            ("organization_id", "uuid", False),
            ("site_id", "uuid", False),
            ("created_at", "timestamp with time zone", True),
        ],
        TABLES[3]: [
            ("source_id", "character varying(64)", True),
            ("resource_type", "character varying(16)", True),
            ("resource_id", "character varying(256)", True),
            ("release_id", "character varying(256)", True),
            ("payload_sha256", "character varying(64)", True),
            ("payload_json", "jsonb", True),
            ("observed_at", "timestamp with time zone", True),
        ],
        TABLES[4]: [
            ("source_id", "character varying(64)", True),
            ("release_id", "character varying(256)", True),
            ("network_resource_type", "character varying(16)", True),
            ("network_resource_id", "character varying(256)", True),
            ("insurance_plan_resource_id", "character varying(256)", True),
            ("network_refs", "jsonb", True),
            ("owned_by_ref", "text", False),
            ("administered_by_ref", "text", False),
            ("plan_payload_sha256", "character varying(64)", True),
            ("plan_payload_json", "jsonb", True),
            ("observed_at", "timestamp with time zone", True),
        ],
    }


def _relations(capture):
    keys_by_table = {
        TABLES[0]: [("dataset_id",)],
        TABLES[1]: [("dataset_id", "resource_type", "resource_id")] * 2,
        TABLES[2]: [("source_id", "resource_type", "resource_id"), ("site_id",)],
        TABLES[3]: [("source_id", "resource_type", "resource_id", "release_id"), ("source_id", "release_id")],
        TABLES[4]: [
            ("source_id", "release_id", "network_resource_id", "insurance_plan_resource_id"),
            ("source_id", "release_id", "insurance_plan_resource_id"),
        ],
    }
    columns_by_table = _relation_columns()
    relation_rows = []
    for table in sorted(TABLES):
        indexes = [
            {
                "name": table + "_pkey" if ordinal == 0 else "retained_lookup_" + str(TABLES.index(table)) + "_0",
                "columns": list(keys),
                "collations": ["native", "C", "C"] if table == TABLES[1] and ordinal else ["native"] * len(keys),
                "primary": ordinal == 0,
                "unique": ordinal == 0,
                "valid": True,
                "ready": True,
                "live": True,
                "method": "btree",
                "native": True,
            }
            for ordinal, keys in enumerate(keys_by_table[table])
        ]
        relation_rows.append(
            dict(
                schema_name=capture.schema_name,
                relname=table,
                relkind="r",
                relpersistence="p",
                relispartition=False,
                relhastriggers=False,
                relrowsecurity=False,
                relforcerowsecurity=False,
                inherited=False,
                defaults=False,
                columns=[
                    dict(name=name, type=kind, required=required, native=True, generated="", identity="")
                    for name, kind, required in columns_by_table[table]
                ],
                indexes=indexes,
                constraints=[
                    dict(
                        type="p",
                        columns=list(keys_by_table[table][0]),
                        validated=True,
                        deferrable=False,
                        deferred=False,
                    )
                ],
            )
        )
    return relation_rows


def _legacy_document(capture):
    """Keep saved physical fields only as discarded historical wire bytes."""
    saved = capture.as_dict()
    for name in ("tables", "contract"):
        saved.pop(name)
    saved.update(
        schema_oid=101,
        relation_oids=[[table, 200 + number] for number, table in enumerate(TABLES)],
        owner_role="saved_owner",
        runtime_roles=["saved_reader"],
        catalog_sha256="9" * 64,
    )
    saved["capture_sha256"] = "8" * 64
    return saved


class _Savepoint:
    def __init__(self, native):
        self.native = native
        self.transaction = native.current_transaction

    async def start(self):
        self.native.calls.append(("savepoint", "start"))

    async def rollback(self):
        self.native.calls.append(("savepoint", "rollback"))
        if self.transaction is not self.native.current_transaction:
            raise RuntimeError("caller transaction ended")


class _Native:
    """Synthetic native metadata exercises validation, never database permissions."""

    def __init__(self):
        self.capture = retained()
        self.relations = _relations(self.capture)
        self.source = pin(self.capture)
        self.active, self.calls, self.current_transaction = True, [], object()
        self.snapshot = {"isolation": "repeatable read", "read_only": "on"}
        self.comment = "retained-normalized-fhir-v1:" + canonical(self.capture.as_dict())
        self.on_catalog = None
        self.terminated = False

    def is_in_transaction(self):
        return self.active

    def transaction(self):
        return _Savepoint(self)

    def terminate(self):
        self.terminated = True

    async def execute(self, sql):
        self.calls.append(("execute", sql))
        assert sql in ("SET LOCAL search_path TO pg_catalog", "SET LOCAL jit = off")

    async def fetchrow(self, sql, *arguments):
        self.calls.append(("fetchrow", sql))
        assert sql == metadata._SNAPSHOT_SQL and not arguments
        return dict(self.snapshot)

    async def fetch(self, sql, *arguments):
        self.calls.append(("fetch", sql))
        assert sql == metadata._CATALOG_SQL and arguments == (self.capture.schema_name,)
        if self.on_catalog:
            self.on_catalog(self)
        return copy.deepcopy(self.relations)

    async def fetchval(self, sql, *arguments):
        self.calls.append(("fetchval", sql))
        assert "obj_description(n.oid,'pg_namespace')" in sql and arguments == (self.capture.schema_name,)
        return self.comment


@pytest.mark.parametrize("isolation", ["repeatable read", "serializable"])
async def test_metadata_uses_complete_named_shape_without_heap_probes(isolation):
    native = _Native()
    native.snapshot["isolation"] = isolation
    assert await metadata.verify_retained_fhir_metadata(native, native.source) == native.capture
    assert native.calls.count(("fetchrow", metadata._SNAPSHOT_SQL)) == 3
    assert native.calls.count(("savepoint", "start")) == native.calls.count(("savepoint", "rollback")) == 1
    assert [statement for kind, statement in native.calls if kind == "execute"] == [
        "SET LOCAL search_path TO pg_catalog",
        "SET LOCAL jit = off",
    ]
    sql = "\n".join(statement for _, statement in native.calls).lower()
    for forbidden in (
        "lock table",
        "for update",
        "for share",
        "where false",
        "limit 0",
        "pg_advisory",
        "copy ",
        "pg_get_indexdef",
        "pg_get_constraintdef",
        "pg_backend_pid",
        "virtualxid",
        "pg_roles",
        "pg_has_role",
        "has_table_privilege",
        "filenode",
        "owner_oid",
    ):
        assert forbidden not in sql
    assert native.capture.schema_name not in sql
    assert "pg_opclass" in sql and "pg_constraint" in sql


@pytest.mark.parametrize("field,value", [("isolation", "read committed"), ("read_only", "off")])
async def test_read_only_repeatable_snapshot_is_required_before_catalog_access(field, value):
    native = _Native()
    native.snapshot[field] = value
    with pytest.raises(RetainedFHIRCaptureError):
        await metadata.verify_retained_fhir_metadata(native, native.source)
    assert not any(kind == "fetch" for kind, _ in native.calls)


async def test_caller_must_already_own_transaction():
    native = _Native()
    native.active = False
    with pytest.raises(RetainedFHIRCaptureError):
        await metadata.verify_retained_fhir_metadata(native, native.source)
    assert native.calls == []


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_id", "cms-npd"),
        ("schema_name", "other_schema"),
        ("endpoint_id", "other_endpoint"),
        ("dataset_id", "other_dataset"),
        ("dataset_sha256", "f" * 64),
        ("resource_relation", {"schema": "source_example", "name": "other_resource"}),
        ("release_id", "other-release"),
        ("retained_capture", None),
        ("retained_epoch", object()),
        ("retained_successor", object()),
        ("source_witness_capture", object()),
        ("source_witness_anchor", object()),
    ],
)
async def test_exact_generic_pin_refuses_mixed_or_grafted_coordinates_before_io(field, value):
    native = _Native()
    object.__setattr__(native.source, field, value)
    with pytest.raises(RetainedFHIRCaptureError):
        await metadata.verify_retained_fhir_metadata(native, native.source)
    assert native.calls == []


async def test_logical_hash_alone_cannot_substitute_for_stored_edition():
    native = _Native()
    capture = replace(native.capture, content_sha256="f" * 64)
    capture = replace(capture, capture_sha256=capture_digest(capture))
    with pytest.raises(RetainedFHIRCaptureError):
        await metadata.verify_retained_fhir_metadata(native, pin(capture))


@pytest.mark.parametrize(
    "field,value",
    [
        ("relkind", "v"),
        ("relpersistence", "u"),
        ("relhastriggers", True),
        ("relrowsecurity", True),
        ("relforcerowsecurity", True),
        ("inherited", True),
        ("defaults", True),
        ("columns", []),
        ("indexes", []),
        ("constraints", []),
    ],
)
async def test_native_data_shape_drift_fails_closed(field, value):
    native = _Native()
    native.relations[0][field] = value
    with pytest.raises(RetainedFHIRCaptureError):
        await metadata.verify_retained_fhir_metadata(native, native.source)


@pytest.mark.parametrize("drift", ["missing", "extra", "cursor", "index_native", "comment"])
async def test_named_inventory_cursor_and_edition_remain_required(drift):
    native = _Native()
    if drift == "missing":
        native.relations.pop()
    elif drift == "extra":
        native.relations.append({**native.relations[-1], "relname": "unexpected_relation"})
    elif drift == "comment":
        native.comment = "changed-comment"
    else:
        resource = next(row for row in native.relations if row["relname"] == TABLES[1])
        cursor = next(index for index in resource["indexes"] if not index["primary"])
        cursor["collations" if drift == "cursor" else "native"] = ["native"] * 3 if drift == "cursor" else False
    with pytest.raises(RetainedFHIRCaptureError):
        await metadata.verify_retained_fhir_metadata(native, native.source)


@pytest.mark.parametrize("drift", ["read_only", "inactive", "restart"])
async def test_caller_snapshot_changes_cannot_return_capture(drift):
    native = _Native()

    def change(connection):
        if drift == "read_only":
            connection.snapshot["read_only"] = "off"
        elif drift == "inactive":
            connection.active = False
        else:
            connection.current_transaction = object()

    native.on_catalog = change
    with pytest.raises(RetainedFHIRCaptureError):
        await metadata.verify_retained_fhir_metadata(native, native.source)
    assert native.calls.count(("savepoint", "rollback")) == 1
    assert native.terminated is (drift == "restart")


@pytest.mark.parametrize("legacy", [False, True])
async def test_saved_named_and_known_legacy_editions_serve_without_identity_rewrite(legacy):
    native = _Native()
    if legacy:
        saved = _legacy_document(native.capture)
        native.capture = validate_capture(saved)
        native.source = pin(native.capture)
        native.comment = "retained-normalized-fhir-v1:" + canonical(saved)
    assert await metadata.verify_retained_fhir_metadata(native, native.source) == native.capture


@pytest.mark.parametrize("damage", ["unknown", "hash", "encoding", "duplicate"])
async def test_stored_receipt_unknown_shapes_or_changed_logical_bytes_refuse(damage):
    native = _Native()
    saved = native.capture.as_dict()
    if damage == "unknown":
        saved["unknown"] = True
    elif damage == "hash":
        saved["capture_sha256"] = "f" * 64
    native.comment = "retained-normalized-fhir-v1:" + canonical(saved)
    if damage == "encoding":
        native.comment += " "
    elif damage == "duplicate":
        native.comment = native.comment.replace('{"', '{"unknown":true,"unknown":true,"', 1)
    with pytest.raises(RetainedFHIRCaptureError):
        await metadata.verify_retained_fhir_metadata(native, native.source)


@pytest.mark.parametrize("failure", [RuntimeError("original metadata failure"), asyncio.CancelledError()])
async def test_original_errors_and_cancellation_survive_borrowed_settings(failure):
    native = _Native()

    def fail(_):
        raise failure

    native.on_catalog = fail
    with pytest.raises(
        type(failure) if isinstance(failure, asyncio.CancelledError) else RetainedFHIRCaptureError
    ) as observed:
        await metadata.verify_retained_fhir_metadata(native, native.source)
    assert (
        observed.value is failure
        if isinstance(failure, asyncio.CancelledError)
        else observed.value.__cause__ is failure
    )
    assert native.calls.count(("savepoint", "rollback")) == 1


@pytest.mark.parametrize(
    "field,value",
    [
        ("name", "other"),
        ("type", "integer"),
        ("required", False),
        ("native", False),
        ("generated", "s"),
        ("identity", "a"),
    ],
)
async def test_native_columns_preserve_required_keys_types_and_derived_shape(field, value):
    native = _Native()
    native.relations[0]["columns"][0][field] = value
    with pytest.raises(RetainedFHIRCaptureError):
        await metadata.verify_retained_fhir_metadata(native, native.source)


@pytest.mark.parametrize("table", TABLES[:2])
async def test_native_source_column_inventory_refuses_unknown_additions(table):
    native = _Native()
    relation = next(row for row in native.relations if row["relname"] == table)
    relation["columns"].append(
        dict(name="unexpected", type="integer", required=False, native=True, generated="", identity="")
    )
    with pytest.raises(RetainedFHIRCaptureError):
        await metadata.verify_retained_fhir_metadata(native, native.source)
