# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Generic capture metadata keeps native custody and never probes source heaps."""

import copy
import json
from dataclasses import replace

import pytest

from api import provider_directory_retained_fhir_metadata as metadata
from process.network_fhir_retained_capture_contract import (
    TABLES,
    RetainedFHIRCaptureError,
    canonical,
    capture_digest,
    digest,
)
from tests.test_network_fhir_retained_capture import pin, retained


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
    relations = []
    for name, oid in capture.relation_oids:
        indexes = []
        for ordinal, columns in enumerate(keys_by_table[name]):
            indexes.append(
                {
                    "name": "retained_lookup_1_0" if name == TABLES[1] and ordinal else "example_index",
                    "columns": list(columns),
                    "primary": ordinal == 0,
                    "unique": ordinal == 0,
                    "valid": True,
                    "ready": True,
                    "live": True,
                    "owner_oid": 11,
                    "method": "btree",
                    "key_count": len(columns),
                    "attribute_count": len(columns),
                    "options": " ".join("0" for _ in columns),
                    "expressions": False,
                    "predicate": False,
                    "definition": f"CREATE INDEX retained_lookup_1_0 ON {capture.schema_name}.provider_directory_dataset_resource USING btree "
                    '(dataset_id, resource_type COLLATE "C", resource_id COLLATE "C")',
                }
            )
        relations.append(
            {
                "oid": oid,
                "schema_oid": capture.schema_oid,
                "relname": name,
                "relkind": b"r",
                "relpersistence": b"p",
                "relowner": 11,
                "nspowner": 11,
                "filenode": oid,
                "relhastriggers": False,
                "relrowsecurity": False,
                "relforcerowsecurity": False,
                "inherited": False,
                "defaults": False,
                "columns": "[]",
                "indexes": json.dumps(indexes),
                "constraints": '[[101,"p",102,"PRIMARY KEY"]]',
            }
        )
    return relations


class _Savepoint:
    def __init__(self, native):
        self.native = native

    async def start(self):
        self.native.calls.append(("savepoint", "start"))

    async def rollback(self):
        self.native.calls.append(("savepoint", "rollback"))


class _Native:
    """Synthetic catalog responses exercise predicates, not native authority proof."""

    def __init__(self):
        capture = retained()
        self.relations = _relations(capture)
        catalog_hash = digest(
            [
                {
                    name: field_value.decode("ascii") if type(field_value) is bytes else field_value
                    for name, field_value in relation_record.items()
                }
                for relation_record in self.relations
            ]
        )
        capture = replace(capture, catalog_sha256=catalog_hash)
        self.capture = replace(capture, capture_sha256=capture_digest(capture))
        self.source = pin(self.capture)
        self.active, self.pid, self.calls = True, 42, []
        self.owner = {
            "oid": 11,
            **dict.fromkeys(
                ("rolcanlogin", "rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls"), False
            ),
        }
        self.snapshot = {
            "backend_pid": self.pid,
            "virtual_xid": "1/2",
            "isolation": "repeatable read",
            "read_only": "on",
            "login": "reader_example",
            "actor": "reader_example",
            "replication_role": "origin",
            "rolcanlogin": True,
            **{key: False for key in self.owner if key not in {"oid", "rolcanlogin"}},
        }
        self.comment = "retained-normalized-fhir-v1:" + canonical(self.capture.as_dict())
        self.closed = self.inert = self.runtime_safe = self.cursor_valid = self.indexes_supported = True
        self.on_catalog = None

    def is_in_transaction(self):
        return self.active

    def get_server_pid(self):
        return self.pid

    def transaction(self):
        return _Savepoint(self)

    async def execute(self, sql):
        self.calls.append(("execute", sql))
        assert sql in ("SET LOCAL search_path TO pg_catalog", "SET LOCAL jit = off")

    async def fetchrow(self, sql, *arguments):
        self.calls.append(("fetchrow", sql))
        if sql == metadata._SNAPSHOT_SQL:
            return dict(self.snapshot)
        assert sql == "SELECT * FROM pg_roles WHERE rolname=$1"
        assert arguments == (self.capture.owner_role,)
        return dict(self.owner)

    async def fetch(self, sql, *arguments):
        self.calls.append(("fetch", sql))
        assert sql == metadata._CATALOG_SQL
        assert "FROM pg_namespace n JOIN pg_class c ON c.relnamespace=n.oid" in sql
        assert arguments == (self.capture.schema_name,)
        if self.on_catalog:
            self.on_catalog(self)
        return [{**row, "indexes_supported": self.indexes_supported} for row in copy.deepcopy(self.relations)]

    async def fetchval(self, sql, *arguments):
        self.calls.append(("fetchval", sql))
        if "native_acl AS" in sql:
            assert arguments == (self.capture.schema_oid, 11, list(self.capture.runtime_roles), list(TABLES))
            return self.closed
        if sql.startswith("SELECT NOT EXISTS(SELECT FROM pg_proc"):
            return self.inert
        if sql.startswith("WITH configured AS"):
            return self.runtime_safe
        if sql.startswith("SELECT count(*)=1 AND bool_and"):
            return self.cursor_valid
        assert sql == "SELECT obj_description($1::oid,'pg_namespace')"
        return self.comment


@pytest.mark.parametrize("isolation", ["repeatable read", "serializable"])
async def test_metadata_uses_complete_catalog_predicates_without_heap_probes(isolation):
    native = _Native()
    native.snapshot["isolation"] = isolation
    assert await metadata.verify_retained_fhir_metadata(native, native.source) == native.capture
    assert native.calls.count(("fetchrow", metadata._SNAPSHOT_SQL)) == 3
    assert native.calls.count(("savepoint", "start")) == native.calls.count(("savepoint", "rollback")) == 1
    assert [statement for kind, statement in native.calls if kind == "execute"] == [
        "SET LOCAL search_path TO pg_catalog",
        "SET LOCAL jit = off",
    ]
    assert native.calls.index(("execute", "SET LOCAL jit = off")) < native.calls.index(("fetch", metadata._CATALOG_SQL))
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
    ):
        assert forbidden not in sql
    assert native.capture.schema_name not in sql
    assert "native_acl as" in sql and "pg_opclass" in sql and "pg_rewrite" in sql


@pytest.mark.parametrize(
    "field,value",
    [
        ("isolation", "read committed"),
        ("read_only", "off"),
        ("login", "different_login"),
        ("actor", "unconfigured_actor"),
        ("rolcanlogin", False),
        ("replication_role", "replica"),
        ("rolsuper", True),
        ("rolcreatedb", True),
        ("rolcreaterole", True),
        ("rolreplication", True),
        ("rolbypassrls", True),
        ("virtual_xid", None),
        ("backend_pid", 43),
    ],
)
async def test_snapshot_and_native_actor_are_mandatory_before_catalog_access(field, value):
    native = _Native()
    native.snapshot[field] = value
    with pytest.raises(RetainedFHIRCaptureError, match="^retained_fhir_capture_unavailable$"):
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
        ("dataset_sha256", "a" * 63 + "f"),
        ("resource_table_oid", 999),
        ("release_id", "other-release"),
        ("retained_capture", None),
        ("retained_epoch", object()),
        ("retained_successor", object()),
        ("custody_owner_role", "other_owner"),
        ("custody_runtime_roles", ("other_reader",)),
        ("custody_catalog_sha256", "f" * 64),
    ],
)
async def test_exact_generic_pin_refuses_mixed_or_grafted_coordinates_before_io(field, value):
    native = _Native()
    object.__setattr__(native.source, field, value)
    with pytest.raises(RetainedFHIRCaptureError):
        await metadata.verify_retained_fhir_metadata(native, native.source)
    assert native.calls == []


@pytest.mark.parametrize(
    "field,value",
    [
        ("catalog_sha256", "f" * 64),
        ("content_sha256", "f" * 64),
        ("schema_oid", 999),
    ],
)
async def test_canonical_wire_validation_alone_never_authenticates_a_receipt(field, value):
    native = _Native()
    capture = replace(native.capture, **{field: value})
    capture = replace(capture, capture_sha256=capture_digest(capture))
    source = replace(native.source, retained_capture=capture)
    with pytest.raises(RetainedFHIRCaptureError):
        await metadata.verify_retained_fhir_metadata(native, source)


@pytest.mark.parametrize(
    "field,value",
    [
        ("relkind", b"v"),
        ("relpersistence", b"u"),
        ("relowner", 99),
        ("nspowner", 99),
        ("relhastriggers", True),
        ("relrowsecurity", True),
        ("relforcerowsecurity", True),
        ("inherited", True),
        ("defaults", True),
        ("oid", 999),
        ("schema_oid", 999),
        ("filenode", 999),
        ("columns", '["changed-column"]'),
        ("indexes", "[]"),
        ("constraints", '[[101,"p"],[103,"f"]]'),
    ],
)
async def test_full_native_catalog_drift_fails_closed(field, value):
    native = _Native()
    native.relations[0][field] = value
    with pytest.raises(RetainedFHIRCaptureError):
        await metadata.verify_retained_fhir_metadata(native, native.source)


@pytest.mark.parametrize(
    "drift",
    ["missing", "extra", "owner", "closed", "inert", "runtime_safe", "cursor_valid", "indexes_supported", "comment"],
)
async def test_every_native_authority_boundary_remains_required(drift):
    native = _Native()
    if drift == "missing":
        native.relations.pop()
    elif drift == "extra":
        native.relations.append({**native.relations[-1], "relname": "unexpected_relation", "oid": 999})
    elif drift == "owner":
        native.owner["rolcanlogin"] = True
    else:
        setattr(native, drift, "changed-comment" if drift == "comment" else False)
    with pytest.raises(RetainedFHIRCaptureError):
        await metadata.verify_retained_fhir_metadata(native, native.source)


@pytest.mark.parametrize(
    "field,value",
    [
        ("virtual_xid", "1/3"),
        ("backend_pid", 43),
        ("actor", "other_actor"),
        ("read_only", "off"),
    ],
)
async def test_mid_verification_native_snapshot_changes_cannot_return_capture(field, value):
    native = _Native()
    native.on_catalog = lambda connection: connection.snapshot.update({field: value})
    with pytest.raises(RetainedFHIRCaptureError):
        await metadata.verify_retained_fhir_metadata(native, native.source)
    assert native.calls.count(("savepoint", "rollback")) == 1
