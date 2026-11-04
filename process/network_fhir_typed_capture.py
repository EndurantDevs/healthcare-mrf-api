# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed typed Source inputs; producer receipts alone never authorize candidate use."""

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from types import SimpleNamespace
from uuid import UUID

from process.network_address_projection import _identifier
from process.network_custom_address_source import _closed, _require_transaction
from process.network_fhir_source_epoch import _catalog_relations
from process.network_fhir_source_witness_capture import _original_source_factory, _require_capture_writer
from process.network_fhir_source_witness_contract import canonical, canonical_timestamp, digest, source_origin
from process.network_fhir_typed_contract import (
    ADDRESS_FIELDS,
    CELL_COLUMNS,
    PAIR_COLUMNS,
    require,
    validate_graph_document,
)
from process.network_fhir_typed_source import (
    _DDL,
    bind_typed_graph_stage,
    produce_typed_source_graph,
    verify_typed_source_graph,
)
from process.network_membership_writer_closure import _protected_owner, _transfer_and_grant
from process.uhc_flex_practitioner_async_safety import drain_operation

PREFIX = "registry_fhir_typed_"
COMPONENT = "registry_fhir_typed_capture"
COMMENT = COMPONENT + ":4:"
TABLES = {"cells": "source_cells", "addresses": "source_addresses", "pairs": "source_pairs"}
COLUMNS = {
    "cells": CELL_COLUMNS,
    "addresses": ("raw_key", *ADDRESS_FIELDS, "address_key", "canonical_identity"),
    "pairs": PAIR_COLUMNS,
}
KEYS = {
    "cells": ("resource_type", "resource_id"),
    "addresses": ("raw_key",),
    "pairs": ("resource_type", "resource_id", "network_source_key", "location_resource_id"),
}


@dataclass(frozen=True)
class TypedGraphAnchor:
    """Independently retained Core coordinates; these are never Source admission."""

    capture_id: str
    observed_at: str

    def __post_init__(self):
        require(
            type(self.capture_id) is str and str(UUID(self.capture_id)) == self.capture_id and UUID(self.capture_id).int
        )
        require(canonical_timestamp(self.observed_at) == self.observed_at)


def validate_typed_capture(document):
    require(
        type(document) is dict
        and set(document)
        == {
            "component",
            "descriptor_version",
            "capture_id",
            "observed_at",
            "schema_name",
            "owner_role",
            "runtime_roles",
            "catalog",
            "content",
            "graph",
            "catalog_descriptor",
            "capture_sha256",
        }
    )
    require(
        document["component"] == COMPONENT
        and type(document["descriptor_version"]) is int
        and document["descriptor_version"] == 4
    )
    TypedGraphAnchor(document["capture_id"], document["observed_at"])
    require(document["schema_name"] == PREFIX + UUID(document["capture_id"]).hex)
    _identifier(document["owner_role"])
    roles = document["runtime_roles"]
    require(
        type(roles) is list
        and 1 <= len(roles) <= 32
        and roles == sorted(set(roles))
        and document["owner_role"] not in roles
    )
    for role in roles:
        _identifier(role)
    validate_graph_document(document["graph"])
    from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
    from process.network_fhir_typed_mapping import require_catalog_graph_scope
    from process.registry_fhir_catalog_sources import validate_fhir_catalog_source_descriptor

    descriptor = validate_fhir_catalog_source_descriptor(document["catalog_descriptor"])
    require_catalog_graph_scope(
        SimpleNamespace(document=document["graph"]),
        RegistryNetworkSourceCoordinates(**descriptor["coordinates"]),
        descriptor,
    )
    require(type(document["catalog"]) is dict and set(document["catalog"]) == {"schema_oid", "relation_oids", "sha256"})
    require(type(document["content"]) is dict and set(document["content"]) == set(TABLES))
    catalog = document["catalog"]
    require(type(catalog["schema_oid"]) is int and 0 < catalog["schema_oid"] < 2**32)
    require(type(catalog["relation_oids"]) is dict and set(catalog["relation_oids"]) == set(TABLES))
    require(
        all(type(oid) is int and 0 < oid < 2**32 for oid in catalog["relation_oids"].values())
        and len(set(catalog["relation_oids"].values())) == len(TABLES)
    )
    hashes = [catalog["sha256"], document["capture_sha256"]]
    for content in document["content"].values():
        require(type(content) is dict and set(content) == {"rows", "sha256"})
        require(type(content["rows"]) is int and 0 <= content["rows"] < 2**63)
        hashes.append(content["sha256"])
    require(all(type(value) is str and re.fullmatch(r"[0-9a-f]{64}", value) for value in hashes))
    require(
        document["content"]["cells"]["rows"] == document["graph"]["counts"]["projected_rows"]
        and document["content"]["pairs"]["rows"] == document["graph"]["counts"]["pair_rows"]
    )
    origin = source_origin(document["graph"]["origin"])
    require(
        document["graph"]["membership_as_of"]
        == (origin.semantic_projection_as_of or datetime.fromisoformat(document["observed_at"]).date().isoformat()),
        "membership_date",
    )
    identity = document["graph"]["database_identity"]
    require(
        identity["database_oid"] == origin.database_oid
        and identity["system_identifier"] == origin.database_system_identifier,
        "database_identity",
    )
    require(
        document["capture_sha256"] == digest({key: value for key, value in document.items() if key != "capture_sha256"})
    )
    return document


async def _catalog(connection, schema, owner_role, runtime_roles):
    relations = await _catalog_relations(connection, schema)
    owner = await connection.fetchrow("SELECT * FROM pg_roles WHERE rolname=$1", owner_role)
    require(
        _protected_owner(owner) and tuple(row["relname"] for row in relations) == tuple(sorted(TABLES.values())),
        "native_catalog",
    )
    inverse = {name: kind for kind, name in TABLES.items()}
    for row in relations:
        kind = inverse[row["relname"]]
        require(
            row["relkind"] == b"r"
            and row["relpersistence"] == b"p"
            and row["relowner"] == row["nspowner"] == owner["oid"],
            "native_owner",
        )
        require(
            not any(
                row[key] for key in ("relhastriggers", "relrowsecurity", "relforcerowsecurity", "inherited", "defaults")
            ),
            "native_shape",
        )
        columns = json.loads(row["columns"])
        require(
            tuple(column[0] for column in columns) == COLUMNS[kind]
            and all(not column[4] and not column[5] for column in columns),
            "native_columns",
        )
        constraints, indexes = json.loads(row["constraints"]), json.loads(row["indexes"])
        require(len(constraints) == 1 and constraints[0][1] == "p", "native_constraints")
        primary = [index for index in indexes if index["primary"]]
        require(len(primary) == 1 and primary[0]["columns"] == list(KEYS[kind]), "native_keys")
        require(
            all(
                index["owner_oid"] == owner["oid"]
                and index["method"] == "btree"
                and index["valid"]
                and index["ready"]
                and index["live"]
                and not index["expressions"]
                and not index["predicate"]
                for index in indexes
            ),
            "native_indexes",
        )
    schema_oid = relations[0]["schema_oid"]
    require(
        await connection.fetchval(
            "SELECT NOT EXISTS(SELECT FROM pg_proc WHERE pronamespace=$1::oid) AND NOT EXISTS(SELECT FROM pg_rewrite r JOIN pg_class c ON c.oid=r.ev_class WHERE c.relnamespace=$1::oid)",
            schema_oid,
        ),
        "native_code",
    )
    await _closed(connection, schema_oid, owner["oid"], runtime_roles, list(TABLES.values()))
    require(
        await connection.fetchval(
            "WITH configured AS (SELECT r.* FROM unnest($1::text[]) names(name) LEFT JOIN pg_roles r ON r.rolname=names.name) SELECT NOT EXISTS(SELECT FROM configured WHERE oid IS NULL OR rolcreatedb OR rolreplication OR rolbypassrls OR has_parameter_privilege(oid,'session_replication_role','SET')) AND NOT EXISTS(SELECT FROM pg_proc p WHERE p.prosecdef AND p.prorettype NOT IN ('trigger'::regtype,'event_trigger'::regtype) AND EXISTS(SELECT FROM pg_roles d WHERE d.oid=p.proowner AND (d.rolsuper OR d.rolcreaterole OR pg_has_role(d.oid,$2::oid,'MEMBER') OR pg_has_role(d.oid,$2::oid,'SET'))) AND EXISTS(SELECT FROM configured r WHERE has_schema_privilege(r.oid,p.pronamespace,'USAGE') AND has_function_privilege(r.oid,p.oid,'EXECUTE')))",
            list(runtime_roles),
            owner["oid"],
        ),
        "native_runtime",
    )
    rows = [
        {key: value.decode("ascii") if type(value) is bytes else value for key, value in row.items()}
        for row in relations
    ]
    return {
        "schema_oid": schema_oid,
        "relation_oids": {inverse[row["relname"]]: row["oid"] for row in relations},
        "sha256": digest(rows),
    }


async def _content(connection, stage):
    """Hash ordered native binary packets without per-edge Python objects or JSON witnesses."""
    content = {}
    for kind, keys in KEYS.items():
        folded = hashlib.sha256((COMPONENT + ":binary-v4:" + kind + "\0").encode())
        query = "SELECT " + ",".join(COLUMNS[kind]) + " FROM " + stage.relation(kind) + " ORDER BY " + ",".join(keys)

        async def consume(packet):
            folded.update(packet)

        copied = await drain_operation(
            connection.copy_from_query(query, output=consume, format="binary"), preserve_cancellation=True
        )
        content[kind] = {"rows": int(copied.removeprefix("COPY ")), "sha256": folded.hexdigest()}
    return content


async def capture_typed_source_graph(
    source_export,
    writer,
    *,
    source_profile,
    reviewed_origin,
    membership_as_of,
    expected_anchor,
    owner_role,
    runtime_roles,
    catalog_descriptor,
):
    """Produce and close one Source candidate; its consumer must independently reopen Source."""
    require(
        type(expected_anchor) is TypedGraphAnchor
        and type(runtime_roles) is tuple
        and tuple(sorted(set(runtime_roles))) == runtime_roles
    )
    expected_anchor.__post_init__()
    publisher = _original_source_factory(source_export, reviewed_origin)
    await _require_capture_writer(writer, (source_profile.owner_role, publisher), owner_role, runtime_roles)
    require(
        canonical_timestamp(await writer.fetchval("SELECT transaction_timestamp()")) == expected_anchor.observed_at,
        "anchor",
    )
    schema = PREFIX + UUID(expected_anchor.capture_id).hex
    original = await writer.fetchval(
        "SELECT CASE WHEN current_setting('role')='none' THEN 'NONE' ELSE quote_ident(current_user) END"
    )
    writer_role = await writer.fetchval("SELECT quote_ident(session_user)")
    try:
        async with writer.transaction():
            require(await writer.fetchval("SELECT to_regnamespace($1)", schema) is None, "restart_from_zero")
            await writer.execute("CREATE SCHEMA " + _identifier(schema) + " AUTHORIZATION " + _identifier(owner_role))
            await writer.execute("SET LOCAL ROLE " + _identifier(owner_role))
            for kind, name in TABLES.items():
                await writer.execute(
                    "CREATE TABLE " + _identifier(schema) + "." + _identifier(name) + "(" + _DDL[kind] + ")"
                )
            await writer.execute(
                "CREATE INDEX ON " + _identifier(schema) + "." + _identifier(TABLES["addresses"]) + "(address_key)"
            )
            await writer.execute("GRANT USAGE ON SCHEMA " + _identifier(schema) + " TO " + writer_role)
            for name in TABLES.values():
                await writer.execute(
                    "GRANT SELECT,INSERT ON " + _identifier(schema) + "." + _identifier(name) + " TO " + writer_role
                )
            await writer.execute("SET LOCAL ROLE " + original)
            stage = await bind_typed_graph_stage(writer, schema_name=schema, table_names=TABLES)
            graph = await produce_typed_source_graph(
                source_export,
                writer,
                stage,
                source_profile=source_profile,
                reviewed_origin=reviewed_origin,
                membership_as_of=membership_as_of,
            )
            relations = await _catalog_relations(writer, schema)
            await _transfer_and_grant(
                writer,
                SimpleNamespace(schema_name=schema),
                {"owner_role": owner_role, "loader_roles": [], "reader_roles": list(runtime_roles)},
                {
                    "namespace": {"oid": relations[0]["schema_oid"], "nspowner": relations[0]["nspowner"]},
                    "relations": [dict(row) for row in relations],
                    "heaps": {row["relname"]: row["oid"] for row in relations},
                },
            )
            await writer.execute("SET LOCAL ROLE " + _identifier(owner_role))
            document = {
                "component": COMPONENT,
                "descriptor_version": 4,
                "capture_id": expected_anchor.capture_id,
                "observed_at": expected_anchor.observed_at,
                "schema_name": schema,
                "owner_role": owner_role,
                "runtime_roles": list(runtime_roles),
                "catalog": await _catalog(writer, schema, owner_role, runtime_roles),
                "content": await _content(writer, stage),
                "graph": graph,
                "catalog_descriptor": catalog_descriptor,
            }
            document["capture_sha256"] = digest(document)
            validate_typed_capture(document)
            await writer.execute(
                "COMMENT ON SCHEMA "
                + _identifier(schema)
                + " IS '"
                + (COMMENT + canonical(document)).replace("'", "''")
                + "'"
            )
            stage.active = False
            return document
    finally:
        if writer.is_in_transaction():
            await drain_operation(writer.execute("SET LOCAL ROLE " + original), preserve_cancellation=True)


async def require_typed_graph_capture(connection, document, *, expected_anchor):
    """Fresh native custody plus a separately supplied Core anchor; no raw Source admission claim."""
    await _require_transaction(connection)
    document = validate_typed_capture(document)
    require(
        type(expected_anchor) is TypedGraphAnchor
        and (document["capture_id"], document["observed_at"])
        == (expected_anchor.capture_id, expected_anchor.observed_at),
        "anchor",
    )
    catalog = await _catalog(connection, document["schema_name"], document["owner_role"], document["runtime_roles"])
    require(catalog == document["catalog"], "native_catalog_changed")
    require(
        await connection.fetchval("SELECT obj_description($1::oid,'pg_namespace')", catalog["schema_oid"])
        == COMMENT + canonical(document),
        "native_seal",
    )
    return await bind_typed_graph_stage(
        connection,
        schema_name=document["schema_name"],
        table_names=TABLES,
        expected_oids=catalog["relation_oids"],
        document=document["graph"],
    )


async def verify_typed_graph_capture(
    source_export, connection, document, *, source_profile, reviewed_origin, expected_anchor, replay_stage=None
):
    """Independent consumer reopens the exact Source edition through true EOF before using typed rows."""
    stage = await require_typed_graph_capture(connection, document, expected_anchor=expected_anchor)
    await verify_typed_source_graph(
        source_export,
        connection,
        stage,
        source_profile=source_profile,
        reviewed_origin=reviewed_origin,
        identity=document["graph"]["database_identity"],
        membership_as_of=document["graph"]["membership_as_of"],
        replay_stage=replay_stage,
    )
    require(await _content(connection, stage) == document["content"], "content_changed")
    await stage.verify(connection)
    return stage


async def resolve_typed_graph_capture_reference(
    connection, *, expected_anchor, capture_sha256, catalog_generation, owner_role, runtime_roles
):
    """Hydrate bounded sealed metadata; native custody never substitutes for raw Source replay."""
    from process.registry_fhir_catalog_sources import fhir_catalog_source_generation

    await _require_transaction(connection)
    require(type(expected_anchor) is TypedGraphAnchor, "anchor")
    expected_anchor.__post_init__()
    require(
        all(
            type(value) is str and re.fullmatch(r"[0-9a-f]{64}", value)
            for value in (capture_sha256, catalog_generation)
        ),
        "reference_hash",
    )
    require(type(runtime_roles) is tuple and tuple(sorted(set(runtime_roles))) == runtime_roles, "reference_roles")
    _identifier(owner_role)
    schema = PREFIX + UUID(expected_anchor.capture_id).hex
    sealed = await connection.fetchval(
        "SELECT CASE WHEN octet_length(obj_description(n.oid,'pg_namespace')) <= 262144 THEN obj_description(n.oid,'pg_namespace') END FROM pg_namespace n WHERE n.nspname=$1",
        schema,
    )
    require(type(sealed) is str and sealed.startswith(COMMENT), "reference_seal")
    try:
        document = validate_typed_capture(json.loads(sealed[len(COMMENT) :]))
    except (TypeError, ValueError, KeyError) as error:
        raise ValueError("registry_fhir_typed_reference_invalid") from error
    require(sealed == COMMENT + canonical(document), "reference_encoding")
    require(
        document["capture_sha256"] == capture_sha256
        and document["owner_role"] == owner_role
        and tuple(document["runtime_roles"]) == runtime_roles,
        "reference_context",
    )
    require(fhir_catalog_source_generation(document["catalog_descriptor"]) == catalog_generation, "reference_catalog")
    stage = await require_typed_graph_capture(connection, document, expected_anchor=expected_anchor)
    require(not stage.checked, "reference_authority")
    return document


_SEALED_READ = object()


@dataclass(frozen=True)
class SealedTypedGraphRead:
    """Producer input capability only; independent Source verification remains mandatory before publication."""

    stage: object
    capture: dict
    anchor: TypedGraphAnchor
    seal: object

    @property
    def document(self):
        return self.stage.document

    def relation(self, kind):
        return self.stage.relation(kind)

    async def verify(self, connection):
        require(self.seal is _SEALED_READ, "sealed_read")
        await self.stage.verify(connection, complete=False)
        validate_graph_document(self.document)


async def load_typed_graph_capture(connection, document, *, expected_anchor):
    """Load closed native producer input without asserting independent raw Source verification."""
    stage = await require_typed_graph_capture(connection, document, expected_anchor=expected_anchor)
    require(await _content(connection, stage) == document["content"], "content_changed")
    require(not stage.checked, "sealed_read_authority")
    return SealedTypedGraphRead(stage, document, expected_anchor, _SEALED_READ)
