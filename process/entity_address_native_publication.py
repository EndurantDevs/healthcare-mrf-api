# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Attempt-fenced ordinary address stages and their protected native publication."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import importlib
import json
import os
import re
from collections import Counter
from contextlib import asynccontextmanager
from uuid import UUID, uuid4

from sqlalchemy import ForeignKeyConstraint, MetaData, text
from sqlalchemy.schema import CreateIndex, CreateTable

from api import ptg2_geo_projection as projection
from process import entity_address_result_generation as generation
from process.control_cancel import raise_if_cancelled
from process.control_lifecycle import suppress_control_run_heartbeat_persistence

HANDOFF_CONTRACT = "entity-address-native-handoff.v1"
PUBLICATION_CONTRACT = "entity-address-native-publication.v1"
ABANDONMENT_CONTRACT = "entity-address-unpublished-abandonment.v1"
CLEANUP_CONTRACT = "entity-address-native-candidate-cleanup.v1"
HANDOFF_PHASE = "entity-address stages awaiting publication"
PUBLICATION_PHASE = "entity-address-unified published"
MAX_RECEIPT_BYTES = 131_072


def _native():
    return importlib.import_module("process.entity_address_unified")


def controlled_publication_enabled(ctx):
    """Require coordinated deployment configuration, never a task-request switch."""
    return (
        is_protected_address_publication_enabled()
        and bool(ctx.get("control_run_id"))
        and not (ctx.get("context") or {}).get("test_mode")
    )


def is_protected_address_publication_enabled():
    """Read the coordinated-deployment switch shared by workers and cancellation."""
    return os.getenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_PROTECTED_PUBLICATION", "false").strip().lower() == "true"


def _json(value):
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
    if len(encoded.encode("ascii")) > MAX_RECEIPT_BYTES:
        raise RuntimeError("address native receipt exceeds its envelope")
    return encoded


def _digest(value):
    return hashlib.sha256(_json(value).encode("ascii")).hexdigest()


def _require(value, reason):
    if not value:
        raise RuntimeError("address native publication " + reason)


def _attempt(ctx):
    context = ctx.get("context") or {}
    run_id = ctx.get("control_run_id")
    attempt_id = context.get("_control_attempt_id")
    started = context.get("_control_attempt_started_at")
    _require(
        isinstance(run_id, str)
        and 0 < len(run_id) <= 64
        and isinstance(attempt_id, str)
        and re.fullmatch(re.escape(run_id) + r":[0-9a-f]{32}", attempt_id)
        and isinstance(started, str)
        and 0 < len(started) <= 64,
        "requires a complete controlled attempt",
    )
    return {"run_id": run_id, "attempt_id": attempt_id, "attempt_started_at": started}


def _attempt_suffix(attempt_id):
    return base64.b32encode(UUID(attempt_id.rsplit(":", 1)[1]).bytes).decode("ascii").rstrip("=").lower()


def bind_controlled_address_attempt(ctx):
    """Use the full attempt UUID in bounded stage identifiers, not a shared date."""
    if not controlled_publication_enabled(ctx) or not ctx["context"].get("publish_requested", True):
        return
    attempt = _attempt(ctx)
    suffix = _attempt_suffix(attempt["attempt_id"])
    # Native writers supply every evidence ID explicitly; this isolated heap owns no sequence.
    native = _native()
    evidence = native.make_class(native.EntityAddressEvidence, suffix).__table__
    evidence.c.evidence_id.autoincrement = False
    _require(evidence.autoincrement_column is None, "attempt evidence retained implicit auto-increment")
    if ctx["context"].get("_address_stage_attempt") == attempt["attempt_id"]:
        _require(ctx["import_date"] == suffix, "attempt stage changed")
        return
    ctx["import_date"] = suffix
    ctx["context"]["_address_stage_attempt"] = attempt["attempt_id"]
    for field in (
        "stage_prepared",
        "stage_indexes_prepared",
        "support_stage_prepared",
        "support_stage_indexes_prepared",
    ):
        ctx["context"][field] = False


def _stage_names(schema, import_date):
    from process.entity_address_snapshot_adoption import _validated_snapshot_destination

    _validated_snapshot_destination(db_schema=schema, import_date=import_date)
    return {name: f"{name}_{import_date}" for name in generation.RELATION_NAMES}


def validate_entity_address_native_handoff(handoff):
    """Decode only the fixed seven-table handoff; it is not content authority."""
    fields = {
        "contract",
        "run_id",
        "attempt_id",
        "attempt_started_at",
        "schema_name",
        "import_date",
        "database_oid",
        "import_run_oid",
        "stage_relations",
        "incumbent",
        "alias_generation",
        "dependency_bindings",
        "row_count",
        "source_contract_sha256",
        "indexes",
        "handoff_sha256",
    }
    _require(
        type(handoff) is dict and set(handoff) == fields and handoff["contract"] == HANDOFF_CONTRACT, "handoff differs"
    )
    handoff = json.loads(_json(handoff))
    _attempt(
        {
            "control_run_id": handoff["run_id"],
            "context": {
                "_control_attempt_id": handoff["attempt_id"],
                "_control_attempt_started_at": handoff["attempt_started_at"],
            },
        }
    )
    names = _stage_names(handoff["schema_name"], handoff["import_date"])
    _require(handoff["import_date"] == _attempt_suffix(handoff["attempt_id"]), "stage attempt differs")
    _require(type(handoff["row_count"]) is int and 0 < handoff["row_count"] < 2**63, "row count differs")
    _require(type(handoff["alias_generation"]) is int and handoff["alias_generation"] >= 0, "alias differs")
    _require(
        all(type(handoff[key]) is int and 0 < handoff[key] < 2**32 for key in ("database_oid", "import_run_oid")),
        "database differs",
    )
    projection.validate_projection_dependency_bindings(handoff["schema_name"], handoff["dependency_bindings"])
    _validate_handoff_relations(handoff["stage_relations"], names)
    _require(type(handoff["indexes"]) is list and 0 < len(handoff["indexes"]) <= 256, "indexes differ")
    _validate_indexes(handoff["indexes"], handoff["stage_relations"])
    _validate_authority(handoff["incumbent"])
    _require(
        type(handoff["source_contract_sha256"]) is str
        and re.fullmatch(r"[0-9a-f]{64}", handoff["source_contract_sha256"]) is not None,
        "source contract differs",
    )
    _require(
        _digest({key: field for key, field in handoff.items() if key != "handoff_sha256"}) == handoff["handoff_sha256"],
        "handoff digest differs",
    )
    return handoff


def _validate_handoff_relations(relations, names):
    _require(type(relations) is list and len(relations) == 7, "family differs")
    _require(all(type(entry) is dict for entry in relations), "family differs")
    _require([entry.get("table_name") for entry in relations] == list(names), "family differs")
    for entry in relations:
        _require(set(entry) == {"table_name", "stage_name", "relation_oid", "relfilenode"}, "stage fields differ")
        _require(entry["stage_name"] == names[entry["table_name"]], "stage name differs")
        _require(
            all(type(entry[key]) is int and 0 < entry[key] < 2**32 for key in ("relation_oid", "relfilenode")),
            "stage identity differs",
        )
    _require(len({entry["relation_oid"] for entry in relations}) == 7, "stage identity repeats")


def _validate_indexes(indexes, relations):
    oids = {entry["relation_oid"] for entry in relations}
    for row in indexes:
        _require(
            type(row) is dict and set(row) == {"relation_oid", "index_oid", "relfilenode", "definition"},
            "index fields differ",
        )
        _require(
            all(type(row[key]) is int and 0 < row[key] < 2**32 for key in ("relation_oid", "index_oid", "relfilenode")),
            "index identity differs",
        )
        _require(
            row["relation_oid"] in oids and type(row["definition"]) is str and 0 < len(row["definition"]) <= 8192,
            "index definition differs",
        )
    _require(
        len({row["index_oid"] for row in indexes}) == len(indexes) and {row["relation_oid"] for row in indexes} == oids,
        "index family differs",
    )


def _validate_authority(value):
    _require(
        type(value) is dict
        and set(value) == {"local_lineage_id", "local_generation", "serving_generation", "relation_oids"},
        "native authority differs",
    )
    _require(
        str(UUID(value["local_lineage_id"])) == value["local_lineage_id"]
        and type(value["local_generation"]) is int
        and 0 <= value["local_generation"] < 2**63,
        "native lineage differs",
    )
    if value["serving_generation"] is None:
        _require(value["relation_oids"] is None, "native authority is partial")
    else:
        generation.validate_entity_address_serving_generation(value["serving_generation"])
        generation._relation_oids(value["relation_oids"])


def validate_entity_address_native_publication(receipt):
    """Preserve native seven-table provenance without pretending it is an archive."""
    _require(
        type(receipt) is dict
        and set(receipt)
        == {
            "contract",
            "handoff",
            "validation",
            "validation_sha256",
            "source_contract_sha256",
            "inventory",
            "alias",
            "result_generation",
        }
        and receipt["contract"] == PUBLICATION_CONTRACT,
        "receipt differs",
    )
    receipt = json.loads(_json(receipt))
    handoff = validate_entity_address_native_handoff(receipt["handoff"])
    _validate_authority(receipt["result_generation"])
    authority = receipt["result_generation"]
    origin = authority["serving_generation"]
    _require(
        origin is not None
        and authority["local_lineage_id"] == handoff["incumbent"]["local_lineage_id"]
        and authority["local_generation"] == handoff["incumbent"]["local_generation"] + 1
        and origin["origin_lineage_id"] == authority["local_lineage_id"]
        and origin["origin_generation"] == authority["local_generation"]
        and authority["relation_oids"] == [entry["relation_oid"] for entry in handoff["stage_relations"]],
        "published authority differs",
    )
    _require(
        type(receipt["validation"]) is dict
        and set(receipt["validation"]) == {"rows", "integrity", "catalog"}
        and type(receipt["validation"]["rows"]) is int
        and receipt["validation"]["rows"] == handoff["row_count"]
        and type(receipt["validation"]["integrity"]) is dict
        and type(receipt["validation"]["catalog"]) is dict
        and _digest(receipt["validation"]) == receipt["validation_sha256"]
        and receipt["source_contract_sha256"] == handoff["source_contract_sha256"],
        "validation receipt differs",
    )
    _validate_publication_inventory(receipt["inventory"], handoff)
    _validate_catalog(receipt["validation"]["catalog"], handoff)
    _require(
        type(receipt["alias"]) is dict
        and set(receipt["alias"]) == {"generation", "catalog_sha256"}
        and receipt["alias"]["generation"] == handoff["alias_generation"]
        and type(receipt["alias"]["catalog_sha256"]) is str
        and re.fullmatch(r"[0-9a-f]{64}", receipt["alias"]["catalog_sha256"]) is not None,
        "alias receipt differs",
    )
    return receipt


def _validate_catalog(catalog, handoff):
    _require(
        set(catalog) == {"shapes", "indexes", "columns", "constraints"}
        and all(type(rows) is list and 0 < len(rows) <= 8192 for rows in catalog.values()),
        "catalog receipt differs",
    )
    expected_relations = [(entry["table_name"], entry["relation_oid"]) for entry in handoff["stage_relations"]]
    _require(
        len(catalog["shapes"]) == 7
        and all(type(row) is list and len(row) == 3 for row in catalog["shapes"])
        and [(row[0], row[1]) for row in catalog["shapes"]] == expected_relations
        and all(type(row[2]) is str and re.fullmatch(r"[0-9a-f]{64}", row[2]) for row in catalog["shapes"]),
        "catalog shapes differ",
    )
    oids = {oid for _name, oid in expected_relations}
    for kind in ("indexes", "columns", "constraints"):
        _require(
            all(type(row) is dict for row in catalog[kind])
            and {row.get("relation_oid") for row in catalog[kind]} == oids,
            "catalog family differs",
        )
    _require(all(row.get("kind") != "f" for row in catalog["constraints"]), "catalog relationship differs")


def _validate_publication_inventory(inventory, handoff):
    _require(
        type(inventory) is dict
        and set(inventory) == {"database_oid", "relations"}
        and inventory["database_oid"] == handoff["database_oid"]
        and type(inventory["relations"]) is list
        and len(inventory["relations"]) == 7,
        "inventory differs",
    )
    relations_by_name = {entry["relation_name"]: entry for entry in inventory["relations"]}
    _require(set(relations_by_name) == set(generation.RELATION_NAMES), "inventory family differs")
    for stage in handoff["stage_relations"]:
        entry = relations_by_name[stage["table_name"]]
        _require(
            set(entry)
            == {
                "schema_name",
                "schema_oid",
                "schema_owner_oid",
                "relation_name",
                "relation_oid",
                "relfilenode",
                "owner_oid",
            }
            and entry["schema_name"] == handoff["schema_name"]
            and entry["relation_oid"] == stage["relation_oid"]
            and entry["relfilenode"] == stage["relfilenode"]
            and all(
                type(entry[key]) is int and 0 < entry[key] < 2**32
                for key in ("schema_oid", "schema_owner_oid", "owner_oid")
            ),
            "physical inventory differs",
        )
    _require(len({entry["owner_oid"] for entry in relations_by_name.values()}) == 1, "inventory owner differs")


async def _locked_run(session, handoff, status, *, lock=True):
    schema = handoff["schema_name"]
    row = (
        (
            await session.execute(
                text(
                    f'SELECT * FROM "{schema}".import_run WHERE run_id=:run_id '
                    "AND importer='entity-address-unified' AND status=:status "
                    "AND progress->>'attempt_id'=:attempt_id AND progress->>'attempt_started_at'=:attempt_started_at "
                    "AND (error IS NULL OR error::jsonb='null'::jsonb)" + (" FOR UPDATE" if lock else "")
                ),
                {**handoff, "status": status},
            )
        )
        .mappings()
        .one_or_none()
    )
    _require(row is not None, "attempt changed or cancelled")
    params = row["params"] or {}
    _require(type(params) is dict and not params.get("test_mode") and not params.get("test"), "source run differs")
    if status != "succeeded":
        _require(row["finished_at"] is None, "attempt already finished")
    return dict(row)


async def controlled_dependency_node(session, schema_name, ctx):
    """Select this running attempt's node without blocking its later handoff transaction."""
    run = await _locked_run(session, {**_attempt(ctx), "schema_name": schema_name}, "running", lock=False)
    node_id = run["node_id"]
    _require(
        type(node_id) is str and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:@-]{0,127}", node_id) is not None,
        "controlled node differs",
    )
    return node_id


def _source_contract(handoff, params):
    return _digest(
        {
            "importer_id": "entity-address-unified",
            "params": params,
            **{key: handoff[key] for key in ("schema_name", "import_date", "alias_generation", "dependency_bindings")},
        }
    )


async def _dependency_bindings(session, schema, bindings_by_name):
    if bindings_by_name is None:
        bindings_by_name = {}
        for namespace, table in projection._PROJECTION_DEPENDENCIES:
            namespace = namespace or schema
            row = (
                (
                    await session.execute(
                        text(
                            "SELECT oid::bigint AS relation_oid,pg_relation_filenode(oid)::bigint AS relfilenode "
                            "FROM pg_class WHERE oid=to_regclass(:name)"
                        ),
                        {"name": f'"{namespace}"."{table}"'},
                    )
                )
                .mappings()
                .one()
            )
            bindings_by_name[f"{namespace}.{table}"] = {"schema_name": namespace, "table_name": table, **dict(row)}
    bindings_by_name = projection.validate_projection_dependency_bindings(schema, bindings_by_name)
    await session.execute(text(projection.projection_dependency_lock_sql(schema, dependency_bindings=bindings_by_name)))
    _require(
        await session.scalar(
            text("SELECT " + projection.projection_dependency_bindings_match_sql(schema, bindings_by_name))
        ),
        "dependencies changed",
    )
    return bindings_by_name


async def _stage_inventory(session, schema, names):
    relation_rows = (
        (
            await session.execute(
                text(
                    "SELECT c.relname,c.oid::bigint,pg_relation_filenode(c.oid)::bigint AS relfilenode "
                    "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                    "WHERE n.nspname=:schema AND c.relname=ANY(:names) AND c.relkind='r' AND c.relpersistence='p' "
                    "AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity AND NOT c.relispartition "
                    "AND NOT EXISTS(SELECT 1 FROM pg_trigger t WHERE t.tgrelid=c.oid) "
                    "AND NOT EXISTS(SELECT 1 FROM pg_rewrite r WHERE r.ev_class=c.oid) "
                    "AND NOT EXISTS(SELECT 1 FROM pg_inherits i WHERE i.inhrelid=c.oid OR i.inhparent=c.oid) "
                    "AND NOT EXISTS(SELECT 1 FROM pg_constraint k WHERE k.conrelid=c.oid AND k.contype='f')"
                    "AND NOT EXISTS(SELECT 1 FROM pg_depend d JOIN pg_class seq ON seq.oid=d.objid AND seq.relkind='S' "
                    "WHERE d.refobjid=c.oid AND d.classid='pg_class'::regclass AND d.deptype IN ('a','i'))"
                ),
                {"schema": schema, "names": list(names.values())},
            )
        )
        .mappings()
        .all()
    )
    by_name = {relation["relname"]: relation for relation in relation_rows}
    _require(set(by_name) == set(names.values()), "stage catalog differs")
    return [
        {
            "table_name": name,
            "stage_name": stage,
            "relation_oid": by_name[stage]["oid"],
            "relfilenode": by_name[stage]["relfilenode"],
        }
        for name, stage in names.items()
    ]


async def _require_model_catalog(session, schema, relations):
    """Require native model constraints and indexes before recording or sealing a handoff."""
    from process import entity_address_snapshot_receipt as receipt
    from process import entity_address_snapshot_restore as restore

    models_by_name = {model.__tablename__: model for model in generation.ENTITY_ADDRESS_RESULT_MODELS}
    _require({relation["table_name"] for relation in relations} == set(models_by_name), "model family differs")
    async with _catalog_search_path(session):
        await session.execute(text("SET LOCAL search_path=pg_catalog,public,pg_temp"))
        for relation in relations:
            name = relation["table_name"]
            temporary = _model_catalog_table(models_by_name[name])
            await session.execute(CreateTable(temporary))
            try:
                await _create_catalog_indexes(session, models_by_name[name], temporary, restore)
                expected_oid = await session.scalar(
                    text("SELECT to_regclass(:relation)::oid"), {"relation": f'pg_temp."{temporary.name}"'}
                )
                expected = await _model_catalog_shape(session, receipt, expected_oid, "pg_temp")
                actual = await _model_catalog_shape(session, receipt, relation["relation_oid"], schema)
                _require(actual[0] == expected[0] and actual[1] == expected[1], "model constraints differ")
                _require(not (Counter(expected[2]) - Counter(actual[2])), "model indexes differ")
            except BaseException as failure:
                try:
                    await session.execute(text(f'DROP TABLE pg_temp."{temporary.name}"'))
                except Exception:
                    raise failure from None
                raise
            else:
                await session.execute(text(f'DROP TABLE pg_temp."{temporary.name}"'))


def _model_catalog_table(model, *, allow_model_relationships=False):
    """Compile the trusted native shape without data, serial allocation or persistent objects."""
    from process.mrf_address_publication import _clone_model_table

    table = _clone_model_table(model.__table__, MetaData(), schema=None, name="address_model_" + uuid4().hex)
    table._prefixes.append("TEMPORARY")
    table.dialect_options["postgresql"]["on_commit"] = "DROP"
    for column in table.columns:
        column.autoincrement = False
    if allow_model_relationships:
        # Index witnesses contain no payload: actual heaps still require indexed set validation.
        for constraint in tuple(table.constraints):
            if isinstance(constraint, ForeignKeyConstraint):
                table.constraints.remove(constraint)
        table.foreign_keys.clear()
        for column in table.columns:
            column.foreign_keys.clear()
    _require(not table.foreign_keys, "model relationships require set validation")
    return table


async def _create_catalog_indexes(session, model, table, restore):
    """Parse the same trusted model index declarations used by isolated archive completion."""
    for index in sorted(table.indexes, key=lambda entry: entry.name):
        await session.execute(CreateIndex(index))
    for index in getattr(model, "__my_additional_indexes__", ()) or ():
        await session.execute(
            text(
                restore._additional_index_sql(
                    schema_name="pg_temp", table_name=table.name, stage_table_name=table.name, index=index
                )
            )
        )


async def _model_catalog_shape(session, receipt, oid, schema):
    """Compare metadata by logical column names, retaining ordinary column-rewrite compatibility."""
    columns = await receipt._catalog_columns(session, oid)
    names_by_number = {column["attnum"]: column["attname"] for column in columns}
    constraints = await receipt._catalog_constraints(session, oid, schema)
    indexes = await receipt._catalog_indexes(session, oid)
    for constraint in constraints:
        constraint["key_columns"] = _catalog_column_names(constraint["key_columns"], names_by_number)
    _catalog_index_column_names(indexes, names_by_number)
    column_values = sorted(
        ({key: value for key, value in column.items() if key != "attnum"} for column in columns),
        key=lambda column: column["attname"],
    )
    return (
        column_values,
        sorted(receipt._canonical_digest(constraint) for constraint in constraints),
        [receipt._canonical_digest(index) for index in indexes],
    )


def _catalog_index_column_names(indexes, names_by_number):
    """Keep logical key/include identities across compatible ordinary column rewrites."""
    for index in indexes:
        index["keys"] = _catalog_column_names(index["keys"], names_by_number)
        for attribute in index["key_attributes"] or ():
            attribute["attribute_number"] = names_by_number.get(attribute["attribute_number"], 0)


def _catalog_column_names(numbers, names_by_number):
    """Translate native int2vector/int2[] catalog positions, never SQL expressions."""
    if numbers is None:
        return None
    positions = str(numbers).strip("{}").replace(",", " ").split()
    return [names_by_number.get(int(number), 0) for number in positions]


async def _index_inventory(session, relations):
    async with _catalog_search_path(session):
        rows = (
            (
                await session.execute(
                    text(
                        "SELECT i.indrelid::bigint AS relation_oid,i.indexrelid::bigint AS index_oid,"
                        "pg_relation_filenode(i.indexrelid)::bigint AS relfilenode,pg_get_indexdef(i.indexrelid) AS definition "
                        "FROM pg_index i WHERE i.indrelid=ANY(:oids) AND i.indisvalid AND i.indisready AND i.indislive ORDER BY i.indexrelid"
                    ),
                    {"oids": [entry["relation_oid"] for entry in relations]},
                )
            )
            .mappings()
            .all()
        )
    _require(
        {row["relation_oid"] for row in rows} == {entry["relation_oid"] for entry in relations}, "indexes incomplete"
    )
    return [dict(row) for row in rows]


@asynccontextmanager
async def _catalog_search_path(session):
    previous = await session.scalar(text("SHOW search_path"))
    await session.execute(text("SET LOCAL search_path=pg_catalog,pg_temp"))
    try:
        yield
    except BaseException as failure:
        # SQL errors abort the transaction; preserve the original NOWAIT/cancellation failure.
        try:
            await session.execute(text("SELECT set_config('search_path',:path,true)"), {"path": previous})
        except Exception:
            raise failure from None
        raise
    else:
        await session.execute(text("SELECT set_config('search_path',:path,true)"), {"path": previous})


async def _catalog_evidence(session, handoff):
    async with _catalog_search_path(session):
        return await _capture_catalog_evidence(session, handoff)


async def _capture_catalog_evidence(session, handoff):
    """Fingerprint bounded catalog metadata only, with stable logical table names."""
    shapes = await _catalog_shapes(session, handoff)
    index_rows = (
        (
            await session.execute(
                text(
                    "SELECT i.indexrelid::bigint AS index_oid,i.indrelid::bigint AS relation_oid,c.relowner::bigint AS owner_oid,"
                    "pg_relation_filenode(c.oid)::bigint AS relfilenode,am.amname AS method,i.indisunique,i.indisprimary,"
                    "i.indnkeyatts,i.indnatts,i.indkey::text,i.indclass::text,i.indcollation::text,i.indoption::text,"
                    "pg_get_expr(i.indexprs,i.indrelid) AS expression,pg_get_expr(i.indpred,i.indrelid) AS predicate,"
                    "i.indisvalid,i.indisready,i.indislive FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid "
                    "JOIN pg_am am ON am.oid=c.relam WHERE i.indrelid=ANY(:oids) ORDER BY i.indexrelid"
                ),
                {"oids": [entry["relation_oid"] for entry in handoff["stage_relations"]]},
            )
        )
        .mappings()
        .all()
    )
    oid_params_dict = {"oids": [entry["relation_oid"] for entry in handoff["stage_relations"]]}
    columns = (
        (
            await session.execute(
                text(
                    "SELECT a.attrelid::bigint AS relation_oid,a.attnum,a.attname,a.atttypid::bigint AS type_oid,"
                    "a.atttypmod AS type_modifier,a.attnotnull AS not_null,a.attidentity::text AS identity,"
                    "a.attgenerated::text AS generated,pg_get_expr(d.adbin,d.adrelid) AS default_expression "
                    "FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum "
                    "WHERE a.attrelid=ANY(:oids) AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attrelid,a.attnum"
                ),
                oid_params_dict,
            )
        )
        .mappings()
        .all()
    )
    constraints = (
        (
            await session.execute(
                text(
                    "SELECT conrelid::bigint AS relation_oid,contype::text AS kind,conkey::text AS columns,"
                    "convalidated AS validated,condeferrable AS deferrable,condeferred AS deferred,"
                    "pg_get_constraintdef(oid) AS definition FROM pg_constraint WHERE conrelid=ANY(:oids) AND contype<>'n' "
                    "ORDER BY conrelid,contype,conkey,definition"
                ),
                oid_params_dict,
            )
        )
        .mappings()
        .all()
    )
    return {
        "shapes": shapes,
        "indexes": [dict(index) for index in index_rows],
        "columns": [dict(column) for column in columns],
        "constraints": [dict(constraint) for constraint in constraints],
    }


async def _catalog_shapes(session, handoff):
    from process import entity_address_snapshot_receipt as receipt

    names_by_stage = {entry["stage_name"]: entry["table_name"] for entry in handoff["stage_relations"]}
    shapes = []
    for entry in handoff["stage_relations"]:
        shape = await receipt._schema_identity(
            session, entry["relation_oid"], handoff["schema_name"], entry["table_name"], names_by_stage=names_by_stage
        )
        shapes.append([entry["table_name"], entry["relation_oid"], shape])
    return shapes


async def _complete_indexes(schema, import_date):
    """Finish all ordinary model indexes on isolated tables; never rebuild live indexes."""
    native = _native()
    index_options_dict = {"complete_stage_indexes": True}
    for model in generation.ENTITY_ADDRESS_RESULT_MODELS:
        stage = native.make_class(model, import_date)
        await native._ensure_promoted_stage_logged(schema, stage.__tablename__)
        await native._ensure_stage_primary_key(stage, schema, context=index_options_dict)
        for _label, statement in native._stage_index_statements(
            stage, schema, list(getattr(stage, "__my_additional_indexes__", ()) or ()), index_options_dict
        ):
            await native.db.status(statement)


async def _capture_handoff(session, ctx, handoff):
    native = _native()
    run = await _locked_run(session, handoff, "running")
    _require(not (run["metrics"] or {}).get("address_handoff"), "handoff already exists")
    await raise_if_cancelled(ctx, {"run_id": handoff["run_id"]})
    schema = handoff["schema_name"]
    names = _stage_names(schema, handoff["import_date"])
    await session.execute(
        text(
            "LOCK TABLE "
            + ",".join(f'"{schema}"."{name}"' for name in sorted(names.values()))
            + " IN ACCESS EXCLUSIVE MODE"
        )
    )
    handoff["dependency_bindings"] = await _dependency_bindings(session, schema, handoff["dependency_bindings"])
    handoff["stage_relations"] = await _stage_inventory(session, schema, names)
    await _require_stage_execution_catalog(session, handoff)
    await _require_model_catalog(session, schema, handoff["stage_relations"])
    handoff["indexes"] = await _index_inventory(session, handoff["stage_relations"])
    _require(await native._address_alias_generation(schema) == handoff["alias_generation"], "alias changed")
    handoff["incumbent"] = (
        await generation.read_entity_address_result_generation_authority(session, schema_name=schema)
    ).as_dict()
    identities = (
        await session.execute(
            text(
                "SELECT (SELECT oid::bigint FROM pg_database WHERE datname=current_database()),to_regclass(:history)::oid::bigint"
            ),
            {"history": f'"{schema}".import_run'},
        )
    ).one()
    handoff.update(
        database_oid=identities[0],
        import_run_oid=identities[1],
        source_contract_sha256=_source_contract(handoff, run["params"] or {}),
    )
    handoff["handoff_sha256"] = _digest(handoff)
    return validate_entity_address_native_handoff(handoff)


async def _write_handoff(session, handoff):
    schema = handoff["schema_name"]
    marker = _json({key: handoff[key] for key in ("contract", "run_id", "attempt_id", "handoff_sha256")}).replace(
        "'", "''"
    )
    for entry in handoff["stage_relations"]:
        await session.execute(text(f'COMMENT ON TABLE "{schema}"."{entry["stage_name"]}" IS \'{marker}\''))
    changed = await session.scalar(
        text(
            f"UPDATE \"{schema}\".import_run SET status='finalizing',phase_detail=CAST(:phase AS text),"
            "metrics=(COALESCE(NULLIF(metrics::jsonb,'null'::jsonb),'{}'::jsonb) "
            "|| jsonb_build_object('address_handoff',CAST(:handoff AS jsonb)))::json,"
            "progress=(progress::jsonb || jsonb_build_object('phase',CAST(:phase AS text),'message','awaiting publication'))::json,"
            "heartbeat_at=clock_timestamp(),finished_at=NULL,error=NULL "
            "WHERE run_id=:run_id AND importer='entity-address-unified' AND status='running' "
            "AND progress->>'attempt_id'=:attempt_id AND progress->>'attempt_started_at'=:attempt_started_at "
            "AND metrics->'address_handoff' IS NULL AND finished_at IS NULL RETURNING run_id"
        ),
        {**handoff, "phase": HANDOFF_PHASE, "handoff": _json(handoff)},
    )
    _require(changed == handoff["run_id"], "attempt changed during handoff")


async def _read_handoff(database, handoff):
    async with database.transaction() as session:
        await session.execute(text("SET LOCAL lock_timeout='30s'"))
        return await session.scalar(
            text(
                f"SELECT metrics->'address_handoff' FROM \"{handoff['schema_name']}\".import_run "
                "WHERE run_id=:run_id AND importer='entity-address-unified' AND status IN ('finalizing','succeeded') "
                "AND progress->>'attempt_id'=:attempt_id AND progress->>'attempt_started_at'=:attempt_started_at "
                "AND (error IS NULL OR error::jsonb='null'::jsonb) FOR UPDATE"
            ),
            handoff,
        )


async def handoff_entity_address_generation(ctx, *, schema_name, import_date, dependency_bindings, row_count):
    """The ordinary worker stops at a durable finalizing handoff, never false success."""
    native = _native()
    handoff_by_field = {
        "contract": HANDOFF_CONTRACT,
        **_attempt(ctx),
        "schema_name": schema_name,
        "import_date": import_date,
        "dependency_bindings": dependency_bindings,
        "row_count": row_count,
        "alias_generation": int(ctx["context"].get("address_alias_generation") or 0),
    }
    _stage_names(schema_name, import_date)
    await _complete_indexes(schema_name, import_date)
    captured = None
    async with suppress_control_run_heartbeat_persistence(handoff_by_field["run_id"]):
        try:
            async with native.db.transaction() as session:
                captured = await _capture_handoff(session, ctx, handoff_by_field)
                await _write_handoff(session, captured)
        except Exception, asyncio.CancelledError:
            if captured is None:
                raise
            recorded = await _reconcile_handoff(native.db, captured)
            if recorded != captured:
                raise
        ctx["context"].update(
            control_run_handoff_committed=True, _control_committed_result={"address_handoff": captured}, run=0
        )
    return ctx["context"]["_control_committed_result"]


async def _reconcile_handoff(database, handoff):
    reconciliation = asyncio.create_task(_read_handoff(database, handoff))
    while True:
        try:
            return await asyncio.shield(reconciliation)
        except asyncio.CancelledError:
            if reconciliation.cancelled():
                raise


async def _require_handoff_stage(session, handoff):
    schema = handoff["schema_name"]
    names = _stage_names(schema, handoff["import_date"])
    await session.execute(
        text(
            "LOCK TABLE "
            + ",".join(f'"{schema}"."{name}"' for name in sorted(names.values()))
            + " IN ACCESS EXCLUSIVE MODE NOWAIT"
        )
    )
    _require(await _stage_inventory(session, schema, names) == handoff["stage_relations"], "stage changed")
    _require(await _index_inventory(session, handoff["stage_relations"]) == handoff["indexes"], "indexes changed")
    await _require_stage_execution_catalog(session, handoff)
    marker = _json({key: handoff[key] for key in ("contract", "run_id", "attempt_id", "handoff_sha256")})
    for entry in handoff["stage_relations"]:
        _require(
            await session.scalar(
                text("SELECT obj_description(CAST(:oid AS oid),'pg_class')"), {"oid": entry["relation_oid"]}
            )
            == marker,
            "stage marker changed",
        )


async def _require_stage_execution_catalog(
    session,
    handoff,
    *,
    allowed_type_oids=(),
    allowed_extensions=("postgis", "intarray", "btree_gin", "btree_gist"),
):
    """Reject builder-installed executable code before publisher-side set queries."""
    safe = await session.scalar(
        text(
            "SELECT NOT EXISTS(SELECT 1 FROM pg_index i JOIN pg_class idx ON idx.oid=i.indexrelid "
            "JOIN pg_am am ON am.oid=idx.relam WHERE i.indrelid=ANY(:oids) AND ("
            "NOT i.indisvalid OR NOT i.indisready OR NOT i.indislive OR am.amname NOT IN ('btree','gin','gist') "
            "OR EXISTS(SELECT 1 FROM pg_opclass op WHERE op.oid=ANY(i.indclass) "
            "AND op.opcnamespace<>'pg_catalog'::regnamespace AND NOT EXISTS(SELECT 1 FROM pg_depend ext "
            "JOIN pg_extension e ON e.oid=ext.refobjid WHERE ext.classid='pg_opclass'::regclass "
            "AND ext.objid=op.oid AND ext.refclassid='pg_extension'::regclass AND ext.deptype='e' "
            "AND e.extname=ANY(CAST(:allowed_extensions AS text[])))) "
            "OR EXISTS(SELECT 1 FROM pg_depend d WHERE d.classid='pg_class'::regclass AND d.objid=idx.oid "
            "AND ((d.refclassid='pg_proc'::regclass AND EXISTS(SELECT 1 FROM pg_proc p WHERE p.oid=d.refobjid "
            "AND p.pronamespace<>'pg_catalog'::regnamespace)) OR (d.refclassid='pg_operator'::regclass "
            "AND EXISTS(SELECT 1 FROM pg_operator op WHERE op.oid=d.refobjid AND op.oprnamespace<>'pg_catalog'::regnamespace))) "
            "AND NOT EXISTS(SELECT 1 FROM pg_depend ext JOIN pg_extension e ON e.oid=ext.refobjid "
            "WHERE ext.classid=d.refclassid AND ext.objid=d.refobjid AND ext.refclassid='pg_extension'::regclass "
            "AND ext.deptype='e' AND e.extname=ANY(CAST(:allowed_extensions AS text[])))))) "
            "AND NOT EXISTS(SELECT 1 FROM pg_attribute a JOIN pg_type ty ON ty.oid=a.atttypid "
            "WHERE a.attrelid=ANY(:oids) AND a.attnum>0 AND NOT a.attisdropped "
            "AND (a.attgenerated NOT IN ('','s') OR (ty.typnamespace<>'pg_catalog'::regnamespace "
            "AND NOT (ty.oid=ANY(CAST(:allowed_type_oids AS oid[]))) AND NOT EXISTS(SELECT 1 FROM pg_depend ext "
            "JOIN pg_extension e ON e.oid=ext.refobjid WHERE ext.classid='pg_type'::regclass AND ext.objid=ty.oid "
            "AND ext.refclassid='pg_extension'::regclass AND ext.deptype='e' AND e.extname='postgis' "
            "AND ty.typname IN ('geometry','geography')))))"
        ),
        {
            "oids": [entry["relation_oid"] for entry in handoff["stage_relations"]],
            "allowed_type_oids": list(allowed_type_oids),
            "allowed_extensions": list(allowed_extensions),
        },
    )
    _require(safe is True, "candidate executable catalog differs")


async def _require_native_attempt(session, handoff):
    run = await _locked_run(session, handoff, "finalizing")
    _require(
        run["phase_detail"] == HANDOFF_PHASE and (run["metrics"] or {}).get("address_handoff") == handoff,
        "handoff changed",
    )
    _require(
        _source_contract(handoff, run["params"] or {}) == handoff["source_contract_sha256"], "source contract changed"
    )
    actual = (
        await session.execute(
            text(
                "SELECT (SELECT oid::bigint FROM pg_database WHERE datname=current_database()),to_regclass(:history)::oid::bigint"
            ),
            {"history": f'"{handoff["schema_name"]}".import_run'},
        )
    ).one()
    _require(tuple(actual) == (handoff["database_oid"], handoff["import_run_oid"]), "native database changed")


async def _receipt_inventory(session, handoff, owner_oid):
    relations = []
    schema = handoff["schema_name"]
    namespace = (
        await session.execute(text("SELECT oid,nspowner FROM pg_namespace WHERE nspname=:schema"), {"schema": schema})
    ).one()
    for entry in handoff["stage_relations"]:
        relations.append(
            {
                "schema_name": schema,
                "schema_oid": namespace[0],
                "schema_owner_oid": namespace[1],
                "relation_name": entry["table_name"],
                "relation_oid": entry["relation_oid"],
                "relfilenode": entry["relfilenode"],
                "owner_oid": owner_oid,
            }
        )
    return {
        "database_oid": handoff["database_oid"],
        "relations": sorted(relations, key=lambda row: row["relation_oid"]),
    }


async def _complete_control(session, handoff, receipt):
    schema = handoff["schema_name"]
    result = await session.scalar(
        text(
            f"UPDATE \"{schema}\".import_run SET status='succeeded',phase_detail=CAST(:phase AS text),error=NULL,"
            "finished_at=clock_timestamp(),heartbeat_at=clock_timestamp(),"
            "metrics=(metrics::jsonb || jsonb_build_object('address_native_publication',CAST(:receipt AS jsonb)))::json,"
            "progress=(progress::jsonb || jsonb_build_object('pct',100,'done',CAST(:rows AS bigint),"
            "'total',CAST(:rows AS bigint),'unit','rows','phase',CAST(:phase AS text),'message','succeeded'))::json "
            "WHERE run_id=:run_id AND importer='entity-address-unified' AND status='finalizing' AND phase_detail=:handoff_phase "
            "AND progress->>'attempt_id'=:attempt_id AND progress->>'attempt_started_at'=:attempt_started_at "
            "AND metrics::jsonb->'address_handoff'=CAST(:handoff AS jsonb) AND finished_at IS NULL "
            "AND (error IS NULL OR error::jsonb='null'::jsonb) RETURNING run_id"
        ),
        {
            **handoff,
            "phase": PUBLICATION_PHASE,
            "handoff_phase": HANDOFF_PHASE,
            "rows": handoff["row_count"],
            "receipt": _json(receipt),
            "handoff": _json(handoff),
        },
    )
    _require(result == handoff["run_id"], "attempt changed during publication")


async def complete_entity_address_unified_handoff(session, handoff, *, dependency_bindings, publication_continuation):
    """Validate protected native heaps, then let trusted local code wrap ONE cutover.

    The caller owns COMMIT and native retention. The continuation must call the
    supplied cutover exactly once and must not commit or change transactions.
    """
    from process import entity_address_snapshot_adoption as adoption
    from process import entity_address_snapshot_preparation as protected

    handoff = validate_entity_address_native_handoff(handoff)
    _require(session.in_transaction() and callable(publication_continuation), "requires a trusted transaction")
    _require(
        projection.validate_projection_dependency_bindings(handoff["schema_name"], dependency_bindings)
        == handoff["dependency_bindings"],
        "dependency selection changed",
    )
    native = _native()
    async with native.db.bind_existing_session(session):
        await _require_native_attempt(session, handoff)
        await protected.alias._lock_alias_relations(session, handoff["schema_name"])
        await _require_handoff_stage(session, handoff)
        await _require_model_catalog(session, handoff["schema_name"], handoff["stage_relations"])
        await _dependency_bindings(session, handoff["schema_name"], dependency_bindings)
        _require(
            await native._address_alias_generation(handoff["schema_name"]) == handoff["alias_generation"],
            "alias changed",
        )
        prepared = await adoption.prepare_entity_address_publisher_reimport(
            session,
            db_schema=handoff["schema_name"],
            import_date=handoff["import_date"],
            dependency_bindings=dependency_bindings,
        )
        row_count = await session.scalar(
            text(f'SELECT count(*) FROM "{handoff["schema_name"]}"."{prepared.stage_cls.__tablename__}"')
        )
        _require(row_count == handoff["row_count"], "row count changed")
        validation_by_field = {
            "rows": row_count,
            "integrity": prepared.publish_validation,
            "catalog": await _catalog_evidence(session, handoff),
        }
        owner_oid = prepared.context["protected_owner_oid"]
        receipt_by_field = {
            "contract": PUBLICATION_CONTRACT,
            "handoff": handoff,
            "validation": validation_by_field,
            "validation_sha256": _digest(validation_by_field),
            "source_contract_sha256": handoff["source_contract_sha256"],
            "inventory": await _receipt_inventory(session, handoff, owner_oid),
            "alias": {
                "generation": handoff["alias_generation"],
                "catalog_sha256": await protected._require_alias_authority(session, handoff["schema_name"], owner_oid),
            },
        }
        return await _publish_prepared(session, handoff, prepared, receipt_by_field, publication_continuation)


async def _publish_prepared(session, handoff, prepared, receipt, continuation):
    native = _native()
    transaction_id = await session.scalar(text("SELECT pg_current_xact_id()::text"))
    cutover_calls = []

    async def cutover():
        """Consume the one local cutover capability inside the caller transaction."""
        _require(not cutover_calls, "cutover already called")
        cutover_calls.append(transaction_id)
        current = await generation.read_entity_address_result_generation_authority(
            session, schema_name=handoff["schema_name"]
        )
        _require(current.as_dict() == handoff["incumbent"], "incumbent changed")
        await _require_native_attempt(session, handoff)
        callbacks = native.ordinary_address_receipt_callbacks(native.db, handoff["schema_name"], native._sql_literal)
        await native._run_entity_address_cutover(
            prepared.db_schema,
            prepared.swaps,
            prepared.patch_statements,
            prepared.relation_names,
            prepared.required_names,
            prepared.context,
            callbacks=callbacks,
            require_caller_owned_transaction=True,
        )
        receipt["result_generation"] = prepared.context["result_generation"]
        validate_entity_address_native_publication(receipt)
        await _complete_control(session, handoff, receipt)
        return json.loads(_json(receipt))

    publication_result = await continuation(session, json.loads(_json(receipt)), cutover)
    _require(
        cutover_calls
        and session.in_transaction()
        and await session.scalar(text("SELECT pg_current_xact_id()::text")) == transaction_id,
        "publication transaction changed",
    )
    _require(await read_entity_address_native_publication(session, handoff) == receipt, "publication outcome changed")
    return publication_result


async def read_entity_address_native_publication(session, handoff):
    """Read exact durable completion, independently of later native table rotations."""
    handoff = validate_entity_address_native_handoff(handoff)
    run_record = (
        (
            await session.execute(
                text(
                    f'SELECT metrics,params,phase_detail,finished_at FROM "{handoff["schema_name"]}".import_run '
                    "WHERE run_id=:run_id AND importer='entity-address-unified' AND status='succeeded' "
                    "AND progress->>'attempt_id'=:attempt_id AND progress->>'attempt_started_at'=:attempt_started_at "
                    "AND (error IS NULL OR error::jsonb='null'::jsonb) FOR SHARE"
                ),
                handoff,
            )
        )
        .mappings()
        .one_or_none()
    )
    if run_record is None:
        return None
    receipt = (run_record["metrics"] or {}).get("address_native_publication")
    _require(
        run_record["finished_at"] is not None
        and run_record["phase_detail"] == PUBLICATION_PHASE
        and (run_record["metrics"] or {}).get("address_handoff") == handoff
        and _source_contract(handoff, run_record["params"] or {}) == handoff["source_contract_sha256"],
        "recorded outcome changed",
    )
    receipt = validate_entity_address_native_publication(receipt)
    _require(receipt["handoff"] == handoff, "recorded receipt changed")
    return receipt


def _validate_abandoned_handoff(handoff, abandonment):
    """Authenticate the existing bounded abandonment receipt without inventing native custody."""
    abandonment = json.loads(_json(abandonment))
    _require(
        set(abandonment)
        == {
            "contract",
            "preparation_id",
            "admission_sha256",
            "generation_id",
            "publication_fence",
            "reason",
            "stage_disposition",
            "physical_cleanup_completed",
            "same_run_readmission",
            "observed_run",
            "candidate_custody",
            "released_pins",
        }
        and abandonment["contract"] == ABANDONMENT_CONTRACT
        and abandonment["stage_disposition"] == "retained-unpublished"
        and abandonment["physical_cleanup_completed"] is False
        and abandonment["same_run_readmission"] == "requires-exact-candidate-cleanup-and-ledger-retirement"
        and abandonment["reason"] in {"terminal-cancellation", "completed-handoff-cancellation", "replaced-attempt"},
        "abandonment differs",
    )
    for key in ("preparation_id", "generation_id", "publication_fence"):
        _require(str(UUID(abandonment[key])) == abandonment[key], "abandonment identity differs")
    _require(re.fullmatch(r"[0-9a-f]{64}", abandonment["admission_sha256"]) is not None, "admission digest differs")
    custody_by_field = {
        key: handoff[key]
        for key in (
            "database_oid",
            "import_run_oid",
            "schema_name",
            "attempt_id",
            "attempt_started_at",
            "handoff_sha256",
            "source_contract_sha256",
            "stage_relations",
            "indexes",
        )
    }
    builder_oid = abandonment["candidate_custody"].get("builder_oid")
    _require(type(builder_oid) is int and 0 < builder_oid < 2**32, "abandoned builder differs")
    _require(
        abandonment["candidate_custody"] == {"builder_oid": builder_oid, **custody_by_field},
        "abandoned custody differs",
    )
    _require_abandoned_run_shape(handoff, abandonment["observed_run"])
    return abandonment


def _require_abandoned_run_shape(handoff, observed):
    _require(
        type(observed) is dict
        and set(observed)
        == {
            "run_id",
            "node_id",
            "status",
            "progress",
            "handoff_sha256",
            "phase_detail",
            "finished_at",
        }
        and observed["run_id"] == handoff["run_id"]
        and type(observed["node_id"]) is str
        and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:@-]{0,127}", observed["node_id"]) is not None,
        "abandoned attempt differs",
    )


async def _require_cleanup_run(session, handoff, abandonment):
    schema = handoff["schema_name"]
    run = (
        (
            await session.execute(
                text(
                    f'SELECT * FROM "{schema}".import_run WHERE run_id=:run_id AND node_id=:node_id '
                    "AND engine='healthcare-mrf-api' AND importer='entity-address-unified' "
                    "AND octet_length(metrics::text)<=393216 AND octet_length(params::text)<=131072 FOR UPDATE NOWAIT"
                ),
                {"run_id": handoff["run_id"], "node_id": abandonment["observed_run"]["node_id"]},
            )
        )
        .mappings()
        .one_or_none()
    )
    _require(run is not None, "cleanup run changed")
    params = run["params"] or {}
    _require(type(params) is dict and not params.get("test") and not params.get("test_mode"), "cleanup source differs")
    metrics = run["metrics"] or {}
    _require(
        metrics.get("address_native_publication") is None
        and (metrics.get("address_native_abandonments") or {}).get(abandonment["preparation_id"]) == abandonment
        and (metrics.get("address_native_candidate_cleanups") or {}).get(abandonment["preparation_id"]) is None
        and _source_contract(handoff, run["params"] or {}) == handoff["source_contract_sha256"],
        "cleanup receipt changed",
    )
    current = validate_entity_address_native_handoff(metrics.get("address_handoff"))
    _require(
        all(current[key] == handoff[key] for key in ("run_id", "schema_name", "database_oid", "import_run_oid"))
        and current["handoff_sha256"] == abandonment["observed_run"]["handoff_sha256"]
        and {key: (run["progress"] or {}).get(key) for key in ("attempt_id", "attempt_started_at")}
        == abandonment["observed_run"]["progress"],
        "cleanup attempt changed",
    )
    _require_cleanup_terminal(run, handoff, current, abandonment)
    actual = (
        await session.execute(
            text(
                "SELECT (SELECT oid::bigint FROM pg_database WHERE datname=current_database()),to_regclass(:history)::oid::bigint"
            ),
            {"history": f'"{schema}".import_run'},
        )
    ).one()
    _require(tuple(actual) == (handoff["database_oid"], handoff["import_run_oid"]), "cleanup database changed")


def _require_cleanup_terminal(run, handoff, current, abandonment):
    progress = run["progress"] or {}
    _require(
        (progress.get("attempt_id"), progress.get("attempt_started_at"))
        == (current["attempt_id"], current["attempt_started_at"]),
        "cleanup attempt changed",
    )
    if abandonment["reason"] == "replaced-attempt":
        _require(
            current["attempt_id"] != handoff["attempt_id"]
            and current["attempt_started_at"] != handoff["attempt_started_at"]
            and run["status"] == "finalizing"
            and run["phase_detail"] == HANDOFF_PHASE
            and run["finished_at"] is None
            and run["error"] is None
            and _source_contract(current, run["params"] or {}) == current["source_contract_sha256"],
            "replacement is not authenticated",
        )
        return
    _require(current == handoff, "cleanup handoff changed")
    is_canceled = run["status"] in {"canceled", "cancelled"} and run["finished_at"] is not None
    is_fenced = (
        run["status"] == "canceling"
        and run["phase_detail"] == progress.get("message") == "cancel requested"
        and run["finished_at"] is None
        and run["error"] is None
    )
    _require(is_canceled or is_fenced, "cancellation is not authenticated")


async def _require_cleanup_owners(session, handoff, runtime_owner_oids, protected_owner_oid):
    _require(
        type(runtime_owner_oids) is tuple
        and 0 < len(runtime_owner_oids) <= 64
        and all(type(oid) is int and 0 < oid < 2**32 and oid != protected_owner_oid for oid in runtime_owner_oids),
        "cleanup runtime authority differs",
    )
    safe = await session.scalar(
        text(
            "SELECT count(DISTINCT relation.relowner)=1 AND count(*)=:count AND bool_and("
            "relation.relowner=ANY(:runtime_owners) AND NOT owner.rolsuper AND NOT owner.rolcreaterole "
            "AND NOT owner.rolcreatedb AND NOT owner.rolreplication AND NOT owner.rolbypassrls "
            "AND NOT pg_has_role(owner.oid,CAST(:protected_owner AS oid),'USAGE') "
            "AND pg_has_role(session_user,owner.oid,'USAGE')) "
            "FROM pg_class relation JOIN pg_roles owner ON owner.oid=relation.relowner WHERE relation.oid=ANY(:oids)"
        ),
        {
            "oids": [entry["relation_oid"] for entry in handoff["stage_relations"]]
            + [entry["index_oid"] for entry in handoff["indexes"]],
            "runtime_owners": list(runtime_owner_oids),
            "protected_owner": protected_owner_oid,
            "count": 7 + len(handoff["indexes"]),
        },
    )
    _require(safe is True, "cleanup owner changed")


async def _require_cleanup_nonserving(session, handoff):
    schema = handoff["schema_name"]
    names = (*generation.RELATION_NAMES, generation.TABLE_NAME)
    await session.execute(
        text("LOCK TABLE " + ",".join(f'"{schema}"."{name}"' for name in sorted(names)) + " IN SHARE MODE NOWAIT")
    )
    safe = await session.scalar(
        text(
            f'SELECT NOT EXISTS(SELECT 1 FROM "{schema}"."{generation.TABLE_NAME}" WHERE relation_oids OPERATOR(pg_catalog.&&) CAST(:oids AS bigint[])) '
            "AND NOT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname=:schema AND c.relname=ANY(:names) AND c.oid=ANY(:oids))"
        ),
        {
            "schema": schema,
            "names": list(generation.RELATION_NAMES),
            "oids": [entry["relation_oid"] for entry in handoff["stage_relations"]],
        },
    )
    _require(safe is True, "candidate is serving")


async def cleanup_entity_address_native_handoff(
    session, handoff, *, abandonment, runtime_owner_oids, assert_unreferenced
):
    """Remove only abandoned exact native7 custody; caller owns ledger retirement and COMMIT."""
    handoff = validate_entity_address_native_handoff(handoff)
    abandonment = _validate_abandoned_handoff(handoff, abandonment)
    _require(session.in_transaction() and callable(assert_unreferenced), "cleanup requires a trusted transaction")
    _require(runtime_owner_oids == (abandonment["candidate_custody"]["builder_oid"],), "cleanup builder differs")
    async with _catalog_search_path(session):
        return await _cleanup_bound_native_candidate(
            session, handoff, abandonment, runtime_owner_oids, assert_unreferenced
        )


async def _cleanup_bound_native_candidate(session, handoff, abandonment, runtime_owner_oids, assert_unreferenced):
    from process import entity_address_snapshot_preparation as protected

    transaction_id = await session.scalar(text("SELECT pg_current_xact_id()::text"))
    protected_owner_oid = await protected._publisher_authority(session)
    await _require_cleanup_run(session, handoff, abandonment)
    _require(await assert_unreferenced(session, handoff, abandonment) is True, "candidate remains referenced")
    await _require_cleanup_nonserving(session, handoff)
    await _require_handoff_stage(session, handoff)
    await _require_cleanup_owners(session, handoff, runtime_owner_oids, protected_owner_oid)
    _require(await assert_unreferenced(session, handoff, abandonment) is True, "candidate remains referenced")
    _require(
        session.in_transaction() and await session.scalar(text("SELECT pg_current_xact_id()::text")) == transaction_id,
        "cleanup transaction changed",
    )
    relations = ",".join(f'"{handoff["schema_name"]}"."{entry["stage_name"]}"' for entry in handoff["stage_relations"])
    await session.execute(text("DROP TABLE " + relations + " RESTRICT"))
    return {
        "contract": CLEANUP_CONTRACT,
        "preparation_id": abandonment["preparation_id"],
        "abandonment_sha256": _digest(abandonment),
        "handoff_sha256": handoff["handoff_sha256"],
        "database_oid": handoff["database_oid"],
        "stage_relations": handoff["stage_relations"],
        "stage_disposition": "removed-unpublished",
        "physical_cleanup_completed": True,
    }
