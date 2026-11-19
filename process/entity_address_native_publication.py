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
from process.entity_address_preparation_admission import stage_indexes

LEGACY_HANDOFF_CONTRACT = "entity-address-native-handoff.v1"
HANDOFF_CONTRACT = "entity-address-native-handoff.v2"
LEGACY_PUBLICATION_CONTRACT = "entity-address-native-publication.v1"
PUBLICATION_CONTRACT = "entity-address-native-publication.v2"
ABANDONMENT_CONTRACT = "entity-address-unpublished-abandonment.v1"
CLEANUP_CONTRACT = "entity-address-native-candidate-cleanup.v2"
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
    """Validate logical attempt/data commitments while preserving retained raw editions."""
    fields = {
        "contract",
        "run_id",
        "attempt_id",
        "attempt_started_at",
        "schema_name",
        "import_date",
        "stage_relations",
        "incumbent",
        "alias_generation",
        "dependency_bindings",
        "row_count",
        "source_contract_sha256",
        "indexes",
        "handoff_sha256",
    }
    legacy = isinstance(handoff, dict) and handoff.get("contract") == LEGACY_HANDOFF_CONTRACT
    fields |= {"database_oid", "import_run_oid"} if legacy else {"stage_integrity"}
    contracts = (LEGACY_HANDOFF_CONTRACT, HANDOFF_CONTRACT)
    _require(
        type(handoff) is dict and set(handoff) == fields and handoff["contract"] in contracts,
        "handoff differs",
    )
    handoff = json.loads(_json(handoff))
    names = _stage_names(handoff["schema_name"], handoff["import_date"])
    _validate_handoff_attempt(handoff)
    _require(type(handoff["row_count"]) is int and 0 < handoff["row_count"] < 2**63, "row count differs")
    _require(type(handoff["alias_generation"]) is int and handoff["alias_generation"] >= 0, "alias differs")
    _named_dependencies(handoff["schema_name"], handoff["dependency_bindings"])
    _validate_handoff_relations(handoff["stage_relations"], names, legacy=legacy)
    _require(type(handoff["indexes"]) is list and 0 < len(handoff["indexes"]) <= 256, "indexes differ")
    _validate_indexes(handoff["indexes"], handoff["stage_relations"], legacy=legacy)
    _validate_authority(handoff["incumbent"])
    if not legacy:
        _validate_named_handoff_data(handoff, names)
    _require(
        type(handoff["source_contract_sha256"]) is str
        and re.fullmatch(r"[0-9a-f]{64}", handoff["source_contract_sha256"]) is not None,
        "source contract differs",
    )
    _require(
        _digest({key: field_value for key, field_value in handoff.items() if key != "handoff_sha256"})
        == handoff["handoff_sha256"],
        "handoff digest differs",
    )
    return handoff


def _validate_named_handoff_data(handoff, names):
    """Require closed named inputs and the full-content receipt for every fresh handoff."""
    _require(
        all(set(binding) == {"schema_name", "table_name"} for binding in handoff["dependency_bindings"].values()),
        "named dependencies differ",
    )
    from process import entity_address_snapshot_receipt as receipt

    integrity = receipt.validate_entity_address_stage_integrity_receipt(
        handoff["stage_integrity"], stage_table_names=names
    )
    _require(
        integrity.contract == receipt.LEGACY_STAGE_INTEGRITY_CONTRACT
        and integrity.tables[0].row_count == handoff["row_count"],
        "stage content differs",
    )


def _validate_handoff_attempt(handoff):
    """Bind the imported stage suffix to the existing controlled attempt."""
    _attempt(
        {
            "control_run_id": handoff["run_id"],
            "context": {
                "_control_attempt_id": handoff["attempt_id"],
                "_control_attempt_started_at": handoff["attempt_started_at"],
            },
        }
    )
    _require(handoff["import_date"] == _attempt_suffix(handoff["attempt_id"]), "stage attempt differs")


def _validate_handoff_relations(relations, names, *, legacy=False):
    _require(type(relations) is list and len(relations) == 7, "family differs")
    _require(all(type(entry) is dict for entry in relations), "family differs")
    _require([entry.get("table_name") for entry in relations] == list(names), "family differs")
    fields = {"table_name", "stage_name"} | ({"relation_oid", "relfilenode"} if legacy else set())
    for entry in relations:
        _require(set(entry) == fields, "stage fields differ")
        _require(entry["stage_name"] == names[entry["table_name"]], "stage name differs")


def _validate_indexes(indexes, relations, *, legacy=False):
    fields = {"relation_oid", "index_oid", "relfilenode", "definition"} if legacy else {"table_name", "definition"}
    for row in indexes:
        _require(type(row) is dict and set(row) == fields, "index fields differ")
        _require(type(row["definition"]) is str and 0 < len(row["definition"]) <= 8192, "index definition differs")
    if not legacy:
        _require(
            {row["table_name"] for row in indexes} == {row["table_name"] for row in relations}, "index family differs"
        )
        _require(len({_json(row) for row in indexes}) == len(indexes), "index repeats")


def _validate_authority(value):
    fields = {"local_lineage_id", "local_generation", "serving_generation"}
    _require(type(value) is dict and set(value) in (fields, fields | {"relation_oids"}), "native authority differs")
    _require(
        str(UUID(value["local_lineage_id"])) == value["local_lineage_id"]
        and type(value["local_generation"]) is int
        and 0 <= value["local_generation"] < 2**63,
        "native lineage differs",
    )
    if value["serving_generation"] is not None:
        generation.validate_entity_address_serving_generation(value["serving_generation"])


def _logical_authority(value):
    """Project portable generation fields without interpreting legacy physical tokens."""
    _validate_authority(value)
    return {key: value[key] for key in ("local_lineage_id", "local_generation", "serving_generation")}


def _named_dependencies(schema, bindings):
    """Keep the existing closed selected-name contract across retained wire editions."""
    _require(
        isinstance(bindings, dict)
        and all(
            isinstance(value, dict)
            and set(value)
            in ({"schema_name", "table_name"}, {"schema_name", "table_name", "relation_oid", "relfilenode"})
            for value in bindings.values()
        ),
        "dependencies differ",
    )
    return projection.validate_projection_dependency_qnames(
        schema,
        {key: {field: value.get(field) for field in ("schema_name", "table_name")} for key, value in bindings.items()},
    )


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
        and receipt["contract"] in {PUBLICATION_CONTRACT, LEGACY_PUBLICATION_CONTRACT},
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
        and origin["origin_generation"] == authority["local_generation"],
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
    _validate_alias_receipt(receipt, handoff)
    return receipt


def _validate_alias_receipt(receipt, handoff):
    """Validate portable alias counters and retain the old raw digest edition."""
    alias = receipt["alias"]
    is_legacy = receipt["contract"] == LEGACY_PUBLICATION_CONTRACT
    _require(
        type(alias) is dict
        and set(alias) == ({"generation", "catalog_sha256"} if is_legacy else {"generation", "state"})
        and alias["generation"] == handoff["alias_generation"],
        "alias receipt differs",
    )
    if is_legacy:
        _require(
            type(alias["catalog_sha256"]) is str and re.fullmatch(r"[0-9a-f]{64}", alias["catalog_sha256"]) is not None,
            "alias receipt differs",
        )
    else:
        _require(
            isinstance(alias["state"], dict)
            and set(alias["state"]) == {"schema_version", "active_ruleset_version", "generation"}
            and all(type(value) is int and value >= 0 for value in alias["state"].values())
            and alias["state"]["schema_version"] > 0
            and alias["state"]["active_ruleset_version"] > 0
            and alias["state"]["generation"] == alias["generation"],
            "alias state differs",
        )


def _validate_catalog(catalog, handoff):
    if set(catalog) != {"tables"}:
        _require(
            set(catalog) == {"shapes", "indexes", "columns", "constraints"}
            and all(type(catalog_rows) is list and 0 < len(catalog_rows) <= 8192 for catalog_rows in catalog.values()),
            "catalog receipt differs",
        )
        _require(
            len(catalog["shapes"]) == 7
            and all(type(catalog_entry) is list and len(catalog_entry) == 3 for catalog_entry in catalog["shapes"])
            and [catalog_entry[0] for catalog_entry in catalog["shapes"]] == list(generation.RELATION_NAMES),
            "catalog shapes differ",
        )
        _require(
            all(
                isinstance(catalog_entry, dict) and catalog_entry.get("kind") != "f"
                for catalog_entry in catalog["constraints"]
            ),
            "catalog relationship differs",
        )
        return
    _require(
        type(catalog) is dict and set(catalog) == {"tables"} and type(catalog["tables"]) is list,
        "catalog receipt differs",
    )
    tables = catalog["tables"]
    _require(
        len(tables) == 7
        and all(
            type(catalog_entry) is dict and set(catalog_entry) == {"table_name", "columns", "constraints", "indexes"}
            for catalog_entry in tables
        )
        and [catalog_entry["table_name"] for catalog_entry in tables] == list(generation.RELATION_NAMES),
        "catalog family differs",
    )
    for catalog_entry in tables:
        _require(
            all(
                type(catalog_entry[key]) is list and bool(catalog_entry[key])
                for key in ("columns", "constraints", "indexes")
            ),
            "catalog shape differs",
        )


def _validate_publication_inventory(inventory, handoff):
    legacy = isinstance(inventory, dict) and "database_oid" in inventory
    _require(
        type(inventory) is dict
        and set(inventory) == ({"database_oid", "relations"} if legacy else {"relations"})
        and type(inventory["relations"]) is list
        and len(inventory["relations"]) == 7,
        "inventory differs",
    )
    relations_by_name = {entry["relation_name"]: entry for entry in inventory["relations"]}
    _require(set(relations_by_name) == set(generation.RELATION_NAMES), "inventory family differs")
    fields = {"schema_name", "relation_name"} | (
        {"schema_oid", "schema_owner_oid", "relation_oid", "relfilenode", "owner_oid"} if legacy else set()
    )
    _require(
        all(
            set(entry) == fields and entry["schema_name"] == handoff["schema_name"]
            for entry in relations_by_name.values()
        ),
        "named inventory differs",
    )


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
    """Pin the same closed selected dependency names without collecting physical tokens."""
    if bindings_by_name is None:
        bindings_by_name = {
            f"{namespace or schema}.{table}": {"schema_name": namespace or schema, "table_name": table}
            for namespace, table in projection._PROJECTION_DEPENDENCIES
        }
    selected_by_name = _named_dependencies(schema, bindings_by_name)
    await session.execute(text(projection.projection_named_dependency_lock_sql(schema, selected_by_name)))
    return selected_by_name


async def _stage_inventory(session, schema, names):
    relation_rows = (
        (
            await session.execute(
                text(
                    "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                    "WHERE n.nspname=:schema AND c.relname=ANY(:names) AND c.relkind='r' AND c.relpersistence='p' "
                    "AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity AND NOT c.relispartition "
                    "AND NOT EXISTS(SELECT 1 FROM pg_trigger t WHERE t.tgrelid=c.oid) "
                    "AND NOT EXISTS(SELECT 1 FROM pg_rewrite r WHERE r.ev_class=c.oid) "
                    "AND NOT EXISTS(SELECT 1 FROM pg_inherits i WHERE i.inhrelid=c.oid OR i.inhparent=c.oid) "
                    "AND NOT EXISTS(SELECT 1 FROM pg_constraint k WHERE k.conrelid=c.oid AND k.contype='f') "
                    "AND NOT EXISTS(SELECT 1 FROM pg_depend d JOIN pg_class seq ON seq.oid=d.objid AND seq.relkind='S' "
                    "WHERE d.refobjid=c.oid AND d.classid='pg_class'::regclass AND d.deptype IN ('a','i'))"
                ),
                {"schema": schema, "names": list(names.values())},
            )
        )
        .mappings()
        .all()
    )
    _require({row["relname"] for row in relation_rows} == set(names.values()), "stage catalog differs")
    return [{"table_name": name, "stage_name": stage} for name, stage in names.items()]


async def _stage_integrity(session, schema, names):
    """Reuse bounded full-row family accounting at the durable named handoff."""
    from process import entity_address_snapshot_receipt as receipt

    return (
        await receipt.capture_entity_address_stage_integrity_receipt(
            session,
            schema_name=schema,
            stage_table_names=names,
            contract=receipt.LEGACY_STAGE_INTEGRITY_CONTRACT,
            read_only=True,
        )
    ).as_dict()


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
                actual_oid = await receipt._relation_oid(session, schema, relation.get("stage_name", name))
                actual = await _model_catalog_shape(session, receipt, actual_oid, schema)
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


async def _index_inventory(session, relations, *, schema):
    names_by_stage = {entry["stage_name"]: entry["table_name"] for entry in relations}
    async with _catalog_search_path(session):
        rows = (
            (
                await session.execute(
                    text(
                        "SELECT c.relname,pg_get_indexdef(i.indexrelid) AS definition FROM pg_index i "
                        "JOIN pg_class c ON c.oid=i.indrelid JOIN pg_namespace n ON n.oid=c.relnamespace "
                        "WHERE n.nspname=:schema AND c.relname=ANY(:names) AND i.indisvalid AND i.indisready AND i.indislive "
                        "ORDER BY c.relname,definition"
                    ),
                    {"schema": schema, "names": list(names_by_stage)},
                )
            )
            .mappings()
            .all()
        )
    _require({row["relname"] for row in rows} == set(names_by_stage), "indexes incomplete")
    return [{"table_name": names_by_stage[row["relname"]], "definition": row["definition"]} for row in rows]


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
    """Record only portable column, constraint and index model shapes by declared name."""
    from process import entity_address_snapshot_receipt as receipt

    tables = []
    schema = handoff["schema_name"]
    for entry in handoff["stage_relations"]:
        relation = await receipt._relation_oid(session, schema, entry["stage_name"])
        columns, constraints, indexes = await _model_catalog_shape(session, receipt, relation, schema)
        tables.append(
            {"table_name": entry["table_name"], "columns": columns, "constraints": constraints, "indexes": indexes}
        )
    return {"tables": tables}


async def _complete_indexes(schema, import_date):
    """Finish all ordinary model indexes on isolated tables; never rebuild live indexes."""
    native = _native()
    index_options_dict = {"complete_stage_indexes": True}
    for model in generation.ENTITY_ADDRESS_RESULT_MODELS:
        stage = native.make_class(model, import_date)
        await native._ensure_promoted_stage_logged(schema, stage.__tablename__)
        await native._ensure_stage_primary_key(stage, schema, context=index_options_dict)
        for _label, statement in native._stage_index_statements(
            stage, schema, stage_indexes(stage), index_options_dict
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
    handoff["indexes"] = await _index_inventory(session, handoff["stage_relations"], schema=schema)
    handoff["stage_integrity"] = await _stage_integrity(session, schema, names)
    _require(await native._address_alias_generation(schema) == handoff["alias_generation"], "alias changed")
    handoff["incumbent"] = _logical_authority(
        (await generation.read_entity_address_result_generation_authority(session, schema_name=schema)).as_dict()
    )
    handoff["source_contract_sha256"] = _source_contract(handoff, run["params"] or {})
    handoff["handoff_sha256"] = _digest(handoff)
    return validate_entity_address_native_handoff(handoff)


async def _write_handoff(session, handoff):
    schema = handoff["schema_name"]
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
                await native.lock_selected_publication_dependencies(native.db, schema_name)
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
    wanted_relations = [
        {key: entry[key] for key in ("table_name", "stage_name")} for entry in handoff["stage_relations"]
    ]
    _require(await _stage_inventory(session, schema, names) == wanted_relations, "stage changed")
    indexes = await _index_inventory(session, wanted_relations, schema=schema)
    if handoff["contract"] == LEGACY_HANDOFF_CONTRACT:
        _require(
            sorted(row["definition"] for row in indexes) == sorted(row["definition"] for row in handoff["indexes"]),
            "indexes changed",
        )
    else:
        _require(indexes == handoff["indexes"], "indexes changed")
        _require(await _stage_integrity(session, schema, names) == handoff["stage_integrity"], "stage content changed")
    await _require_stage_execution_catalog(session, handoff)


async def _require_stage_execution_catalog(
    session, handoff, *, allowed_type_oids=(), allowed_extensions=("postgis", "intarray", "btree_gin", "btree_gist")
):
    """Apply the ordinary native layout gate to the handoff's closed selected stage names."""
    await _require_named_execution_catalog(
        session,
        tuple((handoff["schema_name"], entry["stage_name"]) for entry in handoff["stage_relations"]),
        allowed_type_oids=allowed_type_oids,
        allowed_extensions=allowed_extensions,
    )


async def _require_named_execution_catalog(
    session,
    relations,
    *,
    allowed_type_oids=(),
    allowed_extensions=("postgis", "intarray", "btree_gin", "btree_gist"),
):
    """Reject builder-installed executable code before publisher-side set queries."""
    from db.plan_release_storage import storage_identifier

    _require(
        isinstance(relations, tuple)
        and 0 < len(relations) <= 256
        and all(isinstance(pair, tuple) and len(pair) == 2 for pair in relations)
        and len(set(relations)) == len(relations),
        "candidate relation names differ",
    )
    for schema, name in relations:
        storage_identifier(schema)
        storage_identifier(name)
    safe = await session.scalar(
        text(
            "SELECT NOT EXISTS(SELECT 1 FROM pg_index i JOIN pg_class idx ON idx.oid=i.indexrelid "
            "JOIN pg_am am ON am.oid=idx.relam WHERE i.indrelid IN (SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN unnest(CAST(:schemas AS text[]),CAST(:names AS text[])) AS selected(schema_name,relation_name) ON (n.nspname,c.relname)=(selected.schema_name,selected.relation_name)) AND ("
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
            "WHERE a.attrelid IN (SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN unnest(CAST(:schemas AS text[]),CAST(:names AS text[])) AS selected(schema_name,relation_name) ON (n.nspname,c.relname)=(selected.schema_name,selected.relation_name)) AND a.attnum>0 AND NOT a.attisdropped "
            "AND (a.attgenerated NOT IN ('','s') OR (ty.typnamespace<>'pg_catalog'::regnamespace "
            "AND NOT (ty.oid=ANY(CAST(:allowed_type_oids AS oid[]))) AND NOT EXISTS(SELECT 1 FROM pg_depend ext "
            "JOIN pg_extension e ON e.oid=ext.refobjid WHERE ext.classid='pg_type'::regclass AND ext.objid=ty.oid "
            "AND ext.refclassid='pg_extension'::regclass AND ext.deptype='e' AND e.extname='postgis' "
            "AND ty.typname IN ('geometry','geography')))))"
        ),
        {
            "schemas": [schema for schema, _name in relations],
            "names": [name for _schema, name in relations],
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


async def _receipt_inventory(session, handoff):
    """Name the complete published family; PostgreSQL controls its actual access."""
    return {
        "relations": [
            {"schema_name": handoff["schema_name"], "relation_name": name} for name in generation.RELATION_NAMES
        ]
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
        _named_dependencies(handoff["schema_name"], dependency_bindings)
        == _named_dependencies(handoff["schema_name"], handoff["dependency_bindings"]),
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
        receipt_by_field = {
            "contract": PUBLICATION_CONTRACT,
            "handoff": handoff,
            "validation": validation_by_field,
            "validation_sha256": _digest(validation_by_field),
            "source_contract_sha256": handoff["source_contract_sha256"],
            "inventory": await _receipt_inventory(session, handoff),
            "alias": {
                "generation": handoff["alias_generation"],
                "state": await _native_alias_state(session, handoff["schema_name"]),
            },
        }
        return await _publish_prepared(session, handoff, prepared, receipt_by_field, publication_continuation)


async def _native_alias_state(session, schema):
    """Name the existing validated native alias-state tuple for the durable receipt."""
    from process.entity_address_snapshot_preparation import _require_alias_authority

    return dict(
        zip(
            ("schema_version", "active_ruleset_version", "generation"),
            await _require_alias_authority(session, schema),
            strict=True,
        )
    )


async def _publish_prepared(session, handoff, prepared, receipt, continuation):
    native = _native()
    cutover_calls = []

    async def cutover():
        """Consume the one local cutover capability inside the caller transaction."""
        _require(not cutover_calls, "cutover already called")
        cutover_calls.append(True)
        current = await generation.read_entity_address_result_generation_authority(
            session, schema_name=handoff["schema_name"]
        )
        _require(_logical_authority(current.as_dict()) == _logical_authority(handoff["incumbent"]), "incumbent changed")
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
        receipt["result_generation"] = _logical_authority(prepared.context["result_generation"])
        validate_entity_address_native_publication(receipt)
        await _complete_control(session, handoff, receipt)
        return json.loads(_json(receipt))

    publication_result = await continuation(session, json.loads(_json(receipt)), cutover)
    _require(
        cutover_calls and session.in_transaction(),
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
    """Retain the original receipt while checking logical cancellation/candidate bindings."""
    abandonment = json.loads(_json(abandonment))
    fields = {
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
        "released_pins",
    }
    candidate_field = "candidate" if "candidate" in abandonment else "candidate_custody"
    _require(
        set(abandonment) == fields | {candidate_field}
        and abandonment["contract"] in {ABANDONMENT_CONTRACT, "entity-address-unpublished-abandonment.v2"}
        and abandonment["stage_disposition"] == "retained-unpublished"
        and abandonment["physical_cleanup_completed"] is False
        and abandonment["same_run_readmission"] == "requires-exact-candidate-cleanup-and-ledger-retirement"
        and abandonment["reason"] in {"terminal-cancellation", "completed-handoff-cancellation", "replaced-attempt"},
        "abandonment differs",
    )
    for key in ("preparation_id", "generation_id", "publication_fence"):
        _require(str(UUID(abandonment[key])) == abandonment[key], "abandonment identity differs")
    _require(re.fullmatch(r"[0-9a-f]{64}", abandonment["admission_sha256"]) is not None, "admission digest differs")
    candidate_fields = {
        "schema_name",
        "attempt_id",
        "attempt_started_at",
        "handoff_sha256",
        "source_contract_sha256",
        "stage_relations",
        "indexes",
    }
    if candidate_field == "candidate_custody":
        candidate_fields |= {"builder_oid", "database_oid", "import_run_oid"}
    _require(
        isinstance(abandonment[candidate_field], dict) and set(abandonment[candidate_field]) == candidate_fields,
        "candidate fields differ",
    )
    _require(
        _logical_candidate(abandonment[candidate_field]) == _logical_candidate(handoff), "abandoned candidate differs"
    )
    _require_abandoned_run_shape(handoff, abandonment["observed_run"])
    return abandonment


def _logical_candidate(value):
    """Read retained candidate names and logical attempt bindings, never old owner tokens."""
    fields = ("schema_name", "attempt_id", "attempt_started_at", "handoff_sha256", "source_contract_sha256")
    _require(
        isinstance(value, dict) and all(field in value for field in (*fields, "stage_relations", "indexes")),
        "candidate fields differ",
    )
    return {
        **{field: value[field] for field in fields},
        "stage_relations": [
            {key: row[key] for key in ("table_name", "stage_name")} for row in value["stage_relations"]
        ],
        "indexes": sorted(row["definition"] for row in value["indexes"]),
    }


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
        all(current[key] == handoff[key] for key in ("run_id", "schema_name"))
        and current["handoff_sha256"] == abandonment["observed_run"]["handoff_sha256"]
        and {key: (run["progress"] or {}).get(key) for key in ("attempt_id", "attempt_started_at")}
        == abandonment["observed_run"]["progress"],
        "cleanup attempt changed",
    )
    _require_cleanup_terminal(run, handoff, current, abandonment)


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


async def _require_cleanup_nonserving(session, handoff):
    """Pin current serving data while refusing any declared canonical cleanup target."""
    schema = handoff["schema_name"]
    names = (*generation.RELATION_NAMES, generation.TABLE_NAME)
    await session.execute(
        text(
            "LOCK TABLE " + ",".join(f'"{schema}"."{name}"' for name in sorted(names)) + " IN ACCESS SHARE MODE NOWAIT"
        )
    )
    candidates = {entry["stage_name"] for entry in handoff["stage_relations"]}
    _require(not candidates.intersection(names), "candidate is serving")
    await generation.read_entity_address_result_generation_authority(session, schema_name=schema)


async def cleanup_entity_address_native_handoff(
    session, handoff, *, abandonment, assert_unreferenced, runtime_owner_oids=None
):
    """Remove only the closed abandoned candidate names under native PostgreSQL access."""
    handoff = validate_entity_address_native_handoff(handoff)
    abandonment = _validate_abandoned_handoff(handoff, abandonment)
    _require(session.in_transaction() and callable(assert_unreferenced), "cleanup requires a trusted transaction")
    async with _catalog_search_path(session):
        return await _cleanup_bound_native_candidate(session, handoff, abandonment, assert_unreferenced)


async def _cleanup_bound_native_candidate(session, handoff, abandonment, assert_unreferenced):
    await _require_cleanup_run(session, handoff, abandonment)
    _require(await assert_unreferenced(session, handoff, abandonment) is True, "candidate remains referenced")
    await _require_cleanup_nonserving(session, handoff)
    await _require_handoff_stage(session, handoff)
    _require(await assert_unreferenced(session, handoff, abandonment) is True, "candidate remains referenced")
    _require(session.in_transaction(), "cleanup requires a transaction")
    relations = ",".join(f'"{handoff["schema_name"]}"."{entry["stage_name"]}"' for entry in handoff["stage_relations"])
    await session.execute(text("DROP TABLE " + relations + " RESTRICT"))
    return {
        "contract": CLEANUP_CONTRACT,
        "preparation_id": abandonment["preparation_id"],
        "abandonment_sha256": _digest(abandonment),
        "handoff_sha256": handoff["handoff_sha256"],
        "schema_name": handoff["schema_name"],
        "stage_relations": [
            {key: entry[key] for key in ("table_name", "stage_name")} for entry in handoff["stage_relations"]
        ],
        "stage_disposition": "removed-unpublished",
        "physical_cleanup_completed": True,
    }
