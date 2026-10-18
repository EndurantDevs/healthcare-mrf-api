# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Raw native control witnesses for the settled managed pricing lineage."""

from db.migration_expression_adoption import _normalized_expression
from db.models import ImportRun
from db.plan_release_storage import plan_release_schema, storage_relation
from process.plan_pricing_admission import _dispatches, require
from process.plan_pricing_control import DELIVERY_FIELD, require_pricing_replay
from process.plan_release_storage_custody import _document

CONTRACT = "managed-pricing-no-work-v1"
NATIVE_RUN_CONTRACT = "managed-pricing-native-run-v1"
NATIVE_LINEAGE_CONTRACT = "managed-pricing-native-lineage-v1"
TERMINAL = frozenset({"succeeded", "failed", "canceled", "cancelled", "dead_letter"})
RETRY_INDEX = "import_run_plan_pricing_retry_census_idx"
RETRY_PREDICATE = "importer = 'plan-pricing-projection' AND retry_of_run_id IS NOT NULL"
RETRY_NATIVE_PREDICATE = "(((importer)::text = 'plan-pricing-projection'::text) AND (retry_of_run_id IS NOT NULL))"
RUN_HASH_SQL = "encode(sha256(convert_to(jsonb_build_object('contract','managed-pricing-native-run-v1','run',to_jsonb(actual))::text,'UTF8')),'hex')"
CHILD_CENSUS_SQL = "SELECT run_id,retry_of_run_id FROM {runs} WHERE importer='plan-pricing-projection' AND retry_of_run_id IS NOT NULL AND retry_of_run_id=ANY($1::varchar[]) ORDER BY retry_of_run_id,run_id LIMIT 4"


def decoded_native_run(native_column):
    """Decode native JSON controls without status overlays or timestamp rewriting."""
    return {
        name: _document(value) if name in {"params", "metrics", "error", "progress"} else value
        for name, value in dict(native_column).items()
    }


async def native_run(connection, run_id):
    """Read exactly this raw persisted run with its all-column native hash."""
    native_column = await connection.fetchrow(
        f"SELECT actual.*,{RUN_HASH_SQL} AS row_digest FROM {storage_relation(plan_release_schema(), 'import_run')} actual WHERE run_id=$1",
        run_id,
    )
    require(native_column is not None, "native_run_missing")
    return decoded_native_run(native_column)


async def require_native_control_shape(connection):
    """Keep the raw hash contract and the complete child census index exact."""
    relation = storage_relation(plan_release_schema(), "import_run")
    columns = await connection.fetch(
        "SELECT attname,format_type(atttypid,atttypmod) AS type FROM pg_attribute WHERE attrelid=$1::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum",
        relation,
    )
    require(
        [native_column["attname"] for native_column in columns] == list(ImportRun.__table__.columns.keys()),
        "native_run_columns_changed",
    )
    require(
        all(
            native_column["type"] == "timestamp without time zone"
            for native_column in columns
            if native_column["attname"] in {"created_at", "started_at", "finished_at", "heartbeat_at"}
        ),
        "native_run_time_shape_changed",
    )
    index = await connection.fetchrow(
        "SELECT i.indisvalid AND i.indisready AND i.indislive AND i.indimmediate AND NOT i.indisunique "
        "AND NOT i.indisexclusion AND i.indnkeyatts=i.indnatts AND i.indexprs IS NULL AND am.amname='btree' "
        "AND NOT EXISTS(SELECT 1 FROM unnest(i.indkey::smallint[],i.indclass::oid[],i.indcollation::oid[],i.indoption::smallint[]) "
        "key(attnum,opclass_oid,collation_oid,sort_flags) JOIN pg_opclass op ON op.oid=key.opclass_oid "
        "JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=key.attnum WHERE op.opcnamespace<>'pg_catalog'::regnamespace "
        "OR op.opcname<>'text_ops' OR key.collation_oid<>a.attcollation OR key.sort_flags<>0) AS valid,"
        "pg_get_expr(i.indpred,i.indrelid) AS predicate,ARRAY(SELECT a.attname FROM unnest(i.indkey::smallint[]) WITH ORDINALITY key(attnum,ordinal) "
        "JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=key.attnum ORDER BY key.ordinal) AS keys "
        "FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am am ON am.oid=c.relam "
        "WHERE i.indrelid=$1::regclass AND c.relname=$2",
        relation,
        RETRY_INDEX,
    )
    require(
        index is not None
        and index["valid"]
        and index["keys"] == ["retry_of_run_id", "run_id"]
        and _normalized_expression(index["predicate"]) == _normalized_expression(RETRY_NATIVE_PREDICATE),
        "native_retry_index_changed",
    )


def _expected_runs(admission):
    require(admission.record_by_field["status"] in {"ready", "unavailable", "failed"}, "admission_not_terminal")
    require(
        admission.record_by_field["preparation_status"] in {"admitting", "sealed", "published", "abandoned"},
        "preparation_state_invalid",
    )
    dispatches = _dispatches(admission.record_by_field, admission.request)
    require(all(dispatch["run_id"] for dispatch in dispatches), "dispatch_unsettled")
    return {dispatch["run_id"]: dispatch for dispatch in dispatches}


def _require_terminal_run(run_by_field, dispatch, admission):
    expected_by_field = {
        "engine": "healthcare-mrf-api",
        "node_id": admission.record_by_field["node_id"],
        "importer": "plan-pricing-projection",
        "idempotency_key": dispatch["key"],
        "retry_of_run_id": dispatch["retry_of"],
        "params": {
            name: admission.request[name] for name in ("binding_manifest_digest", "projection_contract", "bindings")
        },
    }
    require_pricing_replay(run_by_field, expected_by_field)
    require(
        run_by_field["triggered_by"] == "release"
        and run_by_field["status"] in TERMINAL
        and run_by_field["finished_at"] is not None,
        "native_run_not_terminal",
    )
    delivery = (run_by_field["metrics"] or {}).get(DELIVERY_FIELD)
    require(
        type(delivery) is dict
        and set(delivery) == {"run_id", "state", "outcome"}
        and delivery["run_id"] == run_by_field["run_id"]
        and delivery["state"] == "settled"
        and delivery["outcome"] in {"queued", "failed"},
        "producer_not_settled",
    )


async def native_lineage_witness(connection, admission):
    """Compare every recorded request and every direct child before returning hashes."""
    expected_by_run = _expected_runs(admission)
    relation = storage_relation(plan_release_schema(), "import_run")
    rows = await connection.fetch(
        f'SELECT actual.*,{RUN_HASH_SQL} AS row_digest FROM {relation} actual WHERE run_id=ANY($1::varchar[]) ORDER BY run_id COLLATE "C"',
        sorted(expected_by_run),
    )
    require(len(rows) == len(expected_by_run), "native_lineage_missing")
    runs = [decoded_native_run(native_column) for native_column in rows]
    for run_by_field in runs:
        _require_terminal_run(run_by_field, expected_by_run[run_by_field["run_id"]], admission)
    children = await connection.fetch(CHILD_CENSUS_SQL.format(runs=relation), sorted(expected_by_run))
    expected_children = sorted(
        (run_id, dispatch["retry_of"]) for run_id, dispatch in expected_by_run.items() if dispatch["retry_of"]
    )
    require(
        len(children) < 4
        and sorted((native_column["run_id"], native_column["retry_of_run_id"]) for native_column in children)
        == expected_children,
        "native_lineage_extra_child",
    )
    lineage_digest = await connection.fetchval(
        f"WITH hashes AS (SELECT actual.run_id,{RUN_HASH_SQL} AS digest FROM {relation} actual WHERE run_id=ANY($1::varchar[])) "
        "SELECT encode(sha256(convert_to(jsonb_build_object('contract','managed-pricing-native-lineage-v1','runs',"
        "jsonb_agg(jsonb_build_array(run_id,digest) ORDER BY run_id COLLATE \"C\"))::text,'UTF8')),'hex') FROM hashes",
        sorted(expected_by_run),
    )
    return runs, lineage_digest


def native_run_coordinates(run_by_field):
    """Expose fixed coordinates only; all request content stays server-side."""
    progress = run_by_field["progress"] or {}
    return {
        **{
            name: run_by_field[name]
            for name in ("run_id", "retry_of_run_id", "idempotency_key", "status", "row_digest")
        },
        "attempt_id": progress.get("attempt_id"),
        "attempt_started_at": progress.get("attempt_started_at"),
        "delivery": run_by_field["metrics"][DELIVERY_FIELD],
    }
