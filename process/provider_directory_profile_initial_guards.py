# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared initial receipt guards and exact native catalog recognition."""

import hashlib

from process.provider_directory_cms_receipt_guard import _assert_guards, _normalized
from process.provider_directory_profile_initial_contract import GEOMETRY_CONTRACT, RECEIPT_TABLE

_TABLE = RECEIPT_TABLE
_SERVING = "provider_directory_profile_serving_generation"
_GEOMETRY = GEOMETRY_CONTRACT
_PROFILE_FIELDS = (
    "status",
    "operation",
    "control_generation",
    "generation_id",
    "selection_proof_id",
    "authority_revision",
    "profile_schema_version",
    "profile_strategy_version",
    "source_vector_hash",
    "source_context_vector_hash",
    "executable_plan_hash",
    "profile_as_of",
    "evidence_target_oid",
    "profile_target_oid",
    "evidence_rows",
    "profile_rows",
)
IMMUTABLE_BODY = "BEGIN RAISE EXCEPTION 'provider_directory_profile_initial_receipt_immutable'; END;"


def _q(value):
    return '"' + value.replace('"', '""') + '"'


def _qt(schema, table):
    return f"{_q(schema)}.{_q(table)}"


def _literal(value):
    return "'" + value.replace("'", "''") + "'"


def _serving_snapshot(alias):
    return "jsonb_build_object(" + ",".join(f"'{name}',{alias}.{name}" for name in _PROFILE_FIELDS) + ")"


def _receipt_matches_sql(schema):
    """Bind the sole initial receipt to the real installed result and previously consumed signed authority."""
    return f"""SELECT COALESCE(EXISTS (
        SELECT 1 FROM {_qt(schema, _SERVING)} serving
        JOIN {_qt(schema, "provider_directory_profile_capacity_lease_consumption")} consumed
          ON consumed.attestation_id=receipt.attestation_id AND consumed.build_id=receipt.build_id
          AND consumed.run_id=receipt.run_id AND consumed.admission_purpose='profile'
        JOIN {_qt(schema, "provider_directory_profile_capacity_preflight_receipt")} preflight
          ON preflight.receipt_sha256=receipt.payload->>'preflight_receipt_sha256'
          AND preflight.consumed_attestation_id=receipt.attestation_id AND preflight.consumed_run_id=receipt.run_id
        WHERE serving.singleton_key='global' AND serving.status='published' AND serving.operation='publish'
          AND serving.generation_id=receipt.generation_id AND receipt.publication_xid=pg_current_xact_id()
          AND serving.capacity_geometry_status='verified'
          AND serving.capacity_geometry_json->>'contract_id'={_literal(_GEOMETRY)}
          AND receipt.payload->'serving'={_serving_snapshot("serving")}
          AND serving.evidence_target_oid=to_regclass({_literal(_qt(schema, "provider_directory_profile_evidence"))})::oid::bigint
          AND serving.profile_target_oid=to_regclass({_literal(_qt(schema, "provider_directory_profile"))})::oid::bigint
          AND receipt.payload->>'contract_id'=receipt.contract_id
          AND receipt.payload->>'build_id'=receipt.build_id AND receipt.payload->>'run_id'=receipt.run_id
          AND receipt.payload->>'attestation_id'=receipt.attestation_id
          AND receipt.payload->>'capacity_geometry_hash'=serving.capacity_geometry_hash
          AND receipt.payload->'capacity_geometry'=serving.capacity_geometry_json
          AND receipt.payload->>'initial_target_state_sha256'=serving.capacity_geometry_json->>'initial_target_state_sha256'
          AND receipt.payload->'source_vector'=serving.source_vector_json
          AND receipt.payload->'source_context_vector'=serving.source_context_vector_json
          AND receipt.payload->>'executable_plan_hash'=serving.executable_plan_hash
          AND consumed.contract_id='provider-directory-database-capacity-lease-v3'
          AND consumed.lease_digest=receipt.payload->>'lease_digest'
          AND consumed.capacity_geometry_hash=serving.capacity_geometry_hash
          AND consumed.executable_plan_hash=serving.executable_plan_hash
          AND consumed.selection_proof_id=serving.selection_proof_id
          AND consumed.source_vector_hash=serving.source_vector_hash
          AND consumed.source_context_vector_hash=serving.source_context_vector_hash
          AND consumed.profile_as_of=serving.profile_as_of
          AND consumed.accepted_at<=receipt.committed_at AND receipt.committed_at<consumed.max_build_deadline
          AND consumed.max_build_deadline<=consumed.expires_at AND clock_timestamp()<consumed.max_build_deadline
          AND consumed.canonical_lease_json::jsonb->>'nonce'=preflight.receipt_sha256
          AND preflight.consumed_at<=receipt.committed_at AND receipt.committed_at<preflight.expires_at
          AND preflight.contract_id='healthporta.provider-directory-profile-capacity-preflight.v5'
          AND preflight.capacity_geometry_hash=serving.capacity_geometry_hash
          AND consumed.canonical_lease_json::jsonb->'signing_preflight_guard'->'healthcare_request'->>'contract_id'
              ='healthporta.provider-directory-profile-capacity-preflight-request.v5'
          AND consumed.canonical_lease_json::jsonb->'signing_preflight_guard'->'healthcare_request'->>'profile_materialization'
              ='initial_full_swap'
          AND consumed.canonical_lease_json::jsonb->'signing_preflight_guard'->'healthcare_receipt'->'capacity_geometry'
              =serving.capacity_geometry_json
          AND consumed.canonical_lease_json::jsonb->'signing_preflight_guard'->'healthcare_receipt'->>'serving_generation_preflight_sha256'
              =receipt.payload->>'initial_target_state_sha256'
    ),false)"""


def _receipt_insert_body(schema):
    return f"""BEGIN
        IF NOT {_qt(schema, "pd_profile_initial_receipt_matches")}(NEW) THEN
            RAISE EXCEPTION 'provider_directory_profile_initial_receipt_binding_invalid';
        END IF;
        RETURN NULL;
    END;"""


def _serving_insert_body(schema):
    return f"""BEGIN
        IF NEW.capacity_geometry_json->>'contract_id'={_literal(_GEOMETRY)} AND NOT EXISTS (
            SELECT 1 FROM {_qt(schema, _TABLE)} receipt
            WHERE receipt.generation_id=NEW.generation_id AND receipt.publication_xid=pg_current_xact_id()
              AND {_qt(schema, "pd_profile_initial_receipt_matches")}(receipt)
        ) THEN RAISE EXCEPTION 'provider_directory_profile_initial_receipt_required'; END IF;
        RETURN NULL;
    END;"""


def initial_serving_guard_expected(schema):
    """Return the one extra serving guard; schema is an unquoted namespace name."""
    return {"pd_profile_initial_serving_insert": (21, True, _serving_insert_body(schema))}


async def _assert_match_function(database, schema):
    """Do not trust an unchanged trigger whose result-checking function has drifted."""
    rows = await database.all(
        """SELECT p.oid::bigint AS function_oid,n.nspname AS function_schema,p.proname AS function_name,
        p.prosrc,p.prosecdef,p.provolatile::text AS provolatile,p.proconfig,p.pronargs,
        p.proargnames,p.prorettype='bool'::regtype AS returns_boolean,p.proargtypes[0]=c.reltype AS receipt_argument,
        l.lanname FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
        JOIN pg_language l ON l.oid=p.prolang JOIN pg_class c ON c.relnamespace=n.oid
        WHERE n.nspname=:schema AND p.proname='pd_profile_initial_receipt_matches'
          AND c.relname='provider_directory_profile_initial_receipt'""",
        schema=schema,
    )
    row = dict(rows[0]._mapping) if len(rows) == 1 else {}
    if (
        row.get("prosecdef") is not False
        or row.get("provolatile") != "v"
        or row.get("proconfig") != ["search_path=pg_catalog"]
        or row.get("pronargs") != 1
        or row.get("proargnames") != ["receipt"]
        or row.get("returns_boolean") is not True
        or row.get("receipt_argument") is not True
        or row.get("lanname") != "sql"
        or _normalized(row.get("prosrc", "")) != _normalized(_receipt_matches_sql(schema))
    ):
        raise RuntimeError("provider_directory_profile_initial_receipt_guard_shape_changed")
    row["function_source_sha256"] = hashlib.sha256(row.pop("prosrc").encode("utf-8")).hexdigest()
    return row


async def initial_guard_dependencies(database, schema):
    """Return the checked helper identity for native layout fingerprinting."""
    return await _assert_match_function(database, schema)


async def assert_initial_receipt_guards(database, relation_map, captured=None):
    """Recognize exactly two immutable guards and the deferred signed-publication check."""
    schema = relation_map["schema_name"]
    expected_by_trigger = {
        "pd_profile_initial_receipt_write_guard": (26, False, IMMUTABLE_BODY),
        "pd_profile_initial_receipt_truncate_guard": (34, False, IMMUTABLE_BODY),
        "pd_profile_initial_receipt_insert": (5, True, _receipt_insert_body(schema)),
    }
    await _assert_guards(database, relation_map, expected_by_trigger, captured)
    await _assert_match_function(database, schema)


_RECEIPT_COLUMNS = (
    ("build_id", 1043, 68, ""),
    ("attestation_id", 1043, 68, ""),
    ("generation_id", 1043, 68, ""),
    ("singleton_key", 1043, 20, "'global'::character varying"),
    ("run_id", 1043, 68, ""),
    ("contract_id", 25, -1, ""),
    ("payload", 3802, -1, ""),
    ("payload_sha256", 1043, 68, ""),
    ("publication_xid", 5069, -1, "pg_current_xact_id()"),
    ("committed_at", 1184, -1, "clock_timestamp()"),
)
_RECEIPT_CHECKS = {
    "CHECK (attestation_id::text ~ '^[0-9a-f]{64}$'::text)",
    "CHECK (build_id::text ~ '^pdpb_[0-9a-f]{32}$'::text)",
    "CHECK (contract_id = 'healthporta.provider-directory-profile-initial-receipt.v1'::text)",
    "CHECK (generation_id::text ~ '^pdprofile_[0-9a-f]{32}$'::text)",
    "CHECK (jsonb_typeof(payload) = 'object'::text)",
    "CHECK (payload_sha256::text = encode(sha256(convert_to(payload::text, 'UTF8'::name)), 'hex'::text))",
    "CHECK (run_id::text ~ '^run_[0-9a-f]{32}$'::text)",
    "CHECK (singleton_key::text = 'global'::text)",
}


def _assert_receipt_columns(oid, attributes):
    columns = [entry for entry in attributes if entry["relation_oid"] == oid]
    if len(columns) != len(_RECEIPT_COLUMNS):
        raise RuntimeError("provider_directory_profile_initial_receipt_storage_shape_changed")
    for position, (column, expected) in enumerate(zip(columns, _RECEIPT_COLUMNS, strict=True), 1):
        name, type_oid, typmod, default = expected
        if (
            (column["attnum"], column["attname"], column["atttypid"], column["atttypmod"], column["default_expression"])
            != (position, name, type_oid, typmod, default)
            or column["attnotnull"] is not True
            or column["attidentity"]
            or column["attgenerated"]
            or column["attcompression"]
            or column["atthasdef"] is not bool(default)
            or column["attcollation"] != (100 if type_oid in {1043, 25} else 0)
            or column["attstorage"] != ("x" if type_oid in {1043, 25, 3802} else "p")
        ):
            raise RuntimeError("provider_directory_profile_initial_receipt_storage_shape_changed")


def _assert_receipt_indexes(oid, indexes):
    primary_indexes = [entry for entry in indexes if entry["relation_oid"] == oid]
    if len(primary_indexes) != 4 or {entry["indkey"] for entry in primary_indexes} != {"1", "2", "3", "4"}:
        raise RuntimeError("provider_directory_profile_initial_receipt_storage_shape_changed")
    for index in primary_indexes:
        if (
            index["index_am"] != "btree"
            or index["reloptions"] != []
            or index["indnatts"] != 1
            or index["indnkeyatts"] != 1
            or index["indisunique"] is not True
            or index["indimmediate"] is not True
            or index["indisprimary"] is not (index["indkey"] == "1")
            or not all(index[name] for name in ("indisvalid", "indisready", "indislive"))
            or any(
                index[name]
                for name in (
                    "indisexclusion",
                    "indisclustered",
                    "indcheckxmin",
                    "indisreplident",
                    "indnullsnotdistinct",
                )
            )
            or index["index_expressions"]
            or index["index_predicate"]
            or (index["indclass"], index["indcollation"], index["indoption"]) != ("3126", "100", "0")
        ):
            raise RuntimeError("provider_directory_profile_initial_receipt_storage_shape_changed")


def assert_initial_receipt_catalog(oid, attributes, indexes, constraints):
    """Allow only the migration's columns, built-in defaults, checks, and four unique indexes."""
    _assert_receipt_columns(oid, attributes)
    _assert_receipt_indexes(oid, indexes)
    expected_constraints = (
        {("c", definition, False, False, False) for definition in _RECEIPT_CHECKS}
        | {("n", "NOT NULL " + column[0], False, False, False) for column in _RECEIPT_COLUMNS}
        | {("p", "PRIMARY KEY (build_id)", False, False, True)}
        | {
            ("u", "UNIQUE (" + name + ")", False, False, True)
            for name in ("attestation_id", "generation_id", "singleton_key")
        }
        | {("t", "TRIGGER DEFERRABLE INITIALLY DEFERRED", True, True, True)}
    )
    primary_constraints = [entry for entry in constraints if entry["relation_oid"] == oid]
    actual_constraints = {
        (
            entry["constraint_type"],
            entry["constraint_definition"],
            entry["condeferrable"],
            entry["condeferred"],
            entry["connoinherit"],
        )
        for entry in primary_constraints
    }
    if (
        len(primary_constraints) != len(expected_constraints)
        or actual_constraints != expected_constraints
        or any(entry["convalidated"] is not True for entry in primary_constraints)
    ):
        raise RuntimeError("provider_directory_profile_initial_receipt_storage_shape_changed")
