"""Add bounded admission receipts for Provider Directory endpoint datasets.

Revision ID: 20260812010000_provider_directory_endpoint_dataset_admission_seal
Revises: 20260811140000_ptg_v12_provider_publication_merge

The application terminal validator is the admission authority.  These nullable
receipts prevent accidental stale or partial writes; they are not cryptographic
authentication against the owner of the table or its functions.
"""

from __future__ import annotations

from functools import lru_cache
import importlib.util
import os
from pathlib import Path
from types import ModuleType

from alembic import op


revision = "20260812010000_provider_directory_endpoint_dataset_admission_seal"
down_revision = "20260811140000_ptg_v12_provider_publication_merge"
branch_labels = None
depends_on = None


_TABLE = "provider_directory_endpoint_dataset"
_DIGEST_FUNCTION = (
    "provider_directory_endpoint_dataset_admission_metadata_sha256"
)
_GUARD_FUNCTION = "guard_provider_directory_endpoint_dataset_admission_seal"
_TRUNCATE_FUNCTION = (
    "guard_provider_directory_endpoint_dataset_admission_truncate"
)
_GUARD_TRIGGER = "provider_directory_endpoint_dataset_admission_seal_guard"
_RAW_GUARD_TRIGGER = (
    "provider_directory_endpoint_dataset_admission_raw_guard"
)
_TRUNCATE_TRIGGER = (
    "provider_directory_endpoint_dataset_admission_truncate_guard"
)
_REPLAY_CHECK = "pd_endpoint_dataset_subset_replay_evidence_check"
_REPLAY_GUARD_FUNCTION = (
    "guard_pd_endpoint_dataset_subset_replay_evidence"
)
_REPLAY_GUARD_TRIGGER = (
    "pd_endpoint_dataset_subset_replay_evidence_guard"
)
_REVIEWED_ROOT_POLICY_FILE = (
    "20260809030000_provider_directory_reviewed_root_policy.py"
)
_PAYLOAD_CANONICAL_FUNCTION = (
    "provider_directory_subset_payload_canonical_json"
)
_PAYLOAD_SHA256_FUNCTION = "provider_directory_subset_payload_sha256"
_CONTRACT = "provider-directory-admission-seal-v1"
_SUMMARY_MAX_BYTES = 1024 * 1024
_RESOURCE_TYPE_MAX_COUNT = 64
_RESOURCE_TYPE_MAX_BYTES = 64
_MUTABLE_SUMMARY_KEYS = (
    "dataset_network_plan",
    "dataset_affiliation_organization",
    "outcome_resource_counts_v1",
    "source_summary_v1",
)

_SEAL_COLUMNS = (
    "publication_metadata_summary_json",
    "publication_metadata_sha256",
    "content_proof_admission_version",
    "content_proof_admission_kind",
    "content_proof_admission_sha256",
    "content_proof_resource_types",
)
_PRE_M1_COLUMNS = (
    "dataset_id",
    "endpoint_id",
    "import_run_id",
    "acquisition_root_run_id",
    "previous_dataset_id",
    "dataset_hash",
    "status",
    "is_current",
    "resource_count",
    "created_at",
    "validated_at",
    "published_at",
    "superseded_at",
    "publication_metadata_json",
    "completion_proof_required_version",
    "completion_proof_json",
    "completion_proof_sha256",
)
_LEGACY_TRIGGER_SHAPES = (
    (
        "tin_npi_connector_endpoint_dataset_guard",
        "BEFORE INSERT OR DELETE OR UPDATE",
        "guard_tin_npi_connector_endpoint_dataset",
        31,
        False,
    ),
    (
        "provider_directory_reviewed_subset_activation_dataset_guard",
        "BEFORE INSERT OR UPDATE",
        "guard_provider_directory_reviewed_subset_activation_dataset",
        23,
        False,
    ),
    (
        "pd_subset_abandonment_dataset_guard",
        "BEFORE INSERT OR DELETE OR UPDATE",
        "guard_provider_directory_subset_abandonment_dataset",
        31,
        False,
    ),
    (
        "pd_subset_abandonment_dataset_consistency_guard",
        "AFTER UPDATE",
        "guard_provider_directory_subset_abandonment_dataset",
        17,
        True,
    ),
    (
        "pd_subset_terminal_disposition_dataset_consistency_guard",
        "AFTER UPDATE",
        "guard_provider_directory_subset_abandonment_dataset",
        17,
        True,
    ),
    (
        "pd_trr_dataset_row",
        "BEFORE INSERT OR DELETE OR UPDATE",
        "guard_provider_directory_terminal_root_retirement_parent",
        31,
        False,
    ),
)


@lru_cache(maxsize=1)
def _reviewed_root_policy() -> ModuleType:
    path = Path(__file__).with_name(_REVIEWED_ROOT_POLICY_FILE)
    module_spec = importlib.util.spec_from_file_location(
        "_provider_directory_admission_seal_reviewed_root_policy",
        path,
    )
    if module_spec is None or module_spec.loader is None:
        raise RuntimeError("provider directory reviewed root policy is unavailable")
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _schema() -> str:
    runtime = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy = os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError(
            "DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema"
        )
    return runtime or legacy or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _ql(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _qf(schema: str, name: str) -> str:
    return f"{_q(schema)}.{_q(name)}"


def _runtime_fence_sql(schema: str) -> str:
    canonical_signature = _qf(schema, _PAYLOAD_CANONICAL_FUNCTION) + "(jsonb)"
    sha256_signature = _qf(schema, _PAYLOAD_SHA256_FUNCTION) + "(jsonb)"
    return f"""
    DO $migration$
    BEGIN
        IF pg_catalog.current_setting('server_encoding') <> 'UTF8'
           OR pg_catalog.to_regprocedure(
                  {_ql(canonical_signature)}
              ) IS NULL
           OR pg_catalog.to_regprocedure(
                  {_ql(sha256_signature)}
              ) IS NULL THEN
            RAISE EXCEPTION
                'provider_directory_endpoint_dataset_admission_runtime_invalid'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _legacy_surface_fence_sql(schema: str, *, scoped: bool) -> str:
    table = _qf(schema, _TABLE)
    expected_rows = ",\n            ".join(
        "(" + ", ".join(
            (
                _ql(trigger_name),
                str(trigger_type),
                _ql(_qf(schema, function_name) + "()"),
                str(is_constraint).lower(),
            )
        ) + ")"
        for (
            trigger_name,
            _event_clause,
            function_name,
            trigger_type,
            is_constraint,
        ) in _LEGACY_TRIGGER_SHAPES
    )
    scoped_sql = str(scoped).lower()
    replay_function = _qf(schema, _REPLAY_GUARD_FUNCTION) + "()"
    return f"""
    DO $migration$
    DECLARE
        expected_attributes text;
        matched_triggers bigint;
        replay_checks bigint;
        replay_triggers bigint;
    BEGIN
        LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE;
        SELECT pg_catalog.string_agg(
                   attribute.attnum::text,
                   ' ' ORDER BY requested.ordinality
               )
          INTO expected_attributes
          FROM pg_catalog.unnest(
                   ARRAY[{', '.join(_ql(column) for column in _PRE_M1_COLUMNS)}]
               ) WITH ORDINALITY AS requested(column_name, ordinality)
          JOIN pg_catalog.pg_attribute AS attribute
            ON attribute.attrelid = {_ql(table)}::regclass
           AND attribute.attname = requested.column_name
           AND attribute.attnum > 0
           AND NOT attribute.attisdropped;
        IF pg_catalog.array_length(
               pg_catalog.string_to_array(expected_attributes, ' '), 1
           ) <> {len(_PRE_M1_COLUMNS)} THEN
            RAISE EXCEPTION
                'provider_directory_endpoint_dataset_admission_legacy_columns_changed'
                USING ERRCODE = '55000';
        END IF;

        SELECT pg_catalog.count(*)
          INTO matched_triggers
          FROM (VALUES
            {expected_rows}
          ) AS expected(
              trigger_name,
              trigger_type,
              function_signature,
              is_constraint
          )
          JOIN pg_catalog.pg_trigger AS trigger_row
            ON trigger_row.tgrelid = {_ql(table)}::regclass
           AND trigger_row.tgname = expected.trigger_name
           AND trigger_row.tgtype = expected.trigger_type
           AND trigger_row.tgfoid = pg_catalog.to_regprocedure(
                   expected.function_signature
               )
           AND trigger_row.tgenabled = 'A'
           AND trigger_row.tgisinternal IS FALSE
           AND (trigger_row.tgconstraint <> 0) = expected.is_constraint
           AND trigger_row.tgdeferrable = expected.is_constraint
           AND trigger_row.tginitdeferred = expected.is_constraint
           AND trigger_row.tgattr::text = CASE
                   WHEN {scoped_sql} THEN expected_attributes
                   ELSE ''
               END;
        SELECT pg_catalog.count(*)
          INTO replay_checks
          FROM pg_catalog.pg_constraint AS constraint_row
         WHERE constraint_row.conrelid = {_ql(table)}::regclass
           AND constraint_row.conname = {_ql(_REPLAY_CHECK)}
           AND constraint_row.contype = 'c'
           AND constraint_row.convalidated IS TRUE
           AND constraint_row.condeferrable IS FALSE
           AND constraint_row.condeferred IS FALSE;
        SELECT pg_catalog.count(*)
          INTO replay_triggers
          FROM pg_catalog.pg_trigger AS trigger_row
         WHERE trigger_row.tgrelid = {_ql(table)}::regclass
           AND trigger_row.tgname = {_ql(_REPLAY_GUARD_TRIGGER)}
           AND trigger_row.tgtype = 23
           AND trigger_row.tgfoid = pg_catalog.to_regprocedure(
                   {_ql(replay_function)}
               )
           AND trigger_row.tgenabled = 'A'
           AND trigger_row.tgisinternal IS FALSE
           AND trigger_row.tgconstraint = 0
           AND trigger_row.tgdeferrable IS FALSE
           AND trigger_row.tginitdeferred IS FALSE
           AND trigger_row.tgattr::text = expected_attributes;
        IF matched_triggers <> {len(_LEGACY_TRIGGER_SHAPES)}
           OR ({scoped_sql} AND (replay_checks <> 0 OR replay_triggers <> 1))
           OR (NOT {scoped_sql} AND (
                  replay_checks <> 1
                  OR replay_triggers <> 0
                  OR pg_catalog.to_regprocedure({_ql(replay_function)})
                       IS NOT NULL
              )) THEN
            RAISE EXCEPTION
                'provider_directory_endpoint_dataset_admission_legacy_surface_changed'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _add_columns_sql(schema: str) -> str:
    table = _qf(schema, _TABLE)
    return f"""
    ALTER TABLE {table}
        ADD COLUMN publication_metadata_summary_json jsonb,
        ADD COLUMN publication_metadata_sha256 varchar(64),
        ADD COLUMN content_proof_admission_version smallint,
        ADD COLUMN content_proof_admission_kind varchar(32),
        ADD COLUMN content_proof_admission_sha256 varchar(64),
        ADD COLUMN content_proof_resource_types varchar(64)[];
    """


def _digest_function_sql(schema: str) -> str:
    digest = _qf(schema, _DIGEST_FUNCTION)
    payload_sha256 = _qf(schema, _PAYLOAD_SHA256_FUNCTION)
    return f"""
    CREATE FUNCTION {digest}(
        metadata_summary jsonb,
        admission_version smallint,
        admission_kind text,
        proof_sha256 text,
        resource_types varchar[]
    ) RETURNS varchar
    LANGUAGE sql
    IMMUTABLE
    STRICT
    PARALLEL SAFE
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
        SELECT {payload_sha256}(
            pg_catalog.jsonb_build_object(
                'contract', {_ql(_CONTRACT)},
                'metadata_summary', metadata_summary,
                'admission_version', admission_version,
                'admission_kind', admission_kind,
                'proof_sha256', proof_sha256,
                'resource_types', pg_catalog.to_jsonb(resource_types)
            )
        )::varchar;
    $function$;
    """


def _guard_function_sql(schema: str) -> str:
    guard = _qf(schema, _GUARD_FUNCTION)
    digest = _qf(schema, _DIGEST_FUNCTION)
    canonical = _qf(schema, _PAYLOAD_CANONICAL_FUNCTION)
    immutable_new_summary = "(" + "\n                   - ".join(
        ("NEW.publication_metadata_summary_json",)
        + tuple(f"'{key}'" for key in _MUTABLE_SUMMARY_KEYS)
    ) + ") #- '{twin_root_verification_v1,baseline_payload_retirement}'"
    immutable_old_summary = "(" + "\n                   - ".join(
        ("OLD.publication_metadata_summary_json",)
        + tuple(f"'{key}'" for key in _MUTABLE_SUMMARY_KEYS)
    ) + ") #- '{twin_root_verification_v1,baseline_payload_retirement}'"
    return f"""
    CREATE FUNCTION {guard}()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    DECLARE
        seal_absent boolean;
        seal_complete boolean;
    BEGIN
        IF TG_NARGS = 1 AND TG_ARGV[0] = 'raw' THEN
            IF OLD.content_proof_admission_version IS NOT NULL THEN
                RAISE EXCEPTION
                    'provider_directory_endpoint_dataset_admission_raw_metadata_immutable'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END IF;
        seal_absent :=
            NEW.publication_metadata_summary_json IS NULL
            AND NEW.publication_metadata_sha256 IS NULL
            AND NEW.content_proof_admission_version IS NULL
            AND NEW.content_proof_admission_kind IS NULL
            AND NEW.content_proof_admission_sha256 IS NULL
            AND NEW.content_proof_resource_types IS NULL;
        seal_complete :=
            NEW.publication_metadata_summary_json IS NOT NULL
            AND NEW.publication_metadata_sha256 IS NOT NULL
            AND NEW.content_proof_admission_version IS NOT NULL
            AND NEW.content_proof_admission_kind IS NOT NULL
            AND NEW.content_proof_admission_sha256 IS NOT NULL
            AND NEW.content_proof_resource_types IS NOT NULL;

        IF NOT seal_absent AND NOT seal_complete THEN
            RAISE EXCEPTION
                'provider_directory_endpoint_dataset_admission_seal_partial'
                USING ERRCODE = '23514';
        END IF;
        IF seal_absent THEN
            IF TG_OP = 'UPDATE'
               AND OLD.content_proof_admission_version IS NOT NULL THEN
                RAISE EXCEPTION
                    'provider_directory_endpoint_dataset_admission_receipt_immutable'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END IF;

        IF NEW.content_proof_admission_version <> 1 THEN
            RAISE EXCEPTION
                'provider_directory_endpoint_dataset_admission_version_invalid'
                USING ERRCODE = '23514';
        END IF;
        IF NEW.content_proof_admission_kind NOT IN (
            'generic',
            'uhc_canonical'
        ) THEN
            RAISE EXCEPTION
                'provider_directory_endpoint_dataset_admission_kind_invalid'
                USING ERRCODE = '23514';
        END IF;
        IF NEW.content_proof_admission_sha256 !~ '^[0-9a-f]{{64}}$' THEN
            RAISE EXCEPTION
                'provider_directory_endpoint_dataset_admission_proof_sha256_invalid'
                USING ERRCODE = '23514';
        END IF;
        IF pg_catalog.jsonb_typeof(
               NEW.publication_metadata_summary_json
           ) <> 'object' THEN
            RAISE EXCEPTION
                'provider_directory_endpoint_dataset_admission_summary_invalid'
                USING ERRCODE = '23514';
        END IF;
        IF pg_catalog.octet_length(
               pg_catalog.convert_to(
                   {canonical}(NEW.publication_metadata_summary_json),
                   'UTF8'
               )
           ) > {_SUMMARY_MAX_BYTES} THEN
            RAISE EXCEPTION
                'provider_directory_endpoint_dataset_admission_summary_unbounded'
                USING ERRCODE = '23514';
        END IF;
        IF pg_catalog.cardinality(NEW.content_proof_resource_types)
               > {_RESOURCE_TYPE_MAX_COUNT}
           OR EXISTS (
                SELECT 1
                  FROM pg_catalog.unnest(
                           NEW.content_proof_resource_types
                       ) AS resource(resource_type)
                 WHERE resource.resource_type IS NULL
                    OR resource.resource_type = ''
                    OR pg_catalog.octet_length(resource.resource_type)
                           > {_RESOURCE_TYPE_MAX_BYTES}
           )
           OR NEW.content_proof_resource_types IS DISTINCT FROM (
                SELECT COALESCE(
                           pg_catalog.array_agg(
                               resource.resource_type
                               ORDER BY resource.resource_type
                                   COLLATE pg_catalog."C"
                           ),
                           ARRAY[]::varchar[]
                       )
                  FROM (
                      SELECT DISTINCT item.resource_type
                        FROM pg_catalog.unnest(
                                 NEW.content_proof_resource_types
                             ) AS item(resource_type)
                  ) AS resource
           ) THEN
            RAISE EXCEPTION
                'provider_directory_endpoint_dataset_admission_resources_invalid'
                USING ERRCODE = '23514';
        END IF;
        IF NEW.publication_metadata_sha256 !~ '^[0-9a-f]{{64}}$'
           OR NEW.publication_metadata_sha256 IS DISTINCT FROM {digest}(
                  NEW.publication_metadata_summary_json,
                  NEW.content_proof_admission_version,
                  NEW.content_proof_admission_kind::text,
                  NEW.content_proof_admission_sha256,
                  NEW.content_proof_resource_types
              ) THEN
            RAISE EXCEPTION
                'provider_directory_endpoint_dataset_admission_metadata_sha256_invalid'
                USING ERRCODE = '23514';
        END IF;

        IF TG_OP = 'UPDATE'
           AND OLD.content_proof_admission_version IS NOT NULL THEN
            IF ROW(
                   {immutable_new_summary},
                   NEW.content_proof_admission_version,
                   NEW.content_proof_admission_kind,
                   NEW.content_proof_admission_sha256,
                   NEW.content_proof_resource_types
               ) IS DISTINCT FROM ROW(
                   {immutable_old_summary},
                   OLD.content_proof_admission_version,
                   OLD.content_proof_admission_kind,
                   OLD.content_proof_admission_sha256,
                   OLD.content_proof_resource_types
               ) THEN
                RAISE EXCEPTION
                    'provider_directory_endpoint_dataset_admission_receipt_immutable'
                    USING ERRCODE = '55000';
            END IF;
        END IF;
        RETURN NEW;
    END;
    $function$;
    """


def _truncate_function_sql(schema: str) -> str:
    guard = _qf(schema, _TRUNCATE_FUNCTION)
    return f"""
    CREATE FUNCTION {guard}()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    BEGIN
        RAISE EXCEPTION
            'provider_directory_endpoint_dataset_admission_truncate_forbidden'
            USING ERRCODE = '55000';
    END;
    $function$;
    """


def _replay_guard_function_sql(schema: str) -> str:
    guard = _qf(schema, _REPLAY_GUARD_FUNCTION)
    subset = _reviewed_root_policy()._subset()
    predicate = subset._subset_replay_evidence_check(
        schema,
        reviewed_root_policy_aware=True,
    )
    return f"""
    CREATE FUNCTION {guard}()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    DECLARE
        replay_evidence_valid boolean;
    BEGIN
        SELECT ({predicate})
          INTO replay_evidence_valid
          FROM (SELECT NEW.*) AS dataset_row;
        IF replay_evidence_valid IS FALSE THEN
            RAISE EXCEPTION
                'new row for relation {_TABLE} violates check constraint {_REPLAY_CHECK}'
                USING ERRCODE = '23514', CONSTRAINT = {_ql(_REPLAY_CHECK)};
        END IF;
        RETURN NEW;
    END;
    $function$;
    """


def _legacy_trigger_sqls(schema: str, *, scoped: bool) -> tuple[str, ...]:
    table = _qf(schema, _TABLE)
    watched_columns = ",\n            ".join(
        _q(column_name) for column_name in _PRE_M1_COLUMNS
    )
    statements: list[str] = []
    for (
        trigger_name,
        event_clause,
        function_name,
        _trigger_type,
        is_constraint,
    ) in _LEGACY_TRIGGER_SHAPES:
        statements.append(
            f"DROP TRIGGER {_q(trigger_name)} ON {table};"
        )
        scoped_event_clause = event_clause
        if scoped:
            scoped_event_clause = event_clause.replace(
                "UPDATE",
                f"UPDATE OF\n            {watched_columns}",
            )
        constraint_prefix = "CONSTRAINT " if is_constraint else ""
        deferral = " DEFERRABLE INITIALLY DEFERRED" if is_constraint else ""
        statements.append(
            f"CREATE {constraint_prefix}TRIGGER {_q(trigger_name)} "
            f"{scoped_event_clause} ON {table}{deferral} FOR EACH ROW "
            f"EXECUTE FUNCTION {_qf(schema, function_name)}();"
        )
        statements.append(
            f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER {_q(trigger_name)};"
        )
    return tuple(statements)


def _install_replay_guard_sqls(schema: str) -> tuple[str, str, str]:
    table = _qf(schema, _TABLE)
    guard = _qf(schema, _REPLAY_GUARD_FUNCTION)
    watched_columns = ",\n            ".join(
        _q(column_name) for column_name in _PRE_M1_COLUMNS
    )
    return (
        f"ALTER TABLE {table} DROP CONSTRAINT {_q(_REPLAY_CHECK)};",
        f"""
        CREATE TRIGGER {_q(_REPLAY_GUARD_TRIGGER)}
        BEFORE INSERT OR UPDATE OF
            {watched_columns}
        ON {table}
        FOR EACH ROW
        EXECUTE FUNCTION {guard}();
        """,
        f"""
        ALTER TABLE {table}
        ENABLE ALWAYS TRIGGER {_q(_REPLAY_GUARD_TRIGGER)};
        """,
    )


def _restore_replay_check_sql(schema: str) -> str:
    table = _qf(schema, _TABLE)
    subset = _reviewed_root_policy()._subset()
    predicate = subset._subset_replay_evidence_check(
        schema,
        reviewed_root_policy_aware=True,
    )
    return (
        f"ALTER TABLE {table} ADD CONSTRAINT {_q(_REPLAY_CHECK)} "
        f"CHECK ({predicate});"
    )


def _downgrade_receipt_fence_sql(schema: str) -> str:
    table = _qf(schema, _TABLE)
    populated = " OR ".join(
        f"row.{_q(column_name)} IS NOT NULL" for column_name in _SEAL_COLUMNS
    )
    return f"""
    DO $migration$
    BEGIN
        IF EXISTS (SELECT 1 FROM {table} AS row WHERE {populated}) THEN
            RAISE EXCEPTION
                'provider_directory_endpoint_dataset_admission_downgrade_blocked'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _install_triggers_sql(schema: str) -> tuple[str, ...]:
    table = _qf(schema, _TABLE)
    guard = _qf(schema, _GUARD_FUNCTION)
    truncate_guard = _qf(schema, _TRUNCATE_FUNCTION)
    watched_columns = ",\n            ".join(
        _q(column_name) for column_name in _SEAL_COLUMNS
    )
    return (
        f"""
        CREATE TRIGGER {_q(_GUARD_TRIGGER)}
        BEFORE INSERT OR UPDATE OF
            {watched_columns}
        ON {table}
        FOR EACH ROW
        EXECUTE FUNCTION {guard}();
        """,
        f"""
        ALTER TABLE {table}
        ENABLE ALWAYS TRIGGER {_q(_GUARD_TRIGGER)};
        """,
        f"""
        CREATE TRIGGER {_q(_RAW_GUARD_TRIGGER)}
        BEFORE UPDATE OF publication_metadata_json ON {table}
        FOR EACH ROW
        EXECUTE FUNCTION {guard}('raw');
        """,
        f"""
        ALTER TABLE {table}
        ENABLE ALWAYS TRIGGER {_q(_RAW_GUARD_TRIGGER)};
        """,
        f"""
        CREATE TRIGGER {_q(_TRUNCATE_TRIGGER)}
        BEFORE TRUNCATE ON {table}
        FOR EACH STATEMENT
        EXECUTE FUNCTION {truncate_guard}();
        """,
        f"""
        ALTER TABLE {table}
        ENABLE ALWAYS TRIGGER {_q(_TRUNCATE_TRIGGER)};
        """,
    )


def _comments_sql(schema: str) -> tuple[str, str]:
    table = _qf(schema, _TABLE)
    digest = _qf(schema, _DIGEST_FUNCTION)
    return (
        f"""
        COMMENT ON FUNCTION {digest}(
            jsonb, smallint, text, text, varchar[]
        ) IS
        'Bounded application-trusted admission digest; not same-owner authentication.';
        """,
        f"""
        COMMENT ON TRIGGER {_q(_GUARD_TRIGGER)} ON {table} IS
        'Application terminal validation is authoritative; this guard rejects partial and stale receipts.';
        """,
    )


def upgrade() -> None:
    schema = _schema()
    table = _qf(schema, _TABLE)
    digest = _qf(schema, _DIGEST_FUNCTION)
    guard = _qf(schema, _GUARD_FUNCTION)
    truncate_guard = _qf(schema, _TRUNCATE_FUNCTION)
    replay_guard = _qf(schema, _REPLAY_GUARD_FUNCTION)
    op.execute(_runtime_fence_sql(schema))
    op.execute(_legacy_surface_fence_sql(schema, scoped=False))
    op.execute(_add_columns_sql(schema))
    op.execute(_digest_function_sql(schema))
    op.execute(_guard_function_sql(schema))
    op.execute(_truncate_function_sql(schema))
    op.execute(_replay_guard_function_sql(schema))
    op.execute(
        f"REVOKE ALL ON FUNCTION {digest}"
        "(jsonb, smallint, text, text, varchar[]) FROM PUBLIC;"
    )
    op.execute(f"REVOKE ALL ON FUNCTION {guard}() FROM PUBLIC;")
    op.execute(f"REVOKE ALL ON FUNCTION {truncate_guard}() FROM PUBLIC;")
    op.execute(f"REVOKE ALL ON FUNCTION {replay_guard}() FROM PUBLIC;")
    for statement in _legacy_trigger_sqls(schema, scoped=True):
        op.execute(statement)
    for statement in _install_replay_guard_sqls(schema):
        op.execute(statement)
    for statement in _install_triggers_sql(schema):
        op.execute(statement)
    for statement in _comments_sql(schema):
        op.execute(statement)
    op.execute(_legacy_surface_fence_sql(schema, scoped=True))


def downgrade() -> None:
    schema = _schema()
    table = _qf(schema, _TABLE)
    digest = _qf(schema, _DIGEST_FUNCTION)
    guard = _qf(schema, _GUARD_FUNCTION)
    truncate_guard = _qf(schema, _TRUNCATE_FUNCTION)
    replay_guard = _qf(schema, _REPLAY_GUARD_FUNCTION)
    op.execute(_legacy_surface_fence_sql(schema, scoped=True))
    op.execute(_downgrade_receipt_fence_sql(schema))
    op.execute(f"DROP TRIGGER {_q(_TRUNCATE_TRIGGER)} ON {table};")
    op.execute(f"DROP TRIGGER {_q(_RAW_GUARD_TRIGGER)} ON {table};")
    op.execute(f"DROP TRIGGER {_q(_GUARD_TRIGGER)} ON {table};")
    op.execute(f"DROP TRIGGER {_q(_REPLAY_GUARD_TRIGGER)} ON {table};")
    op.execute(f"DROP FUNCTION {replay_guard}();")
    op.execute(_restore_replay_check_sql(schema))
    for statement in _legacy_trigger_sqls(schema, scoped=False):
        op.execute(statement)
    op.execute(f"DROP FUNCTION {truncate_guard}();")
    op.execute(f"DROP FUNCTION {guard}();")
    op.execute(
        f"DROP FUNCTION {digest}"
        "(jsonb, smallint, text, text, varchar[]);"
    )
    columns = ",\n        ".join(
        f"DROP COLUMN {_q(column_name)}" for column_name in _SEAL_COLUMNS
    )
    op.execute(f"ALTER TABLE {table}\n        {columns};")
    op.execute(_legacy_surface_fence_sql(schema, scoped=False))
