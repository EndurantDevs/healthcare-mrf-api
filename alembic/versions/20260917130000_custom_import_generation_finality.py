# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fence and seal immutable custom-import generation materialization.

Revision ID: 20260917130000_custom_import_generation_finality
Revises: 20260914120000_npi_result_generation

This is deliberately schema-only.  Retained pre-fence generations receive no
fabricated authority or seal: they remain readable but cannot become current.
"""

from __future__ import annotations

import os

from alembic import op

revision = "20260917130000_custom_import_generation_finality"
down_revision = "20260914120000_npi_result_generation"
branch_labels = None
depends_on = None


_DDL_SCHEMA_TOKEN = "mrf."
_APPEND_GUARD_TABLES = (
    "custom_import_generation_family",
    "custom_import_winner",
    "custom_import_family_child",
    "custom_import_root_scalar",
    "custom_import_child_scalar",
    "custom_import_field",
    "custom_import_child_collection",
    "custom_import_source_stream",
    "custom_import_field_alias",
    "custom_import_selection_profile",
    "custom_import_capture",
    "custom_import_pack",
    "custom_import_rejection",
    # Root records and entity bindings are intentionally shared immutable
    # interning identities.  Every candidate graph row below is instead
    # attributed transitively to a fenced pack/family/generation attempt.
    "custom_import_root_revision",
    "custom_import_child_revision",
    "custom_import_family_revision",
)

# These are the only immutable top-level rows with a direct dataset FK.
# Their FK checks would otherwise take KEY SHARE before a later output trigger
# upgrades to FOR UPDATE, creating a lock-queue deadlock with concurrent
# finality work.  Nested definition/capture rows inherit this ordering through
# their immutable parents; output rows take the same lock in their own guard.
_DATASET_FIRST_INSERT_TABLES = (
    "custom_import_schema_revision",
    "custom_import_field_slot",
    "custom_import_root_record",
    "custom_import_entity_binding",
)


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime_schema or legacy_schema or "mrf"


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _in_schema(statement: str, schema: str) -> str:
    return statement.replace(_DDL_SCHEMA_TOKEN, f"{_quote(schema)}.")


def _qualified(schema: str, object_name: str) -> str:
    return f"{_quote(schema)}.{_quote(object_name)}"


def _generation_columns_sql(schema: str) -> str:
    return f"""
    ALTER TABLE {_qualified(schema, "custom_import_generation")}
        ADD COLUMN producing_fence BIGINT,
        ADD COLUMN producing_token_sha256 BYTEA,
        ADD CONSTRAINT custom_import_generation_producing_authority_check CHECK (
            (producing_fence IS NULL AND producing_token_sha256 IS NULL) OR
            (producing_fence > 0 AND octet_length(producing_token_sha256) = 32)
        ),
        ADD CONSTRAINT custom_import_generation_seal_reference_key UNIQUE (
            generation_id,
            dataset_id,
            definition_revision_id,
            schema_revision_id,
            execution_id,
            capture_bundle_id
        )
    """


def _generation_attempt_identity_sql(schema: str) -> tuple[str, str, str]:
    """Replace v1's one-attempt/content claims with per-fence candidates.

    ``candidate_sha256`` intentionally retains the old bytes under a name
    that cannot imply authoritative materialization content.  The immutable
    seal stores the computed content identity, and candidates from separate
    lease fences may carry identical worker fingerprints.
    """

    generation = _qualified(schema, "custom_import_generation")
    return (
        f"""
        ALTER TABLE {generation}
        DROP CONSTRAINT custom_import_generation_execution_key,
        DROP CONSTRAINT custom_import_generation_content_key
        """,
        f"ALTER TABLE {generation} RENAME COLUMN generation_sha256 TO candidate_sha256",
        f"""
        ALTER TABLE {generation}
        ADD CONSTRAINT custom_import_generation_execution_fence_key
        UNIQUE (execution_id, producing_fence)
        """,
    )


_PACK_ATTEMPT_COLUMNS_SQL = """
    ALTER TABLE {pack}
        ADD COLUMN producing_fence BIGINT,
        ADD COLUMN producing_token_sha256 BYTEA,
        DROP CONSTRAINT custom_import_pack_execution_key,
        ADD CONSTRAINT custom_import_pack_attempt_key
            UNIQUE (execution_id, producing_fence, stream_slot, pack_ordinal),
        DROP CONSTRAINT custom_import_pack_shape_check,
        ADD CONSTRAINT custom_import_pack_shape_check CHECK (
            pack_ordinal >= 0 AND record_count >= 0 AND octet_length(pack_sha256) = 32 AND
            ((producing_fence IS NULL AND producing_token_sha256 IS NULL) OR
             (producing_fence > 0 AND octet_length(producing_token_sha256) = 32))
        )
    """

_REJECTION_ATTEMPT_COLUMNS_SQL = """
    ALTER TABLE {rejection}
        ADD COLUMN rejection_id BIGINT GENERATED BY DEFAULT AS IDENTITY,
        ADD COLUMN producing_fence BIGINT,
        ADD COLUMN producing_token_sha256 BYTEA
    """

_REJECTION_ATTEMPT_CONSTRAINTS_SQL = """
    ALTER TABLE {rejection}
        ALTER COLUMN rejection_id SET NOT NULL,
        DROP CONSTRAINT custom_import_rejection_pkey,
        ADD CONSTRAINT custom_import_rejection_pkey PRIMARY KEY (rejection_id),
        ADD CONSTRAINT custom_import_rejection_attempt_key
            UNIQUE (execution_id, producing_fence, rejection_ordinal),
        DROP CONSTRAINT custom_import_rejection_shape_check,
        ADD CONSTRAINT custom_import_rejection_shape_check CHECK (
            rejection_ordinal >= 0 AND code ~ '^[a-z][a-z0-9_]{{0,62}}$' AND
            (source_ordinal IS NULL OR source_ordinal >= 0) AND
            (field_slot IS NULL OR field_slot > 0) AND
            (root_key_sha256 IS NULL OR octet_length(root_key_sha256) = 32) AND
            ((producing_fence IS NULL AND producing_token_sha256 IS NULL) OR
             (producing_fence > 0 AND octet_length(producing_token_sha256) = 32))
        )
    """

_FAMILY_ATTEMPT_COLUMNS_SQL = """
    ALTER TABLE {family}
        ADD COLUMN producing_execution_id BIGINT,
        ADD COLUMN producing_fence BIGINT,
        ADD COLUMN producing_token_sha256 BYTEA,
        DROP CONSTRAINT custom_import_family_revision_content_key,
        ADD CONSTRAINT custom_import_family_revision_attempt_content_key
            UNIQUE (
                dataset_id, schema_revision_id, root_record_id, family_sha256,
                producing_execution_id, producing_fence
            ),
        DROP CONSTRAINT custom_import_family_revision_shape_check,
        ADD CONSTRAINT custom_import_family_revision_shape_check CHECK (
            child_count >= 0 AND octet_length(family_sha256) = 32 AND
            ((producing_execution_id IS NULL AND producing_fence IS NULL
              AND producing_token_sha256 IS NULL) OR
             (producing_execution_id > 0 AND producing_fence > 0
              AND octet_length(producing_token_sha256) = 32))
        )
    """


def _attempt_output_columns_sql(schema: str) -> tuple[str, ...]:
    """Add fence-scoped output provenance without fabricating legacy authority."""

    return (
        _PACK_ATTEMPT_COLUMNS_SQL.format(pack=_qualified(schema, "custom_import_pack")),
        _REJECTION_ATTEMPT_COLUMNS_SQL.format(rejection=_qualified(schema, "custom_import_rejection")),
        _REJECTION_ATTEMPT_CONSTRAINTS_SQL.format(rejection=_qualified(schema, "custom_import_rejection")),
        _FAMILY_ATTEMPT_COLUMNS_SQL.format(family=_qualified(schema, "custom_import_family_revision")),
    )


def _generation_seal_sql(schema: str) -> str:
    return f"""
    CREATE TABLE {_qualified(schema, "custom_import_generation_seal")} (
        generation_id BIGINT NOT NULL,
        dataset_id BIGINT NOT NULL,
        definition_revision_id BIGINT NOT NULL,
        schema_revision_id BIGINT NOT NULL,
        execution_id BIGINT NOT NULL,
        capture_bundle_id BIGINT NOT NULL,
        seal_contract VARCHAR(63) NOT NULL,
        sealing_fence BIGINT NOT NULL,
        sealing_token_sha256 BYTEA NOT NULL,
        root_count BIGINT NOT NULL,
        family_count BIGINT NOT NULL,
        generation_family_count BIGINT NOT NULL,
        family_child_count BIGINT NOT NULL,
        winner_count BIGINT NOT NULL,
        profile_count BIGINT NOT NULL,
        root_scalar_count BIGINT NOT NULL,
        child_scalar_count BIGINT NOT NULL,
        materialization_sha256 BYTEA NOT NULL,
        effective_output_sha256 BYTEA NOT NULL,
        sealed_at TIMESTAMP WITH TIME ZONE DEFAULT transaction_timestamp() NOT NULL,
        CONSTRAINT custom_import_generation_seal_pkey PRIMARY KEY (generation_id),
        CONSTRAINT custom_import_generation_seal_owner_key UNIQUE (
            generation_id, dataset_id, definition_revision_id, schema_revision_id
        ),
        CONSTRAINT custom_import_generation_seal_generation_fkey FOREIGN KEY (
            generation_id, dataset_id, definition_revision_id, schema_revision_id,
            execution_id, capture_bundle_id
        ) REFERENCES {_qualified(schema, "custom_import_generation")} (
            generation_id, dataset_id, definition_revision_id, schema_revision_id,
            execution_id, capture_bundle_id
        ) ON DELETE RESTRICT,
        CONSTRAINT custom_import_generation_seal_execution_fkey FOREIGN KEY (
            execution_id, dataset_id, definition_revision_id, schema_revision_id,
            capture_bundle_id
        ) REFERENCES {_qualified(schema, "custom_import_execution")} (
            execution_id, dataset_id, definition_revision_id, schema_revision_id,
            capture_bundle_id
        ) ON DELETE RESTRICT,
        CONSTRAINT custom_import_generation_seal_shape_check CHECK (
            seal_contract = 'custom-import-generation-seal/v1' AND
            sealing_fence > 0 AND root_count >= 0 AND family_count >= 0 AND
            generation_family_count >= 0 AND family_child_count >= 0 AND
            winner_count >= 0 AND profile_count >= 0 AND root_scalar_count >= 0 AND
            child_scalar_count >= 0 AND octet_length(sealing_token_sha256) = 32 AND
            octet_length(materialization_sha256) = 32 AND
            octet_length(effective_output_sha256) = 32
        )
    )
    """


_NO_CHANGE_SEAL_SQL = """
    CREATE TABLE {no_change_seal} (
        execution_id BIGINT NOT NULL,
        dataset_id BIGINT NOT NULL,
        definition_revision_id BIGINT NOT NULL,
        schema_revision_id BIGINT NOT NULL,
        capture_bundle_id BIGINT NOT NULL,
        base_generation_id BIGINT NOT NULL,
        candidate_generation_id BIGINT NOT NULL,
        base_pointer_version BIGINT NOT NULL,
        seal_contract VARCHAR(63) NOT NULL,
        base_source_bundle_sha256 BYTEA NOT NULL,
        candidate_source_bundle_sha256 BYTEA NOT NULL,
        effective_output_sha256 BYTEA NOT NULL,
        sealing_fence BIGINT NOT NULL,
        sealing_token_sha256 BYTEA NOT NULL,
        canonical_receipt TEXT NOT NULL,
        receipt_sha256 BYTEA NOT NULL,
        sealed_at TIMESTAMP WITH TIME ZONE DEFAULT transaction_timestamp() NOT NULL,
        CONSTRAINT custom_import_no_change_seal_pkey PRIMARY KEY (execution_id),
        CONSTRAINT custom_import_no_change_seal_owner_key UNIQUE (
            execution_id, dataset_id, definition_revision_id, schema_revision_id
        ),
        CONSTRAINT custom_import_no_change_seal_execution_fkey FOREIGN KEY (
            execution_id, dataset_id, definition_revision_id, schema_revision_id,
            capture_bundle_id
        ) REFERENCES {execution} (
            execution_id, dataset_id, definition_revision_id, schema_revision_id,
            capture_bundle_id
        ) ON DELETE RESTRICT,
        CONSTRAINT custom_import_no_change_seal_candidate_fkey FOREIGN KEY (
            candidate_generation_id, dataset_id, definition_revision_id, schema_revision_id,
            execution_id, capture_bundle_id
        ) REFERENCES {generation} (
            generation_id, dataset_id, definition_revision_id, schema_revision_id,
            execution_id, capture_bundle_id
        ) ON DELETE RESTRICT,
        CONSTRAINT custom_import_no_change_seal_candidate_finality_fkey FOREIGN KEY (
            candidate_generation_id, dataset_id, definition_revision_id, schema_revision_id
        ) REFERENCES {generation_seal} (
            generation_id, dataset_id, definition_revision_id, schema_revision_id
        ) ON DELETE RESTRICT,
        CONSTRAINT custom_import_no_change_seal_base_fkey FOREIGN KEY (
            base_generation_id, dataset_id, definition_revision_id, schema_revision_id
        ) REFERENCES {generation} (
            generation_id, dataset_id, definition_revision_id, schema_revision_id
        ) ON DELETE RESTRICT,
        CONSTRAINT custom_import_no_change_seal_base_finality_fkey FOREIGN KEY (
            base_generation_id, dataset_id, definition_revision_id, schema_revision_id
        ) REFERENCES {generation_seal} (
            generation_id, dataset_id, definition_revision_id, schema_revision_id
        ) ON DELETE RESTRICT,
        CONSTRAINT custom_import_no_change_seal_shape_check CHECK (
            seal_contract = 'custom-import-no-change-seal/v1' AND
            base_pointer_version >= 0 AND candidate_generation_id <> base_generation_id AND
            sealing_fence > 0 AND octet_length(base_source_bundle_sha256) = 32 AND
            octet_length(candidate_source_bundle_sha256) = 32 AND
            octet_length(effective_output_sha256) = 32 AND
            octet_length(sealing_token_sha256) = 32 AND octet_length(receipt_sha256) = 32
        )
    )
"""


def _no_change_seal_sql(schema: str) -> str:
    """Create immutable no-change receipts tied to both sealed generations."""

    return _NO_CHANGE_SEAL_SQL.format(
        no_change_seal=_qualified(schema, "custom_import_no_change_seal"),
        execution=_qualified(schema, "custom_import_execution"),
        generation=_qualified(schema, "custom_import_generation"),
        generation_seal=_qualified(schema, "custom_import_generation_seal"),
    )


def _sealed_target_fkeys_sql(schema: str) -> tuple[str, str]:
    """Fail closed for new unsealed pointer/event targets without backfilling legacy rows.

    Publication APIs remain the authority that pairs a pointer transition with
    its immutable event receipt.  These foreign keys deliberately establish
    the narrower database boundary: direct writes cannot point at an unsealed
    generation, while retained pre-finality rows remain readable through the
    ``NOT VALID`` legacy-safe constraints.
    """

    generation_seal = _qualified(schema, "custom_import_generation_seal")
    return (
        f"""
        ALTER TABLE {_qualified(schema, "custom_import_current_generation")}
        ADD CONSTRAINT custom_import_current_generation_seal_fkey FOREIGN KEY (
            generation_id, dataset_id, definition_revision_id, schema_revision_id
        ) REFERENCES {generation_seal} (
            generation_id, dataset_id, definition_revision_id, schema_revision_id
        ) ON DELETE RESTRICT NOT VALID
        """,
        f"""
        ALTER TABLE {_qualified(schema, "custom_import_publication_event")}
        ADD CONSTRAINT custom_import_publication_event_to_seal_fkey FOREIGN KEY (
            to_generation_id, dataset_id, definition_revision_id, schema_revision_id
        ) REFERENCES {generation_seal} (
            generation_id, dataset_id, definition_revision_id, schema_revision_id
        ) ON DELETE RESTRICT NOT VALID
        """,
    )


def _index_sql(schema: str) -> tuple[str, ...]:
    events = _qualified(schema, "custom_import_publication_event")
    return (
        f"""
        ALTER TABLE {events}
            ADD COLUMN finality_contract VARCHAR(63),
            ADD CONSTRAINT custom_import_publication_event_finality_contract_check CHECK (
                finality_contract IS NULL OR finality_contract = 'custom-import-finality/v1'
            )
        """,
        f"""
        CREATE UNIQUE INDEX custom_import_publication_event_identity_key
        ON {events} (
            dataset_id,
            execution_id,
            event_kind,
            COALESCE(from_generation_id, 0),
            to_generation_id,
            expected_pointer_version,
            committed_pointer_version
        )
        WHERE finality_contract = 'custom-import-finality/v1'
        """,
        f"""
        CREATE UNIQUE INDEX custom_import_publication_event_pointer_version_key
        ON {events} (dataset_id, committed_pointer_version)
        WHERE finality_contract = 'custom-import-finality/v1'
          AND event_kind IN ('activated', 'rolled_back')
        """,
        f"""
        CREATE UNIQUE INDEX custom_import_publication_event_no_change_execution_key
        ON {events} (execution_id)
        WHERE finality_contract = 'custom-import-finality/v1' AND event_kind = 'no_change'
        """,
    )


def _read_committed_guard() -> str:
    return """
        IF current_setting('transaction_isolation') <> 'read committed' THEN
            RAISE EXCEPTION 'custom_import_finality_requires_read_committed'
                USING ERRCODE = 'P0001';
        END IF;
    """


def _trigger_function_definition_sql(qualified: str, body: str) -> str:
    """Wrap a fixed PL/pgSQL trigger body with its restricted definition."""

    return f"""
    CREATE FUNCTION {qualified}()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    {body}
    $function$
    """


def _with_read_committed_guard(body: str) -> str:
    """Insert the shared isolation guard into a static trigger template."""

    return body.replace("__READ_COMMITTED_GUARD__", _read_committed_guard())


def _generation_insert_function_sql(schema: str) -> str:
    qualified = _qualified(schema, "guard_custom_import_generation_insert")
    return f"""
    CREATE FUNCTION {qualified}()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    DECLARE
        locked_dataset_id BIGINT;
        locked_execution_id BIGINT;
        locked_execution_state TEXT;
        locked_fence BIGINT;
        locked_token_sha256 BYTEA;
        locked_expires_at TIMESTAMP WITH TIME ZONE;
    BEGIN
        {_read_committed_guard()}
        EXECUTE format(
            'SELECT dataset_id FROM %I.custom_import_dataset WHERE dataset_id = $1 FOR UPDATE',
            TG_TABLE_SCHEMA
        ) INTO locked_dataset_id USING NEW.dataset_id;
        IF locked_dataset_id IS NULL THEN
            RAISE EXCEPTION 'custom_import_generation_dataset_missing' USING ERRCODE = 'P0001';
        END IF;
        IF NEW.producing_fence IS NULL OR NEW.producing_token_sha256 IS NULL OR
           octet_length(NEW.producing_token_sha256) <> 32 THEN
            RAISE EXCEPTION 'custom_import_generation_missing_producing_authority'
                USING ERRCODE = 'P0001';
        END IF;
        EXECUTE format(
            'SELECT execution_id, state FROM %I.custom_import_execution '
            || 'WHERE execution_id = $1 AND dataset_id = $2 AND definition_revision_id = $3 '
            || 'AND schema_revision_id = $4 AND capture_bundle_id = $5 FOR UPDATE',
            TG_TABLE_SCHEMA
        ) INTO locked_execution_id, locked_execution_state USING NEW.execution_id, NEW.dataset_id,
            NEW.definition_revision_id, NEW.schema_revision_id, NEW.capture_bundle_id;
        IF locked_execution_id IS NULL THEN
            RAISE EXCEPTION 'custom_import_generation_execution_identity_mismatch'
                USING ERRCODE = 'P0001';
        END IF;
        EXECUTE format(
            'SELECT fence, token_sha256, expires_at FROM %I.custom_import_lease '
            || 'WHERE execution_id = $1 FOR UPDATE',
            TG_TABLE_SCHEMA
        ) INTO locked_fence, locked_token_sha256, locked_expires_at USING NEW.execution_id;
        IF locked_fence IS NULL OR locked_expires_at IS NULL OR locked_expires_at <= clock_timestamp() OR
           locked_fence <> NEW.producing_fence OR locked_token_sha256 <> NEW.producing_token_sha256 THEN
            RAISE EXCEPTION 'custom_import_generation_producing_lease_lost'
                USING ERRCODE = 'P0001';
        END IF;
        IF locked_execution_state <> 'running' THEN
            RAISE EXCEPTION 'custom_import_generation_producer_not_running'
                USING ERRCODE = 'P0001';
        END IF;
        RETURN NEW;
    END;
    $function$
    """


_DATASET_FIRST_INSERT_FUNCTION_BODY = """
    DECLARE
        locked_dataset_id BIGINT;
    BEGIN
        __READ_COMMITTED_GUARD__
        EXECUTE format(
            'SELECT dataset_id FROM %I.custom_import_dataset WHERE dataset_id = $1 FOR UPDATE',
            TG_TABLE_SCHEMA
        ) INTO locked_dataset_id USING NEW.dataset_id;
        IF locked_dataset_id IS NULL THEN
            RAISE EXCEPTION 'custom_import_dataset_first_insert_dataset_missing' USING ERRCODE = 'P0001';
        END IF;
        RETURN NEW;
    END;
"""


def _dataset_first_insert_function_sql(schema: str) -> str:
    return _trigger_function_definition_sql(
        _qualified(schema, "guard_custom_import_dataset_first_insert"),
        _with_read_committed_guard(_DATASET_FIRST_INSERT_FUNCTION_BODY),
    )


_GENERATION_SEAL_FUNCTION_BODY = """
    DECLARE
        locked_dataset_id BIGINT;
        locked_execution_id BIGINT;
        generation_fence BIGINT;
        generation_token_sha256 BYTEA;
        generation_root_count BIGINT;
        generation_family_count BIGINT;
        lease_fence BIGINT;
        lease_token_sha256 BYTEA;
        lease_expires_at TIMESTAMP WITH TIME ZONE;
        exact_family_count BIGINT;
        exact_family_child_count BIGINT;
        exact_winner_count BIGINT;
        exact_profile_count BIGINT;
        exact_root_scalar_count BIGINT;
        exact_child_scalar_count BIGINT;
        bundle_stream_count BIGINT;
        definition_stream_count BIGINT;
        exact_capture_count BIGINT;
        has_capture_stream_mismatch BOOLEAN;
        has_family_child_mismatch BOOLEAN;
        has_duplicate_child_key BOOLEAN;
        has_child_parent_mismatch BOOLEAN;
    BEGIN
        __READ_COMMITTED_GUARD__
        EXECUTE format(
            'SELECT dataset_id FROM %I.custom_import_dataset WHERE dataset_id = $1 FOR UPDATE',
            TG_TABLE_SCHEMA
        ) INTO locked_dataset_id USING NEW.dataset_id;
        IF locked_dataset_id IS NULL THEN
            RAISE EXCEPTION 'custom_import_generation_seal_dataset_missing' USING ERRCODE = 'P0001';
        END IF;
        EXECUTE format(
            'SELECT execution_id FROM %I.custom_import_execution '
            || 'WHERE execution_id = $1 AND dataset_id = $2 AND definition_revision_id = $3 '
            || 'AND schema_revision_id = $4 AND capture_bundle_id = $5 AND state = ''running'' FOR UPDATE',
            TG_TABLE_SCHEMA
        ) INTO locked_execution_id USING NEW.execution_id, NEW.dataset_id,
            NEW.definition_revision_id, NEW.schema_revision_id, NEW.capture_bundle_id;
        IF locked_execution_id IS NULL THEN
            RAISE EXCEPTION 'custom_import_generation_seal_execution_not_running' USING ERRCODE = 'P0001';
        END IF;
        EXECUTE format(
            'SELECT fence, token_sha256, expires_at FROM %I.custom_import_lease '
            || 'WHERE execution_id = $1 FOR UPDATE',
            TG_TABLE_SCHEMA
        ) INTO lease_fence, lease_token_sha256, lease_expires_at USING NEW.execution_id;
        IF lease_fence IS NULL OR lease_expires_at IS NULL OR lease_expires_at <= clock_timestamp() OR
           lease_fence <> NEW.sealing_fence OR lease_token_sha256 <> NEW.sealing_token_sha256 THEN
            RAISE EXCEPTION 'custom_import_generation_seal_lease_lost' USING ERRCODE = 'P0001';
        END IF;
        EXECUTE format(
            'SELECT producing_fence, producing_token_sha256, root_count, family_count '
            || 'FROM %I.custom_import_generation WHERE generation_id = $1 AND dataset_id = $2 AND definition_revision_id = $3 '
            || 'AND schema_revision_id = $4 AND execution_id = $5 AND capture_bundle_id = $6 FOR UPDATE',
            TG_TABLE_SCHEMA
        ) INTO generation_fence, generation_token_sha256, generation_root_count, generation_family_count
            USING NEW.generation_id, NEW.dataset_id,
            NEW.definition_revision_id, NEW.schema_revision_id, NEW.execution_id, NEW.capture_bundle_id;
        IF generation_fence IS NULL OR generation_fence <> NEW.sealing_fence OR
           generation_token_sha256 <> NEW.sealing_token_sha256 THEN
            RAISE EXCEPTION 'custom_import_generation_seal_authority_mismatch' USING ERRCODE = 'P0001';
        END IF;
        EXECUTE format(
            'SELECT stream_count FROM %I.custom_import_capture_bundle '
            || 'WHERE capture_bundle_id = $1 AND dataset_id = $2 '
            || 'AND definition_revision_id = $3 AND schema_revision_id = $4', TG_TABLE_SCHEMA
        ) INTO bundle_stream_count USING NEW.capture_bundle_id, NEW.dataset_id,
            NEW.definition_revision_id, NEW.schema_revision_id;
        EXECUTE format(
            'SELECT count(*) FROM %I.custom_import_source_stream '
            || 'WHERE definition_revision_id = $1 AND dataset_id = $2 AND schema_revision_id = $3', TG_TABLE_SCHEMA
        ) INTO definition_stream_count USING NEW.definition_revision_id, NEW.dataset_id, NEW.schema_revision_id;
        EXECUTE format(
            'SELECT count(*) FROM %I.custom_import_capture '
            || 'WHERE capture_bundle_id = $1 AND dataset_id = $2 '
            || 'AND definition_revision_id = $3 AND schema_revision_id = $4', TG_TABLE_SCHEMA
        ) INTO exact_capture_count USING NEW.capture_bundle_id, NEW.dataset_id,
            NEW.definition_revision_id, NEW.schema_revision_id;
        EXECUTE format(
            'SELECT EXISTS (SELECT 1 FROM %I.custom_import_source_stream stream '
            || 'WHERE stream.definition_revision_id = $3 AND stream.dataset_id = $2 '
            || 'AND stream.schema_revision_id = $4 AND NOT EXISTS (SELECT 1 '
            || 'FROM %I.custom_import_capture capture WHERE capture.capture_bundle_id = $1 '
            || 'AND capture.dataset_id = $2 AND capture.definition_revision_id = $3 '
            || 'AND capture.schema_revision_id = $4 AND capture.stream_slot = stream.stream_slot) '
            || 'UNION ALL SELECT 1 FROM %I.custom_import_capture capture '
            || 'WHERE capture.capture_bundle_id = $1 AND capture.dataset_id = $2 '
            || 'AND capture.definition_revision_id = $3 AND capture.schema_revision_id = $4 '
            || 'AND NOT EXISTS (SELECT 1 FROM %I.custom_import_source_stream stream '
            || 'WHERE stream.definition_revision_id = $3 AND stream.dataset_id = $2 '
            || 'AND stream.schema_revision_id = $4 AND stream.stream_slot = capture.stream_slot))',
            TG_TABLE_SCHEMA, TG_TABLE_SCHEMA, TG_TABLE_SCHEMA, TG_TABLE_SCHEMA
        ) INTO has_capture_stream_mismatch USING NEW.capture_bundle_id, NEW.dataset_id,
            NEW.definition_revision_id, NEW.schema_revision_id;
        IF bundle_stream_count IS NULL OR bundle_stream_count <> definition_stream_count OR
           bundle_stream_count <> exact_capture_count OR has_capture_stream_mismatch THEN
            RAISE EXCEPTION 'custom_import_generation_seal_capture_incomplete' USING ERRCODE = 'P0001';
        END IF;
        EXECUTE format(
            'SELECT count(*) FROM %I.custom_import_generation_family '
            || 'WHERE generation_id = $1 AND dataset_id = $2', TG_TABLE_SCHEMA
        ) INTO exact_family_count USING NEW.generation_id, NEW.dataset_id;
        EXECUTE format(
            'SELECT count(*) FROM %I.custom_import_family_child child '
            || 'JOIN %I.custom_import_generation_family family '
            || 'ON family.family_revision_id = child.family_revision_id '
            || 'AND family.dataset_id = child.dataset_id '
            || 'WHERE family.generation_id = $1 AND family.dataset_id = $2',
            TG_TABLE_SCHEMA, TG_TABLE_SCHEMA
        ) INTO exact_family_child_count USING NEW.generation_id, NEW.dataset_id;
        EXECUTE format(
            'SELECT EXISTS (SELECT 1 FROM %I.custom_import_generation_family family '
            || 'JOIN %I.custom_import_family_revision family_revision '
            || 'ON family_revision.family_revision_id = family.family_revision_id '
            || 'AND family_revision.dataset_id = family.dataset_id '
            || 'LEFT JOIN LATERAL (SELECT count(*) AS child_count '
            || 'FROM %I.custom_import_family_child child '
            || 'WHERE child.family_revision_id = family.family_revision_id '
            || 'AND child.dataset_id = family.dataset_id) exact_child ON TRUE '
            || 'WHERE family.generation_id = $1 AND family.dataset_id = $2 '
            || 'AND family_revision.child_count <> exact_child.child_count)',
            TG_TABLE_SCHEMA, TG_TABLE_SCHEMA, TG_TABLE_SCHEMA
        ) INTO has_family_child_mismatch USING NEW.generation_id, NEW.dataset_id;
        EXECUTE format(
            'SELECT EXISTS (SELECT 1 FROM ('
            || 'SELECT child.family_revision_id, child.collection_slot, child_revision.child_key_sha256 '
            || 'FROM %I.custom_import_family_child child '
            || 'JOIN %I.custom_import_child_revision child_revision '
            || 'ON child_revision.child_revision_id = child.child_revision_id '
            || 'AND child_revision.dataset_id = child.dataset_id '
            || 'JOIN %I.custom_import_generation_family family '
            || 'ON family.family_revision_id = child.family_revision_id '
            || 'AND family.dataset_id = child.dataset_id '
            || 'WHERE family.generation_id = $1 AND family.dataset_id = $2 '
            || 'GROUP BY child.family_revision_id, child.collection_slot, child_revision.child_key_sha256 '
            || 'HAVING count(*) > 1) duplicate_child_key)',
            TG_TABLE_SCHEMA, TG_TABLE_SCHEMA, TG_TABLE_SCHEMA
        ) INTO has_duplicate_child_key USING NEW.generation_id, NEW.dataset_id;
        EXECUTE format(
            'SELECT EXISTS (SELECT 1 FROM %I.custom_import_generation_family family '
            || 'JOIN %I.custom_import_family_child child '
            || 'ON child.family_revision_id = family.family_revision_id '
            || 'AND child.dataset_id = family.dataset_id '
            || 'JOIN %I.custom_import_child_revision child_revision '
            || 'ON child_revision.child_revision_id = child.child_revision_id '
            || 'AND child_revision.dataset_id = child.dataset_id '
            || 'JOIN %I.custom_import_root_record root_record '
            || 'ON root_record.root_record_id = family.root_record_id '
            || 'AND root_record.dataset_id = family.dataset_id '
            || 'WHERE family.generation_id = $1 AND family.dataset_id = $2 '
            || 'AND (child_revision.canonical_parent_key <> root_record.canonical_logical_key '
            || 'OR child_revision.parent_key_sha256 <> root_record.logical_key_sha256))',
            TG_TABLE_SCHEMA, TG_TABLE_SCHEMA, TG_TABLE_SCHEMA, TG_TABLE_SCHEMA
        ) INTO has_child_parent_mismatch USING NEW.generation_id, NEW.dataset_id;
        EXECUTE format(
            'SELECT count(*) FROM %I.custom_import_winner '
            || 'WHERE generation_id = $1 AND dataset_id = $2', TG_TABLE_SCHEMA
        ) INTO exact_winner_count USING NEW.generation_id, NEW.dataset_id;
        EXECUTE format(
            'SELECT count(*) FROM %I.custom_import_selection_profile '
            || 'WHERE definition_revision_id = $1 AND dataset_id = $2 AND schema_revision_id = $3',
            TG_TABLE_SCHEMA
        ) INTO exact_profile_count USING NEW.definition_revision_id, NEW.dataset_id, NEW.schema_revision_id;
        EXECUTE format(
            'SELECT count(*) FROM %I.custom_import_root_scalar scalar '
            || 'JOIN %I.custom_import_family_revision family_revision '
            || 'ON family_revision.root_revision_id = scalar.root_revision_id '
            || 'AND family_revision.dataset_id = scalar.dataset_id '
            || 'JOIN %I.custom_import_generation_family family '
            || 'ON family.family_revision_id = family_revision.family_revision_id '
            || 'AND family.dataset_id = family_revision.dataset_id '
            || 'WHERE family.generation_id = $1 AND family.dataset_id = $2',
            TG_TABLE_SCHEMA, TG_TABLE_SCHEMA, TG_TABLE_SCHEMA
        ) INTO exact_root_scalar_count USING NEW.generation_id, NEW.dataset_id;
        EXECUTE format(
            'SELECT count(*) FROM %I.custom_import_child_scalar scalar '
            || 'JOIN %I.custom_import_family_child child '
            || 'ON child.child_revision_id = scalar.child_revision_id '
            || 'AND child.dataset_id = scalar.dataset_id '
            || 'JOIN %I.custom_import_generation_family family '
            || 'ON family.family_revision_id = child.family_revision_id '
            || 'AND family.dataset_id = child.dataset_id '
            || 'WHERE family.generation_id = $1 AND family.dataset_id = $2',
            TG_TABLE_SCHEMA, TG_TABLE_SCHEMA, TG_TABLE_SCHEMA
        ) INTO exact_child_scalar_count USING NEW.generation_id, NEW.dataset_id;
        IF has_duplicate_child_key THEN
            RAISE EXCEPTION 'custom_import_generation_seal_duplicate_child_key' USING ERRCODE = 'P0001';
        END IF;
        IF has_child_parent_mismatch THEN
            RAISE EXCEPTION 'custom_import_generation_seal_child_parent_mismatch' USING ERRCODE = 'P0001';
        END IF;
        IF has_family_child_mismatch OR generation_root_count <> exact_family_count OR
           generation_family_count <> exact_family_count OR NEW.root_count <> exact_family_count OR
           NEW.family_count <> exact_family_count OR
           NEW.generation_family_count <> exact_family_count OR
           NEW.family_child_count <> exact_family_child_count OR NEW.winner_count <> exact_winner_count OR
           NEW.profile_count <> exact_profile_count OR NEW.root_scalar_count <> exact_root_scalar_count OR
           NEW.child_scalar_count <> exact_child_scalar_count THEN
            RAISE EXCEPTION 'custom_import_generation_seal_count_mismatch' USING ERRCODE = 'P0001';
        END IF;
        RETURN NEW;
    END;
"""


def _generation_seal_function_sql(schema: str) -> str:
    return _trigger_function_definition_sql(
        _qualified(schema, "guard_custom_import_generation_seal_insert"),
        _with_read_committed_guard(_GENERATION_SEAL_FUNCTION_BODY),
    )


_APPEND_GUARD_FUNCTION_BODY = """
    DECLARE
        locked_dataset_id BIGINT;
        is_sealed BOOLEAN := FALSE;
        requires_attempt_authority BOOLEAN := FALSE;
        authority_execution_id BIGINT;
        authority_fence BIGINT;
        authority_token_sha256 BYTEA;
        authority_state TEXT;
        authority_expires_at TIMESTAMP WITH TIME ZONE;
        related_execution_id BIGINT;
        related_fence BIGINT;
        related_token_sha256 BYTEA;
        expected_context_collection_slot SMALLINT;
    BEGIN
        __READ_COMMITTED_GUARD__
        EXECUTE format(
            'SELECT dataset_id FROM %I.custom_import_dataset WHERE dataset_id = $1 FOR UPDATE',
            TG_TABLE_SCHEMA
        ) INTO locked_dataset_id USING NEW.dataset_id;
        IF locked_dataset_id IS NULL THEN
            RAISE EXCEPTION 'custom_import_finality_dataset_missing' USING ERRCODE = 'P0001';
        END IF;
        CASE TG_TABLE_NAME
            WHEN 'custom_import_generation_family', 'custom_import_winner' THEN
                EXECUTE format(
                    'SELECT EXISTS (SELECT 1 FROM %I.custom_import_generation_seal '
                    || 'WHERE generation_id = $1 AND dataset_id = $2)', TG_TABLE_SCHEMA
                ) INTO is_sealed USING NEW.generation_id, NEW.dataset_id;
            WHEN 'custom_import_root_revision' THEN
                EXECUTE format(
                    'SELECT EXISTS (SELECT 1 FROM %I.custom_import_generation_seal seal '
                    || 'JOIN %I.custom_import_generation_family family '
                    || 'ON family.generation_id = seal.generation_id '
                    || 'AND family.dataset_id = seal.dataset_id '
                    || 'JOIN %I.custom_import_family_revision family_revision '
                    || 'ON family_revision.family_revision_id = family.family_revision_id '
                    || 'AND family_revision.dataset_id = family.dataset_id '
                    || 'WHERE family_revision.root_revision_id = $1 AND family.dataset_id = $2)',
                    TG_TABLE_SCHEMA, TG_TABLE_SCHEMA, TG_TABLE_SCHEMA
                ) INTO is_sealed USING NEW.root_revision_id, NEW.dataset_id;
            WHEN 'custom_import_child_revision' THEN
                EXECUTE format(
                    'SELECT EXISTS (SELECT 1 FROM %I.custom_import_generation_seal seal '
                    || 'JOIN %I.custom_import_generation_family family '
                    || 'ON family.generation_id = seal.generation_id '
                    || 'AND family.dataset_id = seal.dataset_id '
                    || 'JOIN %I.custom_import_family_child child '
                    || 'ON child.family_revision_id = family.family_revision_id '
                    || 'AND child.dataset_id = family.dataset_id '
                    || 'WHERE child.child_revision_id = $1 AND family.dataset_id = $2)',
                    TG_TABLE_SCHEMA, TG_TABLE_SCHEMA, TG_TABLE_SCHEMA
                ) INTO is_sealed USING NEW.child_revision_id, NEW.dataset_id;
            WHEN 'custom_import_family_revision' THEN
                EXECUTE format(
                    'SELECT EXISTS (SELECT 1 FROM %I.custom_import_generation_seal seal '
                    || 'JOIN %I.custom_import_generation_family family '
                    || 'ON family.generation_id = seal.generation_id '
                    || 'AND family.dataset_id = seal.dataset_id '
                    || 'WHERE family.family_revision_id = $1 AND family.dataset_id = $2)',
                    TG_TABLE_SCHEMA, TG_TABLE_SCHEMA
                ) INTO is_sealed USING NEW.family_revision_id, NEW.dataset_id;
            WHEN 'custom_import_family_child' THEN
                EXECUTE format(
                    'SELECT EXISTS (SELECT 1 FROM %I.custom_import_generation_seal seal '
                    || 'JOIN %I.custom_import_generation_family family '
                    || 'ON family.generation_id = seal.generation_id '
                    || 'AND family.dataset_id = seal.dataset_id '
                    || 'WHERE family.family_revision_id = $1 AND family.dataset_id = $2)',
                    TG_TABLE_SCHEMA, TG_TABLE_SCHEMA
                ) INTO is_sealed USING NEW.family_revision_id, NEW.dataset_id;
            WHEN 'custom_import_root_scalar' THEN
                EXECUTE format(
                    'SELECT EXISTS (SELECT 1 FROM %I.custom_import_generation_seal seal '
                    || 'JOIN %I.custom_import_generation_family family '
                    || 'ON family.generation_id = seal.generation_id '
                    || 'AND family.dataset_id = seal.dataset_id '
                    || 'JOIN %I.custom_import_family_revision family_revision '
                    || 'ON family_revision.family_revision_id = family.family_revision_id '
                    || 'AND family_revision.dataset_id = family.dataset_id '
                    || 'WHERE family_revision.root_revision_id = $1 AND family.dataset_id = $2)',
                    TG_TABLE_SCHEMA, TG_TABLE_SCHEMA, TG_TABLE_SCHEMA
                ) INTO is_sealed USING NEW.root_revision_id, NEW.dataset_id;
            WHEN 'custom_import_child_scalar' THEN
                EXECUTE format(
                    'SELECT EXISTS (SELECT 1 FROM %I.custom_import_generation_seal seal '
                    || 'JOIN %I.custom_import_generation_family family '
                    || 'ON family.generation_id = seal.generation_id '
                    || 'AND family.dataset_id = seal.dataset_id '
                    || 'JOIN %I.custom_import_family_child child '
                    || 'ON child.family_revision_id = family.family_revision_id '
                    || 'AND child.dataset_id = family.dataset_id '
                    || 'WHERE child.child_revision_id = $1 AND family.dataset_id = $2)',
                    TG_TABLE_SCHEMA, TG_TABLE_SCHEMA, TG_TABLE_SCHEMA
                ) INTO is_sealed USING NEW.child_revision_id, NEW.dataset_id;
            WHEN 'custom_import_field', 'custom_import_child_collection' THEN
                EXECUTE format(
                    'SELECT EXISTS (SELECT 1 FROM %I.custom_import_generation_seal seal '
                    || 'WHERE seal.dataset_id = $1 AND seal.schema_revision_id = $2)',
                    TG_TABLE_SCHEMA
                ) INTO is_sealed USING NEW.dataset_id, NEW.schema_revision_id;
            WHEN 'custom_import_source_stream', 'custom_import_field_alias', 'custom_import_selection_profile' THEN
                EXECUTE format(
                    'SELECT EXISTS (SELECT 1 FROM %I.custom_import_generation_seal seal '
                    || 'WHERE seal.dataset_id = $1 AND seal.definition_revision_id = $2 '
                    || 'AND seal.schema_revision_id = $3)', TG_TABLE_SCHEMA
                ) INTO is_sealed USING NEW.dataset_id, NEW.definition_revision_id, NEW.schema_revision_id;
            WHEN 'custom_import_capture' THEN
                EXECUTE format(
                    'SELECT EXISTS (SELECT 1 FROM %I.custom_import_generation_seal '
                    || 'WHERE capture_bundle_id = $1 AND dataset_id = $2 '
                    || 'UNION ALL SELECT 1 FROM %I.custom_import_no_change_seal '
                    || 'WHERE capture_bundle_id = $1 AND dataset_id = $2)',
                    TG_TABLE_SCHEMA, TG_TABLE_SCHEMA
                ) INTO is_sealed USING NEW.capture_bundle_id, NEW.dataset_id;
            WHEN 'custom_import_pack', 'custom_import_rejection' THEN
                EXECUTE format(
                    'SELECT EXISTS (SELECT 1 FROM %I.custom_import_generation_seal '
                    || 'WHERE execution_id = $1 AND dataset_id = $2 '
                    || 'UNION ALL SELECT 1 FROM %I.custom_import_no_change_seal '
                    || 'WHERE execution_id = $1 AND dataset_id = $2)',
                    TG_TABLE_SCHEMA, TG_TABLE_SCHEMA
                ) INTO is_sealed USING NEW.execution_id, NEW.dataset_id;
            ELSE
                RAISE EXCEPTION 'custom_import_finality_unknown_append_table' USING ERRCODE = 'P0001';
        END CASE;
        IF is_sealed THEN
            RAISE EXCEPTION 'custom_import_sealed_append' USING ERRCODE = 'P0001';
        END IF;
        CASE TG_TABLE_NAME
            WHEN 'custom_import_pack' THEN
                authority_execution_id := NEW.execution_id;
                authority_fence := NEW.producing_fence;
                authority_token_sha256 := NEW.producing_token_sha256;
                requires_attempt_authority := TRUE;
            WHEN 'custom_import_rejection' THEN
                authority_execution_id := NEW.execution_id;
                authority_fence := NEW.producing_fence;
                authority_token_sha256 := NEW.producing_token_sha256;
                requires_attempt_authority := TRUE;
                IF NEW.pack_id IS NOT NULL THEN
                    EXECUTE format(
                        'SELECT execution_id, producing_fence, producing_token_sha256 '
                        || 'FROM %I.custom_import_pack WHERE pack_id = $1 AND dataset_id = $2 '
                        || 'AND definition_revision_id = $3 AND schema_revision_id = $4', TG_TABLE_SCHEMA
                    ) INTO related_execution_id, related_fence, related_token_sha256 USING NEW.pack_id,
                        NEW.dataset_id, NEW.definition_revision_id, NEW.schema_revision_id;
                    IF related_execution_id IS NULL OR related_execution_id <> authority_execution_id OR
                       related_fence IS DISTINCT FROM authority_fence OR
                       related_token_sha256 IS DISTINCT FROM authority_token_sha256 THEN
                        RAISE EXCEPTION 'custom_import_rejection_pack_authority_mismatch' USING ERRCODE = 'P0001';
                    END IF;
                END IF;
            WHEN 'custom_import_root_revision', 'custom_import_child_revision' THEN
                EXECUTE format(
                    'SELECT execution_id, producing_fence, producing_token_sha256 '
                    || 'FROM %I.custom_import_pack WHERE pack_id = $1 AND dataset_id = $2 '
                    || 'AND definition_revision_id = $3 AND schema_revision_id = $4', TG_TABLE_SCHEMA
                ) INTO authority_execution_id, authority_fence, authority_token_sha256 USING NEW.pack_id,
                    NEW.dataset_id, NEW.definition_revision_id, NEW.schema_revision_id;
                requires_attempt_authority := TRUE;
            WHEN 'custom_import_family_revision' THEN
                EXECUTE format(
                    'SELECT pack.execution_id, pack.producing_fence, pack.producing_token_sha256 '
                    || 'FROM %I.custom_import_root_revision root '
                    || 'JOIN %I.custom_import_pack pack ON pack.pack_id = root.pack_id '
                    || 'AND pack.dataset_id = root.dataset_id '
                    || 'AND pack.definition_revision_id = root.definition_revision_id '
                    || 'AND pack.schema_revision_id = root.schema_revision_id '
                    || 'WHERE root.root_revision_id = $1 AND root.dataset_id = $2 '
                    || 'AND root.schema_revision_id = $3 AND root.root_record_id = $4', TG_TABLE_SCHEMA, TG_TABLE_SCHEMA
                ) INTO authority_execution_id, authority_fence, authority_token_sha256 USING NEW.root_revision_id,
                    NEW.dataset_id, NEW.schema_revision_id, NEW.root_record_id;
                IF NEW.producing_execution_id IS DISTINCT FROM authority_execution_id OR
                   NEW.producing_fence IS DISTINCT FROM authority_fence OR
                   NEW.producing_token_sha256 IS DISTINCT FROM authority_token_sha256 THEN
                    RAISE EXCEPTION 'custom_import_family_revision_authority_mismatch' USING ERRCODE = 'P0001';
                END IF;
                requires_attempt_authority := TRUE;
            WHEN 'custom_import_family_child' THEN
                EXECUTE format(
                    'SELECT producing_execution_id, producing_fence, producing_token_sha256 '
                    || 'FROM %I.custom_import_family_revision WHERE family_revision_id = $1 '
                    || 'AND dataset_id = $2 AND schema_revision_id = $3 AND root_record_id = $4', TG_TABLE_SCHEMA
                ) INTO authority_execution_id, authority_fence, authority_token_sha256 USING NEW.family_revision_id,
                    NEW.dataset_id, NEW.schema_revision_id, NEW.root_record_id;
                EXECUTE format(
                    'SELECT pack.execution_id, pack.producing_fence, pack.producing_token_sha256 '
                    || 'FROM %I.custom_import_child_revision child '
                    || 'JOIN %I.custom_import_pack pack ON pack.pack_id = child.pack_id '
                    || 'AND pack.dataset_id = child.dataset_id '
                    || 'AND pack.definition_revision_id = child.definition_revision_id '
                    || 'AND pack.schema_revision_id = child.schema_revision_id '
                    || 'WHERE child.child_revision_id = $1 AND child.dataset_id = $2 '
                    || 'AND child.schema_revision_id = $3 AND child.root_record_id = $4 '
                    || 'AND child.collection_slot = $5', TG_TABLE_SCHEMA, TG_TABLE_SCHEMA
                ) INTO related_execution_id, related_fence, related_token_sha256 USING NEW.child_revision_id,
                    NEW.dataset_id, NEW.schema_revision_id, NEW.root_record_id, NEW.collection_slot;
                IF authority_execution_id IS NULL OR related_execution_id IS NULL OR
                   authority_execution_id <> related_execution_id OR authority_fence <> related_fence OR
                   authority_token_sha256 <> related_token_sha256 THEN
                    RAISE EXCEPTION 'custom_import_family_child_authority_mismatch' USING ERRCODE = 'P0001';
                END IF;
                requires_attempt_authority := TRUE;
            WHEN 'custom_import_root_scalar' THEN
                EXECUTE format(
                    'SELECT pack.execution_id, pack.producing_fence, pack.producing_token_sha256 '
                    || 'FROM %I.custom_import_root_revision root '
                    || 'JOIN %I.custom_import_pack pack ON pack.pack_id = root.pack_id '
                    || 'AND pack.dataset_id = root.dataset_id '
                    || 'AND pack.definition_revision_id = root.definition_revision_id '
                    || 'AND pack.schema_revision_id = root.schema_revision_id '
                    || 'WHERE root.root_revision_id = $1 AND root.dataset_id = $2 '
                    || 'AND root.schema_revision_id = $3 AND root.root_record_id = $4', TG_TABLE_SCHEMA, TG_TABLE_SCHEMA
                ) INTO authority_execution_id, authority_fence, authority_token_sha256 USING NEW.root_revision_id,
                    NEW.dataset_id, NEW.schema_revision_id, NEW.root_record_id;
                requires_attempt_authority := TRUE;
            WHEN 'custom_import_child_scalar' THEN
                EXECUTE format(
                    'SELECT pack.execution_id, pack.producing_fence, pack.producing_token_sha256 '
                    || 'FROM %I.custom_import_child_revision child '
                    || 'JOIN %I.custom_import_pack pack ON pack.pack_id = child.pack_id '
                    || 'AND pack.dataset_id = child.dataset_id '
                    || 'AND pack.definition_revision_id = child.definition_revision_id '
                    || 'AND pack.schema_revision_id = child.schema_revision_id '
                    || 'WHERE child.child_revision_id = $1 AND child.dataset_id = $2 '
                    || 'AND child.schema_revision_id = $3 AND child.root_record_id = $4 '
                    || 'AND child.collection_slot = $5', TG_TABLE_SCHEMA, TG_TABLE_SCHEMA
                ) INTO authority_execution_id, authority_fence, authority_token_sha256 USING NEW.child_revision_id,
                    NEW.dataset_id, NEW.schema_revision_id, NEW.root_record_id, NEW.collection_slot;
                requires_attempt_authority := TRUE;
            WHEN 'custom_import_generation_family' THEN
                EXECUTE format(
                    'SELECT execution_id, producing_fence, producing_token_sha256 '
                    || 'FROM %I.custom_import_generation WHERE generation_id = $1 AND dataset_id = $2 '
                    || 'AND definition_revision_id = $3 AND schema_revision_id = $4', TG_TABLE_SCHEMA
                ) INTO authority_execution_id, authority_fence, authority_token_sha256 USING NEW.generation_id,
                    NEW.dataset_id, NEW.definition_revision_id, NEW.schema_revision_id;
                EXECUTE format(
                    'SELECT producing_execution_id, producing_fence, producing_token_sha256 '
                    || 'FROM %I.custom_import_family_revision WHERE family_revision_id = $1 AND dataset_id = $2 '
                    || 'AND schema_revision_id = $3 AND root_record_id = $4', TG_TABLE_SCHEMA
                ) INTO related_execution_id, related_fence, related_token_sha256 USING NEW.family_revision_id,
                    NEW.dataset_id, NEW.schema_revision_id, NEW.root_record_id;
                IF authority_execution_id IS NULL OR related_execution_id IS NULL OR
                   authority_execution_id <> related_execution_id OR authority_fence <> related_fence OR
                   authority_token_sha256 <> related_token_sha256 THEN
                    RAISE EXCEPTION 'custom_import_generation_family_authority_mismatch' USING ERRCODE = 'P0001';
                END IF;
                requires_attempt_authority := TRUE;
            WHEN 'custom_import_winner' THEN
                EXECUTE format(
                    'SELECT execution_id, producing_fence, producing_token_sha256 '
                    || 'FROM %I.custom_import_generation WHERE generation_id = $1 AND dataset_id = $2 '
                    || 'AND definition_revision_id = $3 AND schema_revision_id = $4', TG_TABLE_SCHEMA
                ) INTO authority_execution_id, authority_fence, authority_token_sha256 USING NEW.generation_id,
                    NEW.dataset_id, NEW.definition_revision_id, NEW.schema_revision_id;
                EXECUTE format(
                    'SELECT producing_execution_id, producing_fence, producing_token_sha256 '
                    || 'FROM %I.custom_import_family_revision WHERE family_revision_id = $1 AND dataset_id = $2', TG_TABLE_SCHEMA
                ) INTO related_execution_id, related_fence, related_token_sha256 USING NEW.family_revision_id,
                    NEW.dataset_id;
                IF authority_execution_id IS NULL OR related_execution_id IS NULL OR
                   authority_execution_id <> related_execution_id OR authority_fence <> related_fence OR
                   authority_token_sha256 <> related_token_sha256 THEN
                    RAISE EXCEPTION 'custom_import_winner_authority_mismatch' USING ERRCODE = 'P0001';
                END IF;
                EXECUTE format(
                    'SELECT context_collection_slot FROM %I.custom_import_selection_profile '
                    || 'WHERE definition_revision_id = $1 AND dataset_id = $2 AND schema_revision_id = $3 '
                    || 'AND profile_slot = $4', TG_TABLE_SCHEMA
                ) INTO expected_context_collection_slot USING NEW.definition_revision_id, NEW.dataset_id,
                    NEW.schema_revision_id, NEW.profile_slot;
                IF (expected_context_collection_slot IS NULL AND NEW.context_collection_slot <> 0) OR
                   (expected_context_collection_slot IS NOT NULL AND
                    NEW.context_collection_slot <> expected_context_collection_slot) THEN
                    RAISE EXCEPTION 'custom_import_winner_profile_context_mismatch' USING ERRCODE = 'P0001';
                END IF;
                IF NEW.context_child_revision_id IS NOT NULL THEN
                    EXECUTE format(
                        'SELECT pack.execution_id, pack.producing_fence, pack.producing_token_sha256 '
                        || 'FROM %I.custom_import_child_revision child '
                        || 'JOIN %I.custom_import_pack pack ON pack.pack_id = child.pack_id '
                        || 'AND pack.dataset_id = child.dataset_id '
                        || 'AND pack.definition_revision_id = child.definition_revision_id '
                        || 'AND pack.schema_revision_id = child.schema_revision_id '
                        || 'WHERE child.child_revision_id = $1 AND child.dataset_id = $2', TG_TABLE_SCHEMA, TG_TABLE_SCHEMA
                    ) INTO related_execution_id, related_fence, related_token_sha256 USING NEW.context_child_revision_id,
                        NEW.dataset_id;
                    IF related_execution_id IS NULL OR authority_execution_id <> related_execution_id OR
                       authority_fence <> related_fence OR authority_token_sha256 <> related_token_sha256 THEN
                        RAISE EXCEPTION 'custom_import_winner_context_authority_mismatch' USING ERRCODE = 'P0001';
                    END IF;
                END IF;
                requires_attempt_authority := TRUE;
            ELSE
                NULL;
        END CASE;
        IF requires_attempt_authority THEN
            IF authority_execution_id IS NULL OR authority_fence IS NULL OR authority_token_sha256 IS NULL OR
               octet_length(authority_token_sha256) <> 32 THEN
                RAISE EXCEPTION 'custom_import_output_missing_producing_authority' USING ERRCODE = 'P0001';
            END IF;
            EXECUTE format(
                'SELECT state FROM %I.custom_import_execution WHERE execution_id = $1 AND dataset_id = $2 FOR UPDATE',
                TG_TABLE_SCHEMA
            ) INTO authority_state USING authority_execution_id, NEW.dataset_id;
            EXECUTE format(
                'SELECT expires_at FROM %I.custom_import_lease WHERE execution_id = $1 '
                || 'AND fence = $2 AND token_sha256 = $3 FOR UPDATE', TG_TABLE_SCHEMA
            ) INTO authority_expires_at USING authority_execution_id, authority_fence, authority_token_sha256;
            IF authority_state <> 'running' OR authority_expires_at IS NULL OR
               authority_expires_at <= clock_timestamp() THEN
                RAISE EXCEPTION 'custom_import_output_producing_lease_lost' USING ERRCODE = 'P0001';
            END IF;
        END IF;
        RETURN NEW;
    END;
"""


def _append_guard_function_sql(schema: str) -> str:
    return _trigger_function_definition_sql(
        _qualified(schema, "guard_custom_import_sealed_append"),
        _with_read_committed_guard(_APPEND_GUARD_FUNCTION_BODY),
    )


_NO_CHANGE_SEAL_FUNCTION_BODY = """
    DECLARE
        locked_dataset_id BIGINT;
        locked_execution_id BIGINT;
        lease_fence BIGINT;
        lease_token_sha256 BYTEA;
        lease_expires_at TIMESTAMP WITH TIME ZONE;
        pointer_generation_id BIGINT;
        pointer_version BIGINT;
        base_source_bundle_sha256 BYTEA;
        candidate_source_bundle_sha256 BYTEA;
        base_effective_output_sha256 BYTEA;
        candidate_effective_output_sha256 BYTEA;
        candidate_sealing_fence BIGINT;
        candidate_sealing_token_sha256 BYTEA;
    BEGIN
        __READ_COMMITTED_GUARD__
        EXECUTE format(
            'SELECT dataset_id FROM %I.custom_import_dataset WHERE dataset_id = $1 FOR UPDATE',
            TG_TABLE_SCHEMA
        ) INTO locked_dataset_id USING NEW.dataset_id;
        IF locked_dataset_id IS NULL THEN
            RAISE EXCEPTION 'custom_import_no_change_seal_dataset_missing' USING ERRCODE = 'P0001';
        END IF;
        EXECUTE format(
            'SELECT execution_id FROM %I.custom_import_execution '
            || 'WHERE execution_id = $1 AND dataset_id = $2 AND definition_revision_id = $3 '
            || 'AND schema_revision_id = $4 AND capture_bundle_id = $5 AND state = ''running'' FOR UPDATE',
            TG_TABLE_SCHEMA
        ) INTO locked_execution_id USING NEW.execution_id, NEW.dataset_id, NEW.definition_revision_id,
            NEW.schema_revision_id, NEW.capture_bundle_id;
        IF locked_execution_id IS NULL THEN
            RAISE EXCEPTION 'custom_import_no_change_execution_not_running' USING ERRCODE = 'P0001';
        END IF;
        EXECUTE format(
            'SELECT fence, token_sha256, expires_at FROM %I.custom_import_lease '
            || 'WHERE execution_id = $1 FOR UPDATE', TG_TABLE_SCHEMA
        ) INTO lease_fence, lease_token_sha256, lease_expires_at USING NEW.execution_id;
        IF lease_fence IS NULL OR lease_expires_at IS NULL OR lease_expires_at <= clock_timestamp() OR
           lease_fence <> NEW.sealing_fence OR lease_token_sha256 <> NEW.sealing_token_sha256 THEN
            RAISE EXCEPTION 'custom_import_no_change_lease_lost' USING ERRCODE = 'P0001';
        END IF;
        EXECUTE format(
            'SELECT generation_id, pointer_version FROM %I.custom_import_current_generation '
            || 'WHERE dataset_id = $1 FOR UPDATE', TG_TABLE_SCHEMA
        ) INTO pointer_generation_id, pointer_version USING NEW.dataset_id;
        IF pointer_generation_id IS NULL OR pointer_generation_id <> NEW.base_generation_id OR
           pointer_version <> NEW.base_pointer_version THEN
            RAISE EXCEPTION 'custom_import_no_change_pointer_mismatch' USING ERRCODE = 'P0001';
        END IF;
        EXECUTE format(
            'SELECT generation.source_bundle_sha256, seal.effective_output_sha256 '
            || 'FROM %I.custom_import_generation generation '
            || 'JOIN %I.custom_import_generation_seal seal ON seal.generation_id = generation.generation_id '
            || 'AND seal.dataset_id = generation.dataset_id '
            || 'AND seal.definition_revision_id = generation.definition_revision_id '
            || 'AND seal.schema_revision_id = generation.schema_revision_id '
            || 'WHERE generation.generation_id = $1 AND generation.dataset_id = $2 '
            || 'AND generation.definition_revision_id = $3 AND generation.schema_revision_id = $4',
            TG_TABLE_SCHEMA, TG_TABLE_SCHEMA
        ) INTO base_source_bundle_sha256, base_effective_output_sha256
            USING NEW.base_generation_id, NEW.dataset_id, NEW.definition_revision_id, NEW.schema_revision_id;
        EXECUTE format(
            'SELECT generation.source_bundle_sha256, seal.effective_output_sha256, '
            || 'seal.sealing_fence, seal.sealing_token_sha256 '
            || 'FROM %I.custom_import_generation generation '
            || 'JOIN %I.custom_import_generation_seal seal ON seal.generation_id = generation.generation_id '
            || 'AND seal.dataset_id = generation.dataset_id '
            || 'AND seal.definition_revision_id = generation.definition_revision_id '
            || 'AND seal.schema_revision_id = generation.schema_revision_id '
            || 'WHERE generation.generation_id = $1 AND generation.dataset_id = $2 '
            || 'AND generation.definition_revision_id = $3 AND generation.schema_revision_id = $4 '
            || 'AND generation.execution_id = $5 AND generation.capture_bundle_id = $6',
            TG_TABLE_SCHEMA, TG_TABLE_SCHEMA
        ) INTO candidate_source_bundle_sha256, candidate_effective_output_sha256,
            candidate_sealing_fence, candidate_sealing_token_sha256
            USING NEW.candidate_generation_id, NEW.dataset_id, NEW.definition_revision_id,
                NEW.schema_revision_id, NEW.execution_id, NEW.capture_bundle_id;
        IF base_source_bundle_sha256 IS NULL OR candidate_source_bundle_sha256 IS NULL OR
           candidate_sealing_fence <> NEW.sealing_fence OR
           candidate_sealing_token_sha256 <> NEW.sealing_token_sha256 OR
           base_source_bundle_sha256 <> NEW.base_source_bundle_sha256 OR
           candidate_source_bundle_sha256 <> NEW.candidate_source_bundle_sha256 OR
           base_effective_output_sha256 <> candidate_effective_output_sha256 OR
           base_effective_output_sha256 <> NEW.effective_output_sha256 THEN
            RAISE EXCEPTION 'custom_import_no_change_candidate_or_output_mismatch' USING ERRCODE = 'P0001';
        END IF;
        RETURN NEW;
    END;
"""


def _no_change_seal_function_sql(schema: str) -> str:
    return _trigger_function_definition_sql(
        _qualified(schema, "guard_custom_import_no_change_seal_insert"),
        _with_read_committed_guard(_NO_CHANGE_SEAL_FUNCTION_BODY),
    )


def _publication_event_function_sql(schema: str) -> str:
    qualified = _qualified(schema, "guard_custom_import_publication_event_insert")
    return f"""
    CREATE FUNCTION {qualified}()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    DECLARE
        locked_dataset_id BIGINT;
        no_change_execution_id BIGINT;
    BEGIN
        {_read_committed_guard()}
        EXECUTE format(
            'SELECT dataset_id FROM %I.custom_import_dataset WHERE dataset_id = $1 FOR UPDATE',
            TG_TABLE_SCHEMA
        ) INTO locked_dataset_id USING NEW.dataset_id;
        IF locked_dataset_id IS NULL THEN
            RAISE EXCEPTION 'custom_import_publication_event_dataset_missing' USING ERRCODE = 'P0001';
        END IF;
        IF NEW.finality_contract IS DISTINCT FROM 'custom-import-finality/v1' THEN
            RAISE EXCEPTION 'custom_import_publication_event_finality_contract_required'
                USING ERRCODE = 'P0001';
        END IF;
        IF NEW.event_kind = 'no_change' THEN
            EXECUTE format(
                'SELECT execution_id FROM %I.custom_import_no_change_seal '
                || 'WHERE execution_id = $1 AND dataset_id = $2 AND definition_revision_id = $3 '
                || 'AND schema_revision_id = $4 AND base_generation_id = $5 '
                || 'AND base_pointer_version = $6', TG_TABLE_SCHEMA
            ) INTO no_change_execution_id USING NEW.execution_id, NEW.dataset_id,
                NEW.definition_revision_id, NEW.schema_revision_id, NEW.to_generation_id,
                NEW.expected_pointer_version;
            IF no_change_execution_id IS NULL THEN
                RAISE EXCEPTION 'custom_import_no_change_receipt_missing' USING ERRCODE = 'P0001';
            END IF;
        END IF;
        RETURN NEW;
    END;
    $function$
    """


def _revoke_function_sql(schema: str, function_name: str) -> str:
    return f"REVOKE ALL ON FUNCTION {_qualified(schema, function_name)}() FROM PUBLIC"


def _trigger_sql(schema: str, trigger_name: str, table_name: str, function_name: str) -> str:
    return f"""
    CREATE TRIGGER {_quote(trigger_name)}
    BEFORE INSERT ON {_qualified(schema, table_name)}
    FOR EACH ROW EXECUTE FUNCTION {_qualified(schema, function_name)}()
    """


def _immutable_trigger_sql(schema: str, table_name: str) -> str:
    return f"""
    CREATE TRIGGER {_quote(f"{table_name}_immutable_row_guard")}
    BEFORE UPDATE OR DELETE ON {_qualified(schema, table_name)}
    FOR EACH ROW EXECUTE FUNCTION {_qualified(schema, "guard_custom_import_immutable_row")}()
    """


def _drop_trigger_sql(schema: str, trigger_name: str, table_name: str) -> str:
    return f"DROP TRIGGER IF EXISTS {_quote(trigger_name)} ON {_qualified(schema, table_name)}"


def _downgrade_finality_data_guard_sql(schema: str) -> str:
    """Refuse a downgrade that would reinterpret fenced candidate fingerprints."""

    generation = _qualified(schema, "custom_import_generation")
    generation_seal = _qualified(schema, "custom_import_generation_seal")
    no_change_seal = _qualified(schema, "custom_import_no_change_seal")
    pack = _qualified(schema, "custom_import_pack")
    rejection = _qualified(schema, "custom_import_rejection")
    family = _qualified(schema, "custom_import_family_revision")
    events = _qualified(schema, "custom_import_publication_event")
    pointer = _qualified(schema, "custom_import_current_generation")
    return f"""
    DO $block$
    BEGIN
        LOCK TABLE {generation}, {generation_seal}, {no_change_seal}, {pack}, {rejection},
            {family}, {events}, {pointer} IN SHARE ROW EXCLUSIVE MODE;
        IF EXISTS (SELECT 1 FROM {generation_seal})
           OR EXISTS (SELECT 1 FROM {no_change_seal})
           OR EXISTS (
               SELECT 1 FROM {generation}
               WHERE producing_fence IS NOT NULL OR producing_token_sha256 IS NOT NULL
           ) OR EXISTS (
               SELECT 1 FROM {pack}
               WHERE producing_fence IS NOT NULL OR producing_token_sha256 IS NOT NULL
           ) OR EXISTS (
               SELECT 1 FROM {rejection}
               WHERE producing_fence IS NOT NULL OR producing_token_sha256 IS NOT NULL
           ) OR EXISTS (
               SELECT 1 FROM {family}
               WHERE producing_execution_id IS NOT NULL OR producing_fence IS NOT NULL
                  OR producing_token_sha256 IS NOT NULL
           ) THEN
            RAISE EXCEPTION 'custom_import_generation_finality_downgrade_blocked'
                USING ERRCODE = 'P0001';
        END IF;
    END;
    $block$
    """


def _install_finality_schema(schema: str) -> None:
    op.execute(_generation_columns_sql(schema))
    for statement in _generation_attempt_identity_sql(schema):
        op.execute(statement)
    for statement in _attempt_output_columns_sql(schema):
        op.execute(statement)
    op.execute(_generation_seal_sql(schema))
    op.execute(_no_change_seal_sql(schema))
    for statement in _sealed_target_fkeys_sql(schema):
        op.execute(statement)
    for statement in _index_sql(schema):
        op.execute(statement)


def _install_finality_functions(schema: str) -> None:
    function_sql_by_name = {
        "guard_custom_import_generation_insert": _generation_insert_function_sql(schema),
        "guard_custom_import_dataset_first_insert": _dataset_first_insert_function_sql(schema),
        "guard_custom_import_generation_seal_insert": _generation_seal_function_sql(schema),
        "guard_custom_import_sealed_append": _append_guard_function_sql(schema),
        "guard_custom_import_no_change_seal_insert": _no_change_seal_function_sql(schema),
        "guard_custom_import_publication_event_insert": _publication_event_function_sql(schema),
    }
    for function_name, statement in function_sql_by_name.items():
        op.execute(statement)
        op.execute(_revoke_function_sql(schema, function_name))


def _install_finality_primary_triggers(schema: str) -> None:
    trigger_specs = (
        (
            "custom_import_generation_producing_authority_guard",
            "custom_import_generation",
            "guard_custom_import_generation_insert",
        ),
        (
            "custom_import_generation_seal_insert_guard",
            "custom_import_generation_seal",
            "guard_custom_import_generation_seal_insert",
        ),
        (
            "custom_import_no_change_seal_insert_guard",
            "custom_import_no_change_seal",
            "guard_custom_import_no_change_seal_insert",
        ),
        (
            "custom_import_publication_event_finality_guard",
            "custom_import_publication_event",
            "guard_custom_import_publication_event_insert",
        ),
    )
    for trigger_name, table_name, function_name in trigger_specs:
        op.execute(_trigger_sql(schema, trigger_name, table_name, function_name))
    for table_name in _DATASET_FIRST_INSERT_TABLES:
        op.execute(
            _trigger_sql(
                schema,
                f"{table_name}_dataset_first_insert_guard",
                table_name,
                "guard_custom_import_dataset_first_insert",
            )
        )


def _install_finality_append_triggers(schema: str) -> None:
    for table_name in _APPEND_GUARD_TABLES:
        op.execute(
            _trigger_sql(
                schema,
                f"{table_name}_sealed_append_guard",
                table_name,
                "guard_custom_import_sealed_append",
            )
        )


def _install_finality_immutability_triggers(schema: str) -> None:
    for table_name in ("custom_import_generation_seal", "custom_import_no_change_seal"):
        op.execute(_immutable_trigger_sql(schema, table_name))


def upgrade() -> None:
    """Add fenced finality without modifying or backfilling retained data."""

    schema = _schema()
    _install_finality_schema(schema)
    _install_finality_functions(schema)
    _install_finality_primary_triggers(schema)
    _install_finality_append_triggers(schema)
    _install_finality_immutability_triggers(schema)


def _drop_finality_triggers(schema: str) -> None:
    for table_name in _APPEND_GUARD_TABLES:
        op.execute(_drop_trigger_sql(schema, f"{table_name}_sealed_append_guard", table_name))
    trigger_specs = (
        ("custom_import_generation_producing_authority_guard", "custom_import_generation"),
        ("custom_import_generation_seal_insert_guard", "custom_import_generation_seal"),
        ("custom_import_generation_seal_immutable_row_guard", "custom_import_generation_seal"),
        ("custom_import_no_change_seal_insert_guard", "custom_import_no_change_seal"),
        ("custom_import_no_change_seal_immutable_row_guard", "custom_import_no_change_seal"),
        ("custom_import_publication_event_finality_guard", "custom_import_publication_event"),
    )
    for trigger_name, table_name in trigger_specs:
        op.execute(_drop_trigger_sql(schema, trigger_name, table_name))
    for table_name in _DATASET_FIRST_INSERT_TABLES:
        op.execute(_drop_trigger_sql(schema, f"{table_name}_dataset_first_insert_guard", table_name))


def _drop_finality_functions_and_indexes(schema: str) -> None:
    function_names = (
        "guard_custom_import_publication_event_insert",
        "guard_custom_import_no_change_seal_insert",
        "guard_custom_import_sealed_append",
        "guard_custom_import_generation_seal_insert",
        "guard_custom_import_generation_insert",
        "guard_custom_import_dataset_first_insert",
    )
    for function_name in function_names:
        op.execute(f"DROP FUNCTION IF EXISTS {_qualified(schema, function_name)}()")
    index_names = (
        "custom_import_publication_event_no_change_execution_key",
        "custom_import_publication_event_pointer_version_key",
        "custom_import_publication_event_identity_key",
    )
    for index_name in index_names:
        op.execute(f"DROP INDEX IF EXISTS {_qualified(schema, index_name)}")
    op.execute(
        f"ALTER TABLE {_qualified(schema, 'custom_import_publication_event')} "
        "DROP CONSTRAINT IF EXISTS custom_import_publication_event_finality_contract_check"
    )
    op.execute(
        f"ALTER TABLE {_qualified(schema, 'custom_import_publication_event')} DROP COLUMN IF EXISTS finality_contract"
    )


def _drop_finality_references_and_tables(schema: str) -> None:
    op.execute(
        f"ALTER TABLE {_qualified(schema, 'custom_import_publication_event')} "
        "DROP CONSTRAINT IF EXISTS custom_import_publication_event_to_seal_fkey"
    )
    op.execute(
        f"ALTER TABLE {_qualified(schema, 'custom_import_current_generation')} "
        "DROP CONSTRAINT IF EXISTS custom_import_current_generation_seal_fkey"
    )
    op.execute(f"DROP TABLE IF EXISTS {_qualified(schema, 'custom_import_no_change_seal')}")
    op.execute(f"DROP TABLE IF EXISTS {_qualified(schema, 'custom_import_generation_seal')}")


def _restore_v1_generation_identity(schema: str) -> None:
    generation = _qualified(schema, "custom_import_generation")
    op.execute(f"ALTER TABLE {generation} DROP CONSTRAINT IF EXISTS custom_import_generation_execution_fence_key")
    op.execute(f"ALTER TABLE {generation} RENAME COLUMN candidate_sha256 TO generation_sha256")
    op.execute(
        f"ALTER TABLE {generation} "
        "ADD CONSTRAINT custom_import_generation_content_key UNIQUE (dataset_id, generation_sha256)"
    )
    op.execute(f"ALTER TABLE {generation} ADD CONSTRAINT custom_import_generation_execution_key UNIQUE (execution_id)")
    op.execute(f"ALTER TABLE {generation} DROP CONSTRAINT IF EXISTS custom_import_generation_seal_reference_key")
    op.execute(f"ALTER TABLE {generation} DROP CONSTRAINT IF EXISTS custom_import_generation_producing_authority_check")
    op.execute(
        f"ALTER TABLE {generation} DROP COLUMN IF EXISTS producing_token_sha256, DROP COLUMN IF EXISTS producing_fence"
    )


def _restore_v1_attempt_output_identity(schema: str) -> None:
    """Restore v1 keys only after the fenced-output downgrade guard passes."""

    pack = _qualified(schema, "custom_import_pack")
    rejection = _qualified(schema, "custom_import_rejection")
    family = _qualified(schema, "custom_import_family_revision")
    op.execute(
        f"ALTER TABLE {pack} DROP CONSTRAINT IF EXISTS custom_import_pack_attempt_key, "
        "DROP CONSTRAINT IF EXISTS custom_import_pack_shape_check, "
        "ADD CONSTRAINT custom_import_pack_execution_key UNIQUE (execution_id, stream_slot, pack_ordinal), "
        "ADD CONSTRAINT custom_import_pack_shape_check CHECK "
        "(pack_ordinal >= 0 AND record_count >= 0 AND octet_length(pack_sha256) = 32), "
        "DROP COLUMN IF EXISTS producing_token_sha256, DROP COLUMN IF EXISTS producing_fence"
    )
    op.execute(
        f"ALTER TABLE {rejection} DROP CONSTRAINT IF EXISTS custom_import_rejection_attempt_key, "
        "DROP CONSTRAINT IF EXISTS custom_import_rejection_pkey, "
        "DROP CONSTRAINT IF EXISTS custom_import_rejection_shape_check, "
        "ADD CONSTRAINT custom_import_rejection_pkey PRIMARY KEY (execution_id, rejection_ordinal), "
        "ADD CONSTRAINT custom_import_rejection_shape_check CHECK "
        "(rejection_ordinal >= 0 AND code ~ '^[a-z][a-z0-9_]{{0,62}}$' AND "
        "(source_ordinal IS NULL OR source_ordinal >= 0) AND "
        "(field_slot IS NULL OR field_slot > 0) AND "
        "(root_key_sha256 IS NULL OR octet_length(root_key_sha256) = 32)), "
        "DROP COLUMN IF EXISTS producing_token_sha256, DROP COLUMN IF EXISTS producing_fence, "
        "DROP COLUMN IF EXISTS rejection_id"
    )
    op.execute(
        f"ALTER TABLE {family} DROP CONSTRAINT IF EXISTS custom_import_family_revision_attempt_content_key, "
        "DROP CONSTRAINT IF EXISTS custom_import_family_revision_shape_check, "
        "ADD CONSTRAINT custom_import_family_revision_content_key "
        "UNIQUE (dataset_id, schema_revision_id, root_record_id, family_sha256), "
        "ADD CONSTRAINT custom_import_family_revision_shape_check CHECK "
        "(child_count >= 0 AND octet_length(family_sha256) = 32), "
        "DROP COLUMN IF EXISTS producing_token_sha256, DROP COLUMN IF EXISTS producing_fence, "
        "DROP COLUMN IF EXISTS producing_execution_id"
    )


def downgrade() -> None:
    """Remove empty finality objects without reinterpreting fenced candidates."""

    schema = _schema()
    op.execute(_downgrade_finality_data_guard_sql(schema))
    _drop_finality_triggers(schema)
    _drop_finality_functions_and_indexes(schema)
    _drop_finality_references_and_tables(schema)
    _restore_v1_attempt_output_identity(schema)
    _restore_v1_generation_identity(schema)
