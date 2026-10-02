# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fenced bounded family builds over the existing immutable import graph.

Protected owners must not be available to deployed writers through ownership,
inheritance, SET ROLE, or guard/function alteration. Admission checks that role
boundary separately. This migration does not populate retained source rows.
"""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path

from alembic import op

revision = "20261002010000_custom_import_bounded_build"
down_revision = "20261002000000_custom_import_segmented_capture"
branch_labels = None
depends_on = None

_TABLES = (
    "custom_import_build_attempt",
    "custom_import_build_stream",
    "custom_import_build_occurrence",
    "custom_import_build_family",
    "custom_import_build_candidate_context",
    "custom_import_build_verification",
)
_GRAPH_TABLES = (
    "custom_import_pack",
    "custom_import_rejection",
    "custom_import_root_revision",
    "custom_import_child_revision",
    "custom_import_family_revision",
    "custom_import_family_child",
    "custom_import_root_scalar",
    "custom_import_child_scalar",
    "custom_import_generation",
    "custom_import_generation_family",
    "custom_import_winner",
)
_SHAPE_TABLES = (
    "custom_import_field",
    "custom_import_child_collection",
    "custom_import_field_alias",
    "custom_import_source_stream",
    "custom_import_selection_profile",
)
_FROZEN_TABLES = _GRAPH_TABLES + _SHAPE_TABLES + ("custom_import_root_record", "custom_import_entity_binding")


def _schema() -> str:
    runtime, legacy = os.getenv("HLTHPRT_DB_SCHEMA"), os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime or legacy or "mrf"


def _q(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _sql(schema: str, value: str) -> str:
    return value.replace("__SCHEMA__", _q(schema))


def _function(schema: str, name: str, arguments: str, returns: str, body: str, *, invoker: bool = False) -> None:
    op.execute(
        _sql(
            schema,
            f"""CREATE OR REPLACE FUNCTION __SCHEMA__.{name}({arguments}) RETURNS {returns}
        LANGUAGE plpgsql SECURITY {"INVOKER" if invoker else "DEFINER"} SET search_path=pg_catalog
        AS $function$ {body} $function$""",
        )
    )
    types = ", ".join(argument.strip().split(" ", 1)[1] for argument in arguments.split(",") if argument.strip())
    op.execute(_sql(schema, f"REVOKE ALL ON FUNCTION __SCHEMA__.{name}({types}) FROM PUBLIC"))


def _trigger(
    schema: str, table: str, name: str, events: str, function: str, *, deferred: bool = False, statement: bool = False
) -> None:
    op.execute(
        _sql(
            schema,
            f"""CREATE {"CONSTRAINT " if deferred else ""}TRIGGER {name}
        {events} ON __SCHEMA__.{table} {"DEFERRABLE INITIALLY DEFERRED" if deferred else ""}
        FOR EACH {"STATEMENT" if statement else "ROW"} EXECUTE FUNCTION __SCHEMA__.{function}()""",
        )
    )
    op.execute(_sql(schema, f"ALTER TABLE __SCHEMA__.{table} ENABLE ALWAYS TRIGGER {name}"))


_DDL = r"""

CREATE TABLE __SCHEMA__.custom_import_build_attempt (
	build_id BIGSERIAL NOT NULL,
	build_contract VARCHAR(63) DEFAULT 'custom-import/build/v1' NOT NULL,
	dataset_id BIGINT NOT NULL,
	definition_revision_id BIGINT NOT NULL,
	schema_revision_id BIGINT NOT NULL,
	execution_id BIGINT NOT NULL,
	capture_bundle_id BIGINT NOT NULL,
	producing_fence BIGINT NOT NULL,
	producing_token_sha256 BYTEA NOT NULL,
	request_identity_sha256 BYTEA,
	base_generation_id BIGINT,
	base_pointer_version BIGINT NOT NULL,
	refresh_mode VARCHAR(16) NOT NULL,
	complete_scope BOOLEAN NOT NULL,
	phase VARCHAR(16) DEFAULT 'source' NOT NULL,
	generation_id BIGINT,
	page_row_limit INTEGER NOT NULL,
	page_byte_limit BIGINT NOT NULL,
	statement_timeout_ms INTEGER NOT NULL,
	build_deadline_at TIMESTAMP WITH TIME ZONE NOT NULL,
	admission_after_occurrence_id BIGINT DEFAULT 0 NOT NULL CHECK (admission_after_occurrence_id >= 0),
	plan_stage VARCHAR(8) DEFAULT 'base' NOT NULL,
	plan_page_sequence BIGINT DEFAULT 0 NOT NULL CHECK (plan_page_sequence >= 0),
	plan_after_base_root_record_id BIGINT DEFAULT 0 NOT NULL CHECK (plan_after_base_root_record_id >= 0),
	plan_after_source_root_record_id BIGINT DEFAULT 0 NOT NULL CHECK (plan_after_source_root_record_id >= 0),
	plan_complete_at TIMESTAMP WITH TIME ZONE,
	output_after_profile_slot SMALLINT,
	output_after_entity_binding_id BIGINT,
	output_after_context_key_sha256 BYTEA,
	source_occurrence_count BIGINT DEFAULT 0 NOT NULL CHECK (source_occurrence_count >= 0),
	candidate_error_count BIGINT DEFAULT 0 NOT NULL CHECK (candidate_error_count >= 0),
	selected_family_count BIGINT DEFAULT 0 NOT NULL CHECK (selected_family_count >= 0),
	completed_family_count BIGINT DEFAULT 0 NOT NULL CHECK (completed_family_count >= 0),
	candidate_context_count BIGINT DEFAULT 0 NOT NULL CHECK (candidate_context_count >= 0),
	generation_family_count BIGINT DEFAULT 0 NOT NULL CHECK (generation_family_count >= 0),
	winner_count BIGINT DEFAULT 0 NOT NULL CHECK (winner_count >= 0),
	next_rejection_ordinal BIGINT DEFAULT 0 NOT NULL CHECK (next_rejection_ordinal >= 0),
	source_frozen_at TIMESTAMP WITH TIME ZONE,
	graph_frozen_at TIMESTAMP WITH TIME ZONE,
	output_frozen_at TIMESTAMP WITH TIME ZONE,
	verified_at TIMESTAMP WITH TIME ZONE,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT transaction_timestamp() NOT NULL,
	PRIMARY KEY (build_id),
	CONSTRAINT custom_import_build_attempt_key UNIQUE (execution_id, producing_fence),
	CONSTRAINT custom_import_build_generation_key UNIQUE (generation_id),
	CONSTRAINT custom_import_build_execution_fkey FOREIGN KEY(execution_id, dataset_id, definition_revision_id, schema_revision_id, capture_bundle_id) REFERENCES __SCHEMA__.custom_import_execution (execution_id, dataset_id, definition_revision_id, schema_revision_id, capture_bundle_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_build_base_fkey FOREIGN KEY(base_generation_id, dataset_id) REFERENCES __SCHEMA__.custom_import_generation (generation_id, dataset_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_build_attempt_shape CHECK (build_contract = 'custom-import/build/v1' AND producing_fence > 0 AND octet_length(producing_token_sha256) = 32 AND (request_identity_sha256 IS NULL OR octet_length(request_identity_sha256) = 32) AND refresh_mode IN ('upsert','snapshot') AND page_row_limit BETWEEN 1 AND 256 AND page_byte_limit BETWEEN 1 AND 268435456 AND statement_timeout_ms > 0 AND ((base_generation_id IS NULL AND base_pointer_version = 0) OR (base_generation_id IS NOT NULL AND base_pointer_version > 0)) AND plan_stage IN ('base','source','complete')),
	CONSTRAINT custom_import_build_attempt_phase CHECK (((phase = 'source' AND source_frozen_at IS NULL AND graph_frozen_at IS NULL AND output_frozen_at IS NULL AND generation_id IS NULL AND verified_at IS NULL) OR (phase IN ('admission','graph','rejected') AND source_frozen_at IS NOT NULL AND graph_frozen_at IS NULL AND output_frozen_at IS NULL AND generation_id IS NULL AND verified_at IS NULL) OR (phase = 'output' AND source_frozen_at IS NOT NULL AND graph_frozen_at IS NOT NULL AND output_frozen_at IS NULL AND generation_id IS NOT NULL AND verified_at IS NULL) OR (phase IN ('verifying','verified') AND source_frozen_at IS NOT NULL AND graph_frozen_at IS NOT NULL AND output_frozen_at IS NOT NULL AND generation_id IS NOT NULL AND ((phase = 'verifying' AND verified_at IS NULL) OR (phase = 'verified' AND verified_at IS NOT NULL))))),
	CONSTRAINT custom_import_build_output_cursor CHECK ((output_after_profile_slot IS NULL AND output_after_entity_binding_id IS NULL AND output_after_context_key_sha256 IS NULL) OR (output_after_profile_slot IS NOT NULL AND output_after_entity_binding_id IS NOT NULL AND output_after_context_key_sha256 IS NOT NULL AND octet_length(output_after_context_key_sha256) = 32)),
	FOREIGN KEY(generation_id) REFERENCES __SCHEMA__.custom_import_generation (generation_id)
)

;
CREATE INDEX custom_import_build_definition_idx ON __SCHEMA__.custom_import_build_attempt (definition_revision_id);
CREATE INDEX custom_import_build_schema_idx ON __SCHEMA__.custom_import_build_attempt (schema_revision_id);

CREATE TABLE __SCHEMA__.custom_import_build_stream (
	build_id BIGINT NOT NULL,
	stream_slot SMALLINT NOT NULL,
	next_part_ordinal INTEGER DEFAULT 1 NOT NULL,
	next_part_row_ordinal BIGINT DEFAULT 0 NOT NULL CHECK (next_part_row_ordinal >= 0),
	next_source_ordinal BIGINT DEFAULT 0 NOT NULL CHECK (next_source_ordinal >= 0),
	next_pack_ordinal INTEGER DEFAULT 0 NOT NULL,
	replay_verified_at TIMESTAMP WITH TIME ZONE,
	PRIMARY KEY (build_id, stream_slot),
	CONSTRAINT custom_import_build_stream_shape CHECK (stream_slot > 0 AND next_part_ordinal > 0 AND next_part_row_ordinal >= 0 AND next_source_ordinal >= 0 AND next_pack_ordinal >= 0),
	FOREIGN KEY(build_id) REFERENCES __SCHEMA__.custom_import_build_attempt (build_id)
)

;


CREATE TABLE __SCHEMA__.custom_import_build_occurrence (
	occurrence_id BIGSERIAL NOT NULL,
	build_id BIGINT NOT NULL,
	stream_slot SMALLINT NOT NULL,
	pack_id BIGINT NOT NULL,
	origin VARCHAR(8) NOT NULL,
	source_part_ordinal INTEGER,
	part_row_ordinal BIGINT,
	source_ordinal BIGINT,
	base_family_revision_id BIGINT,
	base_root_revision_id BIGINT,
	base_child_revision_id BIGINT,
	record_kind VARCHAR(8) NOT NULL,
	collection_slot SMALLINT NOT NULL,
	raw_parent_key_canonical TEXT,
	raw_parent_key_sha256 BYTEA,
	root_record_id BIGINT,
	child_key_sha256 BYTEA,
	root_revision_id BIGINT,
	child_revision_id BIGINT,
	rejection_id BIGINT,
	resolved_rejection_id BIGINT,
	PRIMARY KEY (occurrence_id),
	CONSTRAINT custom_import_build_occurrence_stream_fkey FOREIGN KEY(build_id, stream_slot) REFERENCES __SCHEMA__.custom_import_build_stream (build_id, stream_slot),
	CONSTRAINT custom_import_build_occurrence_shape CHECK (((origin = 'source' AND source_part_ordinal IS NOT NULL AND source_part_ordinal > 0 AND part_row_ordinal IS NOT NULL AND part_row_ordinal >= 0 AND source_ordinal IS NOT NULL AND source_ordinal >= 0 AND base_family_revision_id IS NULL AND base_root_revision_id IS NULL AND base_child_revision_id IS NULL) OR (origin = 'retained' AND source_part_ordinal IS NULL AND part_row_ordinal IS NULL AND source_ordinal IS NULL AND base_family_revision_id IS NOT NULL AND num_nonnulls(base_root_revision_id,base_child_revision_id) = 1 AND raw_parent_key_canonical IS NULL AND raw_parent_key_sha256 IS NULL AND rejection_id IS NULL AND resolved_rejection_id IS NULL)) AND ((record_kind = 'root' AND collection_slot = 0 AND child_revision_id IS NULL AND child_key_sha256 IS NULL AND base_child_revision_id IS NULL) OR (record_kind = 'child' AND collection_slot > 0 AND root_revision_id IS NULL AND base_root_revision_id IS NULL)) AND num_nonnulls(root_revision_id,child_revision_id,rejection_id) = 1 AND ((raw_parent_key_canonical IS NULL AND raw_parent_key_sha256 IS NULL) OR (raw_parent_key_canonical IS NOT NULL AND raw_parent_key_sha256 IS NOT NULL AND octet_length(raw_parent_key_sha256) = 32)) AND ((child_revision_id IS NULL AND child_key_sha256 IS NULL) OR (child_revision_id IS NOT NULL AND child_key_sha256 IS NOT NULL AND octet_length(child_key_sha256) = 32))),
	FOREIGN KEY(pack_id) REFERENCES __SCHEMA__.custom_import_pack (pack_id),
	FOREIGN KEY(base_family_revision_id) REFERENCES __SCHEMA__.custom_import_family_revision (family_revision_id),
	FOREIGN KEY(base_root_revision_id) REFERENCES __SCHEMA__.custom_import_root_revision (root_revision_id),
	FOREIGN KEY(base_child_revision_id) REFERENCES __SCHEMA__.custom_import_child_revision (child_revision_id),
	FOREIGN KEY(root_record_id) REFERENCES __SCHEMA__.custom_import_root_record (root_record_id),
	UNIQUE (root_revision_id),
	FOREIGN KEY(root_revision_id) REFERENCES __SCHEMA__.custom_import_root_revision (root_revision_id),
	UNIQUE (child_revision_id),
	FOREIGN KEY(child_revision_id) REFERENCES __SCHEMA__.custom_import_child_revision (child_revision_id),
	FOREIGN KEY(rejection_id) REFERENCES __SCHEMA__.custom_import_rejection (rejection_id),
	FOREIGN KEY(resolved_rejection_id) REFERENCES __SCHEMA__.custom_import_rejection (rejection_id)
)

;
CREATE UNIQUE INDEX custom_import_build_copy_child_key ON __SCHEMA__.custom_import_build_occurrence (build_id, base_family_revision_id, base_child_revision_id) WHERE base_child_revision_id IS NOT NULL;
CREATE UNIQUE INDEX custom_import_build_copy_root_key ON __SCHEMA__.custom_import_build_occurrence (build_id, base_family_revision_id) WHERE base_root_revision_id IS NOT NULL;
CREATE INDEX custom_import_build_graph_child_idx ON __SCHEMA__.custom_import_build_occurrence (build_id, origin, root_record_id, collection_slot, child_key_sha256, child_revision_id) WHERE child_revision_id IS NOT NULL;
CREATE INDEX custom_import_build_occurrence_pack_idx ON __SCHEMA__.custom_import_build_occurrence (pack_id, occurrence_id);
CREATE INDEX custom_import_build_occurrence_page_idx ON __SCHEMA__.custom_import_build_occurrence (build_id, origin, occurrence_id);
CREATE INDEX custom_import_build_raw_parent_idx ON __SCHEMA__.custom_import_build_occurrence (build_id, record_kind, raw_parent_key_sha256, occurrence_id);
CREATE INDEX custom_import_build_source_child_idx ON __SCHEMA__.custom_import_build_occurrence (build_id, raw_parent_key_sha256, collection_slot, child_key_sha256, occurrence_id) WHERE origin = 'source';
CREATE UNIQUE INDEX custom_import_build_source_ordinal_key ON __SCHEMA__.custom_import_build_occurrence (build_id, stream_slot, source_ordinal) WHERE origin = 'source';
CREATE UNIQUE INDEX custom_import_build_source_position_key ON __SCHEMA__.custom_import_build_occurrence (build_id, stream_slot, source_part_ordinal, part_row_ordinal) WHERE origin = 'source';
CREATE INDEX custom_import_build_typed_root_idx ON __SCHEMA__.custom_import_build_occurrence (build_id, origin, record_kind, root_record_id, occurrence_id);

CREATE TABLE __SCHEMA__.custom_import_build_family (
	build_id BIGINT NOT NULL,
	root_record_id BIGINT NOT NULL,
	root_key_sha256 BYTEA NOT NULL,
	selection_kind VARCHAR(8) NOT NULL,
	source_root_occurrence_id BIGINT,
	base_family_revision_id BIGINT,
	family_revision_id BIGINT,
	last_child_collection_slot SMALLINT,
	last_child_key_sha256 BYTEA,
	last_input_child_revision_id BIGINT,
	attached_child_count BIGINT DEFAULT 0 NOT NULL CHECK (attached_child_count >= 0),
	complete_at TIMESTAMP WITH TIME ZONE,
	PRIMARY KEY (build_id, root_record_id),
	CONSTRAINT custom_import_build_family_output_key UNIQUE (build_id, family_revision_id),
	CONSTRAINT custom_import_build_family_shape CHECK (((selection_kind = 'source' AND source_root_occurrence_id IS NOT NULL AND base_family_revision_id IS NULL) OR (selection_kind = 'retained' AND source_root_occurrence_id IS NULL AND base_family_revision_id IS NOT NULL)) AND (complete_at IS NULL OR family_revision_id IS NOT NULL) AND ((last_child_collection_slot IS NULL AND last_child_key_sha256 IS NULL AND last_input_child_revision_id IS NULL) OR (last_child_collection_slot IS NOT NULL AND last_input_child_revision_id IS NOT NULL AND ((selection_kind = 'source' AND last_child_key_sha256 IS NOT NULL AND octet_length(last_child_key_sha256)=32) OR (selection_kind = 'retained' AND last_child_key_sha256 IS NULL))))),
	CONSTRAINT custom_import_build_family_hash_shape CHECK (octet_length(root_key_sha256) = 32),
	FOREIGN KEY(build_id) REFERENCES __SCHEMA__.custom_import_build_attempt (build_id),
	FOREIGN KEY(root_record_id) REFERENCES __SCHEMA__.custom_import_root_record (root_record_id),
	FOREIGN KEY(source_root_occurrence_id) REFERENCES __SCHEMA__.custom_import_build_occurrence (occurrence_id),
	FOREIGN KEY(base_family_revision_id) REFERENCES __SCHEMA__.custom_import_family_revision (family_revision_id),
	FOREIGN KEY(family_revision_id) REFERENCES __SCHEMA__.custom_import_family_revision (family_revision_id)
)

;
CREATE INDEX custom_import_build_family_hash_idx ON __SCHEMA__.custom_import_build_family (build_id, root_key_sha256, root_record_id);
CREATE INDEX custom_import_build_family_pending_idx ON __SCHEMA__.custom_import_build_family (build_id, root_record_id) WHERE complete_at IS NULL;

CREATE TABLE __SCHEMA__.custom_import_build_candidate_context (
	candidate_context_id BIGSERIAL NOT NULL,
	build_id BIGINT NOT NULL,
	profile_slot SMALLINT NOT NULL,
	entity_binding_id BIGINT NOT NULL,
	family_revision_id BIGINT NOT NULL,
	context_collection_slot SMALLINT NOT NULL,
	context_child_revision_id BIGINT,
	canonical_context_key TEXT NOT NULL,
	context_key_sha256 BYTEA NOT NULL,
	PRIMARY KEY (candidate_context_id),
	CONSTRAINT custom_import_build_context_family_fkey FOREIGN KEY(build_id, family_revision_id) REFERENCES __SCHEMA__.custom_import_build_family (build_id, family_revision_id) DEFERRABLE INITIALLY DEFERRED,
	CONSTRAINT custom_import_build_context_candidate_key UNIQUE NULLS NOT DISTINCT (build_id, profile_slot, family_revision_id, context_child_revision_id),
	CONSTRAINT custom_import_build_context_shape CHECK (profile_slot > 0 AND octet_length(canonical_context_key) BETWEEN 1 AND 8192 AND octet_length(context_key_sha256) = 32 AND ((context_collection_slot = 0 AND context_child_revision_id IS NULL) OR (context_collection_slot > 0 AND context_child_revision_id IS NOT NULL))),
	FOREIGN KEY(entity_binding_id) REFERENCES __SCHEMA__.custom_import_entity_binding (entity_binding_id),
	FOREIGN KEY(context_child_revision_id) REFERENCES __SCHEMA__.custom_import_child_revision (child_revision_id)
)

;
CREATE INDEX custom_import_build_context_order_idx ON __SCHEMA__.custom_import_build_candidate_context (build_id, profile_slot, entity_binding_id, context_key_sha256, candidate_context_id);

CREATE TABLE __SCHEMA__.custom_import_build_verification (
	build_id BIGINT NOT NULL,
	generation_id BIGINT NOT NULL,
	verification_contract VARCHAR(63) DEFAULT 'custom-import/build-structure/v1' NOT NULL,
	verification_state VARCHAR(16) DEFAULT 'scanning' NOT NULL,
	scan_stage VARCHAR(16) DEFAULT 'capture' NOT NULL,
	page_sequence BIGINT DEFAULT 0 NOT NULL CHECK (page_sequence >= 0),
	after_capture_stream_slot SMALLINT,
	after_profile_slot SMALLINT,
	after_root_record_id BIGINT,
	current_family_revision_id BIGINT,
	current_family_expected_child_count BIGINT,
	current_family_seen_child_count BIGINT,
	after_child_collection_slot SMALLINT,
	after_child_revision_id BIGINT,
	after_root_scalar_revision_id BIGINT,
	after_root_scalar_field_slot SMALLINT,
	after_child_scalar_revision_id BIGINT,
	after_child_scalar_field_slot SMALLINT,
	after_winner_profile_slot SMALLINT,
	after_winner_entity_binding_id BIGINT,
	after_winner_context_key_sha256 BYTEA,
	root_count BIGINT DEFAULT 0 NOT NULL CHECK (root_count >= 0),
	family_count BIGINT DEFAULT 0 NOT NULL CHECK (family_count >= 0),
	generation_family_count BIGINT DEFAULT 0 NOT NULL CHECK (generation_family_count >= 0),
	family_child_count BIGINT DEFAULT 0 NOT NULL CHECK (family_child_count >= 0),
	winner_count BIGINT DEFAULT 0 NOT NULL CHECK (winner_count >= 0),
	profile_count BIGINT DEFAULT 0 NOT NULL CHECK (profile_count >= 0),
	root_scalar_count BIGINT DEFAULT 0 NOT NULL CHECK (root_scalar_count >= 0),
	child_scalar_count BIGINT DEFAULT 0 NOT NULL CHECK (child_scalar_count >= 0),
	source_frozen_at TIMESTAMP WITH TIME ZONE NOT NULL,
	graph_frozen_at TIMESTAMP WITH TIME ZONE NOT NULL,
	output_frozen_at TIMESTAMP WITH TIME ZONE NOT NULL,
	verified_at TIMESTAMP WITH TIME ZONE,
	PRIMARY KEY (build_id),
	CONSTRAINT custom_import_build_verification_shape CHECK (verification_contract = 'custom-import/build-structure/v1' AND ((verification_state = 'scanning' AND scan_stage IN ('capture','profiles','families','root_scalars','child_scalars','winners') AND verified_at IS NULL) OR (verification_state = 'complete' AND scan_stage = 'complete' AND verified_at IS NOT NULL)) AND ((current_family_revision_id IS NULL AND current_family_expected_child_count IS NULL AND current_family_seen_child_count IS NULL) OR (current_family_revision_id IS NOT NULL AND current_family_expected_child_count IS NOT NULL AND current_family_seen_child_count IS NOT NULL AND current_family_expected_child_count >= 0 AND current_family_seen_child_count >= 0))),
	CONSTRAINT custom_import_build_verification_cursors CHECK (num_nonnulls(after_child_collection_slot,after_child_revision_id) IN (0,2) AND num_nonnulls(after_root_scalar_revision_id,after_root_scalar_field_slot) IN (0,2) AND num_nonnulls(after_child_scalar_revision_id,after_child_scalar_field_slot) IN (0,2) AND (num_nonnulls(after_winner_profile_slot,after_winner_entity_binding_id,after_winner_context_key_sha256)=0 OR (num_nonnulls(after_winner_profile_slot,after_winner_entity_binding_id,after_winner_context_key_sha256)=3 AND octet_length(after_winner_context_key_sha256)=32))),
	FOREIGN KEY(build_id) REFERENCES __SCHEMA__.custom_import_build_attempt (build_id),
	UNIQUE (generation_id),
	FOREIGN KEY(generation_id) REFERENCES __SCHEMA__.custom_import_generation (generation_id)
)

;
"""


_BUILD_SEAL_BRANCH = r"""
        IF TG_RELID<>'__SCHEMA__.custom_import_generation_seal'::regclass OR TG_OP<>'INSERT' OR TG_WHEN<>'BEFORE' OR TG_LEVEL<>'ROW' THEN
            RAISE EXCEPTION 'custom_import_build_protected_write'; END IF;
        SELECT * INTO build FROM __SCHEMA__.custom_import_build_attempt WHERE execution_id=NEW.execution_id AND producing_fence=NEW.sealing_fence;
        IF build.build_id IS NOT NULL THEN
            build:=__SCHEMA__.lock_custom_import_build(build.build_id);
            SELECT * INTO built_generation FROM __SCHEMA__.custom_import_generation WHERE generation_id=NEW.generation_id FOR UPDATE;
            SELECT * INTO proof FROM __SCHEMA__.custom_import_build_verification WHERE build_id=build.build_id;
            IF build.phase<>'verified' OR build.generation_id IS DISTINCT FROM NEW.generation_id
                OR proof.verification_state IS DISTINCT FROM 'complete' OR proof.scan_stage IS DISTINCT FROM 'complete'
                OR proof.verified_at IS NULL OR proof.generation_id IS DISTINCT FROM NEW.generation_id
                OR ROW(proof.source_frozen_at,proof.graph_frozen_at,proof.output_frozen_at,proof.verified_at) IS DISTINCT FROM
                    ROW(build.source_frozen_at,build.graph_frozen_at,build.output_frozen_at,build.verified_at)
                OR ROW(NEW.dataset_id,NEW.definition_revision_id,NEW.schema_revision_id,NEW.execution_id,NEW.capture_bundle_id,NEW.sealing_fence,NEW.sealing_token_sha256)
                    IS DISTINCT FROM ROW(build.dataset_id,build.definition_revision_id,build.schema_revision_id,build.execution_id,build.capture_bundle_id,
                        build.producing_fence,build.producing_token_sha256)
                OR ROW(built_generation.root_count,built_generation.family_count) IS DISTINCT FROM ROW(proof.root_count,proof.family_count)
                OR ROW(NEW.root_count,NEW.family_count,NEW.generation_family_count,NEW.family_child_count,NEW.winner_count,NEW.profile_count,
                    NEW.root_scalar_count,NEW.child_scalar_count) IS DISTINCT FROM ROW(proof.root_count,proof.family_count,proof.generation_family_count,
                    proof.family_child_count,proof.winner_count,proof.profile_count,proof.root_scalar_count,proof.child_scalar_count) THEN
                RAISE EXCEPTION 'custom_import_build_structure_mismatch'; END IF;
            RETURN NEW;
        END IF;
    """

_LOCK_CUSTOM_IMPORT_BUILD_BODY = r"""
    DECLARE b __SCHEMA__.custom_import_build_attempt; e __SCHEMA__.custom_import_execution;
        l __SCHEMA__.custom_import_lease; now_at timestamptz; timeout_ms numeric;
    BEGIN
        IF current_setting('transaction_isolation') <> 'read committed' THEN
            RAISE EXCEPTION 'custom_import_build_identity_mismatch' USING ERRCODE='P0001';
        END IF;
        SELECT * INTO b FROM __SCHEMA__.custom_import_build_attempt WHERE build_id=p_build_id;
        IF b.build_id IS NULL THEN RAISE EXCEPTION 'custom_import_build_identity_mismatch'; END IF;
        PERFORM 1 FROM __SCHEMA__.custom_import_dataset WHERE dataset_id=b.dataset_id FOR UPDATE;
        SELECT * INTO e FROM __SCHEMA__.custom_import_execution WHERE execution_id=b.execution_id FOR UPDATE;
        SELECT * INTO l FROM __SCHEMA__.custom_import_lease WHERE execution_id=b.execution_id FOR UPDATE;
        SELECT * INTO b FROM __SCHEMA__.custom_import_build_attempt WHERE build_id=p_build_id FOR UPDATE;
        now_at := clock_timestamp();
        IF e.state IS DISTINCT FROM 'running' OR l.fence IS DISTINCT FROM b.producing_fence
           OR l.token_sha256 IS DISTINCT FROM b.producing_token_sha256 OR l.expires_at IS NULL
           OR l.expires_at <= now_at OR b.build_deadline_at <= now_at THEN
            RAISE EXCEPTION 'custom_import_build_lease_lost' USING ERRCODE='P0001';
        END IF;
        IF ROW(e.dataset_id,e.definition_revision_id,e.schema_revision_id,e.capture_bundle_id,e.request_identity_sha256)
           IS DISTINCT FROM ROW(b.dataset_id,b.definition_revision_id,b.schema_revision_id,b.capture_bundle_id,b.request_identity_sha256) THEN
            RAISE EXCEPTION 'custom_import_build_identity_mismatch';
        END IF;
        timeout_ms := extract(epoch FROM current_setting('statement_timeout')::interval)*1000;
        IF timeout_ms <= 0 OR timeout_ms > b.statement_timeout_ms
           OR timeout_ms >= extract(epoch FROM least(l.expires_at,b.build_deadline_at)-now_at)*1000 THEN
            RAISE EXCEPTION 'custom_import_build_invalid_bounds';
        END IF;
        RETURN b;
    END;
    """

_BEGIN_CUSTOM_IMPORT_BUILD_BODY = r"""
    DECLARE b __SCHEMA__.custom_import_build_attempt; e __SCHEMA__.custom_import_execution;
        l __SCHEMA__.custom_import_lease; c __SCHEMA__.custom_import_capture_bundle;
        p __SCHEMA__.custom_import_current_generation; mode text; now_at timestamptz;
    BEGIN
        IF current_setting('transaction_isolation') <> 'read committed' THEN RAISE EXCEPTION 'custom_import_build_identity_mismatch'; END IF;
        SELECT * INTO e FROM __SCHEMA__.custom_import_execution WHERE execution_id=p_execution_id;
        IF e.execution_id IS NULL THEN RAISE EXCEPTION 'custom_import_build_identity_mismatch'; END IF;
        PERFORM 1 FROM __SCHEMA__.custom_import_dataset WHERE dataset_id=e.dataset_id FOR UPDATE;
        SELECT * INTO e FROM __SCHEMA__.custom_import_execution WHERE execution_id=p_execution_id FOR UPDATE;
        SELECT * INTO l FROM __SCHEMA__.custom_import_lease WHERE execution_id=p_execution_id FOR UPDATE;
        now_at := clock_timestamp();
        IF e.state IS DISTINCT FROM 'running' OR p_fence IS NULL OR p_fence <= 0 OR p_token_sha256 IS NULL
           OR octet_length(p_token_sha256) <> 32 OR l.fence IS DISTINCT FROM p_fence
           OR l.token_sha256 IS DISTINCT FROM p_token_sha256 OR l.expires_at IS NULL OR l.expires_at <= now_at THEN
            RAISE EXCEPTION 'custom_import_build_lease_lost';
        END IF;
        IF p_page_row_limit IS NULL OR p_page_row_limit NOT BETWEEN 1 AND 256 OR p_page_byte_limit IS NULL
           OR p_page_byte_limit NOT BETWEEN 1 AND 268435456 OR p_statement_timeout_ms IS NULL
           OR p_statement_timeout_ms <= 0 OR p_build_deadline_at IS NULL OR p_build_deadline_at <= now_at
           OR p_complete_scope IS NULL THEN RAISE EXCEPTION 'custom_import_build_invalid_bounds'; END IF;
        SELECT * INTO c FROM __SCHEMA__.custom_import_capture_bundle WHERE capture_bundle_id=e.capture_bundle_id;
        IF c.capture_state IS DISTINCT FROM 'sealed' OR c.payload_contract IS DISTINCT FROM 'custom-import/parquet-parts/v2'
           OR ROW(c.dataset_id,c.definition_revision_id,c.schema_revision_id) IS DISTINCT FROM
              ROW(e.dataset_id,e.definition_revision_id,e.schema_revision_id) THEN
            RAISE EXCEPTION 'custom_import_build_identity_mismatch';
        END IF;
        SELECT refresh_mode INTO mode FROM __SCHEMA__.custom_import_definition_revision WHERE definition_revision_id=e.definition_revision_id;
        -- Existing definition limits: one root stream plus at most eight child collections,
        -- four selection profiles, and twenty hot scalar fields. These are metadata limits,
        -- not source-record page defaults.
        IF c.stream_count>9 OR (SELECT count(*) FROM (SELECT 1 FROM __SCHEMA__.custom_import_source_stream
            WHERE definition_revision_id=e.definition_revision_id LIMIT 10) s)<>c.stream_count
            OR (SELECT count(*) FROM (SELECT 1 FROM __SCHEMA__.custom_import_child_collection
                WHERE schema_revision_id=e.schema_revision_id LIMIT 9) s)>8
            OR (SELECT count(*) FROM (SELECT 1 FROM __SCHEMA__.custom_import_field
                WHERE schema_revision_id=e.schema_revision_id AND projection_slot>0 LIMIT 21) s)>20
            OR EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_source_stream s WHERE s.definition_revision_id=e.definition_revision_id
                AND NOT EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_capture cap WHERE cap.capture_bundle_id=c.capture_bundle_id
                    AND cap.stream_slot=s.stream_slot AND cap.capture_state='sealed')) THEN
            RAISE EXCEPTION 'custom_import_build_structure_mismatch: definition shape'; END IF;
        SELECT * INTO b FROM __SCHEMA__.custom_import_build_attempt WHERE execution_id=p_execution_id AND producing_fence=p_fence;
        IF b.build_id IS NOT NULL THEN
            IF ROW(b.producing_token_sha256,b.base_generation_id,b.base_pointer_version,b.complete_scope,
                   b.page_row_limit,b.page_byte_limit,b.statement_timeout_ms,b.build_deadline_at)
               IS DISTINCT FROM ROW(p_token_sha256,p_expected_base_generation_id,p_expected_pointer_version,p_complete_scope,
                   p_page_row_limit,p_page_byte_limit,p_statement_timeout_ms,p_build_deadline_at) THEN
                RAISE EXCEPTION 'custom_import_build_retry_conflict';
            END IF;
            PERFORM __SCHEMA__.lock_custom_import_build(b.build_id);
            RETURN b.build_id;
        END IF;
        SELECT * INTO p FROM __SCHEMA__.custom_import_current_generation WHERE dataset_id=e.dataset_id;
        IF ROW(p.generation_id,coalesce(p.pointer_version,0)) IS DISTINCT FROM
           ROW(p_expected_base_generation_id,p_expected_pointer_version) THEN RAISE EXCEPTION 'custom_import_build_pointer_mismatch'; END IF;
        IF p.generation_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM __SCHEMA__.custom_import_generation_seal WHERE generation_id=p.generation_id) THEN
            RAISE EXCEPTION 'custom_import_build_identity_mismatch';
        END IF;
        IF EXISTS (SELECT 1 FROM __SCHEMA__.custom_import_pack WHERE execution_id=e.execution_id AND producing_fence=p_fence)
           OR EXISTS (SELECT 1 FROM __SCHEMA__.custom_import_generation WHERE execution_id=e.execution_id AND producing_fence=p_fence)
           OR EXISTS (SELECT 1 FROM __SCHEMA__.custom_import_rejection WHERE execution_id=e.execution_id AND producing_fence=p_fence) THEN
            RAISE EXCEPTION 'custom_import_build_retry_conflict';
        END IF;
        INSERT INTO __SCHEMA__.custom_import_build_attempt(dataset_id,definition_revision_id,schema_revision_id,execution_id,
            capture_bundle_id,producing_fence,producing_token_sha256,request_identity_sha256,base_generation_id,base_pointer_version,
            refresh_mode,complete_scope,page_row_limit,page_byte_limit,statement_timeout_ms,build_deadline_at)
        VALUES(e.dataset_id,e.definition_revision_id,e.schema_revision_id,e.execution_id,e.capture_bundle_id,p_fence,p_token_sha256,
            e.request_identity_sha256,p_expected_base_generation_id,p_expected_pointer_version,mode,p_complete_scope,
            p_page_row_limit,p_page_byte_limit,p_statement_timeout_ms,p_build_deadline_at) RETURNING * INTO b;
        INSERT INTO __SCHEMA__.custom_import_build_stream(build_id,stream_slot)
            SELECT b.build_id,stream_slot FROM __SCHEMA__.custom_import_capture WHERE capture_bundle_id=b.capture_bundle_id;
        PERFORM __SCHEMA__.lock_custom_import_build(b.build_id);
        RETURN b.build_id;
    END;
    """

_COMMIT_CUSTOM_IMPORT_BUILD_SOURCE_PAGE_BODY = r"""
    DECLARE b __SCHEMA__.custom_import_build_attempt; s __SCHEMA__.custom_import_build_stream;
        p __SCHEMA__.custom_import_pack; o record; n bigint:=0; valid_n bigint:=0; size_n bigint:=0;
        first_row bigint; first_source bigint; part_no integer; part_rows bigint; hashes bytea:=''::bytea; label text;
    BEGIN
        b:=__SCHEMA__.lock_custom_import_build(p_build_id);
        IF b.phase <> 'source' THEN RAISE EXCEPTION 'custom_import_build_phase_mismatch'; END IF;
        SELECT * INTO p FROM __SCHEMA__.custom_import_pack WHERE pack_id=p_pack_id;
        IF ROW(p.execution_id,p.producing_fence,p.producing_token_sha256,p.capture_bundle_id)
            IS DISTINCT FROM ROW(b.execution_id,b.producing_fence,b.producing_token_sha256,b.capture_bundle_id) THEN
            RAISE EXCEPTION 'custom_import_build_identity_mismatch'; END IF;
        SELECT * INTO s FROM __SCHEMA__.custom_import_build_stream WHERE build_id=b.build_id AND stream_slot=p.stream_slot FOR UPDATE;
        IF s.build_id IS NULL OR s.replay_verified_at IS NOT NULL THEN RAISE EXCEPTION 'custom_import_build_phase_mismatch'; END IF;
        FOR o IN SELECT x.*,coalesce(octet_length(r.canonical_payload),octet_length(c.canonical_payload),octet_length(j.canonical_evidence),0)
                   +coalesce(octet_length(x.raw_parent_key_canonical),0)+coalesce(octet_length(k.canonical_logical_key),0)
                   +coalesce(octet_length(c.canonical_parent_key),0)+coalesce(octet_length(c.canonical_child_key),0)
                   +coalesce(octet_length(j.canonical_root_key),0) AS bytes,
                   coalesce(r.payload_sha256,c.payload_sha256) AS payload_hash
            FROM __SCHEMA__.custom_import_build_occurrence x
            LEFT JOIN __SCHEMA__.custom_import_root_revision r ON r.root_revision_id=x.root_revision_id
            LEFT JOIN __SCHEMA__.custom_import_child_revision c ON c.child_revision_id=x.child_revision_id
            LEFT JOIN __SCHEMA__.custom_import_rejection j ON j.rejection_id=x.rejection_id
            LEFT JOIN __SCHEMA__.custom_import_root_record k ON k.root_record_id=x.root_record_id
            WHERE x.pack_id=p_pack_id ORDER BY x.source_ordinal LIMIT b.page_row_limit+1
        LOOP
            IF o.build_id<>b.build_id OR o.origin<>'source' OR o.stream_slot<>p.stream_slot THEN RAISE EXCEPTION 'custom_import_build_identity_mismatch'; END IF;
            IF n=0 THEN first_row:=o.part_row_ordinal; first_source:=o.source_ordinal; part_no:=o.source_part_ordinal; END IF;
            IF o.part_row_ordinal<>first_row+n OR o.source_ordinal<>first_source+n OR o.source_part_ordinal<>part_no THEN
                RAISE EXCEPTION 'custom_import_build_source_gap'; END IF;
            n:=n+1; size_n:=size_n+o.bytes;
            IF n>b.page_row_limit OR size_n>b.page_byte_limit THEN RAISE EXCEPTION 'custom_import_build_page_too_large'; END IF;
            IF o.payload_hash IS NOT NULL THEN valid_n:=valid_n+1; END IF;
        END LOOP;
        IF n=0 OR valid_n<>p.record_count THEN RAISE EXCEPTION 'custom_import_build_structure_mismatch'; END IF;
        SELECT string_agg(h,''::bytea ORDER BY h) INTO hashes FROM (
            SELECT coalesce(r.payload_sha256,c.payload_sha256) h FROM __SCHEMA__.custom_import_build_occurrence x
            LEFT JOIN __SCHEMA__.custom_import_root_revision r ON r.root_revision_id=x.root_revision_id
            LEFT JOIN __SCHEMA__.custom_import_child_revision c ON c.child_revision_id=x.child_revision_id
            WHERE x.pack_id=p_pack_id) v;
        SELECT CASE WHEN stream.record_kind='root' THEN 'root' ELSE collection.collection_name END INTO label
            FROM __SCHEMA__.custom_import_source_stream stream LEFT JOIN __SCHEMA__.custom_import_child_collection collection
              ON collection.schema_revision_id=stream.schema_revision_id AND collection.collection_slot=stream.collection_slot
            WHERE stream.definition_revision_id=b.definition_revision_id AND stream.stream_slot=p.stream_slot;
        IF p.pack_sha256 IS DISTINCT FROM sha256(decode('637573746f6d2d696d706f72742f76310063616e6469646174652d72756e6e65722f31007061636b00','hex')
             ||convert_to(label,'UTF8')||decode('00','hex')||coalesce(hashes,''::bytea)) THEN
            RAISE EXCEPTION 'custom_import_build_structure_mismatch'; END IF;
        IF p.pack_ordinal<s.next_pack_ordinal AND first_source+n<=s.next_source_ordinal THEN RETURN s.next_source_ordinal; END IF;
        IF ROW(p.pack_ordinal,part_no,first_row,first_source) IS DISTINCT FROM
           ROW(s.next_pack_ordinal,s.next_part_ordinal,s.next_part_row_ordinal,s.next_source_ordinal) THEN
            RAISE EXCEPTION 'custom_import_build_source_gap'; END IF;
        SELECT record_count INTO part_rows FROM __SCHEMA__.custom_import_capture_parquet_part
            WHERE capture_bundle_id=b.capture_bundle_id AND stream_slot=s.stream_slot AND part_ordinal=part_no;
        IF part_rows IS NULL OR first_row+n>part_rows THEN RAISE EXCEPTION 'custom_import_build_source_gap'; END IF;
        UPDATE __SCHEMA__.custom_import_build_stream SET next_part_row_ordinal=first_row+n,
            next_source_ordinal=first_source+n,next_pack_ordinal=next_pack_ordinal+1
            WHERE build_id=b.build_id AND stream_slot=s.stream_slot;
        UPDATE __SCHEMA__.custom_import_build_attempt SET source_occurrence_count=source_occurrence_count+n WHERE build_id=b.build_id;
        RETURN first_source+n;
    END;
    """

_FINISH_CUSTOM_IMPORT_BUILD_SOURCE_PART_BODY = r"""
    DECLARE b __SCHEMA__.custom_import_build_attempt; s __SCHEMA__.custom_import_build_stream; n bigint; parts integer;
    BEGIN
        b:=__SCHEMA__.lock_custom_import_build(p_build_id);
        IF b.phase<>'source' THEN RAISE EXCEPTION 'custom_import_build_phase_mismatch'; END IF;
        SELECT * INTO s FROM __SCHEMA__.custom_import_build_stream WHERE build_id=b.build_id AND stream_slot=p_stream_slot FOR UPDATE;
        IF s.build_id IS NULL THEN RAISE EXCEPTION 'custom_import_build_identity_mismatch'; END IF;
        IF p_part_ordinal<s.next_part_ordinal THEN RETURN s.next_part_ordinal; END IF;
        SELECT record_count INTO n FROM __SCHEMA__.custom_import_capture_parquet_part
            WHERE capture_bundle_id=b.capture_bundle_id AND stream_slot=p_stream_slot AND part_ordinal=p_part_ordinal;
        SELECT payload_part_count INTO parts FROM __SCHEMA__.custom_import_capture
            WHERE capture_bundle_id=b.capture_bundle_id AND stream_slot=p_stream_slot AND capture_state='sealed';
        IF n IS NULL OR parts IS NULL OR p_part_ordinal<>s.next_part_ordinal OR n<>s.next_part_row_ordinal THEN
            RAISE EXCEPTION 'custom_import_build_incomplete'; END IF;
        UPDATE __SCHEMA__.custom_import_build_stream SET next_part_ordinal=p_part_ordinal+1,next_part_row_ordinal=0,
            replay_verified_at=CASE WHEN p_part_ordinal=parts THEN clock_timestamp() ELSE NULL END
            WHERE build_id=b.build_id AND stream_slot=p_stream_slot;
        RETURN p_part_ordinal+1;
    END;
    """

_FREEZE_CUSTOM_IMPORT_BUILD_SOURCE_BODY = r"""
    DECLARE b __SCHEMA__.custom_import_build_attempt;
    BEGIN
        b:=__SCHEMA__.lock_custom_import_build(p_build_id);
        IF b.phase='admission' THEN RETURN b.phase; END IF;
        IF b.phase<>'source' THEN RAISE EXCEPTION 'custom_import_build_phase_mismatch'; END IF;
        IF EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_build_stream WHERE build_id=b.build_id AND replay_verified_at IS NULL)
           OR EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_source_stream x WHERE x.definition_revision_id=b.definition_revision_id
                AND NOT EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_build_stream s WHERE s.build_id=b.build_id AND s.stream_slot=x.stream_slot)) THEN
            RAISE EXCEPTION 'custom_import_build_incomplete'; END IF;
        UPDATE __SCHEMA__.custom_import_build_attempt SET phase='admission',source_frozen_at=clock_timestamp() WHERE build_id=b.build_id;
        RETURN 'admission';
    END;
    """

_GUARD_CUSTOM_IMPORT_BUILD_OWNED_BODY = r"""
    DECLARE owner_name name; allowed text[]; b __SCHEMA__.custom_import_build_attempt;
    BEGIN
        IF TG_TABLE_SCHEMA <> '__SCHEMA_NAME__' OR TG_RELID NOT IN (
            '__SCHEMA__.custom_import_build_attempt'::regclass,'__SCHEMA__.custom_import_build_stream'::regclass,
            '__SCHEMA__.custom_import_build_family'::regclass,'__SCHEMA__.custom_import_build_verification'::regclass)
            OR TG_WHEN<>'BEFORE' THEN RAISE EXCEPTION 'custom_import_build_protected_write'; END IF;
        IF TG_OP IN ('DELETE','TRUNCATE') THEN RAISE EXCEPTION 'custom_import_build_immutable'; END IF;
        SELECT pg_get_userbyid(proowner) INTO owner_name FROM pg_proc WHERE oid=
          '__SCHEMA__.begin_custom_import_build(bigint,bigint,bytea,bigint,bigint,boolean,integer,bigint,integer,timestamptz)'::regprocedure;
        IF TG_LEVEL<>'ROW' OR pg_trigger_depth() NOT IN (1,2) OR current_user<>owner_name THEN
            RAISE EXCEPTION 'custom_import_build_protected_write'; END IF;
        IF TG_OP='INSERT' THEN RETURN NEW; END IF;
        IF TG_OP<>'UPDATE' THEN RAISE EXCEPTION 'custom_import_build_protected_write'; END IF;
        CASE TG_TABLE_NAME
        WHEN 'custom_import_build_attempt' THEN
            allowed:=ARRAY['phase','generation_id','admission_after_occurrence_id','plan_stage','plan_page_sequence',
                'plan_after_base_root_record_id','plan_after_source_root_record_id','plan_complete_at',
                'output_after_profile_slot','output_after_entity_binding_id','output_after_context_key_sha256',
                'source_occurrence_count','candidate_error_count','selected_family_count','completed_family_count',
                'candidate_context_count','generation_family_count','winner_count','next_rejection_ordinal',
                'source_frozen_at','graph_frozen_at','output_frozen_at','verified_at'];
            IF OLD.phase<>NEW.phase AND NOT (
                (OLD.phase='source' AND NEW.phase='admission') OR (OLD.phase='admission' AND NEW.phase IN ('graph','rejected'))
                OR (OLD.phase='graph' AND NEW.phase='output') OR (OLD.phase='output' AND NEW.phase='verifying')
                OR (OLD.phase='verifying' AND NEW.phase='verified')) THEN RAISE EXCEPTION 'custom_import_build_immutable'; END IF;
            IF (OLD.generation_id IS NOT NULL AND OLD.generation_id IS DISTINCT FROM NEW.generation_id)
               OR (OLD.source_frozen_at IS NOT NULL AND OLD.source_frozen_at IS DISTINCT FROM NEW.source_frozen_at)
               OR (OLD.graph_frozen_at IS NOT NULL AND OLD.graph_frozen_at IS DISTINCT FROM NEW.graph_frozen_at)
               OR (OLD.output_frozen_at IS NOT NULL AND OLD.output_frozen_at IS DISTINCT FROM NEW.output_frozen_at)
               OR OLD.phase IN ('verified','rejected') THEN RAISE EXCEPTION 'custom_import_build_immutable'; END IF;
        WHEN 'custom_import_build_stream' THEN
            allowed:=ARRAY['next_part_ordinal','next_part_row_ordinal','next_source_ordinal','next_pack_ordinal','replay_verified_at'];
            SELECT * INTO b FROM __SCHEMA__.custom_import_build_attempt WHERE build_id=OLD.build_id;
            IF b.phase<>'source' AND ((to_jsonb(NEW)-'next_pack_ordinal') IS DISTINCT FROM (to_jsonb(OLD)-'next_pack_ordinal')
                OR b.phase<>'graph') THEN RAISE EXCEPTION 'custom_import_build_immutable'; END IF;
        WHEN 'custom_import_build_family' THEN
            allowed:=ARRAY['family_revision_id','last_child_collection_slot','last_child_key_sha256','last_input_child_revision_id',
                'attached_child_count','complete_at'];
            SELECT * INTO b FROM __SCHEMA__.custom_import_build_attempt WHERE build_id=OLD.build_id;
            IF b.phase<>'graph' OR OLD.complete_at IS NOT NULL
               OR (OLD.family_revision_id IS NOT NULL AND OLD.family_revision_id IS DISTINCT FROM NEW.family_revision_id)
               OR NEW.attached_child_count<OLD.attached_child_count THEN RAISE EXCEPTION 'custom_import_build_immutable'; END IF;
        WHEN 'custom_import_build_verification' THEN
            allowed:=ARRAY['verification_state','scan_stage','page_sequence','after_capture_stream_slot','after_profile_slot',
                'after_root_record_id','current_family_revision_id','current_family_expected_child_count','current_family_seen_child_count',
                'after_child_collection_slot','after_child_revision_id','after_root_scalar_revision_id','after_root_scalar_field_slot',
                'after_child_scalar_revision_id','after_child_scalar_field_slot','after_winner_profile_slot',
                'after_winner_entity_binding_id','after_winner_context_key_sha256','root_count','family_count','generation_family_count',
                'family_child_count','winner_count','profile_count','root_scalar_count','child_scalar_count','verified_at'];
            SELECT * INTO b FROM __SCHEMA__.custom_import_build_attempt WHERE build_id=OLD.build_id;
            IF OLD.verification_state='complete' OR b.phase<>'verifying' OR NEW.page_sequence<>OLD.page_sequence+1 THEN
                RAISE EXCEPTION 'custom_import_build_immutable'; END IF;
        ELSE RAISE EXCEPTION 'custom_import_build_protected_write';
        END CASE;
        IF to_jsonb(OLD)-allowed IS DISTINCT FROM to_jsonb(NEW)-allowed THEN RAISE EXCEPTION 'custom_import_build_immutable'; END IF;
        RETURN NEW;
    END;
    """

_GUARD_CUSTOM_IMPORT_BUILD_EVIDENCE_BODY = r"""
    DECLARE b __SCHEMA__.custom_import_build_attempt; p __SCHEMA__.custom_import_pack; r record; f record; owner_name name; next_pack integer;
    BEGIN
        IF TG_RELID NOT IN ('__SCHEMA__.custom_import_build_occurrence'::regclass,
            '__SCHEMA__.custom_import_build_candidate_context'::regclass) OR TG_WHEN<>'BEFORE' THEN
            RAISE EXCEPTION 'custom_import_build_protected_write'; END IF;
        IF TG_OP IN ('DELETE','TRUNCATE') THEN RAISE EXCEPTION 'custom_import_build_immutable'; END IF;
        IF TG_LEVEL<>'ROW' OR TG_OP NOT IN ('INSERT','UPDATE') THEN RAISE EXCEPTION 'custom_import_build_protected_write'; END IF;
        IF TG_OP='UPDATE' THEN
            SELECT pg_get_userbyid(proowner) INTO owner_name FROM pg_proc WHERE oid=
                '__SCHEMA__.admit_custom_import_build_page(bigint,bigint)'::regprocedure;
            SELECT * INTO b FROM __SCHEMA__.custom_import_build_attempt WHERE build_id=OLD.build_id;
            IF TG_TABLE_NAME<>'custom_import_build_occurrence' OR current_user<>owner_name OR pg_trigger_depth()<>1
               OR b.phase<>'admission' OR OLD.resolved_rejection_id IS NOT NULL OR NEW.resolved_rejection_id IS NULL
               OR to_jsonb(NEW)-'resolved_rejection_id' IS DISTINCT FROM to_jsonb(OLD)-'resolved_rejection_id' THEN
                RAISE EXCEPTION 'custom_import_build_immutable'; END IF;
            RETURN NEW;
        END IF;
        b:=__SCHEMA__.lock_custom_import_build(NEW.build_id);
        IF TG_TABLE_NAME='custom_import_build_occurrence' THEN
            IF (NEW.origin='source' AND b.phase<>'source') OR (NEW.origin='retained' AND b.phase<>'graph') THEN
                RAISE EXCEPTION 'custom_import_build_phase_mismatch'; END IF;
            SELECT * INTO p FROM __SCHEMA__.custom_import_pack WHERE pack_id=NEW.pack_id;
            IF ROW(p.execution_id,p.producing_fence,p.producing_token_sha256,p.capture_bundle_id,p.stream_slot)
               IS DISTINCT FROM ROW(b.execution_id,b.producing_fence,b.producing_token_sha256,b.capture_bundle_id,NEW.stream_slot) THEN
                RAISE EXCEPTION 'custom_import_build_identity_mismatch'; END IF;
            SELECT next_pack_ordinal INTO next_pack FROM __SCHEMA__.custom_import_build_stream WHERE build_id=b.build_id AND stream_slot=NEW.stream_slot;
            IF p.pack_ordinal<next_pack THEN RAISE EXCEPTION 'custom_import_build_immutable: committed pack'; END IF;
            IF NEW.resolved_rejection_id IS NOT NULL OR (NEW.root_record_id IS NOT NULL AND (NEW.root_record_id<=0 OR NOT EXISTS(
                SELECT 1 FROM __SCHEMA__.custom_import_root_record WHERE root_record_id=NEW.root_record_id AND dataset_id=b.dataset_id))) THEN
                RAISE EXCEPTION 'custom_import_build_identity_mismatch'; END IF;
            IF NOT EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_source_stream s WHERE s.definition_revision_id=b.definition_revision_id
                AND s.stream_slot=NEW.stream_slot AND s.record_kind=NEW.record_kind AND coalesce(s.collection_slot,0)=NEW.collection_slot) THEN
                RAISE EXCEPTION 'custom_import_build_identity_mismatch'; END IF;
            IF NEW.root_revision_id IS NOT NULL THEN
                SELECT pack_id,root_record_id,NULL::bytea key_hash,canonical_payload payload,source_ordinal,payload_sha256 payload_hash,NULL::text child_key INTO r
                    FROM __SCHEMA__.custom_import_root_revision WHERE root_revision_id=NEW.root_revision_id;
            ELSIF NEW.child_revision_id IS NOT NULL THEN
                SELECT pack_id,root_record_id,child_key_sha256 key_hash,canonical_payload payload,source_ordinal,payload_sha256 payload_hash,canonical_child_key child_key INTO r
                    FROM __SCHEMA__.custom_import_child_revision WHERE child_revision_id=NEW.child_revision_id AND collection_slot=NEW.collection_slot;
            ELSE
                SELECT pack_id,NULL::bigint root_record_id,NULL::bytea key_hash,canonical_evidence payload,source_ordinal,NULL::bytea payload_hash,NULL::text child_key INTO r
                    FROM __SCHEMA__.custom_import_rejection WHERE rejection_id=NEW.rejection_id AND execution_id=b.execution_id
                    AND producing_fence=b.producing_fence AND producing_token_sha256=b.producing_token_sha256;
            END IF;
            IF r.pack_id IS DISTINCT FROM NEW.pack_id OR (NEW.rejection_id IS NULL AND r.root_record_id IS DISTINCT FROM NEW.root_record_id)
               OR r.key_hash IS DISTINCT FROM NEW.child_key_sha256 OR (NEW.origin='source' AND r.source_ordinal IS DISTINCT FROM NEW.source_ordinal)
               THEN RAISE EXCEPTION 'custom_import_build_identity_mismatch'; END IF;
            IF coalesce(octet_length(r.payload),0)+coalesce(octet_length(NEW.raw_parent_key_canonical),0)>b.page_byte_limit THEN
                RAISE EXCEPTION 'custom_import_build_page_too_large'; END IF;
            IF NEW.raw_parent_key_canonical IS NOT NULL AND NEW.raw_parent_key_sha256 IS DISTINCT FROM
                sha256(convert_to('custom-import/raw-family-key/v1:'||NEW.raw_parent_key_canonical,'UTF8')) THEN
                RAISE EXCEPTION 'custom_import_build_identity_mismatch'; END IF;
            IF NEW.origin='retained' THEN
                IF NOT EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_build_family bf WHERE bf.build_id=b.build_id
                    AND bf.root_record_id=NEW.root_record_id AND bf.selection_kind='retained'
                    AND bf.base_family_revision_id=NEW.base_family_revision_id) THEN RAISE EXCEPTION 'custom_import_build_identity_mismatch'; END IF;
                IF NEW.base_root_revision_id IS NOT NULL THEN
                    IF NOT EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_family_revision bf
                        JOIN __SCHEMA__.custom_import_root_revision br ON br.root_revision_id=bf.root_revision_id
                        WHERE bf.family_revision_id=NEW.base_family_revision_id AND br.root_revision_id=NEW.base_root_revision_id
                          AND br.canonical_payload=r.payload AND br.payload_sha256=r.payload_hash AND br.root_record_id=NEW.root_record_id) THEN
                        RAISE EXCEPTION 'custom_import_build_structure_mismatch'; END IF;
                ELSE
                    IF NOT EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_family_child fc
                        JOIN __SCHEMA__.custom_import_child_revision bc ON bc.child_revision_id=fc.child_revision_id
                        WHERE fc.family_revision_id=NEW.base_family_revision_id AND bc.child_revision_id=NEW.base_child_revision_id
                          AND bc.collection_slot=NEW.collection_slot AND bc.canonical_payload=r.payload AND bc.payload_sha256=r.payload_hash
                          AND bc.child_key_sha256=NEW.child_key_sha256 AND bc.canonical_child_key=r.child_key AND bc.root_record_id=NEW.root_record_id) THEN
                        RAISE EXCEPTION 'custom_import_build_structure_mismatch'; END IF;
                END IF;
            END IF;
        ELSE
            IF b.phase<>'graph' THEN RAISE EXCEPTION 'custom_import_build_phase_mismatch'; END IF;
            SELECT * INTO f FROM __SCHEMA__.custom_import_family_revision WHERE family_revision_id=NEW.family_revision_id;
            IF ROW(f.producing_execution_id,f.producing_fence,f.producing_token_sha256,f.entity_binding_id)
               IS DISTINCT FROM ROW(b.execution_id,b.producing_fence,b.producing_token_sha256,NEW.entity_binding_id)
               OR NOT EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_selection_profile WHERE definition_revision_id=b.definition_revision_id
                   AND profile_slot=NEW.profile_slot AND coalesce(context_collection_slot,0)=NEW.context_collection_slot)
               OR (NEW.context_child_revision_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_family_child
                   WHERE family_revision_id=NEW.family_revision_id AND collection_slot=NEW.context_collection_slot
                     AND child_revision_id=NEW.context_child_revision_id)) THEN RAISE EXCEPTION 'custom_import_build_identity_mismatch'; END IF;
            IF NEW.context_key_sha256 IS DISTINCT FROM sha256(decode('637573746f6d2d696d706f72742f76310077696e6e65722d636f6e7465787400','hex')
                ||convert_to(NEW.canonical_context_key,'UTF8')) THEN RAISE EXCEPTION 'custom_import_build_structure_mismatch'; END IF;
        END IF;
        RETURN NEW;
    END;
    """

_ADMIT_CUSTOM_IMPORT_BUILD_PAGE_BODY = r"""
    DECLARE b __SCHEMA__.custom_import_build_attempt; o record; parent_row __SCHEMA__.custom_import_build_occurrence; other_row record;
        n integer:=0; bytes bigint:=0; last_id bigint; errors bigint:=0; code text; rejection bigint; root_key text; root_hash bytea;
    BEGIN
        b:=__SCHEMA__.lock_custom_import_build(p_build_id);
        IF b.phase<>'admission' THEN RAISE EXCEPTION 'custom_import_build_phase_mismatch'; END IF;
        IF p_expected_after_id IS DISTINCT FROM b.admission_after_occurrence_id THEN
            RAISE EXCEPTION 'custom_import_build_progress_conflict' USING ERRCODE='40001'; END IF;
        last_id:=b.admission_after_occurrence_id;
        FOR o IN SELECT x.*,j.code initial_code,
            coalesce(octet_length(x.raw_parent_key_canonical),0)+coalesce(octet_length(k.canonical_logical_key),0)
            +coalesce((SELECT octet_length(first_key.raw_parent_key_canonical) FROM __SCHEMA__.custom_import_build_occurrence first_key
                WHERE first_key.build_id=b.build_id AND first_key.record_kind=x.record_kind
                  AND first_key.raw_parent_key_sha256=x.raw_parent_key_sha256 ORDER BY first_key.occurrence_id LIMIT 1),0)
            +coalesce((SELECT octet_length(parent_key.raw_parent_key_canonical) FROM __SCHEMA__.custom_import_build_occurrence parent_key
                WHERE x.record_kind='child' AND parent_key.build_id=b.build_id AND parent_key.record_kind='root'
                  AND parent_key.raw_parent_key_sha256=x.raw_parent_key_sha256 ORDER BY parent_key.occurrence_id LIMIT 1),0)
            +coalesce(octet_length(child_key.canonical_child_key),0)*2 raw_bytes
            FROM __SCHEMA__.custom_import_build_occurrence x LEFT JOIN __SCHEMA__.custom_import_rejection j ON j.rejection_id=x.rejection_id
            LEFT JOIN __SCHEMA__.custom_import_root_record k ON k.root_record_id=x.root_record_id
            LEFT JOIN __SCHEMA__.custom_import_child_revision child_key ON child_key.child_revision_id=x.child_revision_id
            WHERE x.build_id=b.build_id AND x.origin='source' AND x.occurrence_id>last_id
            ORDER BY x.occurrence_id LIMIT b.page_row_limit
        LOOP
            IF coalesce(o.raw_bytes,0)>b.page_byte_limit THEN RAISE EXCEPTION 'custom_import_build_page_too_large'; END IF;
            EXIT WHEN n>0 AND bytes+coalesce(o.raw_bytes,0)>b.page_byte_limit;
            n:=n+1; bytes:=bytes+coalesce(o.raw_bytes,0); last_id:=o.occurrence_id; code:=o.initial_code; rejection:=o.rejection_id;
            SELECT resolved_rejection_id INTO o.resolved_rejection_id FROM __SCHEMA__.custom_import_build_occurrence
                WHERE occurrence_id=o.occurrence_id;
            IF o.resolved_rejection_id IS NOT NULL THEN
                IF code IN ('root_not_object','root_key_missing','child_not_object','orphan_child') THEN errors:=errors+1; END IF;
                CONTINUE;
            END IF;
            IF o.raw_parent_key_sha256 IS NOT NULL THEN
                SELECT x.raw_parent_key_canonical INTO other_row FROM __SCHEMA__.custom_import_build_occurrence x
                    WHERE x.build_id=b.build_id AND x.record_kind=o.record_kind AND x.raw_parent_key_sha256=o.raw_parent_key_sha256
                    ORDER BY x.occurrence_id LIMIT 1;
                IF other_row.raw_parent_key_canonical IS DISTINCT FROM o.raw_parent_key_canonical THEN
                    RAISE EXCEPTION 'custom_import_build_structure_mismatch: raw key digest collision'; END IF;
            END IF;
            IF o.record_kind='root' THEN
                IF code IS NULL AND EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_build_occurrence x
                    WHERE x.build_id=b.build_id AND x.origin='source' AND x.record_kind='root'
                    AND x.raw_parent_key_sha256=o.raw_parent_key_sha256 AND x.occurrence_id<>o.occurrence_id) THEN code:='duplicate_root_key'; END IF;
                IF code IS NULL AND o.root_record_id IS NOT NULL AND EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_build_occurrence x
                    WHERE x.build_id=b.build_id AND x.origin='source' AND x.record_kind='root'
                    AND x.root_record_id=o.root_record_id AND x.occurrence_id<>o.occurrence_id) THEN code:='duplicate_root_key'; END IF;
            ELSE
                parent_row:=NULL;
                IF code IS DISTINCT FROM 'child_not_object' THEN
                SELECT * INTO parent_row FROM __SCHEMA__.custom_import_build_occurrence x
                    WHERE x.build_id=b.build_id AND x.origin='source' AND x.record_kind='root'
                      AND x.raw_parent_key_sha256=o.raw_parent_key_sha256 ORDER BY x.occurrence_id LIMIT 1;
                IF parent_row.occurrence_id IS NULL OR parent_row.raw_parent_key_canonical IS DISTINCT FROM o.raw_parent_key_canonical THEN
                    code:='orphan_child';
                ELSIF code IS NULL AND EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_build_occurrence x
                    WHERE x.build_id=b.build_id AND x.origin='source' AND x.record_kind='child'
                      AND x.root_record_id=o.root_record_id AND x.raw_parent_key_sha256=o.raw_parent_key_sha256 AND x.collection_slot=o.collection_slot
                      AND x.child_key_sha256=o.child_key_sha256 AND x.occurrence_id<>o.occurrence_id) THEN code:='duplicate_child_key';
                END IF;
                END IF;
                IF o.child_revision_id IS NOT NULL THEN
                    SELECT other_child.canonical_child_key INTO other_row FROM __SCHEMA__.custom_import_build_occurrence x
                        JOIN __SCHEMA__.custom_import_child_revision other_child ON other_child.child_revision_id=x.child_revision_id
                        WHERE x.build_id=b.build_id AND x.origin='source' AND x.root_record_id=o.root_record_id
                          AND x.collection_slot=o.collection_slot AND x.child_key_sha256=o.child_key_sha256
                        ORDER BY x.child_key_sha256,x.child_revision_id LIMIT 1;
                    IF other_row.canonical_child_key IS DISTINCT FROM (SELECT canonical_child_key FROM __SCHEMA__.custom_import_child_revision
                        WHERE child_revision_id=o.child_revision_id) THEN
                        RAISE EXCEPTION 'custom_import_build_structure_mismatch: child key digest collision'; END IF;
                END IF;
            END IF;
            IF code IS NOT NULL THEN
                IF code IS DISTINCT FROM o.initial_code THEN
                    SELECT canonical_logical_key,logical_key_sha256 INTO root_key,root_hash FROM __SCHEMA__.custom_import_root_record
                        WHERE root_record_id=o.root_record_id AND dataset_id=b.dataset_id;
                    SELECT next_rejection_ordinal INTO b.next_rejection_ordinal FROM __SCHEMA__.custom_import_build_attempt WHERE build_id=b.build_id;
                    INSERT INTO __SCHEMA__.custom_import_rejection(execution_id,rejection_ordinal,dataset_id,definition_revision_id,
                        schema_revision_id,pack_id,root_key_sha256,canonical_root_key,collection_slot,source_ordinal,code,canonical_evidence,
                        producing_fence,producing_token_sha256)
                    VALUES(b.execution_id,b.next_rejection_ordinal,b.dataset_id,b.definition_revision_id,b.schema_revision_id,o.pack_id,
                        root_hash,root_key,nullif(o.collection_slot,0),o.source_ordinal,code,'{}',b.producing_fence,b.producing_token_sha256)
                    RETURNING rejection_id INTO rejection;
                END IF;
                UPDATE __SCHEMA__.custom_import_build_occurrence SET resolved_rejection_id=rejection
                    WHERE occurrence_id=o.occurrence_id AND resolved_rejection_id IS NULL;
                IF o.record_kind='child' AND code<>'orphan_child' AND parent_row.occurrence_id IS NOT NULL THEN
                    UPDATE __SCHEMA__.custom_import_build_occurrence SET resolved_rejection_id=rejection
                        WHERE occurrence_id=parent_row.occurrence_id AND resolved_rejection_id IS NULL;
                END IF;
                IF code IN ('root_not_object','root_key_missing','child_not_object','orphan_child') THEN errors:=errors+1; END IF;
            END IF;
        END LOOP;
        UPDATE __SCHEMA__.custom_import_build_attempt SET admission_after_occurrence_id=last_id,
            candidate_error_count=custom_import_build_attempt.candidate_error_count+errors WHERE build_id=b.build_id RETURNING * INTO b;
        IF NOT EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_build_occurrence x WHERE x.build_id=b.build_id AND x.origin='source'
            AND x.occurrence_id>last_id) THEN
            IF b.refresh_mode='snapshot' AND NOT b.complete_scope THEN errors:=1; ELSE errors:=0; END IF;
            UPDATE __SCHEMA__.custom_import_build_attempt SET candidate_error_count=custom_import_build_attempt.candidate_error_count+errors,
                phase=CASE WHEN custom_import_build_attempt.candidate_error_count+errors>0 THEN 'rejected' ELSE 'graph' END
                WHERE build_id=b.build_id RETURNING * INTO b;
        END IF;
        RETURN QUERY SELECT b.phase::text,b.admission_after_occurrence_id,n,b.candidate_error_count;
    END;
    """

_PLAN_CUSTOM_IMPORT_BUILD_FAMILY_PAGE_BODY = r"""
    DECLARE b __SCHEMA__.custom_import_build_attempt; r record; o record; base_family bigint; root_hash bytea;
        rejected boolean; n integer:=0; selected bigint:=0; base_schema bigint;
    BEGIN
        b:=__SCHEMA__.lock_custom_import_build(p_build_id);
        IF b.phase<>'graph' OR b.plan_complete_at IS NOT NULL THEN RAISE EXCEPTION 'custom_import_build_phase_mismatch'; END IF;
        IF p_expected_page_sequence IS DISTINCT FROM b.plan_page_sequence THEN
            RAISE EXCEPTION 'custom_import_build_progress_conflict' USING ERRCODE='40001'; END IF;
        WHILE n<b.page_row_limit LOOP
            IF b.plan_stage='base' THEN
                SELECT gf.root_record_id INTO r FROM __SCHEMA__.custom_import_generation_family gf
                    WHERE gf.generation_id=b.base_generation_id AND gf.root_record_id>b.plan_after_base_root_record_id
                    ORDER BY gf.root_record_id LIMIT 1;
            ELSE
                SELECT x.root_record_id INTO r FROM __SCHEMA__.custom_import_build_occurrence x
                    WHERE x.build_id=b.build_id AND x.origin='source' AND x.record_kind='root'
                      AND x.root_record_id>b.plan_after_source_root_record_id ORDER BY x.root_record_id LIMIT 1;
            END IF;
            EXIT WHEN r.root_record_id IS NULL;
            n:=n+1;
            IF b.plan_stage='base' THEN b.plan_after_base_root_record_id:=r.root_record_id;
            ELSE b.plan_after_source_root_record_id:=r.root_record_id; END IF;
            SELECT gf.family_revision_id INTO base_family FROM __SCHEMA__.custom_import_generation_family gf
                WHERE gf.generation_id=b.base_generation_id AND gf.root_record_id=r.root_record_id;
            IF b.plan_stage='source' AND base_family IS NOT NULL THEN CONTINUE; END IF;
            SELECT * INTO o FROM __SCHEMA__.custom_import_build_occurrence x WHERE x.build_id=b.build_id AND x.origin='source'
                AND x.record_kind='root' AND x.root_record_id=r.root_record_id ORDER BY x.occurrence_id LIMIT 1;
            rejected:=o.resolved_rejection_id IS NOT NULL;
            SELECT logical_key_sha256 INTO root_hash FROM __SCHEMA__.custom_import_root_record WHERE root_record_id=r.root_record_id;
            IF o.occurrence_id IS NOT NULL AND NOT rejected AND o.root_revision_id IS NOT NULL THEN
                INSERT INTO __SCHEMA__.custom_import_build_family(build_id,root_record_id,root_key_sha256,selection_kind,source_root_occurrence_id)
                    VALUES(b.build_id,r.root_record_id,root_hash,'source',o.occurrence_id); selected:=selected+1;
            ELSIF base_family IS NOT NULL AND (b.refresh_mode='upsert' OR rejected) THEN
                SELECT schema_revision_id INTO base_schema FROM __SCHEMA__.custom_import_family_revision WHERE family_revision_id=base_family;
                IF base_schema<>b.schema_revision_id THEN RAISE EXCEPTION 'custom_import_build_structure_mismatch: retained schema differs'; END IF;
                INSERT INTO __SCHEMA__.custom_import_build_family(build_id,root_record_id,root_key_sha256,selection_kind,base_family_revision_id)
                    VALUES(b.build_id,r.root_record_id,root_hash,'retained',base_family); selected:=selected+1;
            END IF;
        END LOOP;
        IF n=0 THEN
            IF b.plan_stage='base' THEN b.plan_stage:='source'; ELSE b.plan_stage:='complete'; b.plan_complete_at:=clock_timestamp(); END IF;
        END IF;
        UPDATE __SCHEMA__.custom_import_build_attempt SET plan_stage=b.plan_stage,plan_page_sequence=b.plan_page_sequence+1,
            plan_after_base_root_record_id=b.plan_after_base_root_record_id,plan_after_source_root_record_id=b.plan_after_source_root_record_id,
            plan_complete_at=b.plan_complete_at,selected_family_count=custom_import_build_attempt.selected_family_count+selected
            WHERE build_id=b.build_id RETURNING * INTO b;
        RETURN QUERY SELECT b.phase::text,b.plan_stage::text,b.plan_page_sequence,n,b.plan_complete_at IS NOT NULL;
    END;
    """

_GUARD_CUSTOM_IMPORT_BUILD_GRAPH_BODY = r"""
    DECLARE b __SCHEMA__.custom_import_build_attempt; exec_id bigint; fence bigint; owner_name name;
        family_id bigint; rev_id bigint; pack bigint; gen bigint; shape_frozen boolean;
        chosen __SCHEMA__.custom_import_build_family; occurrence __SCHEMA__.custom_import_build_occurrence;
        current_name text; after_name text;
    BEGIN
        IF TG_WHEN<>'BEFORE' OR TG_LEVEL<>'ROW' OR TG_OP<>'INSERT' OR TG_RELID NOT IN (__ALLOWED__) THEN
            RAISE EXCEPTION 'custom_import_build_protected_write'; END IF;
        IF TG_TABLE_NAME IN ('custom_import_field','custom_import_child_collection','custom_import_field_alias',
            'custom_import_source_stream','custom_import_selection_profile') THEN
            PERFORM 1 FROM __SCHEMA__.custom_import_dataset WHERE dataset_id=NEW.dataset_id FOR UPDATE;
            IF TG_TABLE_NAME IN ('custom_import_field','custom_import_child_collection') THEN
                SELECT EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_build_attempt WHERE schema_revision_id=NEW.schema_revision_id) INTO shape_frozen;
            ELSE
                SELECT EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_build_attempt WHERE definition_revision_id=NEW.definition_revision_id) INTO shape_frozen;
            END IF;
            IF shape_frozen THEN RAISE EXCEPTION 'custom_import_build_immutable'; END IF;
            RETURN NEW;
        END IF;
        CASE TG_TABLE_NAME
        WHEN 'custom_import_pack','custom_import_rejection','custom_import_generation' THEN
            exec_id:=NEW.execution_id; fence:=NEW.producing_fence;
        WHEN 'custom_import_root_revision','custom_import_child_revision' THEN pack:=NEW.pack_id;
        WHEN 'custom_import_root_scalar' THEN
            SELECT pack_id INTO pack FROM __SCHEMA__.custom_import_root_revision WHERE root_revision_id=NEW.root_revision_id;
        WHEN 'custom_import_child_scalar' THEN
            SELECT pack_id INTO pack FROM __SCHEMA__.custom_import_child_revision WHERE child_revision_id=NEW.child_revision_id;
        WHEN 'custom_import_family_revision' THEN exec_id:=NEW.producing_execution_id; fence:=NEW.producing_fence;
        WHEN 'custom_import_family_child' THEN family_id:=NEW.family_revision_id;
        WHEN 'custom_import_generation_family','custom_import_winner' THEN gen:=NEW.generation_id;
        ELSE RAISE EXCEPTION 'custom_import_build_protected_write'; END CASE;
        IF pack IS NOT NULL THEN SELECT execution_id,producing_fence INTO exec_id,fence FROM __SCHEMA__.custom_import_pack WHERE pack_id=pack; END IF;
        IF family_id IS NOT NULL THEN SELECT producing_execution_id,producing_fence INTO exec_id,fence
            FROM __SCHEMA__.custom_import_family_revision WHERE family_revision_id=family_id; END IF;
        IF gen IS NOT NULL THEN SELECT execution_id,producing_fence INTO exec_id,fence FROM __SCHEMA__.custom_import_generation WHERE generation_id=gen; END IF;
        SELECT * INTO b FROM __SCHEMA__.custom_import_build_attempt WHERE execution_id=exec_id AND producing_fence=fence;
        IF b.build_id IS NULL THEN RETURN NEW; END IF;
        b:=__SCHEMA__.lock_custom_import_build(b.build_id);
        CASE TG_TABLE_NAME
        WHEN 'custom_import_pack','custom_import_root_revision','custom_import_child_revision' THEN
            IF b.phase NOT IN ('source','graph') THEN RAISE EXCEPTION 'custom_import_build_phase_mismatch'; END IF;
        WHEN 'custom_import_rejection' THEN
            IF b.phase NOT IN ('source','admission') THEN RAISE EXCEPTION 'custom_import_build_phase_mismatch'; END IF;
            IF b.phase='admission' THEN
                SELECT pg_get_userbyid(proowner) INTO owner_name FROM pg_proc WHERE oid=
                    '__SCHEMA__.admit_custom_import_build_page(bigint,bigint)'::regprocedure;
                IF current_user<>owner_name THEN RAISE EXCEPTION 'custom_import_build_protected_write'; END IF;
            END IF;
        WHEN 'custom_import_family_revision','custom_import_family_child','custom_import_root_scalar','custom_import_child_scalar' THEN
            IF b.phase<>'graph' THEN RAISE EXCEPTION 'custom_import_build_phase_mismatch'; END IF;
            IF TG_TABLE_NAME='custom_import_family_revision' AND NOT EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_build_family
                WHERE build_id=b.build_id AND root_record_id=NEW.root_record_id AND family_revision_id IS NULL) THEN
                RAISE EXCEPTION 'custom_import_build_identity_mismatch'; END IF;
            IF TG_TABLE_NAME='custom_import_root_scalar' THEN
                IF EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_build_family bf JOIN __SCHEMA__.custom_import_family_revision f
                    ON f.family_revision_id=bf.family_revision_id WHERE bf.build_id=b.build_id AND f.root_revision_id=NEW.root_revision_id) THEN
                    RAISE EXCEPTION 'custom_import_build_immutable: committed root page'; END IF;
            ELSIF TG_TABLE_NAME='custom_import_child_scalar' THEN
                SELECT * INTO occurrence FROM __SCHEMA__.custom_import_build_occurrence
                    WHERE build_id=b.build_id AND child_revision_id=NEW.child_revision_id;
                SELECT * INTO chosen FROM __SCHEMA__.custom_import_build_family WHERE build_id=b.build_id AND root_record_id=occurrence.root_record_id;
                IF chosen.complete_at IS NOT NULL THEN RAISE EXCEPTION 'custom_import_build_immutable: committed child page'; END IF;
                IF chosen.last_child_collection_slot IS NOT NULL THEN
                    IF occurrence.origin='retained' THEN
                        IF (occurrence.collection_slot,occurrence.base_child_revision_id)<=(chosen.last_child_collection_slot,chosen.last_input_child_revision_id) THEN
                            RAISE EXCEPTION 'custom_import_build_immutable: committed child page'; END IF;
                    ELSE
                        SELECT collection_name INTO current_name FROM __SCHEMA__.custom_import_child_collection
                            WHERE schema_revision_id=b.schema_revision_id AND collection_slot=occurrence.collection_slot;
                        SELECT collection_name INTO after_name FROM __SCHEMA__.custom_import_child_collection
                            WHERE schema_revision_id=b.schema_revision_id AND collection_slot=chosen.last_child_collection_slot;
                        IF (current_name COLLATE "C",occurrence.child_key_sha256,occurrence.child_revision_id)<=(after_name COLLATE "C",chosen.last_child_key_sha256,chosen.last_input_child_revision_id) THEN
                            RAISE EXCEPTION 'custom_import_build_immutable: committed child page'; END IF;
                    END IF;
                END IF;
            END IF;
        WHEN 'custom_import_generation' THEN
            IF b.phase<>'graph' OR b.plan_complete_at IS NULL OR b.completed_family_count<>b.selected_family_count THEN
                RAISE EXCEPTION 'custom_import_build_incomplete'; END IF;
            IF NEW.root_count<>b.selected_family_count OR NEW.family_count<>b.selected_family_count
               OR NEW.base_generation_id IS DISTINCT FROM b.base_generation_id THEN RAISE EXCEPTION 'custom_import_build_structure_mismatch'; END IF;
        WHEN 'custom_import_generation_family','custom_import_winner' THEN
            IF b.phase<>'output' OR b.generation_id IS DISTINCT FROM gen THEN RAISE EXCEPTION 'custom_import_build_phase_mismatch'; END IF;
            IF NOT EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_build_family bf WHERE bf.build_id=b.build_id
                AND bf.family_revision_id=NEW.family_revision_id AND bf.complete_at IS NOT NULL) THEN
                RAISE EXCEPTION 'custom_import_build_identity_mismatch'; END IF;
        END CASE;
        RETURN NEW;
    END;
    """

_CHARGE_CUSTOM_IMPORT_BUILD_ROW_BODY = r"""
    DECLARE b __SCHEMA__.custom_import_build_attempt; exec_id bigint; fence bigint; build bigint;
    BEGIN
        IF TG_WHEN<>'AFTER' OR TG_LEVEL<>'ROW' OR TG_OP<>'INSERT' OR TG_RELID NOT IN (
            '__SCHEMA__.custom_import_rejection'::regclass,'__SCHEMA__.custom_import_build_candidate_context'::regclass,
            '__SCHEMA__.custom_import_generation_family'::regclass,'__SCHEMA__.custom_import_winner'::regclass) THEN
            RAISE EXCEPTION 'custom_import_build_protected_write'; END IF;
        IF TG_TABLE_NAME='custom_import_rejection' THEN exec_id:=NEW.execution_id; fence:=NEW.producing_fence;
        ELSIF TG_TABLE_NAME='custom_import_build_candidate_context' THEN build:=NEW.build_id;
        ELSE SELECT execution_id,producing_fence INTO exec_id,fence FROM __SCHEMA__.custom_import_generation WHERE generation_id=NEW.generation_id;
        END IF;
        SELECT * INTO b FROM __SCHEMA__.custom_import_build_attempt WHERE build_id=build OR (execution_id=exec_id AND producing_fence=fence);
        IF b.build_id IS NULL THEN RETURN NEW; END IF;
        b:=__SCHEMA__.lock_custom_import_build(b.build_id);
        CASE TG_TABLE_NAME
        WHEN 'custom_import_rejection' THEN
            IF NEW.rejection_ordinal<>b.next_rejection_ordinal THEN RAISE EXCEPTION 'custom_import_build_source_gap'; END IF;
            UPDATE __SCHEMA__.custom_import_build_attempt SET next_rejection_ordinal=next_rejection_ordinal+1 WHERE build_id=b.build_id;
        WHEN 'custom_import_build_candidate_context' THEN
            UPDATE __SCHEMA__.custom_import_build_attempt SET candidate_context_count=candidate_context_count+1 WHERE build_id=b.build_id;
        WHEN 'custom_import_generation_family' THEN
            UPDATE __SCHEMA__.custom_import_build_attempt SET generation_family_count=generation_family_count+1 WHERE build_id=b.build_id;
        WHEN 'custom_import_winner' THEN
            UPDATE __SCHEMA__.custom_import_build_attempt SET winner_count=winner_count+1 WHERE build_id=b.build_id;
        END CASE;
        RETURN NEW;
    END;
    """

_GUARD_CUSTOM_IMPORT_BUILD_FROZEN_ROW_BODY = r"""
    BEGIN
        IF TG_WHEN<>'BEFORE' OR TG_RELID NOT IN (__ALLOWED__)
            OR NOT ((TG_OP IN ('UPDATE','DELETE') AND TG_LEVEL='ROW') OR (TG_OP='TRUNCATE' AND TG_LEVEL='STATEMENT')) THEN
            RAISE EXCEPTION 'custom_import_build_protected_write'; END IF;
        IF EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_build_attempt) THEN RAISE EXCEPTION 'custom_import_build_immutable'; END IF;
        IF TG_OP='UPDATE' THEN RETURN NEW; END IF;
        RETURN OLD;
    END;
    """

_CHECK_CUSTOM_IMPORT_BUILD_LINK_BODY = r"""
    DECLARE b __SCHEMA__.custom_import_build_attempt; p __SCHEMA__.custom_import_pack; gen bigint; family_id bigint;
        o __SCHEMA__.custom_import_build_occurrence; bf __SCHEMA__.custom_import_build_family; child_id bigint; current_name text; after_name text;
    BEGIN
        IF TG_WHEN<>'AFTER' OR TG_LEVEL<>'ROW' OR TG_OP<>'INSERT' OR TG_RELID NOT IN (__ALLOWED__) THEN
            RAISE EXCEPTION 'custom_import_build_protected_write'; END IF;
        CASE TG_TABLE_NAME
        WHEN 'custom_import_pack' THEN p:=NEW;
        WHEN 'custom_import_root_revision','custom_import_child_revision','custom_import_rejection' THEN
            SELECT * INTO p FROM __SCHEMA__.custom_import_pack WHERE pack_id=NEW.pack_id;
        WHEN 'custom_import_build_occurrence' THEN
            o:=NEW; SELECT * INTO p FROM __SCHEMA__.custom_import_pack WHERE pack_id=NEW.pack_id;
        WHEN 'custom_import_build_candidate_context' THEN family_id:=NEW.family_revision_id; child_id:=NEW.context_child_revision_id;
        WHEN 'custom_import_root_scalar' THEN
            SELECT * INTO p FROM __SCHEMA__.custom_import_pack WHERE pack_id=(SELECT pack_id FROM __SCHEMA__.custom_import_root_revision WHERE root_revision_id=NEW.root_revision_id);
        WHEN 'custom_import_child_scalar' THEN
            child_id:=NEW.child_revision_id;
            SELECT * INTO p FROM __SCHEMA__.custom_import_pack WHERE pack_id=(SELECT pack_id FROM __SCHEMA__.custom_import_child_revision WHERE child_revision_id=child_id);
        WHEN 'custom_import_family_revision','custom_import_family_child' THEN family_id:=NEW.family_revision_id;
        WHEN 'custom_import_generation','custom_import_generation_family','custom_import_winner' THEN gen:=NEW.generation_id;
        END CASE;
        IF p.pack_id IS NOT NULL THEN SELECT * INTO b FROM __SCHEMA__.custom_import_build_attempt
            WHERE execution_id=p.execution_id AND producing_fence=p.producing_fence;
        ELSIF gen IS NOT NULL THEN SELECT a.* INTO b FROM __SCHEMA__.custom_import_generation g JOIN __SCHEMA__.custom_import_build_attempt a
            ON a.execution_id=g.execution_id AND a.producing_fence=g.producing_fence WHERE g.generation_id=gen;
        ELSIF family_id IS NOT NULL THEN SELECT a.* INTO b FROM __SCHEMA__.custom_import_family_revision f JOIN __SCHEMA__.custom_import_build_attempt a
            ON a.execution_id=f.producing_execution_id AND a.producing_fence=f.producing_fence WHERE f.family_revision_id=family_id;
        END IF;
        IF b.build_id IS NULL THEN RETURN NEW; END IF;
        IF p.pack_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_build_stream s
            WHERE s.build_id=b.build_id AND s.stream_slot=p.stream_slot AND s.next_pack_ordinal>p.pack_ordinal) THEN
            RAISE EXCEPTION 'custom_import_build_incomplete: uncommitted pack'; END IF;
        IF TG_TABLE_NAME='custom_import_root_revision' THEN
            IF NOT EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_build_occurrence WHERE build_id=b.build_id AND root_revision_id=NEW.root_revision_id)
                THEN RAISE EXCEPTION 'custom_import_build_incomplete'; END IF;
        ELSIF TG_TABLE_NAME='custom_import_child_revision' THEN
            IF NOT EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_build_occurrence WHERE build_id=b.build_id AND child_revision_id=NEW.child_revision_id)
                THEN RAISE EXCEPTION 'custom_import_build_incomplete'; END IF;
        ELSIF TG_TABLE_NAME='custom_import_rejection' THEN
            IF NOT EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_build_occurrence WHERE build_id=b.build_id
                AND (rejection_id=NEW.rejection_id OR resolved_rejection_id=NEW.rejection_id)) THEN RAISE EXCEPTION 'custom_import_build_incomplete'; END IF;
        END IF;
        IF family_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_build_family
            WHERE build_id=b.build_id AND family_revision_id=family_id) THEN RAISE EXCEPTION 'custom_import_build_incomplete'; END IF;
        IF TG_TABLE_NAME='custom_import_build_occurrence' THEN
            IF o.origin='source' AND NOT EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_build_stream s WHERE s.build_id=b.build_id AND s.stream_slot=o.stream_slot
                AND o.source_ordinal<s.next_source_ordinal AND (o.source_part_ordinal<s.next_part_ordinal
                    OR (o.source_part_ordinal=s.next_part_ordinal AND o.part_row_ordinal<s.next_part_row_ordinal))) THEN
                RAISE EXCEPTION 'custom_import_build_incomplete'; END IF;
            IF o.origin='retained' THEN
                SELECT * INTO bf FROM __SCHEMA__.custom_import_build_family WHERE build_id=b.build_id AND root_record_id=o.root_record_id;
                family_id:=bf.family_revision_id; child_id:=o.child_revision_id;
                IF family_id IS NULL OR (o.root_revision_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_family_revision
                    WHERE family_revision_id=family_id AND root_revision_id=o.root_revision_id)) THEN RAISE EXCEPTION 'custom_import_build_incomplete'; END IF;
            END IF;
        END IF;
        IF TG_TABLE_NAME='custom_import_root_scalar' THEN
            IF NOT EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_build_family chosen JOIN __SCHEMA__.custom_import_family_revision f ON f.family_revision_id=chosen.family_revision_id
                WHERE chosen.build_id=b.build_id AND f.root_revision_id=NEW.root_revision_id) THEN RAISE EXCEPTION 'custom_import_build_incomplete'; END IF;
        END IF;
        IF TG_TABLE_NAME='custom_import_family_child' THEN child_id:=NEW.child_revision_id; END IF;
        IF child_id IS NOT NULL THEN
            SELECT * INTO o FROM __SCHEMA__.custom_import_build_occurrence WHERE build_id=b.build_id AND child_revision_id=child_id;
            SELECT * INTO bf FROM __SCHEMA__.custom_import_build_family WHERE build_id=b.build_id AND root_record_id=o.root_record_id;
            IF o.occurrence_id IS NULL OR o.origin IS DISTINCT FROM bf.selection_kind OR bf.last_child_collection_slot IS NULL
                OR (family_id IS NOT NULL AND bf.family_revision_id IS DISTINCT FROM family_id)
                OR NOT EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_family_child WHERE family_revision_id=bf.family_revision_id
                    AND collection_slot=o.collection_slot AND child_revision_id=child_id) THEN RAISE EXCEPTION 'custom_import_build_incomplete'; END IF;
            IF o.origin='retained' THEN
                IF (o.collection_slot,o.base_child_revision_id)>(bf.last_child_collection_slot,bf.last_input_child_revision_id) THEN
                    RAISE EXCEPTION 'custom_import_build_incomplete'; END IF;
            ELSE
                SELECT collection_name INTO current_name FROM __SCHEMA__.custom_import_child_collection WHERE schema_revision_id=b.schema_revision_id AND collection_slot=o.collection_slot;
                SELECT collection_name INTO after_name FROM __SCHEMA__.custom_import_child_collection WHERE schema_revision_id=b.schema_revision_id AND collection_slot=bf.last_child_collection_slot;
                IF (current_name COLLATE "C",o.child_key_sha256,o.child_revision_id)>(after_name COLLATE "C",bf.last_child_key_sha256,bf.last_input_child_revision_id) THEN
                    RAISE EXCEPTION 'custom_import_build_incomplete'; END IF;
            END IF;
        END IF;
        IF gen IS NOT NULL AND b.generation_id IS DISTINCT FROM gen THEN RAISE EXCEPTION 'custom_import_build_incomplete'; END IF;
        IF TG_TABLE_NAME='custom_import_winner' THEN
            IF b.output_after_profile_slot IS NULL OR (NEW.profile_slot,NEW.entity_binding_id,NEW.context_key_sha256)>
                (b.output_after_profile_slot,b.output_after_entity_binding_id,b.output_after_context_key_sha256) THEN RAISE EXCEPTION 'custom_import_build_incomplete'; END IF;
        END IF;
        RETURN NEW;
    END;
    """

_NEXT_CUSTOM_IMPORT_BUILD_CHILD_BODY = r"""
    DECLARE b __SCHEMA__.custom_import_build_attempt; bf __SCHEMA__.custom_import_build_family; c record; prior_name text;
    BEGIN
        SELECT * INTO b FROM __SCHEMA__.custom_import_build_attempt WHERE build_id=p_build_id;
        SELECT * INTO bf FROM __SCHEMA__.custom_import_build_family WHERE build_id=p_build_id AND root_record_id=p_root_record_id;
        IF bf.selection_kind='retained' THEN
            RETURN QUERY SELECT fc.collection_slot,NULL::bytea,fc.child_revision_id,NULL::bigint,0::bigint
                FROM __SCHEMA__.custom_import_family_child fc WHERE fc.family_revision_id=bf.base_family_revision_id
                AND (p_slot IS NULL OR (fc.collection_slot,fc.child_revision_id)>(p_slot,p_revision_id))
                ORDER BY fc.collection_slot,fc.child_revision_id LIMIT 1;
            RETURN;
        END IF;
        SELECT collection_name INTO prior_name FROM __SCHEMA__.custom_import_child_collection
            WHERE schema_revision_id=b.schema_revision_id AND custom_import_child_collection.collection_slot=p_slot;
        FOR c IN SELECT cc.collection_slot,cc.collection_name FROM __SCHEMA__.custom_import_child_collection cc
            WHERE cc.schema_revision_id=b.schema_revision_id AND (prior_name IS NULL OR cc.collection_name COLLATE "C">=prior_name COLLATE "C")
            ORDER BY cc.collection_name COLLATE "C"
        LOOP
            RETURN QUERY SELECT o.collection_slot,o.child_key_sha256,o.child_revision_id,o.pack_id,
                coalesce(octet_length(o.raw_parent_key_canonical),0)::bigint FROM __SCHEMA__.custom_import_build_occurrence o
                WHERE o.build_id=p_build_id AND o.origin='source' AND o.root_record_id=p_root_record_id
                AND o.collection_slot=c.collection_slot AND o.child_revision_id IS NOT NULL
                AND (prior_name IS NULL OR c.collection_name COLLATE "C">prior_name COLLATE "C" OR (o.child_key_sha256,o.child_revision_id)>(p_key,p_revision_id))
                ORDER BY o.child_key_sha256,o.child_revision_id LIMIT 1;
            IF FOUND THEN RETURN; END IF;
        END LOOP;
    END;
    """

_CHECK_CUSTOM_IMPORT_BUILD_RECORD_WORK_BODY = r"""
    DECLARE b __SCHEMA__.custom_import_build_attempt; f __SCHEMA__.custom_import_family_revision; payload jsonb; slot smallint;
        rev_id bigint; scalar_n bigint; scalar_bytes bigint; context_n bigint; context_bytes bigint; payload_bytes bigint; key_bytes bigint;
    BEGIN
        SELECT * INTO b FROM __SCHEMA__.custom_import_build_attempt WHERE build_id=p_build_id;
        SELECT * INTO f FROM __SCHEMA__.custom_import_family_revision WHERE family_revision_id=p_family_id;
        IF num_nonnulls(p_root_revision_id,p_child_revision_id)<>1 THEN RAISE EXCEPTION 'custom_import_build_identity_mismatch'; END IF;
        IF p_root_revision_id IS NOT NULL THEN
            IF f.root_revision_id IS DISTINCT FROM p_root_revision_id THEN RAISE EXCEPTION 'custom_import_build_identity_mismatch'; END IF;
            SELECT r.canonical_payload::jsonb,octet_length(r.canonical_payload),octet_length(k.canonical_logical_key)
                INTO payload,payload_bytes,key_bytes FROM __SCHEMA__.custom_import_root_revision r JOIN __SCHEMA__.custom_import_root_record k
                ON k.root_record_id=r.root_record_id WHERE r.root_revision_id=p_root_revision_id;
            slot:=0; rev_id:=p_root_revision_id;
            SELECT count(*),coalesce(sum(octet_length(to_jsonb(s)::text)),0) INTO scalar_n,scalar_bytes
                FROM __SCHEMA__.custom_import_root_scalar s WHERE s.root_revision_id=rev_id;
        ELSE
            SELECT c.canonical_payload::jsonb,octet_length(c.canonical_payload),c.collection_slot,
                octet_length(c.canonical_parent_key)+octet_length(c.canonical_child_key)+octet_length(k.canonical_logical_key)
                INTO payload,payload_bytes,slot,key_bytes FROM __SCHEMA__.custom_import_child_revision c JOIN __SCHEMA__.custom_import_root_record k
                ON k.root_record_id=c.root_record_id WHERE c.child_revision_id=p_child_revision_id;
            rev_id:=p_child_revision_id;
            IF NOT EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_family_child WHERE family_revision_id=f.family_revision_id
                AND collection_slot=slot AND child_revision_id=rev_id) THEN RAISE EXCEPTION 'custom_import_build_structure_mismatch'; END IF;
            SELECT count(*),coalesce(sum(octet_length(to_jsonb(s)::text)),0) INTO scalar_n,scalar_bytes
                FROM __SCHEMA__.custom_import_child_scalar s WHERE s.child_revision_id=rev_id;
        END IF;
        IF EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_field field WHERE field.schema_revision_id=b.schema_revision_id
            AND field.collection_slot=slot AND field.projection_slot>0 AND EXISTS(
                SELECT 1 FROM jsonb_array_elements(payload->'fields') e WHERE e->>'field'=field.field_name AND e->'value'->>'state'<>'missing')
            AND NOT EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_root_scalar s WHERE p_root_revision_id IS NOT NULL
                AND s.root_revision_id=rev_id AND s.field_slot=field.field_slot
                UNION ALL SELECT 1 FROM __SCHEMA__.custom_import_child_scalar s WHERE p_child_revision_id IS NOT NULL
                AND s.child_revision_id=rev_id AND s.field_slot=field.field_slot)) THEN RAISE EXCEPTION 'custom_import_build_incomplete'; END IF;
        IF EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_selection_profile profile WHERE profile.definition_revision_id=b.definition_revision_id
            AND coalesce(profile.context_collection_slot,0)=slot AND NOT EXISTS(
                SELECT 1 FROM __SCHEMA__.custom_import_build_candidate_context c WHERE c.build_id=b.build_id
                AND c.family_revision_id=f.family_revision_id AND c.profile_slot=profile.profile_slot
                AND c.context_child_revision_id IS NOT DISTINCT FROM p_child_revision_id)) THEN
            RAISE EXCEPTION 'custom_import_build_incomplete'; END IF;
        SELECT count(*),coalesce(sum(octet_length(c.canonical_context_key)),0) INTO context_n,context_bytes
            FROM __SCHEMA__.custom_import_build_candidate_context c WHERE c.build_id=b.build_id
            AND c.family_revision_id=f.family_revision_id AND c.context_child_revision_id IS NOT DISTINCT FROM p_child_revision_id;
        RETURN QUERY SELECT 3+scalar_n+context_n,coalesce(payload_bytes,0)+coalesce(key_bytes,0)+scalar_bytes+context_bytes;
    END;
    """

_COMMIT_CUSTOM_IMPORT_BUILD_COPY_PACK_BODY = r"""
    DECLARE b __SCHEMA__.custom_import_build_attempt; p __SCHEMA__.custom_import_pack; s __SCHEMA__.custom_import_build_stream;
        n bigint; hashes bytea; label text; first_position bigint; last_position bigint; positions bigint;
    BEGIN
        SELECT * INTO b FROM __SCHEMA__.custom_import_build_attempt WHERE build_id=p_build_id;
        SELECT * INTO p FROM __SCHEMA__.custom_import_pack WHERE pack_id=p_pack_id;
        SELECT * INTO s FROM __SCHEMA__.custom_import_build_stream WHERE build_id=b.build_id AND stream_slot=p.stream_slot FOR UPDATE;
        IF p.pack_ordinal<s.next_pack_ordinal THEN RETURN; END IF;
        IF p.pack_ordinal<>s.next_pack_ordinal THEN RAISE EXCEPTION 'custom_import_build_source_gap'; END IF;
        SELECT count(*),string_agg(coalesce(r.payload_sha256,c.payload_sha256),''::bytea ORDER BY coalesce(r.payload_sha256,c.payload_sha256)),
            min(coalesce(r.source_ordinal,c.source_ordinal)),max(coalesce(r.source_ordinal,c.source_ordinal)),count(DISTINCT coalesce(r.source_ordinal,c.source_ordinal))
            INTO n,hashes,first_position,last_position,positions FROM (SELECT * FROM __SCHEMA__.custom_import_build_occurrence WHERE pack_id=p_pack_id LIMIT b.page_row_limit+1) x
            LEFT JOIN __SCHEMA__.custom_import_root_revision r ON r.root_revision_id=x.root_revision_id
            LEFT JOIN __SCHEMA__.custom_import_child_revision c ON c.child_revision_id=x.child_revision_id;
        IF n>b.page_row_limit THEN RAISE EXCEPTION 'custom_import_build_page_too_large'; END IF;
        SELECT CASE WHEN source_row.record_kind='root' THEN 'root' ELSE c.collection_name END INTO label
            FROM __SCHEMA__.custom_import_source_stream source_row LEFT JOIN __SCHEMA__.custom_import_child_collection c
            ON c.schema_revision_id=source_row.schema_revision_id AND c.collection_slot=source_row.collection_slot
            WHERE source_row.definition_revision_id=b.definition_revision_id AND source_row.stream_slot=p.stream_slot;
        IF n<>p.record_count OR n=0 OR first_position<>0 OR last_position<>n-1 OR positions<>n OR p.pack_sha256 IS DISTINCT FROM sha256(
            decode('637573746f6d2d696d706f72742f76310063616e6469646174652d72756e6e65722f31007061636b00','hex')
            ||convert_to(label,'UTF8')||decode('00','hex')||coalesce(hashes,''::bytea)) THEN
            RAISE EXCEPTION 'custom_import_build_structure_mismatch'; END IF;
        UPDATE __SCHEMA__.custom_import_build_stream SET next_pack_ordinal=next_pack_ordinal+1 WHERE build_id=b.build_id AND stream_slot=p.stream_slot;
    END;
    """

_COMMIT_CUSTOM_IMPORT_BUILD_FAMILY_PAGE_BODY = r"""
    DECLARE b __SCHEMA__.custom_import_build_attempt; bf __SCHEMA__.custom_import_build_family; f __SCHEMA__.custom_import_family_revision;
        root_o __SCHEMA__.custom_import_build_occurrence; input record; work record; output_child bigint; output_pack bigint;
        row_n bigint:=0; byte_n bigint:=0; n bigint:=0; exhausted boolean:=true; current_name text; page_packs bigint[]:='{}';
    BEGIN
        b:=__SCHEMA__.lock_custom_import_build(p_build_id);
        IF b.phase<>'graph' THEN RAISE EXCEPTION 'custom_import_build_phase_mismatch'; END IF;
        SELECT * INTO bf FROM __SCHEMA__.custom_import_build_family WHERE build_id=b.build_id AND root_record_id=p_root_record_id FOR UPDATE;
        SELECT * INTO f FROM __SCHEMA__.custom_import_family_revision WHERE family_revision_id=p_family_revision_id;
        IF bf.build_id IS NULL OR ROW(f.dataset_id,f.schema_revision_id,f.root_record_id,f.producing_execution_id,f.producing_fence,f.producing_token_sha256)
            IS DISTINCT FROM ROW(b.dataset_id,b.schema_revision_id,p_root_record_id,b.execution_id,b.producing_fence,b.producing_token_sha256)
            OR (bf.family_revision_id IS NOT NULL AND bf.family_revision_id<>p_family_revision_id) THEN
            RAISE EXCEPTION 'custom_import_build_identity_mismatch'; END IF;
        IF bf.complete_at IS NOT NULL THEN
            RETURN QUERY SELECT bf.attached_child_count,true,bf.last_child_collection_slot,bf.last_child_key_sha256,bf.last_input_child_revision_id; RETURN;
        END IF;
        IF bf.family_revision_id IS NULL THEN
            SELECT * INTO root_o FROM __SCHEMA__.custom_import_build_occurrence WHERE build_id=b.build_id AND root_revision_id=f.root_revision_id;
            IF (bf.selection_kind='source' AND root_o.occurrence_id IS DISTINCT FROM bf.source_root_occurrence_id)
                OR (bf.selection_kind='retained' AND (root_o.origin IS DISTINCT FROM 'retained'
                    OR root_o.base_family_revision_id IS DISTINCT FROM bf.base_family_revision_id)) THEN
                RAISE EXCEPTION 'custom_import_build_identity_mismatch'; END IF;
            SELECT * INTO work FROM __SCHEMA__.check_custom_import_build_record_work(b.build_id,f.family_revision_id,f.root_revision_id,NULL);
            row_n:=work.work_rows; byte_n:=work.work_bytes+coalesce(octet_length(root_o.raw_parent_key_canonical),0);
            IF bf.selection_kind='retained' THEN page_packs:=array_append(page_packs,root_o.pack_id); row_n:=row_n+1; END IF;
        END IF;
        IF bf.last_child_collection_slot IS NOT NULL THEN SELECT collection_name INTO current_name FROM __SCHEMA__.custom_import_child_collection
            WHERE schema_revision_id=b.schema_revision_id AND collection_slot=bf.last_child_collection_slot; END IF;
        LOOP
            SELECT * INTO input FROM __SCHEMA__.next_custom_import_build_child(b.build_id,bf.root_record_id,
                bf.last_child_collection_slot,bf.last_child_key_sha256,bf.last_input_child_revision_id);
            EXIT WHEN input.child_revision_id IS NULL;
            IF bf.selection_kind='source' THEN output_child:=input.child_revision_id; output_pack:=input.pack_id;
            ELSE SELECT child_revision_id,pack_id INTO output_child,output_pack FROM __SCHEMA__.custom_import_build_occurrence
                WHERE build_id=b.build_id AND base_family_revision_id=bf.base_family_revision_id AND base_child_revision_id=input.child_revision_id; END IF;
            IF output_child IS NULL OR NOT EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_family_child
                WHERE family_revision_id=f.family_revision_id AND collection_slot=input.collection_slot AND child_revision_id=output_child) THEN
                exhausted:=false; EXIT; END IF;
            SELECT * INTO work FROM __SCHEMA__.check_custom_import_build_record_work(b.build_id,f.family_revision_id,NULL,output_child);
            row_n:=row_n+work.work_rows; byte_n:=byte_n+work.work_bytes+input.raw_bytes; n:=n+1;
            IF row_n>b.page_row_limit OR byte_n>b.page_byte_limit THEN RAISE EXCEPTION 'custom_import_build_page_too_large'; END IF;
            bf.last_child_collection_slot:=input.collection_slot; bf.last_child_key_sha256:=input.child_key_sha256;
            bf.last_input_child_revision_id:=input.child_revision_id;
            IF bf.selection_kind='retained' AND NOT output_pack=ANY(page_packs) THEN
                page_packs:=array_append(page_packs,output_pack); row_n:=row_n+1; END IF;
        END LOOP;
        IF row_n>b.page_row_limit OR byte_n>b.page_byte_limit THEN RAISE EXCEPTION 'custom_import_build_page_too_large'; END IF;
        IF exhausted AND bf.attached_child_count+n<>f.child_count THEN RAISE EXCEPTION 'custom_import_build_structure_mismatch'; END IF;
        FOR output_pack IN SELECT DISTINCT unnest(page_packs) ORDER BY 1 LOOP
            PERFORM __SCHEMA__.commit_custom_import_build_copy_pack(b.build_id,output_pack);
        END LOOP;
        UPDATE __SCHEMA__.custom_import_build_family SET family_revision_id=f.family_revision_id,attached_child_count=bf.attached_child_count+n,
            last_child_collection_slot=bf.last_child_collection_slot,last_child_key_sha256=bf.last_child_key_sha256,
            last_input_child_revision_id=bf.last_input_child_revision_id,complete_at=CASE WHEN exhausted THEN clock_timestamp() ELSE NULL END
            WHERE build_id=b.build_id AND root_record_id=bf.root_record_id RETURNING * INTO bf;
        IF exhausted THEN UPDATE __SCHEMA__.custom_import_build_attempt SET completed_family_count=completed_family_count+1 WHERE build_id=b.build_id; END IF;
        RETURN QUERY SELECT bf.attached_child_count,bf.complete_at IS NOT NULL,bf.last_child_collection_slot,bf.last_child_key_sha256,bf.last_input_child_revision_id;
    END;
    """

_OPEN_CUSTOM_IMPORT_BUILD_OUTPUT_BODY = r"""
    DECLARE b __SCHEMA__.custom_import_build_attempt; g __SCHEMA__.custom_import_generation;
    BEGIN
        b:=__SCHEMA__.lock_custom_import_build(p_build_id);
        IF b.phase='output' AND b.generation_id=p_generation_id THEN RETURN b.phase; END IF;
        IF b.phase<>'graph' THEN RAISE EXCEPTION 'custom_import_build_phase_mismatch'; END IF;
        IF b.plan_complete_at IS NULL OR b.completed_family_count<>b.selected_family_count THEN RAISE EXCEPTION 'custom_import_build_incomplete'; END IF;
        SELECT * INTO g FROM __SCHEMA__.custom_import_generation WHERE generation_id=p_generation_id FOR UPDATE;
        IF ROW(g.dataset_id,g.definition_revision_id,g.schema_revision_id,g.execution_id,g.capture_bundle_id,g.producing_fence,g.producing_token_sha256,
            g.base_generation_id,g.root_count,g.family_count) IS DISTINCT FROM ROW(b.dataset_id,b.definition_revision_id,b.schema_revision_id,b.execution_id,
            b.capture_bundle_id,b.producing_fence,b.producing_token_sha256,b.base_generation_id,b.selected_family_count,b.selected_family_count) THEN
            RAISE EXCEPTION 'custom_import_build_identity_mismatch'; END IF;
        UPDATE __SCHEMA__.custom_import_build_attempt SET generation_id=g.generation_id,graph_frozen_at=clock_timestamp(),phase='output'
            WHERE build_id=b.build_id;
        RETURN 'output';
    END;
    """

_COMMIT_CUSTOM_IMPORT_BUILD_WINNER_GROUP_BODY = r"""
    DECLARE b __SCHEMA__.custom_import_build_attempt; c __SCHEMA__.custom_import_build_candidate_context;
        next_c __SCHEMA__.custom_import_build_candidate_context; w __SCHEMA__.custom_import_winner;
    BEGIN
        b:=__SCHEMA__.lock_custom_import_build(p_build_id);
        IF b.phase<>'output' THEN RAISE EXCEPTION 'custom_import_build_phase_mismatch'; END IF;
        SELECT * INTO c FROM __SCHEMA__.custom_import_build_candidate_context WHERE candidate_context_id=p_candidate_context_id AND build_id=b.build_id;
        IF c.candidate_context_id IS NULL THEN RAISE EXCEPTION 'custom_import_build_identity_mismatch'; END IF;
        SELECT * INTO w FROM __SCHEMA__.custom_import_winner WHERE generation_id=b.generation_id AND custom_import_winner.profile_slot=c.profile_slot
            AND custom_import_winner.entity_binding_id=c.entity_binding_id AND custom_import_winner.context_key_sha256=c.context_key_sha256;
        IF ROW(w.family_revision_id,w.context_collection_slot,w.context_child_revision_id) IS DISTINCT FROM
            ROW(c.family_revision_id,c.context_collection_slot,c.context_child_revision_id) THEN
            RAISE EXCEPTION 'custom_import_build_structure_mismatch'; END IF;
        IF b.output_after_profile_slot IS NOT NULL AND (c.profile_slot,c.entity_binding_id,c.context_key_sha256)<=
            (b.output_after_profile_slot,b.output_after_entity_binding_id,b.output_after_context_key_sha256) THEN
            RETURN QUERY SELECT c.profile_slot,c.entity_binding_id,c.context_key_sha256,b.winner_count; RETURN; END IF;
        SELECT * INTO next_c FROM __SCHEMA__.custom_import_build_candidate_context x WHERE x.build_id=b.build_id
            AND (b.output_after_profile_slot IS NULL OR (x.profile_slot,x.entity_binding_id,x.context_key_sha256)>
                (b.output_after_profile_slot,b.output_after_entity_binding_id,b.output_after_context_key_sha256))
            ORDER BY x.profile_slot,x.entity_binding_id,x.context_key_sha256,x.candidate_context_id LIMIT 1;
        IF ROW(c.profile_slot,c.entity_binding_id,c.context_key_sha256,c.canonical_context_key) IS DISTINCT FROM
            ROW(next_c.profile_slot,next_c.entity_binding_id,next_c.context_key_sha256,next_c.canonical_context_key) THEN
            RAISE EXCEPTION 'custom_import_build_progress_conflict' USING ERRCODE='40001'; END IF;
        UPDATE __SCHEMA__.custom_import_build_attempt SET output_after_profile_slot=c.profile_slot,
            output_after_entity_binding_id=c.entity_binding_id,output_after_context_key_sha256=c.context_key_sha256 WHERE build_id=b.build_id;
        RETURN QUERY SELECT c.profile_slot,c.entity_binding_id,c.context_key_sha256,b.winner_count;
    END;
    """

_FREEZE_CUSTOM_IMPORT_BUILD_OUTPUT_BODY = r"""
    DECLARE b __SCHEMA__.custom_import_build_attempt;
    BEGIN
        b:=__SCHEMA__.lock_custom_import_build(p_build_id);
        IF b.phase='verifying' THEN RETURN b.phase; END IF;
        IF b.phase<>'output' THEN RAISE EXCEPTION 'custom_import_build_phase_mismatch'; END IF;
        IF b.selected_family_count<>b.generation_family_count OR EXISTS(
            SELECT 1 FROM __SCHEMA__.custom_import_build_candidate_context x WHERE x.build_id=b.build_id
            AND (b.output_after_profile_slot IS NULL OR (x.profile_slot,x.entity_binding_id,x.context_key_sha256)>
                (b.output_after_profile_slot,b.output_after_entity_binding_id,b.output_after_context_key_sha256))) THEN
            RAISE EXCEPTION 'custom_import_build_incomplete'; END IF;
        UPDATE __SCHEMA__.custom_import_build_attempt SET phase='verifying',output_frozen_at=clock_timestamp() WHERE build_id=b.build_id;
        RETURN 'verifying';
    END;
    """

_VERIFY_CUSTOM_IMPORT_BUILD_STRUCTURE_BODY = r"""
    DECLARE b __SCHEMA__.custom_import_build_attempt; v __SCHEMA__.custom_import_build_verification; seq bigint;
        f __SCHEMA__.custom_import_family_revision; r record; n integer:=0; bytes bigint:=0; count_n bigint; deadline timestamptz;
    BEGIN
        SELECT * INTO b FROM __SCHEMA__.custom_import_build_attempt WHERE build_id=p_build_id;
        IF b.phase NOT IN ('verifying','verified') THEN RAISE EXCEPTION 'custom_import_build_phase_mismatch'; END IF;
        SELECT * INTO v FROM __SCHEMA__.custom_import_build_verification WHERE build_id=p_build_id;
        IF v.verification_state='complete' THEN
            PERFORM __SCHEMA__.lock_custom_import_build(p_build_id);
            RETURN QUERY SELECT v.verification_state::text,v.scan_stage::text,v.page_sequence,0; RETURN; END IF;
        IF v.build_id IS NULL THEN
            b:=__SCHEMA__.lock_custom_import_build(p_build_id);
            INSERT INTO __SCHEMA__.custom_import_build_verification(build_id,generation_id,source_frozen_at,graph_frozen_at,output_frozen_at)
                VALUES(b.build_id,b.generation_id,b.source_frozen_at,b.graph_frozen_at,b.output_frozen_at)
                ON CONFLICT DO NOTHING RETURNING * INTO v;
            IF v.build_id IS NULL THEN RAISE EXCEPTION 'custom_import_build_progress_conflict' USING ERRCODE='40001'; END IF;
            RETURN QUERY SELECT v.verification_state::text,v.scan_stage::text,v.page_sequence,0; RETURN;
        END IF;
        -- Frozen rows are read before acquiring any dataset, execution or lease lock.
        SELECT least(l.expires_at,b.build_deadline_at) INTO deadline FROM __SCHEMA__.custom_import_lease l
            JOIN __SCHEMA__.custom_import_execution e ON e.execution_id=l.execution_id WHERE l.execution_id=b.execution_id
            AND e.state='running' AND l.fence=b.producing_fence AND l.token_sha256=b.producing_token_sha256;
        IF deadline IS NULL OR deadline<=clock_timestamp() THEN RAISE EXCEPTION 'custom_import_build_lease_lost'; END IF;
        IF current_setting('transaction_isolation')<>'read committed' OR current_setting('statement_timeout')::interval<=interval '0'
            OR current_setting('statement_timeout')::interval>make_interval(secs=>b.statement_timeout_ms/1000.0)
            OR current_setting('statement_timeout')::interval>=deadline-clock_timestamp() THEN RAISE EXCEPTION 'custom_import_build_invalid_bounds'; END IF;
        seq:=v.page_sequence;
        CASE v.scan_stage
        WHEN 'capture' THEN
            FOR r IN SELECT c.stream_slot,c.capture_state,s.stream_slot declared_slot,bs.replay_verified_at,
                c.committed_record_count AS record_count,bs.next_source_ordinal FROM __SCHEMA__.custom_import_capture c
                LEFT JOIN __SCHEMA__.custom_import_source_stream s ON s.definition_revision_id=b.definition_revision_id AND s.stream_slot=c.stream_slot
                LEFT JOIN __SCHEMA__.custom_import_build_stream bs ON bs.build_id=b.build_id AND bs.stream_slot=c.stream_slot
                WHERE c.capture_bundle_id=b.capture_bundle_id AND c.stream_slot>coalesce(v.after_capture_stream_slot,0)
                ORDER BY c.stream_slot LIMIT b.page_row_limit
            LOOP
                IF r.declared_slot IS NULL OR r.capture_state<>'sealed' OR r.replay_verified_at IS NULL
                    OR r.record_count<>r.next_source_ordinal THEN RAISE EXCEPTION 'custom_import_build_structure_mismatch'; END IF;
                v.after_capture_stream_slot:=r.stream_slot; n:=n+1;
            END LOOP;
            IF n=0 THEN
                SELECT count(*) INTO count_n FROM __SCHEMA__.custom_import_source_stream WHERE definition_revision_id=b.definition_revision_id;
                IF count_n<>(SELECT stream_count FROM __SCHEMA__.custom_import_capture_bundle WHERE capture_bundle_id=b.capture_bundle_id)
                    THEN RAISE EXCEPTION 'custom_import_build_structure_mismatch'; END IF;
                v.scan_stage:='profiles';
            END IF;
        WHEN 'profiles' THEN
            FOR r IN SELECT profile_slot FROM __SCHEMA__.custom_import_selection_profile WHERE definition_revision_id=b.definition_revision_id
                AND profile_slot>coalesce(v.after_profile_slot,0) ORDER BY profile_slot LIMIT b.page_row_limit
            LOOP v.profile_count:=v.profile_count+1; v.after_profile_slot:=r.profile_slot; n:=n+1; END LOOP;
            IF n=0 THEN v.scan_stage:='families'; END IF;
        WHEN 'families' THEN
            IF v.current_family_revision_id IS NULL THEN
                SELECT g.root_record_id,g.family_revision_id,bf.complete_at,family_row.child_count INTO r
                    FROM __SCHEMA__.custom_import_generation_family g
                    JOIN __SCHEMA__.custom_import_family_revision family_row ON family_row.family_revision_id=g.family_revision_id
                    LEFT JOIN __SCHEMA__.custom_import_build_family bf ON bf.build_id=b.build_id AND bf.root_record_id=g.root_record_id
                        AND bf.family_revision_id=g.family_revision_id
                    WHERE g.generation_id=b.generation_id AND g.root_record_id>coalesce(v.after_root_record_id,0)
                    ORDER BY g.root_record_id LIMIT 1;
                IF r.root_record_id IS NULL THEN v.scan_stage:='winners';
                ELSE
                    IF r.complete_at IS NULL THEN RAISE EXCEPTION 'custom_import_build_structure_mismatch'; END IF;
                    v.after_root_record_id:=r.root_record_id; v.current_family_revision_id:=r.family_revision_id;
                    v.current_family_expected_child_count:=r.child_count; v.current_family_seen_child_count:=0;
                    v.root_count:=v.root_count+1; v.family_count:=v.family_count+1; v.generation_family_count:=v.generation_family_count+1;
                    v.after_child_collection_slot:=NULL; v.after_child_revision_id:=NULL;
                    v.after_root_scalar_revision_id:=NULL; v.after_root_scalar_field_slot:=NULL; v.scan_stage:='root_scalars'; n:=1;
                END IF;
            ELSE
                SELECT fc.collection_slot,fc.child_revision_id,octet_length(c.canonical_parent_key)+octet_length(k.canonical_logical_key) bytes,
                    c.parent_key_sha256,k.logical_key_sha256,c.canonical_parent_key=k.canonical_logical_key parent_equal,c.child_key_sha256
                    INTO r FROM __SCHEMA__.custom_import_family_child fc JOIN __SCHEMA__.custom_import_child_revision c
                    ON c.child_revision_id=fc.child_revision_id JOIN __SCHEMA__.custom_import_root_record k ON k.root_record_id=fc.root_record_id
                    WHERE fc.family_revision_id=v.current_family_revision_id AND (v.after_child_collection_slot IS NULL
                        OR (fc.collection_slot,fc.child_revision_id)>(v.after_child_collection_slot,v.after_child_revision_id))
                    ORDER BY fc.collection_slot,fc.child_revision_id LIMIT 1;
                IF r.child_revision_id IS NULL THEN
                    IF v.current_family_seen_child_count<>v.current_family_expected_child_count THEN RAISE EXCEPTION 'custom_import_build_structure_mismatch'; END IF;
                    v.current_family_revision_id:=NULL; v.current_family_expected_child_count:=NULL; v.current_family_seen_child_count:=NULL;
                ELSE
                    IF r.bytes>b.page_byte_limit THEN RAISE EXCEPTION 'custom_import_build_page_too_large'; END IF;
                    IF NOT r.parent_equal OR r.parent_key_sha256<>r.logical_key_sha256 THEN RAISE EXCEPTION 'custom_import_build_structure_mismatch'; END IF;
                    IF EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_build_occurrence o
                        JOIN __SCHEMA__.custom_import_family_child actual ON actual.child_revision_id=o.child_revision_id
                          AND actual.family_revision_id=v.current_family_revision_id AND actual.collection_slot=r.collection_slot
                        WHERE o.build_id=b.build_id AND o.origin=(SELECT selection_kind FROM __SCHEMA__.custom_import_build_family
                            WHERE build_id=b.build_id AND root_record_id=v.after_root_record_id)
                          AND o.root_record_id=v.after_root_record_id AND o.collection_slot=r.collection_slot
                          AND o.child_key_sha256=r.child_key_sha256 AND o.child_revision_id<>r.child_revision_id) THEN
                        RAISE EXCEPTION 'custom_import_build_structure_mismatch: duplicate selected child'; END IF;
                    v.after_child_collection_slot:=r.collection_slot; v.after_child_revision_id:=r.child_revision_id;
                    v.current_family_seen_child_count:=v.current_family_seen_child_count+1; v.family_child_count:=v.family_child_count+1;
                    v.after_child_scalar_revision_id:=NULL; v.after_child_scalar_field_slot:=NULL; v.scan_stage:='child_scalars'; n:=1;
                END IF;
            END IF;
        WHEN 'root_scalars' THEN
            SELECT * INTO f FROM __SCHEMA__.custom_import_family_revision WHERE family_revision_id=v.current_family_revision_id;
            FOR r IN SELECT s.field_slot,octet_length(to_jsonb(s)::text) bytes FROM __SCHEMA__.custom_import_root_scalar s
                WHERE s.root_revision_id=f.root_revision_id AND s.field_slot>coalesce(v.after_root_scalar_field_slot,0)
                ORDER BY s.field_slot LIMIT b.page_row_limit
            LOOP
                IF r.bytes>b.page_byte_limit THEN RAISE EXCEPTION 'custom_import_build_page_too_large'; END IF;
                EXIT WHEN n>0 AND bytes+r.bytes>b.page_byte_limit;
                n:=n+1; bytes:=bytes+r.bytes; v.root_scalar_count:=v.root_scalar_count+1;
                v.after_root_scalar_revision_id:=f.root_revision_id; v.after_root_scalar_field_slot:=r.field_slot;
            END LOOP;
            IF n=0 THEN v.scan_stage:='families'; END IF;
        WHEN 'child_scalars' THEN
            FOR r IN SELECT s.field_slot,octet_length(to_jsonb(s)::text) bytes FROM __SCHEMA__.custom_import_child_scalar s
                WHERE s.child_revision_id=v.after_child_revision_id AND s.field_slot>coalesce(v.after_child_scalar_field_slot,0)
                ORDER BY s.field_slot LIMIT b.page_row_limit
            LOOP
                IF r.bytes>b.page_byte_limit THEN RAISE EXCEPTION 'custom_import_build_page_too_large'; END IF;
                EXIT WHEN n>0 AND bytes+r.bytes>b.page_byte_limit;
                n:=n+1; bytes:=bytes+r.bytes; v.child_scalar_count:=v.child_scalar_count+1;
                v.after_child_scalar_revision_id:=v.after_child_revision_id; v.after_child_scalar_field_slot:=r.field_slot;
            END LOOP;
            IF n=0 THEN v.scan_stage:='families'; END IF;
        WHEN 'winners' THEN
            FOR r IN SELECT w.* FROM __SCHEMA__.custom_import_winner w WHERE w.generation_id=b.generation_id
                AND (v.after_winner_profile_slot IS NULL OR (w.profile_slot,w.entity_binding_id,w.context_key_sha256)>
                    (v.after_winner_profile_slot,v.after_winner_entity_binding_id,v.after_winner_context_key_sha256))
                ORDER BY w.profile_slot,w.entity_binding_id,w.context_key_sha256 LIMIT b.page_row_limit
            LOOP
                IF NOT EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_build_candidate_context c WHERE c.build_id=b.build_id
                    AND c.profile_slot=r.profile_slot AND c.entity_binding_id=r.entity_binding_id AND c.context_key_sha256=r.context_key_sha256
                    AND c.family_revision_id=r.family_revision_id AND c.context_child_revision_id IS NOT DISTINCT FROM r.context_child_revision_id)
                    THEN RAISE EXCEPTION 'custom_import_build_structure_mismatch'; END IF;
                v.winner_count:=v.winner_count+1; n:=n+1; v.after_winner_profile_slot:=r.profile_slot;
                v.after_winner_entity_binding_id:=r.entity_binding_id; v.after_winner_context_key_sha256:=r.context_key_sha256;
            END LOOP;
            IF n=0 THEN
                IF v.family_count<>b.selected_family_count OR v.generation_family_count<>b.generation_family_count OR v.winner_count<>b.winner_count THEN
                    RAISE EXCEPTION 'custom_import_build_structure_mismatch'; END IF;
                v.verification_state:='complete'; v.scan_stage:='complete'; v.verified_at:=clock_timestamp();
            END IF;
        ELSE RAISE EXCEPTION 'custom_import_build_structure_mismatch'; END CASE;
        b:=__SCHEMA__.lock_custom_import_build(p_build_id);
        PERFORM 1 FROM __SCHEMA__.custom_import_build_verification WHERE build_id=b.build_id AND custom_import_build_verification.page_sequence=seq FOR UPDATE;
        IF NOT FOUND THEN RAISE EXCEPTION 'custom_import_build_progress_conflict' USING ERRCODE='40001'; END IF;
        UPDATE __SCHEMA__.custom_import_build_verification SET verification_state=v.verification_state,scan_stage=v.scan_stage,page_sequence=seq+1,
            after_capture_stream_slot=v.after_capture_stream_slot,after_profile_slot=v.after_profile_slot,after_root_record_id=v.after_root_record_id,
            current_family_revision_id=v.current_family_revision_id,current_family_expected_child_count=v.current_family_expected_child_count,
            current_family_seen_child_count=v.current_family_seen_child_count,after_child_collection_slot=v.after_child_collection_slot,
            after_child_revision_id=v.after_child_revision_id,after_root_scalar_revision_id=v.after_root_scalar_revision_id,
            after_root_scalar_field_slot=v.after_root_scalar_field_slot,after_child_scalar_revision_id=v.after_child_scalar_revision_id,
            after_child_scalar_field_slot=v.after_child_scalar_field_slot,after_winner_profile_slot=v.after_winner_profile_slot,
            after_winner_entity_binding_id=v.after_winner_entity_binding_id,after_winner_context_key_sha256=v.after_winner_context_key_sha256,
            root_count=v.root_count,family_count=v.family_count,generation_family_count=v.generation_family_count,family_child_count=v.family_child_count,
            winner_count=v.winner_count,profile_count=v.profile_count,root_scalar_count=v.root_scalar_count,child_scalar_count=v.child_scalar_count,
            verified_at=v.verified_at WHERE build_id=b.build_id;
        IF v.verification_state='complete' THEN
            UPDATE __SCHEMA__.custom_import_build_attempt SET phase='verified',verified_at=v.verified_at WHERE build_id=b.build_id;
        END IF;
        RETURN QUERY SELECT v.verification_state::text,v.scan_stage::text,seq+1,n;
    END;
    """


def _authority_functions(schema: str) -> None:
    _function(
        schema,
        "lock_custom_import_build",
        "p_build_id bigint",
        "__SCHEMA__.custom_import_build_attempt",
        _LOCK_CUSTOM_IMPORT_BUILD_BODY,
    )
    _function(
        schema,
        "begin_custom_import_build",
        "p_execution_id bigint, p_fence bigint, p_token_sha256 bytea, "
        "p_expected_base_generation_id bigint, p_expected_pointer_version bigint, p_complete_scope boolean, "
        "p_page_row_limit integer, p_page_byte_limit bigint, p_statement_timeout_ms integer, p_build_deadline_at timestamptz",
        "bigint",
        _BEGIN_CUSTOM_IMPORT_BUILD_BODY,
    )


def _source_functions(schema: str) -> None:
    _function(
        schema,
        "commit_custom_import_build_source_page",
        "p_build_id bigint, p_pack_id bigint",
        "bigint",
        _COMMIT_CUSTOM_IMPORT_BUILD_SOURCE_PAGE_BODY,
    )
    _function(
        schema,
        "finish_custom_import_build_source_part",
        "p_build_id bigint, p_stream_slot smallint, p_part_ordinal integer",
        "integer",
        _FINISH_CUSTOM_IMPORT_BUILD_SOURCE_PART_BODY,
    )
    _function(
        schema,
        "freeze_custom_import_build_source",
        "p_build_id bigint",
        "text",
        _FREEZE_CUSTOM_IMPORT_BUILD_SOURCE_BODY,
    )


def _row_guard_functions(schema: str) -> None:
    _function(
        schema,
        "guard_custom_import_build_owned",
        "",
        "trigger",
        _GUARD_CUSTOM_IMPORT_BUILD_OWNED_BODY.replace("__SCHEMA_NAME__", schema.replace("'", "''")),
        invoker=True,
    )
    _function(
        schema,
        "guard_custom_import_build_evidence",
        "",
        "trigger",
        _GUARD_CUSTOM_IMPORT_BUILD_EVIDENCE_BODY,
        invoker=True,
    )


def _admission_functions(schema: str) -> None:
    _function(
        schema,
        "admit_custom_import_build_page",
        "p_build_id bigint, p_expected_after_id bigint",
        "TABLE(phase text, after_occurrence_id bigint, rows_processed integer, candidate_error_count bigint)",
        _ADMIT_CUSTOM_IMPORT_BUILD_PAGE_BODY,
    )
    _function(
        schema,
        "plan_custom_import_build_family_page",
        "p_build_id bigint, p_expected_page_sequence bigint",
        "TABLE(phase text, plan_stage text, page_sequence bigint, rows_processed integer, plan_complete boolean)",
        _PLAN_CUSTOM_IMPORT_BUILD_FAMILY_PAGE_BODY,
    )


def _graph_guards(schema: str) -> None:
    _function(
        schema,
        "guard_custom_import_build_graph",
        "",
        "trigger",
        _GUARD_CUSTOM_IMPORT_BUILD_GRAPH_BODY.replace(
            "__ALLOWED__", ",".join(f"'__SCHEMA__.{table}'::regclass" for table in _GRAPH_TABLES + _SHAPE_TABLES)
        ),
        invoker=True,
    )
    _function(schema, "charge_custom_import_build_row", "", "trigger", _CHARGE_CUSTOM_IMPORT_BUILD_ROW_BODY)
    _function(
        schema,
        "guard_custom_import_build_frozen_row",
        "",
        "trigger",
        _GUARD_CUSTOM_IMPORT_BUILD_FROZEN_ROW_BODY.replace(
            "__ALLOWED__", ",".join(f"'__SCHEMA__.{table}'::regclass" for table in _FROZEN_TABLES)
        ),
    )
    _function(
        schema,
        "check_custom_import_build_link",
        "",
        "trigger",
        _CHECK_CUSTOM_IMPORT_BUILD_LINK_BODY.replace(
            "__ALLOWED__",
            ",".join(
                f"'__SCHEMA__.{table}'::regclass"
                for table in _GRAPH_TABLES + ("custom_import_build_occurrence", "custom_import_build_candidate_context")
            ),
        ),
        invoker=True,
    )


def _family_functions(schema: str) -> None:
    _function(
        schema,
        "next_custom_import_build_child",
        "p_build_id bigint, p_root_record_id bigint, p_slot smallint, p_key bytea, p_revision_id bigint",
        "TABLE(collection_slot smallint, child_key_sha256 bytea, child_revision_id bigint, pack_id bigint, raw_bytes bigint)",
        _NEXT_CUSTOM_IMPORT_BUILD_CHILD_BODY,
    )
    _function(
        schema,
        "check_custom_import_build_record_work",
        "p_build_id bigint, p_family_id bigint, p_root_revision_id bigint, p_child_revision_id bigint",
        "TABLE(work_rows bigint, work_bytes bigint)",
        _CHECK_CUSTOM_IMPORT_BUILD_RECORD_WORK_BODY,
    )
    _function(
        schema,
        "commit_custom_import_build_copy_pack",
        "p_build_id bigint, p_pack_id bigint",
        "void",
        _COMMIT_CUSTOM_IMPORT_BUILD_COPY_PACK_BODY,
    )
    _function(
        schema,
        "commit_custom_import_build_family_page",
        "p_build_id bigint, p_root_record_id bigint, p_family_revision_id bigint",
        "TABLE(attached_child_count bigint, complete boolean, last_child_collection_slot smallint, last_child_key_sha256 bytea, last_input_child_revision_id bigint)",
        _COMMIT_CUSTOM_IMPORT_BUILD_FAMILY_PAGE_BODY,
    )


def _output_functions(schema: str) -> None:
    _function(
        schema,
        "open_custom_import_build_output",
        "p_build_id bigint, p_generation_id bigint",
        "text",
        _OPEN_CUSTOM_IMPORT_BUILD_OUTPUT_BODY,
    )
    _function(
        schema,
        "commit_custom_import_build_winner_group",
        "p_build_id bigint, p_candidate_context_id bigint",
        "TABLE(profile_slot smallint, entity_binding_id bigint, context_key_sha256 bytea, winner_count bigint)",
        _COMMIT_CUSTOM_IMPORT_BUILD_WINNER_GROUP_BODY,
    )
    _function(
        schema,
        "freeze_custom_import_build_output",
        "p_build_id bigint",
        "text",
        _FREEZE_CUSTOM_IMPORT_BUILD_OUTPUT_BODY,
    )


def _verification_function(schema: str) -> None:
    _function(
        schema,
        "verify_custom_import_build_structure",
        "p_build_id bigint",
        "TABLE(verification_state text, scan_stage text, page_sequence bigint, rows_processed integer)",
        _VERIFY_CUSTOM_IMPORT_BUILD_STRUCTURE_BODY,
    )


def _finality_module():
    path = Path(__file__).with_name("20260917130000_custom_import_generation_finality.py")
    spec = importlib.util.spec_from_file_location("bounded_build_legacy_finality", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _seal_function(schema: str) -> None:
    legacy = _finality_module()
    body = legacy._GENERATION_SEAL_FUNCTION_BODY.replace(
        "DECLARE",
        """DECLARE
        build __SCHEMA__.custom_import_build_attempt;
        proof __SCHEMA__.custom_import_build_verification;
        built_generation __SCHEMA__.custom_import_generation;""",
        1,
    )
    branch = _BUILD_SEAL_BRANCH
    body = legacy._with_read_committed_guard(
        body.replace("__READ_COMMITTED_GUARD__", "__READ_COMMITTED_GUARD__" + branch, 1)
    )
    _function(schema, "guard_custom_import_generation_seal_insert", "", "trigger", body)


def upgrade() -> None:
    """Install bounded build evidence, phase guards and structural verification."""
    schema = _schema()
    for statement in _sql(schema, _DDL).split(";"):
        if statement.strip():
            op.execute(statement)
    _authority_functions(schema)
    _source_functions(schema)
    _row_guard_functions(schema)
    _admission_functions(schema)
    _graph_guards(schema)
    _family_functions(schema)
    _output_functions(schema)
    _verification_function(schema)
    _seal_function(schema)
    for table in _TABLES:
        function = (
            "guard_custom_import_build_evidence"
            if table in ("custom_import_build_occurrence", "custom_import_build_candidate_context")
            else "guard_custom_import_build_owned"
        )
        _trigger(schema, table, table + "_guard", "BEFORE INSERT OR UPDATE OR DELETE", function)
        _trigger(schema, table, table + "_truncate", "BEFORE TRUNCATE", function, statement=True)
        op.execute(_sql(schema, f"REVOKE ALL ON __SCHEMA__.{table} FROM PUBLIC"))
    for table in _GRAPH_TABLES + _SHAPE_TABLES:
        _trigger(schema, table, table + "_build_phase", "BEFORE INSERT", "guard_custom_import_build_graph")
    for table in _FROZEN_TABLES:
        _trigger(
            schema, table, table + "_build_frozen", "BEFORE UPDATE OR DELETE", "guard_custom_import_build_frozen_row"
        )
        _trigger(
            schema,
            table,
            table + "_build_truncate",
            "BEFORE TRUNCATE",
            "guard_custom_import_build_frozen_row",
            statement=True,
        )
    for table in _GRAPH_TABLES + ("custom_import_build_occurrence", "custom_import_build_candidate_context"):
        _trigger(schema, table, table + "_build_link", "AFTER INSERT", "check_custom_import_build_link", deferred=True)
    for table in (
        "custom_import_rejection",
        "custom_import_build_candidate_context",
        "custom_import_generation_family",
        "custom_import_winner",
    ):
        _trigger(schema, table, table + "_build_charge", "AFTER INSERT", "charge_custom_import_build_row")


def downgrade() -> None:
    """Remove the empty build schema without discarding retained evidence."""
    schema = _schema()
    op.execute(
        _sql(
            schema,
            """DO $block$ BEGIN
        IF EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_build_attempt) THEN
            RAISE EXCEPTION 'custom_import_build_immutable: retained build evidence prevents downgrade';
        END IF; END $block$""",
        )
    )
    op.execute(
        _finality_module()
        ._generation_seal_function_sql(schema)
        .replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1)
    )
    for table in _GRAPH_TABLES + _SHAPE_TABLES:
        op.execute(_sql(schema, f"DROP TRIGGER {table}_build_phase ON __SCHEMA__.{table}"))
    for table in _FROZEN_TABLES:
        op.execute(_sql(schema, f"DROP TRIGGER {table}_build_frozen ON __SCHEMA__.{table}"))
        op.execute(_sql(schema, f"DROP TRIGGER {table}_build_truncate ON __SCHEMA__.{table}"))
    for table in _GRAPH_TABLES:
        op.execute(_sql(schema, f"DROP TRIGGER {table}_build_link ON __SCHEMA__.{table}"))
    for table in ("custom_import_rejection", "custom_import_generation_family", "custom_import_winner"):
        op.execute(_sql(schema, f"DROP TRIGGER {table}_build_charge ON __SCHEMA__.{table}"))
    op.execute(_sql(schema, "DROP FUNCTION __SCHEMA__.lock_custom_import_build(bigint)"))
    for table in reversed(_TABLES):
        op.execute(_sql(schema, f"DROP TABLE __SCHEMA__.{table}"))
    _drop_build_functions(schema)


def _drop_build_functions(schema: str) -> None:
    # Every match is an exact task-owned function; no wildcard or CASCADE removal.
    names = (
        "begin_custom_import_build",
        "lock_custom_import_build",
        "commit_custom_import_build_source_page",
        "finish_custom_import_build_source_part",
        "freeze_custom_import_build_source",
        "admit_custom_import_build_page",
        "plan_custom_import_build_family_page",
        "next_custom_import_build_child",
        "check_custom_import_build_record_work",
        "commit_custom_import_build_copy_pack",
        "commit_custom_import_build_family_page",
        "open_custom_import_build_output",
        "commit_custom_import_build_winner_group",
        "freeze_custom_import_build_output",
        "verify_custom_import_build_structure",
        "guard_custom_import_build_owned",
        "guard_custom_import_build_evidence",
        "guard_custom_import_build_graph",
        "charge_custom_import_build_row",
        "check_custom_import_build_link",
        "guard_custom_import_build_frozen_row",
    )
    from sqlalchemy import text

    function_signatures = op.get_bind().execute(
        text(
            "SELECT p.oid::regprocedure::text FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
            "WHERE n.nspname=:schema AND p.proname=ANY(:names)"
        ),
        {"schema": schema, "names": list(names)},
    )
    for function_signature in function_signatures:
        op.execute(f"DROP FUNCTION {function_signature[0]}")
