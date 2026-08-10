# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Additive semantic-content-v4 terminal-root retirement SQL."""

from __future__ import annotations

from db import (
    migration_provider_directory_terminal_root_retirement_evidence as legacy_evidence,
)
from db import (
    migration_provider_directory_terminal_root_retirement_guards as legacy_guards,
)


STATUS = legacy_evidence.STATUS
MARKER = "provider_directory_terminal_root_retirement_v2"
CONTRACT = "healthporta.provider-directory.terminal-root-retirement.v2"
RESOURCE_HASH_CONTRACT = "semantic_content_v4"
EVIDENCE_FUNCTION = "provider_directory_terminal_root_retirement_v2_evidence"
ELIGIBLE_FUNCTION = "provider_directory_terminal_root_retirement_v2_eligible"
MARKER_FUNCTION = "provider_directory_terminal_root_retirement_v2_marker_valid"
VALID_FUNCTION = "provider_directory_terminal_root_retirement_v2_valid"


def _replace_once(rendered_sql: str, old: str, new: str) -> str:
    """Replace one frozen SQL fragment or reject generator drift."""

    if rendered_sql.count(old) != 1:
        raise RuntimeError("terminal retirement v1 SQL shape changed")
    return rendered_sql.replace(old, new, 1)


def evidence_function_sql(schema: str) -> str:
    """Clone the frozen evidence envelope with a v2 parent marker identity."""

    rendered_sql = legacy_evidence.evidence_function_sql(schema)
    rendered_sql = _replace_once(
        rendered_sql,
        legacy_evidence._qf(schema, legacy_evidence.EVIDENCE_FUNCTION),
        legacy_evidence._qf(schema, EVIDENCE_FUNCTION),
    )
    return _replace_once(
        rendered_sql,
        legacy_evidence._ql(legacy_evidence.MARKER),
        legacy_evidence._ql(MARKER),
    )


def _eligible_parent_shape_sql() -> str:
    """Return the exact v4-only parent predicates."""

    return f"""
        parent.status IN ('acquiring', {legacy_evidence._ql(STATUS)})
        AND parent.is_current IS FALSE
        AND parent.previous_dataset_id IS NOT NULL
        AND parent.dataset_hash IS NULL
        AND parent.validated_at IS NULL
        AND parent.published_at IS NULL
        AND parent.superseded_at IS NULL
        AND parent.completion_proof_required_version IS NULL
        AND parent.completion_proof_json IS NULL
        AND parent.completion_proof_sha256 IS NULL
        AND COALESCE(
                parent.publication_metadata_json::jsonb,
                '{{}}'::jsonb
            ) ? 'resource_hash_contract'
        AND parent.publication_metadata_json::jsonb
              ->> 'resource_hash_contract' =
                {legacy_evidence._ql(RESOURCE_HASH_CONTRACT)}
        AND minimum_age BETWEEN 900 AND 604800
    """


def eligible_function_sql(schema: str) -> str:
    """Return v2 eligibility with explicit semantic-content-v4 identity."""

    function_ref = legacy_evidence._qf(schema, ELIGIBLE_FUNCTION)
    lineage_sql = legacy_guards._eligible_lineage_sql(schema)
    dataset = legacy_evidence._qf(schema, "provider_directory_endpoint_dataset")
    return f"""
    CREATE FUNCTION {function_ref}(candidate_dataset_id text, minimum_age integer)
    RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER
    SET search_path = pg_catalog AS $function$
    {lineage_sql}
    SELECT COALESCE(
        {_eligible_parent_shape_sql()}
        AND (SELECT lineage_shape.is_linear FROM lineage_shape)
        AND EXISTS (
            SELECT 1 FROM {dataset} AS predecessor
             WHERE predecessor.dataset_id = parent.previous_dataset_id
               AND predecessor.endpoint_id = parent.endpoint_id
        )
        AND (SELECT pg_catalog.bool_and(
                 lineage.importer = 'provider-directory-fhir'
                 AND lineage.status IN (
                     'canceled', 'cancelled', 'dead_letter', 'failed'
                 )
                 AND lineage.finished_at IS NOT NULL
             ) FROM lineage)
        AND (SELECT pg_catalog.max(lineage.finished_at) FROM lineage)
              <= pg_catalog.transaction_timestamp()
                 - pg_catalog.make_interval(secs => minimum_age),
        FALSE
    ) FROM parent;
    $function$;
    """


def marker_function_sql(schema: str) -> str:
    """Clone frozen marker validation for the v2 contract and evidence."""

    rendered_sql = legacy_guards.marker_function_sql(schema)
    replacements = (
        (
            legacy_evidence._qf(schema, legacy_guards.MARKER_FUNCTION),
            legacy_evidence._qf(schema, MARKER_FUNCTION),
        ),
        (
            legacy_evidence._qf(schema, legacy_evidence.EVIDENCE_FUNCTION),
            legacy_evidence._qf(schema, EVIDENCE_FUNCTION),
        ),
        (
            legacy_evidence._ql(legacy_guards.CONTRACT),
            legacy_evidence._ql(CONTRACT),
        ),
    )
    for old_fragment, new_fragment in replacements:
        rendered_sql = _replace_once(rendered_sql, old_fragment, new_fragment)
    return rendered_sql


def valid_function_sql(schema: str) -> str:
    """Return validation for one persisted v2 retired parent."""

    dataset = legacy_evidence._qf(schema, "provider_directory_endpoint_dataset")
    function_ref = legacy_evidence._qf(schema, VALID_FUNCTION)
    marker_valid = legacy_evidence._qf(schema, MARKER_FUNCTION)
    eligible = legacy_evidence._qf(schema, ELIGIBLE_FUNCTION)
    return f"""
    CREATE FUNCTION {function_ref}(candidate_dataset_id text)
    RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER
    SET search_path = pg_catalog AS $function$
    SELECT COALESCE(
        row.status = {legacy_evidence._ql(STATUS)}
        AND {marker_valid}(
                row.dataset_id,
                row.publication_metadata_json::jsonb ->
                    {legacy_evidence._ql(MARKER)}
            )
        AND {eligible}(
                row.dataset_id,
                (row.publication_metadata_json::jsonb ->
                    {legacy_evidence._ql(MARKER)}
                    ->> 'minimum_terminal_age_seconds')::integer
            ),
        FALSE
    ) FROM {dataset} AS row
    WHERE row.dataset_id = candidate_dataset_id;
    $function$;
    """


def _marker_keys_sql() -> str:
    """Return the closed set of accepted retirement marker keys."""

    return (
        f"ARRAY[{legacy_evidence._ql(legacy_evidence.MARKER)}, "
        f"{legacy_evidence._ql(MARKER)}]::text[]"
    )


def _transition_predicates_sql(
    schema: str,
    *,
    marker_key: str,
    eligible_function: str,
    marker_function: str,
    evidence_function: str,
) -> str:
    """Return shared exact-parent predicates for one selected profile."""

    dataset = legacy_evidence._qf(schema, "provider_directory_endpoint_dataset")
    eligible = legacy_evidence._qf(schema, eligible_function)
    marker_valid = legacy_evidence._qf(schema, marker_function)
    evidence = legacy_evidence._qf(schema, evidence_function)
    return f"""
        OLD.status <> 'acquiring'
        OR pg_catalog.to_jsonb(NEW) - ARRAY[
             'status', 'publication_metadata_json'
           ]::text[] IS DISTINCT FROM pg_catalog.to_jsonb(OLD) - ARRAY[
             'status', 'publication_metadata_json'
           ]::text[]
        OR COALESCE(NEW.publication_metadata_json::jsonb, '{{}}'::jsonb)
             - {legacy_evidence._ql(marker_key)} IS DISTINCT FROM
           COALESCE(OLD.publication_metadata_json::jsonb, '{{}}'::jsonb)
             - {legacy_evidence._ql(marker_key)}
        OR COALESCE(OLD.publication_metadata_json::jsonb, '{{}}'::jsonb)
             ?| {_marker_keys_sql()}
        OR {eligible}(OLD.dataset_id, minimum_age) IS DISTINCT FROM TRUE
        OR {marker_valid}(NULL, marker) IS DISTINCT FROM TRUE
        OR (marker -> 'evidence') IS DISTINCT FROM {evidence}(OLD.dataset_id)
        OR NOT EXISTS (SELECT 1 FROM {dataset} AS predecessor
             WHERE predecessor.dataset_id = OLD.previous_dataset_id
               AND predecessor.endpoint_id = OLD.endpoint_id
               AND predecessor.status = 'published'
               AND predecessor.is_current IS TRUE)
        OR EXISTS (SELECT 1 FROM {dataset} AS competing
             WHERE competing.endpoint_id = OLD.endpoint_id
               AND competing.dataset_id <> OLD.dataset_id
               AND competing.status IN ('acquiring', 'incomplete'))
    """


def _profile_transition_sql(
    schema: str,
    *,
    marker_key: str,
    eligible_function: str,
    marker_function: str,
    evidence_function: str,
) -> str:
    """Return the marker age and evidence validation for one profile."""

    predicates = _transition_predicates_sql(
        schema,
        marker_key=marker_key,
        eligible_function=eligible_function,
        marker_function=marker_function,
        evidence_function=evidence_function,
    )
    return f"""
        marker := NEW.publication_metadata_json::jsonb ->
            {legacy_evidence._ql(marker_key)};
        IF pg_catalog.jsonb_typeof(marker) <> 'object'
           OR pg_catalog.jsonb_typeof(
                marker -> 'minimum_terminal_age_seconds'
              ) <> 'number'
           OR marker ->> 'minimum_terminal_age_seconds'
                !~ '^(0|[1-9][0-9]*)$'
           OR pg_catalog.length(
                marker ->> 'minimum_terminal_age_seconds'
              ) > 6
           OR (marker ->> 'minimum_terminal_age_seconds')::numeric
                NOT BETWEEN 900 AND 604800 THEN
            RAISE EXCEPTION
                'provider_directory_terminal_root_retirement_transition_invalid'
                USING ERRCODE = '55000';
        END IF;
        minimum_age := (marker ->> 'minimum_terminal_age_seconds')::integer;
        IF {predicates} THEN
            RAISE EXCEPTION
                'provider_directory_terminal_root_retirement_transition_invalid'
                USING ERRCODE = '55000';
        END IF;
    """


def _parent_guard_preamble_sql(schema: str) -> str:
    """Return status-wide immutability shared by both marker versions."""

    dataset = legacy_evidence._qf(schema, "provider_directory_endpoint_dataset")
    marker_keys = _marker_keys_sql()
    return f"""
        IF TG_OP = 'TRUNCATE' THEN
            IF EXISTS (SELECT 1 FROM {dataset}
                        WHERE status = {legacy_evidence._ql(STATUS)}) THEN
                RAISE EXCEPTION
                    'provider_directory_terminal_root_retirement_truncate_forbidden'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NULL;
        END IF;
        IF TG_OP <> 'DELETE' AND NEW.previous_dataset_id IS NOT NULL
           AND EXISTS (SELECT 1 FROM {dataset} AS retired
                        WHERE retired.dataset_id = NEW.previous_dataset_id
                          AND retired.status = {legacy_evidence._ql(STATUS)}) THEN
            RAISE EXCEPTION
                'provider_directory_terminal_root_retirement_reference_forbidden'
                USING ERRCODE = '55000';
        END IF;
        IF TG_OP = 'INSERT' THEN
            IF NEW.status = {legacy_evidence._ql(STATUS)} OR COALESCE(
                 NEW.publication_metadata_json::jsonb, '{{}}'::jsonb
               ) ?| {marker_keys} THEN
                RAISE EXCEPTION
                    'provider_directory_terminal_root_retirement_insert_forbidden'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END IF;
        IF TG_OP = 'DELETE' THEN
            IF OLD.status = {legacy_evidence._ql(STATUS)} THEN
                RAISE EXCEPTION
                    'provider_directory_terminal_root_retirement_immutable'
                    USING ERRCODE = '55000';
            END IF;
            RETURN OLD;
        END IF;
        IF OLD.status = {legacy_evidence._ql(STATUS)} THEN
            RAISE EXCEPTION
                'provider_directory_terminal_root_retirement_immutable'
                USING ERRCODE = '55000';
        END IF;
    """


def _parent_transition_sql(schema: str) -> str:
    """Return exact-one-profile transition dispatch and marker exclusion."""

    legacy_branch = _profile_transition_sql(
        schema,
        marker_key=legacy_evidence.MARKER,
        eligible_function=legacy_guards.ELIGIBLE_FUNCTION,
        marker_function=legacy_guards.MARKER_FUNCTION,
        evidence_function=legacy_evidence.EVIDENCE_FUNCTION,
    )
    v2_branch = _profile_transition_sql(
        schema,
        marker_key=MARKER,
        eligible_function=ELIGIBLE_FUNCTION,
        marker_function=MARKER_FUNCTION,
        evidence_function=EVIDENCE_FUNCTION,
    )
    marker_keys = _marker_keys_sql()
    return f"""
        IF NEW.status = {legacy_evidence._ql(STATUS)} THEN
            legacy_marker_present := COALESCE(
                NEW.publication_metadata_json::jsonb, '{{}}'::jsonb
            ) ? {legacy_evidence._ql(legacy_evidence.MARKER)};
            v2_marker_present := COALESCE(
                NEW.publication_metadata_json::jsonb, '{{}}'::jsonb
            ) ? {legacy_evidence._ql(MARKER)};
            IF legacy_marker_present = v2_marker_present THEN
                RAISE EXCEPTION
                    'provider_directory_terminal_root_retirement_transition_invalid'
                    USING ERRCODE = '55000';
            END IF;
            IF legacy_marker_present THEN
                {legacy_branch}
            ELSE
                {v2_branch}
            END IF;
            RETURN NEW;
        END IF;
        IF COALESCE(NEW.publication_metadata_json::jsonb, '{{}}'::jsonb)
             ?| {marker_keys} THEN
            RAISE EXCEPTION
                'provider_directory_terminal_root_retirement_marker_forbidden'
                USING ERRCODE = '55000';
        END IF;
        RETURN NEW;
    """


def parent_guard_function_sql(schema: str) -> str:
    """Return the dual v1/v2 acquiring-to-retired parent guard."""

    function_ref = legacy_evidence._qf(schema, legacy_guards.PARENT_GUARD)
    return f"""
    CREATE FUNCTION {function_ref}() RETURNS trigger LANGUAGE plpgsql VOLATILE
    SECURITY DEFINER SET search_path = pg_catalog AS $function$
    DECLARE
        marker jsonb;
        minimum_age integer;
        legacy_marker_present boolean;
        v2_marker_present boolean;
    BEGIN
        {_parent_guard_preamble_sql(schema)}
        {_parent_transition_sql(schema)}
    END;
    $function$;
    """
