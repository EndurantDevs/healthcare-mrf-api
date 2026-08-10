# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Frozen database guards for terminal Provider Directory root retirement."""

from __future__ import annotations

from db.migration_provider_directory_terminal_root_retirement_evidence import (
    EVIDENCE_FUNCTION,
    MARKER,
    STATUS,
    _q,
    _qf,
    _ql,
    lineage_ctes_sql,
)


CONTRACT = "healthporta.provider-directory.terminal-root-retirement.v1"
REASON = "terminal_retry_lineage_exhausted"
RESOURCE_HASH_CONTRACT = "transport_bound_v1"
VALID_FUNCTION = "provider_directory_terminal_root_retirement_valid"
ELIGIBLE_FUNCTION = "provider_directory_terminal_root_retirement_eligible"
MARKER_FUNCTION = "provider_directory_terminal_root_retirement_marker_valid"
RUN_RETIRED_FUNCTION = "provider_directory_terminal_root_run_retired"
PARENT_GUARD = "guard_provider_directory_terminal_root_retirement_parent"
CHILD_GUARD = "guard_provider_directory_terminal_root_retirement_child"
IMPORT_RUN_GUARD = "guard_provider_directory_terminal_root_retirement_run"
_MARKER_FIELDS_SQL = (
    "ARRAY['contract_version', 'evidence', 'minimum_terminal_age_seconds', "
    "'reason_code', 'retired_at']::text[]"
)
_EVIDENCE_FIELDS_SQL = (
    "ARRAY['actual_resource_count', 'child_relations', 'lineage_finished_at', "
    "'lineage_sha256', 'parent_identity_sha256', 'parent_resource_count', "
    "'predecessor_identity_sha256', 'prior_status', 'proof_shard_count', "
    "'proof_row_count', 'resource_counts', 'source_identity_sha256', "
    "'target_identity_sha256', 'terminal_run_count']::text[]"
)
_DRIFTABLE_EVIDENCE_SQL = (
    "ARRAY['predecessor_identity_sha256', 'source_identity_sha256', "
    "'target_identity_sha256']::text[]"
)

CHILD_TRIGGER_SUFFIXES = {
    "provider_directory_bulk_acquisition_checkpoint": "bulk_acq",
    "provider_directory_bulk_output_checkpoint": "bulk_output",
    "provider_directory_dataset_affiliation_organization": "affiliation",
    "provider_directory_dataset_insurance_plan": "insurance",
    "provider_directory_dataset_network_plan": "network",
    "provider_directory_dataset_proof_shard": "proof",
    "provider_directory_dataset_rehydration_checkpoint": "rehydration",
    "provider_directory_dataset_resource": "resource",
    "provider_directory_pagination_checkpoint": "pagination",
    "provider_directory_uhc_flex_npi_cohort": "flex_cohort",
    "provider_directory_uhc_flex_practitioner_dataset": "flex_dataset",
    "provider_directory_uhc_flex_practitioner_dataset_resource": "flex_resource",
}


def _eligible_lineage_sql(schema: str) -> str:
    dataset = _qf(schema, "provider_directory_endpoint_dataset")
    return f"""
    WITH RECURSIVE parent AS (
        SELECT row.* FROM {dataset} AS row
         WHERE row.dataset_id = candidate_dataset_id
    )
    {lineage_ctes_sql(schema)}
    """


def _eligible_predicates_sql(schema: str) -> str:
    dataset = _qf(schema, "provider_directory_endpoint_dataset")
    return f"""
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
    """


def _eligible_parent_shape_sql() -> str:
    return f"""
        parent.status IN ('acquiring', {_ql(STATUS)})
        AND parent.is_current IS FALSE
        AND parent.previous_dataset_id IS NOT NULL
        AND parent.dataset_hash IS NULL
        AND parent.validated_at IS NULL
        AND parent.published_at IS NULL
        AND parent.superseded_at IS NULL
        AND parent.completion_proof_required_version IS NULL
        AND parent.completion_proof_json IS NULL
        AND parent.completion_proof_sha256 IS NULL
        AND (
            NOT (COALESCE(
                parent.publication_metadata_json::jsonb, '{{}}'::jsonb
            ) ? 'resource_hash_contract')
            OR parent.publication_metadata_json::jsonb
                  ->> 'resource_hash_contract' = {_ql(RESOURCE_HASH_CONTRACT)}
        )
        AND minimum_age BETWEEN 900 AND 604800
    """


def eligible_function_sql(schema: str) -> str:
    """Return SQL for stable terminal-lineage eligibility checks."""

    function = _qf(schema, ELIGIBLE_FUNCTION)
    return f"""
    CREATE FUNCTION {function}(candidate_dataset_id text, minimum_age integer)
    RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER
    SET search_path = pg_catalog AS $function$
    {_eligible_lineage_sql(schema)}
    {_eligible_predicates_sql(schema)}
    $function$;
    """


def marker_function_sql(schema: str) -> str:
    """Return SQL validating marker shape and immutable replay evidence."""

    evidence = _qf(schema, EVIDENCE_FUNCTION)
    function = _qf(schema, MARKER_FUNCTION)
    return f"""
    CREATE FUNCTION {function}(candidate_dataset_id text, marker jsonb)
    RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER
    SET search_path = pg_catalog AS $function$
    SELECT COALESCE(
        pg_catalog.jsonb_typeof(marker) = 'object'
        AND marker ?& {_MARKER_FIELDS_SQL}
        AND marker - {_MARKER_FIELDS_SQL} = '{{}}'::jsonb
        AND marker ->> 'contract_version' = {_ql(CONTRACT)}
        AND marker ->> 'reason_code' = {_ql(REASON)}
        AND pg_catalog.jsonb_typeof(marker -> 'evidence') = 'object'
        AND (marker -> 'evidence') ?& {_EVIDENCE_FIELDS_SQL}
        AND (marker -> 'evidence') - {_EVIDENCE_FIELDS_SQL} = '{{}}'::jsonb
        AND NOT EXISTS (
            SELECT 1 FROM pg_catalog.jsonb_each_text(
                pg_catalog.jsonb_object(
                    ARRAY['predecessor_identity_sha256',
                          'source_identity_sha256',
                          'target_identity_sha256'],
                    ARRAY[marker -> 'evidence' ->>
                              'predecessor_identity_sha256',
                          marker -> 'evidence' ->> 'source_identity_sha256',
                          marker -> 'evidence' ->> 'target_identity_sha256']
                )
            ) AS digest(name, value)
            WHERE digest.value IS NULL
               OR digest.value !~ '^[0-9a-f]{{64}}$'
        )
        AND pg_catalog.jsonb_typeof(
                marker -> 'minimum_terminal_age_seconds'
            ) = 'number'
        AND marker ->> 'minimum_terminal_age_seconds'
              ~ '^(0|[1-9][0-9]*)$'
        AND (marker ->> 'minimum_terminal_age_seconds')::numeric
              BETWEEN 900 AND 604800
        AND pg_catalog.jsonb_typeof(marker -> 'retired_at') = 'string'
        AND marker ->> 'retired_at' ~ 'T.*(Z|[+-][0-9]{{2}}:[0-9]{{2}})$'
        AND pg_catalog.pg_input_is_valid(
                marker ->> 'retired_at', 'pg_catalog.timestamptz'
            )
        AND CASE
            WHEN candidate_dataset_id IS NULL THEN TRUE
            ELSE (marker -> 'evidence') - {_DRIFTABLE_EVIDENCE_SQL}
                 IS NOT DISTINCT FROM {evidence}(candidate_dataset_id)
                    - {_DRIFTABLE_EVIDENCE_SQL}
        END
        AND marker -> 'evidence' -> 'child_relations'
              -> 'provider_directory_endpoint_dataset_previous_reference'
              -> 'row_count' = '0'::jsonb,
        FALSE
    );
    $function$;
    """


def valid_function_sql(schema: str) -> str:
    """Return SQL validating one persisted retired dataset marker."""

    dataset = _qf(schema, "provider_directory_endpoint_dataset")
    marker_valid = _qf(schema, MARKER_FUNCTION)
    eligible = _qf(schema, ELIGIBLE_FUNCTION)
    function = _qf(schema, VALID_FUNCTION)
    return f"""
    CREATE FUNCTION {function}(candidate_dataset_id text)
    RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER
    SET search_path = pg_catalog AS $function$
    SELECT COALESCE(
        row.status = {_ql(STATUS)}
        AND {marker_valid}(
                row.dataset_id,
                row.publication_metadata_json::jsonb -> {_ql(MARKER)}
            )
        AND {eligible}(
                row.dataset_id,
                (row.publication_metadata_json::jsonb -> {_ql(MARKER)}
                    ->> 'minimum_terminal_age_seconds')::integer
            ),
        FALSE
    ) FROM {dataset} AS row
    WHERE row.dataset_id = candidate_dataset_id;
    $function$;
    """


def run_retired_function_sql(schema: str) -> str:
    """Return SQL resolving whether an import run descends from retirement."""

    dataset = _qf(schema, "provider_directory_endpoint_dataset")
    import_run = _qf(schema, "import_run")
    function = _qf(schema, RUN_RETIRED_FUNCTION)
    return f"""
    CREATE FUNCTION {function}(candidate_run_id text)
    RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER
    SET search_path = pg_catalog AS $function$
    WITH RECURSIVE ancestors(run_id, retry_of_run_id, path, depth) AS (
        SELECT run.run_id, run.retry_of_run_id, ARRAY[run.run_id]::text[], 1
          FROM {import_run} AS run WHERE run.run_id = candidate_run_id
        UNION ALL
        SELECT parent.run_id, parent.retry_of_run_id,
               child.path || parent.run_id, child.depth + 1
          FROM {import_run} AS parent JOIN ancestors AS child
            ON parent.run_id = child.retry_of_run_id
           AND NOT parent.run_id = ANY(child.path) AND child.depth < 128
    )
    SELECT EXISTS (
        SELECT 1 FROM ancestors JOIN {dataset} AS retired
          ON retired.acquisition_root_run_id = ancestors.run_id
         AND retired.status = {_ql(STATUS)}
    );
    $function$;
    """


def _parent_guard_preamble_sql(schema: str) -> str:
    dataset = _qf(schema, "provider_directory_endpoint_dataset")
    return f"""
        IF TG_OP = 'TRUNCATE' THEN
            IF EXISTS (SELECT 1 FROM {dataset} WHERE status = {_ql(STATUS)}) THEN
                RAISE EXCEPTION 'provider_directory_terminal_root_retirement_truncate_forbidden'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NULL;
        END IF;
        IF TG_OP <> 'DELETE' AND NEW.previous_dataset_id IS NOT NULL
           AND EXISTS (SELECT 1 FROM {dataset} AS retired
                        WHERE retired.dataset_id = NEW.previous_dataset_id
                          AND retired.status = {_ql(STATUS)}) THEN
            RAISE EXCEPTION 'provider_directory_terminal_root_retirement_reference_forbidden'
                USING ERRCODE = '55000';
        END IF;
        IF TG_OP = 'INSERT' THEN
            IF NEW.status = {_ql(STATUS)} OR COALESCE(
                 NEW.publication_metadata_json::jsonb, '{{}}'::jsonb
               ) ? {_ql(MARKER)} THEN
                RAISE EXCEPTION 'provider_directory_terminal_root_retirement_insert_forbidden'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END IF;
        IF TG_OP = 'DELETE' THEN
            IF OLD.status = {_ql(STATUS)} THEN
                RAISE EXCEPTION 'provider_directory_terminal_root_retirement_immutable'
                    USING ERRCODE = '55000';
            END IF;
            RETURN OLD;
        END IF;
        IF OLD.status = {_ql(STATUS)} THEN
            RAISE EXCEPTION 'provider_directory_terminal_root_retirement_immutable'
                USING ERRCODE = '55000';
        END IF;
    """


def _parent_transition_predicates_sql(schema: str) -> str:
    dataset = _qf(schema, "provider_directory_endpoint_dataset")
    marker_valid = _qf(schema, MARKER_FUNCTION)
    eligible = _qf(schema, ELIGIBLE_FUNCTION)
    evidence = _qf(schema, EVIDENCE_FUNCTION)
    return f"""
        OLD.status <> 'acquiring'
        OR pg_catalog.to_jsonb(NEW) - ARRAY[
             'status', 'publication_metadata_json'
           ]::text[] IS DISTINCT FROM pg_catalog.to_jsonb(OLD) - ARRAY[
             'status', 'publication_metadata_json'
           ]::text[]
        OR COALESCE(NEW.publication_metadata_json::jsonb, '{{}}'::jsonb)
             - {_ql(MARKER)} IS DISTINCT FROM
           COALESCE(OLD.publication_metadata_json::jsonb, '{{}}'::jsonb)
             - {_ql(MARKER)}
        OR COALESCE(OLD.publication_metadata_json::jsonb, '{{}}'::jsonb)
             ? {_ql(MARKER)}
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


def _parent_transition_sql(schema: str) -> str:
    return f"""
        IF NEW.status = {_ql(STATUS)} THEN
            marker := NEW.publication_metadata_json::jsonb -> {_ql(MARKER)};
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
                RAISE EXCEPTION 'provider_directory_terminal_root_retirement_transition_invalid'
                    USING ERRCODE = '55000';
            END IF;
            minimum_age := (marker ->> 'minimum_terminal_age_seconds')::integer;
            IF {_parent_transition_predicates_sql(schema)} THEN
                RAISE EXCEPTION 'provider_directory_terminal_root_retirement_transition_invalid'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END IF;
        IF COALESCE(NEW.publication_metadata_json::jsonb, '{{}}'::jsonb)
             ? {_ql(MARKER)} THEN
            RAISE EXCEPTION 'provider_directory_terminal_root_retirement_marker_forbidden'
                USING ERRCODE = '55000';
        END IF;
        RETURN NEW;
    """


def parent_guard_function_sql(schema: str) -> str:
    """Return SQL guarding the sole acquiring-to-retired parent mutation."""

    function = _qf(schema, PARENT_GUARD)
    return f"""
    CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql VOLATILE
    SECURITY DEFINER SET search_path = pg_catalog AS $function$
    DECLARE marker jsonb; minimum_age integer;
    BEGIN
        {_parent_guard_preamble_sql(schema)}
        {_parent_transition_sql(schema)}
    END;
    $function$;
    """


def child_guard_function_sql(schema: str) -> str:
    """Return SQL freezing direct and indirect retired-dataset children."""

    dataset = _qf(schema, "provider_directory_endpoint_dataset")
    bulk = _qf(schema, "provider_directory_bulk_acquisition_checkpoint")
    function = _qf(schema, CHILD_GUARD)
    return f"""
    CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql VOLATILE
    SECURITY DEFINER SET search_path = pg_catalog AS $function$
    DECLARE old_id text; new_id text; old_reference text; new_reference text;
    BEGIN
        IF TG_OP = 'TRUNCATE' THEN
            IF EXISTS (SELECT 1 FROM {dataset} WHERE status = {_ql(STATUS)}) THEN
                RAISE EXCEPTION 'provider_directory_terminal_root_retirement_child_truncate_forbidden'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NULL;
        END IF;
        IF TG_TABLE_NAME = 'provider_directory_bulk_output_checkpoint' THEN
            IF TG_OP <> 'INSERT' THEN SELECT dataset_id INTO old_id FROM {bulk}
                WHERE checkpoint_id = OLD.checkpoint_id; END IF;
            IF TG_OP <> 'DELETE' THEN SELECT dataset_id INTO new_id FROM {bulk}
                WHERE checkpoint_id = NEW.checkpoint_id; END IF;
        ELSIF TG_TABLE_NAME = 'provider_directory_uhc_flex_npi_cohort' THEN
            IF TG_OP <> 'INSERT' THEN old_id := OLD.official_dataset_id; END IF;
            IF TG_OP <> 'DELETE' THEN new_id := NEW.official_dataset_id; END IF;
        ELSE
            IF TG_OP <> 'INSERT' THEN old_id := pg_catalog.to_jsonb(OLD) ->> 'dataset_id'; END IF;
            IF TG_OP <> 'DELETE' THEN new_id := pg_catalog.to_jsonb(NEW) ->> 'dataset_id'; END IF;
        END IF;
        IF TG_TABLE_NAME = 'provider_directory_uhc_flex_practitioner_dataset' THEN
            IF TG_OP <> 'INSERT' THEN old_reference := OLD.previous_dataset_id; END IF;
            IF TG_OP <> 'DELETE' THEN new_reference := NEW.previous_dataset_id; END IF;
        END IF;
        IF EXISTS (SELECT 1 FROM {dataset} AS retired
                    WHERE retired.status = {_ql(STATUS)}
                      AND retired.dataset_id IN (
                          old_id, new_id, old_reference, new_reference
                      )) THEN
            RAISE EXCEPTION 'provider_directory_terminal_root_retirement_child_immutable'
                USING ERRCODE = '55000';
        END IF;
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END;
    $function$;
    """


def import_run_guard_function_sql(schema: str) -> str:
    """Return SQL freezing retired lineage rows and rejecting late retries."""

    dataset = _qf(schema, "provider_directory_endpoint_dataset")
    run_retired = _qf(schema, RUN_RETIRED_FUNCTION)
    function = _qf(schema, IMPORT_RUN_GUARD)
    return f"""
    CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql VOLATILE
    SECURITY DEFINER SET search_path = pg_catalog AS $function$
    BEGIN
        IF TG_OP = 'TRUNCATE' THEN
            IF EXISTS (SELECT 1 FROM {dataset} WHERE status = {_ql(STATUS)}) THEN
                RAISE EXCEPTION 'provider_directory_terminal_root_retirement_run_truncate_forbidden'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NULL;
        END IF;
        IF (TG_OP <> 'INSERT' AND {run_retired}(OLD.run_id))
           OR (TG_OP <> 'DELETE' AND {run_retired}(NEW.run_id))
           OR (TG_OP <> 'DELETE' AND NEW.retry_of_run_id IS NOT NULL
               AND {run_retired}(NEW.retry_of_run_id)) THEN
            RAISE EXCEPTION 'provider_directory_terminal_root_retirement_run_immutable'
                USING ERRCODE = '55000';
        END IF;
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END;
    $function$;
    """


def function_names() -> tuple[str, ...]:
    """Return installed guard functions in dependency order."""

    return (
        VALID_FUNCTION, MARKER_FUNCTION, ELIGIBLE_FUNCTION,
        RUN_RETIRED_FUNCTION, PARENT_GUARD, CHILD_GUARD, IMPORT_RUN_GUARD,
    )
