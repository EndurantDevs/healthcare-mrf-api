# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bind partial FHIR publication to a strict source-wide logical-request budget.

Historical nullable proofs retain their original identities and validation path.
"""

from __future__ import annotations

from functools import lru_cache
import importlib.util
from pathlib import Path

from alembic import op


revision = "20260909120000_fhir_request_failure_budget"
down_revision = "20260907220000_hospital_price_missing_plan"
branch_labels = None
depends_on = None

_POLICY = "healthporta.fhir.request-failure-budget.v1"
_PUBLICATION = "healthporta.provider-directory.rooted-graph-publication.v2"
_COVERAGE = "provider_directory_rooted_graph_request_failure_coverage"
_COVERAGE_VALID = "provider_directory_fhir_request_failure_coverage_valid"
_PROOF = "request_failure_coverage"


@lru_cache(maxsize=1)
def _partial():
    path = Path(__file__).with_name(
        "20260830100000_provider_directory_rooted_partial_lineage.py"
    )
    spec = importlib.util.spec_from_file_location("_fhir_failure_partial", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("FHIR request failure predecessor unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _q(identifier: str) -> str:
    return _partial()._q(identifier)


def _qf(schema: str, identifier: str) -> str:
    return _partial()._qf(schema, identifier)


def _ql(value: str) -> str:
    return _partial()._ql(value)


def _replace(sql: str, old: str, new: str, label: str) -> str:
    return _partial()._replace_once(sql, old, new, label)


def _function(sql: str) -> str:
    return _partial()._replace_function(sql, "request failure function")


def _coverage_valid_sql(schema: str) -> str:
    return f"""
    CREATE FUNCTION {_qf(schema, _COVERAGE_VALID)}(proof jsonb) RETURNS boolean
    LANGUAGE plpgsql IMMUTABLE SET search_path = pg_catalog AS $function$
    DECLARE field text;
    BEGIN
        IF proof IS NULL OR jsonb_typeof(proof) <> 'object' THEN RETURN FALSE; END IF;
        IF (SELECT count(*) FROM jsonb_object_keys(proof)) <> 6
           OR proof ->> 'policy_id' IS DISTINCT FROM {_ql(_POLICY)}
           OR proof ->> 'resource_coverage' IS DISTINCT FROM 'unknown' THEN
            RETURN FALSE;
        END IF;
        FOREACH field IN ARRAY ARRAY['total_requests', 'failed_requests',
                                     'rooted_total_requests', 'rooted_failed_requests']
        LOOP
            IF jsonb_typeof(proof -> field) IS DISTINCT FROM 'number'
               OR (proof ->> field) !~ '^(0|[1-9][0-9]{{0,18}})$'
               OR (proof ->> field)::numeric > 9007199254740991 THEN
                RETURN FALSE;
            END IF;
        END LOOP;
        RETURN (proof ->> 'total_requests')::numeric > 0
           AND (proof ->> 'failed_requests')::numeric > 0
           AND 50 * (proof ->> 'failed_requests')::numeric
                   < (proof ->> 'total_requests')::numeric
           AND (proof ->> 'rooted_failed_requests')::numeric
                   <= (proof ->> 'rooted_total_requests')::numeric
           AND (proof ->> 'rooted_total_requests')::numeric > 0
           AND (proof ->> 'rooted_total_requests')::numeric
                   < (proof ->> 'total_requests')::numeric
           AND (proof ->> 'rooted_failed_requests')::numeric
                   <= (proof ->> 'failed_requests')::numeric
           AND (proof ->> 'failed_requests')::numeric
                   - (proof ->> 'rooted_failed_requests')::numeric
               <= (proof ->> 'total_requests')::numeric
                   - (proof ->> 'rooted_total_requests')::numeric;
    END;
    $function$;
    """


def _coverage_lineage_sql(schema: str) -> str:
    """Bind inherited counts to the original cohort and exact retained parent."""

    rooted = _partial()._rooted()
    acquisition = _qf(schema, rooted._ACQUISITION)
    parent = _qf(schema, rooted._DATASET)
    cohort = _qf(schema, rooted._LEGACY_COHORT)
    legacy = _qf(schema, rooted._LEGACY_DATASET)
    header = _qf(schema, rooted._ROOTED_DATASET)
    return f"""
        SELECT official_cohort.npi_count, root_parent.publication_metadata_json::jsonb
                   AS metadata, root_header.cohort_complete
          INTO lineage
          FROM {acquisition} AS candidate
          JOIN {parent} AS root_parent
            ON root_parent.dataset_id = candidate.root_dataset_id
           AND root_parent.endpoint_id = candidate.root_endpoint_id
           AND root_parent.dataset_hash = candidate.root_dataset_hash
           AND root_parent.status IN ('published', 'superseded')
          JOIN (
                SELECT dataset_id, source_id, endpoint_id, publication_contract_id,
                       dataset_hash, cohort_id, resource_count,
                       terminal_set_sha256 AS content_proof, cohort_complete
                  FROM {legacy}
                UNION ALL
                SELECT dataset_id, source_id, endpoint_id, publication_contract_id,
                       dataset_hash, root_cohort_id, practitioner_resource_count,
                       root_content_proof_sha256, cohort_complete
                  FROM {header}
          ) AS root_header
            ON root_header.dataset_id = candidate.root_dataset_id
           AND root_header.source_id = candidate.root_source_id
           AND root_header.endpoint_id = candidate.root_endpoint_id
           AND root_header.publication_contract_id = candidate.root_publication_contract_id
           AND root_header.dataset_hash = candidate.root_dataset_hash
           AND root_header.cohort_id = candidate.root_cohort_id
           AND root_header.resource_count = candidate.root_resource_count
           AND root_header.content_proof = candidate.root_content_proof_sha256
          JOIN {cohort} AS official_cohort
            ON official_cohort.cohort_id = candidate.root_cohort_id
           AND official_cohort.contract_id = {_ql(rooted._OFFICIAL_COHORT_CONTRACT)}
           AND official_cohort.authority_id = candidate.source_authority_id
           AND official_cohort.official_source_id = {_ql(rooted._OFFICIAL_SOURCE_ID)}
           AND official_cohort.resource_type = 'Practitioner'
           AND official_cohort.cohort_complete IS TRUE
           AND official_cohort.endpoint_collection_complete IS FALSE
           AND official_cohort.endpoint_complete IS FALSE
         WHERE candidate.acquisition_id = target_acquisition_id;
    """


def _coverage_sql(schema: str) -> str:
    """Count unique terminal work and inherited requests, never transport attempts."""

    work = _qf(schema, _partial()._rooted()._WORK)
    return f"""
    CREATE FUNCTION {_qf(schema, _COVERAGE)}(target_acquisition_id text) RETURNS jsonb
    LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = pg_catalog
    AS $function$
    DECLARE
        lineage record; rooted_total bigint; rooted_failed bigint;
        incomplete bigint; invalid_errors bigint; inherited_failed bigint;
        proof jsonb;
    BEGIN
        {_coverage_lineage_sql(schema)}
        IF NOT FOUND OR lineage.npi_count IS NULL OR lineage.npi_count <= 0
           OR lineage.metadata -> 'cohort_complete' IS DISTINCT FROM
                   to_jsonb(lineage.cohort_complete)
           OR (lineage.metadata ? 'retry_exhausted_count' AND
               (jsonb_typeof(lineage.metadata -> 'retry_exhausted_count') <> 'number'
                OR (lineage.metadata ->> 'retry_exhausted_count')
                    !~ '^(0|[1-9][0-9]{{0,15}})$'
                OR (lineage.metadata ->> 'retry_exhausted_count')::numeric
                    > 9007199254740991)) THEN
            RAISE EXCEPTION 'provider_directory_fhir_request_failure_lineage_invalid'
                USING ERRCODE = '23514';
        END IF;
        inherited_failed := COALESCE(
            (lineage.metadata ->> 'retry_exhausted_count')::bigint, 0);
        IF inherited_failed > lineage.npi_count
           OR lineage.cohort_complete IS DISTINCT FROM (inherited_failed = 0) THEN
            RAISE EXCEPTION 'provider_directory_fhir_request_failure_lineage_invalid'
                USING ERRCODE = '23514';
        END IF;
        SELECT count(*), count(*) FILTER (WHERE status = 'error'),
               count(*) FILTER (WHERE status NOT IN ('completed', 'error')),
               count(*) FILTER (WHERE status = 'error'
                                 AND error_code IS DISTINCT FROM 'transport_timeout')
          INTO rooted_total, rooted_failed, incomplete, invalid_errors
          FROM {work} WHERE acquisition_id = target_acquisition_id;
        IF rooted_total = 0 OR incomplete <> 0 OR invalid_errors <> 0 THEN
            RAISE EXCEPTION 'provider_directory_fhir_request_failure_terminal_invalid'
                USING ERRCODE = '23514';
        END IF;
        IF rooted_failed + inherited_failed = 0 THEN RETURN NULL; END IF;
        proof := jsonb_build_object(
            'policy_id', {_ql(_POLICY)},
            'total_requests', lineage.npi_count + rooted_total,
            'failed_requests', inherited_failed + rooted_failed,
            'rooted_total_requests', rooted_total,
            'rooted_failed_requests', rooted_failed,
            'resource_coverage', 'unknown');
        IF {_qf(schema, _COVERAGE_VALID)}(proof) IS DISTINCT FROM TRUE THEN
            RAISE EXCEPTION 'provider_directory_fhir_request_failure_budget_exceeded'
                USING ERRCODE = '23514';
        END IF;
        RETURN proof;
    END;
    $function$;
    """


def _terminal_sql(row: str) -> str:
    return (
        f"({row}.status = 'completed' OR (NEW.{_PROOF} IS NOT NULL "
        f"AND {row}.status = 'error' AND {row}.error_code = 'transport_timeout'))"
    )


def _acquisition_guard_sql(schema: str) -> str:
    sql = _partial()._acquisition_guard_sql(schema, partial=True)
    old = "NEW.root_publication_contract_id =\n                            " + _ql(_partial()._rooted()._PUBLICATION_CONTRACT)
    sql = _replace(sql, old, "NEW.root_publication_contract_id IN (" +
                   _ql(_partial()._rooted()._PUBLICATION_CONTRACT) + ", " + _ql(_PUBLICATION) + ")",
                   "exact versioned current root")
    sql = _replace(sql, "IF TG_OP = 'INSERT' THEN", f"""IF TG_OP = 'INSERT' THEN
            IF NEW.{_PROOF} IS NOT NULL THEN
                RAISE EXCEPTION 'provider_directory_fhir_request_failure_insert_invalid'
                    USING ERRCODE = '23514';
            END IF;""", "new acquisition proof")
    sql = _replace(sql, "AND NEW.used_work_items >= OLD.used_work_items",
                   f"AND NEW.{_PROOF} IS NOT DISTINCT FROM OLD.{_PROOF}\n"
                   "           AND NEW.used_work_items >= OLD.used_work_items",
                   "building proof immutable")
    sql = _replace(sql, "OR NEW.rooted_graph_complete IS DISTINCT FROM TRUE",
                   "OR NEW.rooted_graph_complete IS DISTINCT FROM (NEW.error_count = 0)",
                   "sealed graph accuracy")
    sql = _replace(sql, "IF actual_pending <> 0 OR actual_leased <> 0 OR actual_error <> 0\n"
                   "           OR actual_completed IS DISTINCT FROM actual_work_count\n"
                   "           OR plan_count <> 1 OR plan_total IS NULL OR plan_pages IS NULL",
                   f"IF NEW.{_PROOF} IS DISTINCT FROM "
                   f"{_qf(schema, _COVERAGE)}(NEW.acquisition_id)\n"
                   "           OR actual_pending <> 0 OR actual_leased <> 0\n"
                   "           OR actual_completed + actual_error IS DISTINCT FROM actual_work_count\n"
                   "           OR plan_count <> 1",
                   "terminal budget and census")
    sql = _replace(sql, "AND kind = 'full_insurance_plan_census' AND status = 'completed';",
                   "AND kind = 'full_insurance_plan_census'\n"
                   "           AND (status = 'completed' OR\n"
                   "                (status = 'error' AND error_code = 'transport_timeout'));",
                   "timed out plan census")
    sql = _replace(sql, "max(advertised_total), max(terminal_page_count)",
                   "max(CASE WHEN status = 'completed' THEN advertised_total END), "
                   "max(CASE WHEN status = 'completed' THEN terminal_page_count END)",
                   "failed census is unknown not zero")
    for query_alias in ("query", "target_query", "affiliation_query"):
        old = f"AND {query_alias}.status = 'completed'"
        # Only the expected-work witnesses may be terminal failures. Retained
        # resource/edge inputs continue to require successful completed work.
        if query_alias == "query":
            old = "AND query.resource_type = 'PractitionerRole'\n               " + old
            new = "AND query.resource_type = 'PractitionerRole'\n               AND " + _terminal_sql(query_alias)
        else:
            new = "AND " + _terminal_sql(query_alias)
        sql = _replace(sql, old, new, f"{query_alias} terminal frontier")
    return sql


def _work_guard_sql(schema: str) -> str:
    sql = _partial()._rooted()._work_guard_sql(schema)
    # The proof is sealed only after the census; this transition may progress
    # past known timeout work, but the final source budget remains mandatory.
    for row in ("root_query", "target_query", "affiliation_query"):
        terminal = (f"({row}.status = 'completed' OR ({row}.status = 'error' "
                    f"AND {row}.error_code = 'transport_timeout'))")
        if row == "root_query":
            sql = _replace(sql, "root_query.status <> 'completed'", "NOT " + terminal,
                           "census excludes unfinished root work")
        sql = _replace(sql, f"AND {row}.status = 'completed'", "AND " + terminal,
                       f"census {row} terminal witness")
    return _function(sql)


def _single_root_guard_sql(schema: str) -> str:
    partial = _partial()
    sql = partial._single_root_guard_sql(schema, partial=True)
    sql = _replace(sql, "OR candidate.rooted_graph_complete IS DISTINCT FROM TRUE",
                   "OR candidate.rooted_graph_complete IS DISTINCT FROM (candidate.error_count = 0)",
                   "single graph accuracy")
    sql = _replace(sql, "OR candidate.error_count IS DISTINCT FROM 0",
                   f"OR NEW.{_PROOF} IS DISTINCT FROM candidate.{_PROOF}\n"
                   f"           OR NEW.{_PROOF} IS DISTINCT FROM "
                   f"{_qf(schema, _COVERAGE)}(candidate.acquisition_id)\n"
                   f"           OR (candidate.{_PROOF} IS NULL AND candidate.error_count <> 0)",
                   "single exact failure proof")
    for field in ("insurance_plan_count", "insurance_plan_page_count"):
        sql = _replace(sql, f"NEW.{field}::text", f"COALESCE(NEW.{field}::text, 'None')",
                       f"nullable {field} identity")
    canonical = partial._single_root()._canonical_json_function(schema)
    sql = _replace(sql, "NEW.acquisition_operation_key), 'UTF8'",
                   f"NEW.acquisition_operation_key, CASE WHEN NEW.{_PROOF} IS NULL "
                   f"THEN NULL ELSE {canonical}(NEW.{_PROOF}) END), 'UTF8'",
                   "single proof identity")
    return sql


def _metadata_sql(header: str, admission: str) -> str:
    historical = _partial()._publication_metadata_sql(header, admission)
    return f"""CASE WHEN {header}.{_PROOF} IS NULL THEN ({historical})
        ELSE (({historical}) || pg_catalog.jsonb_build_object(
            'request_failure_coverage', {header}.{_PROOF},
            'retry_exhausted_count', {_partial()._retry_count_sql('root_parent')},
            'rooted_graph_complete', {header}.rooted_graph_complete)) END"""


def _intrinsic_valid_sql(schema: str) -> str:
    partial = _partial()
    rooted = partial._rooted()
    sql = partial._intrinsic_valid_sql(schema, partial=True)
    sql = _replace(sql, partial._publication_metadata_sql("header", "admitted"),
                   _metadata_sql("header", "admitted"), "versioned closed metadata")
    sql = _replace(sql, f"AND header.publication_contract_id = {_ql(rooted._PUBLICATION_CONTRACT)}",
                   f"AND ((header.{_PROOF} IS NULL AND header.publication_contract_id = "
                   f"{_ql(rooted._PUBLICATION_CONTRACT)}) OR (header.{_PROOF} IS NOT NULL AND "
                   f"header.publication_contract_id = {_ql(_PUBLICATION)}))\n"
                   f"           AND header.{_PROOF} IS NOT DISTINCT FROM admitted.{_PROOF}\n"
                   f"           AND header.{_PROOF} IS NOT DISTINCT FROM candidate.{_PROOF}",
                   "publication exact proof branch")
    # Only the dataset identifier uses the selected publication contract; the
    # acquisition-root identifier deliberately retains its existing v1 contract.
    sql = _replace(sql, "pg_catalog.concat_ws(pg_catalog.chr(31), " + _ql(rooted._PUBLICATION_CONTRACT) + ",",
                   "pg_catalog.concat_ws(pg_catalog.chr(31), header.publication_contract_id,",
                   "versioned dataset identity")
    sql = _replace(sql, "AND header.rooted_graph_complete IS TRUE",
                   "AND header.rooted_graph_complete = (candidate.error_count = 0)",
                   "published graph accuracy")
    sql = _replace(sql, "AND candidate.rooted_graph_complete IS TRUE",
                   "AND candidate.rooted_graph_complete = header.rooted_graph_complete",
                   "sealed graph lineage")
    for column, header_column in (("insurance_plan_count", "census_insurance_plan_count"),
                                  ("insurance_plan_page_count", "insurance_plan_page_count")):
        sql = _replace(sql, f"AND admitted.{column} =\n               header.{header_column}",
                       f"AND admitted.{column} IS NOT DISTINCT FROM\n               header.{header_column}",
                       f"nullable published {column}")
    sql = _replace(sql, "header.root_publication_contract_id =\n                     " + _ql(rooted._PUBLICATION_CONTRACT),
                   "header.root_publication_contract_id IN (" + _ql(rooted._PUBLICATION_CONTRACT) + ", " + _ql(_PUBLICATION) + ")",
                   "versioned parent lineage")
    sql = _replace(sql, "AND root_header.source_id = header.root_source_id\n"
                   "                       AND root_header.endpoint_id = header.root_endpoint_id\n"
                   "                       AND root_header.dataset_hash = header.root_dataset_hash\n"
                   "                       AND root_header.practitioner_resource_count =",
                   "AND root_header.source_id = header.root_source_id\n"
                   "                       AND root_header.endpoint_id = header.root_endpoint_id\n"
                   "                       AND root_header.dataset_hash = header.root_dataset_hash\n"
                   "                       AND root_header.publication_contract_id = header.root_publication_contract_id\n"
                   "                       AND root_header.practitioner_resource_count =",
                   "parent exact publication version")
    for reference_column in ("network_id", "organization_id"):
        anchor = f"AND organization.resource_id = expected.{reference_column}\n                 WHERE organization.resource_id IS NULL"
        witness = f"""{anchor}
                   AND NOT (header.{_PROOF} IS NOT NULL AND EXISTS (
                       SELECT 1 FROM {_qf(schema, rooted._WORK)} AS failed_target
                        WHERE failed_target.acquisition_id = header.publication_acquisition_id
                          AND failed_target.kind = 'direct_read'
                          AND failed_target.reference_type = 'Organization'
                          AND failed_target.reference_id = expected.{reference_column}
                          AND failed_target.closure_scope IN ('root', 'plan')
                          AND failed_target.status = 'error'
                          AND failed_target.error_code = 'transport_timeout'))"""
        sql = _replace(sql, anchor, witness, f"exact missing {reference_column} timeout")
    return sql


def _header_guard_sql(schema: str) -> str:
    sql = _partial()._rooted()._rooted_header_guard_sql(schema)
    sql = _replace(sql, "IF ROW(NEW.dataset_id,", f"IF NEW.{_PROOF} IS DISTINCT FROM OLD.{_PROOF}\n"
                   "           OR ROW(NEW.dataset_id,", "published proof immutable")
    return _function(sql)


def _twin_guards_sql(schema: str) -> tuple[str, str]:
    rooted = _partial()._rooted()
    attempt = _replace(rooted._twin_attempt_guard_sql(schema),
                       "OR first_root.rooted_graph_complete IS NOT TRUE",
                       "OR first_root.rooted_graph_complete IS NOT TRUE\n"
                       f"           OR first_root.{_PROOF} IS NOT NULL\n"
                       f"           OR second_root.{_PROOF} IS NOT NULL", "legacy twin root proof fence")
    admission = _replace(rooted._twin_admission_guard_sql(schema),
                         "OR candidate.acquisition_id IS NULL",
                         "OR candidate.acquisition_id IS NULL\n"
                         f"           OR candidate.{_PROOF} IS NOT NULL\n"
                         f"           OR baseline.{_PROOF} IS NOT NULL\n"
                         f"           OR NEW.{_PROOF} IS NOT NULL", "legacy twin admission proof fence")
    return _function(attempt), _function(admission)


def _flex_guard_sql(schema: str, *, single: bool) -> str:
    retry = _partial()._retry()
    if single:
        sql = retry._single_root_guard_sql(schema, partial=True)
        return _replace(sql, "OR candidate.cohort_complete IS DISTINCT FROM (candidate.error_count = 0)",
                        "OR candidate.cohort_complete IS DISTINCT FROM (candidate.error_count = 0)\n"
                        "           OR candidate.error_count::numeric * 50 >= candidate.expected_npi_count",
                        "fresh Flex admission budget")
    sql = retry._acquisition_guard_sql(schema, partial=True)
    return _replace(sql, "IF actual_member_count IS DISTINCT FROM NEW.expected_npi_count",
                    "IF actual_error_count::numeric * 50 >= NEW.expected_npi_count\n"
                    "           OR actual_member_count IS DISTINCT FROM NEW.expected_npi_count", "fresh Flex seal budget")


def _check_expression(name: str) -> str:
    return next(expression for _, constraint, expression
                in _partial()._rooted()._owned_check_constraints("unused")
                if constraint == name)


def _acquisition_check_sql(schema: str) -> str:
    name = "provider_directory_rooted_graph_acquisition_state_check"
    original = _check_expression(name)
    partial = _replace(original, "rooted_graph_complete IS TRUE", "rooted_graph_complete = (error_count = 0)",
                       "partial state graph")
    partial = _replace(partial, "error_count = 0 AND resource_count", "error_count >= 0 AND resource_count",
                       "partial state errors")
    partial = _replace(partial, "insurance_plan_count >= 0 AND insurance_plan_page_count > 0",
                       "((insurance_plan_count IS NULL AND insurance_plan_page_count IS NULL AND error_count > 0) "
                       "OR (insurance_plan_count IS NOT NULL AND insurance_plan_page_count IS NOT NULL "
                       "AND insurance_plan_count >= 0 AND insurance_plan_page_count > 0))",
                       "partial census unknown")
    expression = (f"(({_PROOF} IS NULL AND ({original})) OR ({_PROOF} IS NOT NULL AND status = 'sealed' "
                  f"AND {_qf(schema, _COVERAGE_VALID)}({_PROOF}) AND ({partial}) "
                  f"AND ({_PROOF} ->> 'rooted_total_requests')::bigint = completed_count + error_count "
                  f"AND ({_PROOF} ->> 'rooted_failed_requests')::bigint = error_count))")
    return _constraint_sql(schema, _partial()._rooted()._ACQUISITION, name, expression)


def _acquisition_identity_check_sql(schema: str) -> str:
    name = "provider_directory_rooted_graph_acquisition_identity_check"
    old = "root_publication_contract_id = " + _ql(_partial()._rooted()._PUBLICATION_CONTRACT)
    expression = _replace(_check_expression(name), old, "root_publication_contract_id IN (" +
                          _ql(_partial()._rooted()._PUBLICATION_CONTRACT) + ", " + _ql(_PUBLICATION) + ")",
                          "versioned root identity constraint")
    return _constraint_sql(schema, _partial()._rooted()._ACQUISITION, name, expression)


def _admission_check_sql(schema: str) -> str:
    sql = _partial()._single_root()._rooted_check_sql(schema, historical=False)
    return _replace(sql, "CHECK (admission_id", f"CHECK ((({_PROOF} IS NULL AND insurance_plan_count IS NOT NULL "
                    "AND insurance_plan_page_count IS NOT NULL) OR "
                    f"({_PROOF} IS NOT NULL AND {_qf(schema, _COVERAGE_VALID)}({_PROOF}) AND "
                    f"admission_contract_id = {_ql(_partial()._single_root()._ROOTED_SINGLE_CONTRACT)})) "
                    "AND admission_id", "single only proof")


def _dataset_check_sql(schema: str) -> str:
    rooted = _partial()._rooted()
    sql = _partial()._dataset_check_sql(schema, partial=True)
    sql = _replace(sql, f"CHECK (publication_contract_id = {_ql(rooted._PUBLICATION_CONTRACT)} AND ",
                   f"CHECK ((({_PROOF} IS NULL AND publication_contract_id = {_ql(rooted._PUBLICATION_CONTRACT)} "
                   "AND census_insurance_plan_count IS NOT NULL AND insurance_plan_page_count IS NOT NULL) "
                   f"OR ({_PROOF} IS NOT NULL AND publication_contract_id = {_ql(_PUBLICATION)} "
                   f"AND {_qf(schema, _COVERAGE_VALID)}({_PROOF}))) AND ",
                   "publication constraint version")
    sql = _replace(sql, "rooted_graph_complete IS TRUE", f"rooted_graph_complete = "
                   f"(COALESCE(({_PROOF} ->> 'rooted_failed_requests')::bigint, 0) = 0)",
                   "publication graph flag")
    sql = _replace(sql, "completed_count = used_work_items", f"completed_count + "
                   f"COALESCE(({_PROOF} ->> 'rooted_failed_requests')::bigint, 0) = used_work_items",
                   "publication terminal conservation")
    sql = _replace(sql, "root_publication_contract_id = " + _ql(rooted._PUBLICATION_CONTRACT),
                   "root_publication_contract_id IN (" + _ql(rooted._PUBLICATION_CONTRACT) + ", " + _ql(_PUBLICATION) + ")",
                   "versioned root publication constraint")
    return sql


def _constraint_sql(schema: str, table: str, name: str, expression: str) -> str:
    return f"ALTER TABLE {_qf(schema, table)} ADD CONSTRAINT {_q(name)} CHECK ({expression}) NOT VALID;"


def _replace_checks(schema: str, *, enabled: bool) -> None:
    rooted = _partial()._rooted()
    for table, name, statement in (
        (rooted._ACQUISITION, "provider_directory_rooted_graph_acquisition_identity_check",
         _acquisition_identity_check_sql(schema) if enabled else _constraint_sql(
             schema, rooted._ACQUISITION, "provider_directory_rooted_graph_acquisition_identity_check",
             _check_expression("provider_directory_rooted_graph_acquisition_identity_check"))),
        (rooted._ACQUISITION, "provider_directory_rooted_graph_acquisition_state_check",
         _acquisition_check_sql(schema) if enabled else _constraint_sql(
             schema, rooted._ACQUISITION, "provider_directory_rooted_graph_acquisition_state_check",
             _check_expression("provider_directory_rooted_graph_acquisition_state_check"))),
        (rooted._TWIN_ADMISSION, "pd_rooted_graph_twin_admission_check",
         _admission_check_sql(schema) if enabled else _partial()._single_root()._rooted_check_sql(schema, historical=False)),
        (rooted._ROOTED_DATASET, "pd_rooted_graph_dataset_check",
         _dataset_check_sql(schema) if enabled else _partial()._dataset_check_sql(schema, partial=True)),
    ):
        op.execute(f"ALTER TABLE {_qf(schema, table)} DROP CONSTRAINT {_q(name)};")
        op.execute(statement)
        op.execute(f"ALTER TABLE {_qf(schema, table)} VALIDATE CONSTRAINT {_q(name)};")


def _lock_sql(schema: str) -> str:
    rooted = _partial()._rooted()
    retry = _partial()._retry()
    tables = (retry._acquisition()._ACQUISITION, retry._acquisition()._WORK,
              retry._single_root()._flex_admission()._ADMISSION,
              rooted._LEGACY_COHORT, rooted._LEGACY_DATASET, rooted._ACQUISITION,
              rooted._WORK, rooted._RESOURCE, rooted._EDGE,
              rooted._TWIN_ATTEMPT, rooted._TWIN_ADMISSION, rooted._ROOTED_DATASET, rooted._DATASET)
    return "LOCK TABLE " + ", ".join(_qf(schema, table) for table in tables) + " IN ACCESS EXCLUSIVE MODE;"


def _columns(schema: str, *, enabled: bool) -> None:
    rooted = _partial()._rooted()
    for table in (rooted._ACQUISITION, rooted._TWIN_ADMISSION, rooted._ROOTED_DATASET):
        action = f"ADD COLUMN {_PROOF} jsonb" if enabled else f"DROP COLUMN {_PROOF}"
        op.execute(f"ALTER TABLE {_qf(schema, table)} {action};")
    for table, count in ((rooted._TWIN_ADMISSION, "insurance_plan_count"),
                         (rooted._ROOTED_DATASET, "census_insurance_plan_count")):
        action = "DROP NOT NULL" if enabled else "SET NOT NULL"
        for column in (count, "insurance_plan_page_count"):
            op.execute(f"ALTER TABLE {_qf(schema, table)} ALTER COLUMN {column} {action};")


def upgrade() -> None:
    """Install new proof gates atomically without rewriting historical receipts."""

    schema = _partial()._rooted()._schema()
    op.execute("SET LOCAL lock_timeout = '5s';")
    op.execute(_lock_sql(schema))
    _columns(schema, enabled=True)
    op.execute(_coverage_valid_sql(schema))
    op.execute(_coverage_sql(schema))
    op.execute(f"REVOKE ALL ON FUNCTION {_qf(schema, _COVERAGE)}(text) FROM PUBLIC;")
    op.execute(f"REVOKE ALL ON FUNCTION {_qf(schema, _COVERAGE_VALID)}(jsonb) FROM PUBLIC;")
    _replace_checks(schema, enabled=True)
    for builder in (_acquisition_guard_sql, _work_guard_sql, _single_root_guard_sql,
                    _intrinsic_valid_sql, _header_guard_sql):
        op.execute(builder(schema))
    op.execute(_flex_guard_sql(schema, single=False))
    op.execute(_flex_guard_sql(schema, single=True))
    for sql in _twin_guards_sql(schema):
        op.execute(sql)


def downgrade() -> None:
    """Restore original functions only when no new partial evidence exists."""

    schema = _partial()._rooted()._schema()
    rooted = _partial()._rooted()
    op.execute("SET LOCAL lock_timeout = '5s';")
    op.execute(_lock_sql(schema))
    conditions = " OR ".join(
        f"EXISTS (SELECT 1 FROM {_qf(schema, table)} WHERE {_PROOF} IS NOT NULL)"
        for table in (rooted._ACQUISITION, rooted._TWIN_ADMISSION, rooted._ROOTED_DATASET)
    )
    op.execute(f"""DO $fence$ BEGIN IF {conditions} THEN
        RAISE EXCEPTION 'provider_directory_fhir_request_failure_downgrade_blocked'
            USING ERRCODE = '55000'; END IF; END; $fence$;""")
    _replace_checks(schema, enabled=False)
    for builder in (_partial()._acquisition_guard_sql, _partial()._single_root_guard_sql,
                    _partial()._intrinsic_valid_sql):
        op.execute(builder(schema, partial=True))
    op.execute(_function(rooted._rooted_header_guard_sql(schema)))
    op.execute(_function(rooted._work_guard_sql(schema)))
    op.execute(_function(rooted._twin_attempt_guard_sql(schema)))
    op.execute(_function(rooted._twin_admission_guard_sql(schema)))
    op.execute(_partial()._retry()._acquisition_guard_sql(schema, partial=True))
    op.execute(_partial()._retry()._single_root_guard_sql(schema, partial=True))
    op.execute(f"DROP FUNCTION {_qf(schema, _COVERAGE)}(text);")
    _columns(schema, enabled=False)
    op.execute(f"DROP FUNCTION {_qf(schema, _COVERAGE_VALID)}(jsonb);")
