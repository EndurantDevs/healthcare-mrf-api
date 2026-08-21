"""Authenticate durable empty allowed-amount terminal receipts.

Revision ID: 20260821010000_ptg_ordinary_terminal_blank_receipt
Revises: 20260820140000_prescription_autocomplete_rollup
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260821010000_ptg_ordinary_terminal_blank_receipt"
down_revision = "20260820140000_prescription_autocomplete_rollup"
branch_labels = None
depends_on = None

_FUNCTION = "ptg_wave_ordinary_terminal_receipt_guard"
_ERROR = "PTG2 allowed-amount import produced no payment evidence"
_OLD_OUTER_ERROR = (
    "(ordinary_run.error IS NOT NULL "
    "AND ordinary_run.error::jsonb IS DISTINCT FROM 'null'::jsonb)"
)
_NEW_OUTER_ERROR = f"""(
                    CASE run_metrics->>'status'
                        WHEN 'blank' THEN ordinary_run.error::jsonb
                            IS DISTINCT FROM jsonb_build_object(
                                'code', 'ptg_import_failed',
                                'message', '{_ERROR}'
                            )
                        ELSE ordinary_run.error IS NOT NULL
                            AND ordinary_run.error::jsonb
                                IS DISTINCT FROM 'null'::jsonb
                    END
                )"""
_OLD_RUN_STATUS = "run_metrics->>'status' IS DISTINCT FROM 'succeeded'"
_NEW_RUN_STATUS = """(
                    run_metrics->>'status' NOT IN ('succeeded', 'blank')
                    OR (
                        run_metrics->>'status' = 'blank'
                        AND (
                            direct_input->>'source_type'
                                IS DISTINCT FROM 'allowed_amounts'
                            OR run_metrics->'file_domains'
                                IS DISTINCT FROM '["allowed_amounts"]'::jsonb
                            OR run_metrics->'allowed_amount_evidence'
                                IS DISTINCT FROM 'false'::jsonb
                            OR run_metrics->'files_attempted'
                                IS DISTINCT FROM '1'::jsonb
                            OR run_metrics->'files_processed'
                                IS DISTINCT FROM '1'::jsonb
                            OR run_metrics->'files_failed'
                                IS DISTINCT FROM '0'::jsonb
                            OR run_metrics->'files_skipped'
                                IS DISTINCT FROM '0'::jsonb
                            OR run_metrics->'allowed_amount_payments'
                                IS DISTINCT FROM '0'::jsonb
                            OR run_metrics->'allowed_amount_provider_payments'
                                IS DISTINCT FROM '0'::jsonb
                        )
                    )
                )"""
_OLD_ENGINE_STATUS = "durable_run.status IS DISTINCT FROM 'validated'"
_NEW_ENGINE_STATUS = """durable_run.status IS DISTINCT FROM (CASE
                    WHEN run_metrics->>'status' = 'blank' THEN 'failed'
                    ELSE 'validated'
                END)"""
_OLD_ENGINE_ERROR = "durable_run.error IS NOT NULL"
_NEW_ENGINE_ERROR = f"""durable_run.error IS DISTINCT FROM (CASE
                    WHEN run_metrics->>'status' = 'blank' THEN '{_ERROR}'
                    ELSE NULL
                END)"""
_OLD_SNAPSHOT_STATUS = (
    "durable_snapshot.status NOT IN ('validated', 'published')"
)
_NEW_SNAPSHOT_STATUS = """(
                    CASE WHEN run_metrics->>'status' = 'blank'
                        THEN durable_snapshot.status IS DISTINCT FROM 'failed'
                        ELSE durable_snapshot.status
                            NOT IN ('validated', 'published')
                    END
                )"""
_OLD_SNAPSHOT_BINDING = """run_metrics->>'snapshot_status'
                    IS DISTINCT FROM durable_snapshot.status"""
_NEW_SNAPSHOT_BINDING = f"""(
                    run_metrics->>'snapshot_status'
                        IS DISTINCT FROM durable_snapshot.status
                    OR (
                        run_metrics->>'status' = 'blank'
                        AND (
                            snapshot_manifest->>'snapshot_id'
                                IS DISTINCT FROM expected_snapshot_id
                            OR snapshot_manifest->>'error'
                                IS DISTINCT FROM '{_ERROR}'
                            OR snapshot_manifest->'allowed_amount_lane'
                                IS DISTINCT FROM
                                    engine_report->'allowed_amount_lane'
                            OR NOT (
                                engine_report @? '$.allowed_amount_lane ? (
                                    @.files_attempted == 1 &&
                                    @.files_processed == 1 &&
                                    @.files_failed == 0 &&
                                    @.files_skipped == 0 &&
                                    @.failed_files.size() == 0 &&
                                    @.successful_files.size() == 1 &&
                                    @.successful_files[0].source_type
                                        == "allowed_amounts" &&
                                    @.successful_files[0].success == true &&
                                    @.successful_files[0].skipped == false &&
                                    @.successful_files[0].error == null &&
                                    @.successful_files[0].summary
                                        .allowed_amount_evidence == false &&
                                    @.successful_files[0].summary
                                        .allowed_amount_payments == 0 &&
                                    @.successful_files[0].summary
                                        .allowed_amount_provider_payments == 0
                                )'
                            )
                            OR (
                                engine_report #>> '{{allowed_amount_lane,
                                    successful_files,0,summary,
                                    allowed_amount_plans}}'
                                ~ '^(0|[1-9][0-9]*)$'
                            ) IS DISTINCT FROM true
                            OR (
                                engine_report #>> '{{allowed_amount_lane,
                                    successful_files,0,summary,
                                    allowed_amount_items}}'
                                ~ '^(0|[1-9][0-9]*)$'
                            ) IS DISTINCT FROM true
                            OR (
                                engine_report #>> '{{allowed_amount_lane,
                                    successful_files,0,summary,
                                    allowed_amount_blocks}}'
                                ~ '^(0|[1-9][0-9]*)$'
                            ) IS DISTINCT FROM true
                            OR (
                                engine_report #>> '{{allowed_amount_lane,
                                    successful_files,0,summary,
                                    allowed_amount_npi_references}}'
                                ~ '^(0|[1-9][0-9]*)$'
                            ) IS DISTINCT FROM true
                            OR (
                                engine_report #>> '{{allowed_amount_lane,
                                    successful_files,0,summary,
                                    allowed_amount_unique_tins}}'
                                ~ '^(0|[1-9][0-9]*)$'
                            ) IS DISTINCT FROM true
                        )
                    )
                )"""
_OLD_TERMINAL_STATUS = """'status', 'succeeded',
                'engine_result_status', 'validated',"""
_NEW_TERMINAL_STATUS = """'status', run_metrics->>'status',
                'engine_result_status', CASE
                    WHEN run_metrics->>'status' = 'blank' THEN 'failed'
                    ELSE 'validated'
                END,"""
_REPLACEMENTS = (
    (
        "ordinary_run.status IS DISTINCT FROM 'succeeded'",
        """ordinary_run.status IS DISTINCT FROM (CASE
                    WHEN run_metrics->>'status' = 'blank' THEN 'failed'
                    ELSE 'succeeded'
                END)""",
    ),
    (_OLD_OUTER_ERROR, _NEW_OUTER_ERROR),
    (_OLD_RUN_STATUS, _NEW_RUN_STATUS),
    (_OLD_ENGINE_STATUS, _NEW_ENGINE_STATUS),
    (_OLD_ENGINE_ERROR, _NEW_ENGINE_ERROR),
    (_OLD_SNAPSHOT_STATUS, _NEW_SNAPSHOT_STATUS),
    (_OLD_SNAPSHOT_BINDING, _NEW_SNAPSHOT_BINDING),
    (_OLD_TERMINAL_STATUS, _NEW_TERMINAL_STATUS),
)


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError(
            "DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema"
        )
    return runtime_schema or legacy_schema or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _literal(fragment: str) -> str:
    return "'" + fragment.replace("'", "''") + "'"


def _replacement_sql(*, upgrade: bool) -> str:
    replacements = _REPLACEMENTS if upgrade else tuple(
        (new, old) for old, new in reversed(_REPLACEMENTS)
    )
    patches = []
    for old_fragment, new_fragment in replacements:
        patches.append(f"""
        old_fragment := {_literal(old_fragment)};
        new_fragment := {_literal(new_fragment)};
        IF pg_catalog.length(definition)
                - pg_catalog.length(pg_catalog.replace(
                    definition, old_fragment, ''
                )) <> pg_catalog.length(old_fragment)
           OR pg_catalog.strpos(definition, new_fragment) <> 0 THEN
            RAISE EXCEPTION
                'PTG_ORDINARY_TERMINAL_BLANK_PATCH_PRECONDITION_FAILED'
                USING ERRCODE = 'P0001';
        END IF;
        definition := pg_catalog.replace(
            definition, old_fragment, new_fragment
        );
        """)
    signature = f"{_q(_schema())}.{_q(_FUNCTION)}()"
    return f"""
    DO $migration$
    DECLARE
        definition text;
        old_fragment text;
        new_fragment text;
    BEGIN
        SELECT pg_catalog.pg_get_functiondef(
            pg_catalog.to_regprocedure({_literal(signature)})
        ) INTO definition;
        IF definition IS NULL THEN
            RAISE EXCEPTION
                'PTG_ORDINARY_TERMINAL_BLANK_PATCH_PRECONDITION_FAILED'
                USING ERRCODE = 'P0001';
        END IF;
        {''.join(patches)}
        EXECUTE definition;
    END;
    $migration$
    """


def upgrade() -> None:
    """Accept only signed failed results with exact empty-payment evidence."""

    op.execute(_replacement_sql(upgrade=True))


def downgrade() -> None:
    """Restore the success-only guard when no blank receipt is durable."""

    receipt_table = (
        f"{_q(_schema())}."
        f"{_q('ptg_import_wave_ordinary_terminal_receipt')}"
    )
    op.execute(f"""
        DO $downgrade$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM {receipt_table}
                 WHERE receipt #>> '{{payload,terminal_result,status}}'
                    = 'blank'
            ) THEN
                RAISE EXCEPTION
                    'PTG_ORDINARY_TERMINAL_BLANK_DOWNGRADE_BLOCKED'
                    USING ERRCODE = 'P0001';
            END IF;
        END;
        $downgrade$
    """)
    op.execute(_replacement_sql(upgrade=False))
