# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Adopt snapshot-effective NPPES lifecycle dates.

Revision ID: 20260809020000_nppes_lifecycle_date_tolerance
Revises: 20260809010000_provider_directory_effective_endpoint_identity
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260809020000_nppes_lifecycle_date_tolerance"
down_revision = "20260809010000_provider_directory_effective_endpoint_identity"
branch_labels = None
depends_on = None


_ADMISSION = "public_evidence_nppes_registry_admission"
_ADMISSION_SEAL = "public_evidence_nppes_registry_admission_seal"
_MEMBER = "public_evidence_nppes_registry_member"
_OLD_VALIDATOR = "validate_public_evidence_nppes_registry_admission"
_NEW_VALIDATOR = (
    "validate_public_evidence_nppes_registry_admission_lifecycle_v2"
)

_OLD_MINIMUM_EFFECTIVE_DATE = """                   min(CASE
                       WHEN npi_deactivation_date IS NOT NULL
                        AND npi_reactivation_date IS NULL
                       THEN npi_deactivation_date
                       ELSE COALESCE(
                           npi_reactivation_date,
                           provider_enumeration_date)
                   END),"""
_NEW_MINIMUM_EFFECTIVE_DATE = """                   min(CASE
                       WHEN npi_deactivation_date IS NOT NULL
                        AND npi_deactivation_date <=
                            (admitted.snapshot_at AT TIME ZONE 'UTC')::date
                        AND (npi_reactivation_date IS NULL
                             OR npi_reactivation_date >
                                (admitted.snapshot_at AT TIME ZONE 'UTC')::date
                             OR npi_reactivation_date < npi_deactivation_date)
                       THEN npi_deactivation_date
                       WHEN npi_reactivation_date IS NOT NULL
                        AND npi_reactivation_date <=
                            (admitted.snapshot_at AT TIME ZONE 'UTC')::date
                       THEN npi_reactivation_date
                       ELSE provider_enumeration_date
                   END),"""

_OLD_ENUMERATION_STATE = """                            OR typed_row.enumeration_state IS DISTINCT FROM CASE
                                WHEN member_row.npi_deactivation_date IS NOT NULL
                                 AND member_row.npi_reactivation_date IS NULL
                                THEN 'deactivated' ELSE 'active' END"""
_NEW_ENUMERATION_STATE = """                            OR typed_row.enumeration_state IS DISTINCT FROM CASE
                                WHEN member_row.npi_deactivation_date IS NOT NULL
                                 AND member_row.npi_deactivation_date <=
                                    (admitted.snapshot_at AT TIME ZONE 'UTC')::date
                                 AND (member_row.npi_reactivation_date IS NULL
                                      OR member_row.npi_reactivation_date >
                                         (admitted.snapshot_at AT TIME ZONE 'UTC')::date
                                      OR member_row.npi_reactivation_date <
                                         member_row.npi_deactivation_date)
                                THEN 'deactivated' ELSE 'active' END"""

_OLD_COMMON_EFFECTIVE_START = """                                (CASE WHEN member_row.npi_deactivation_date IS NOT NULL
                                           AND member_row.npi_reactivation_date IS NULL
                                    THEN member_row.npi_deactivation_date
                                    ELSE COALESCE(member_row.npi_reactivation_date,
                                                  member_row.provider_enumeration_date)
                                 END)::timestamp AT TIME ZONE 'UTC'"""
_NEW_COMMON_EFFECTIVE_START = """                                (CASE
                                    WHEN member_row.npi_deactivation_date IS NOT NULL
                                     AND member_row.npi_deactivation_date <=
                                        (admitted.snapshot_at AT TIME ZONE 'UTC')::date
                                     AND (member_row.npi_reactivation_date IS NULL
                                          OR member_row.npi_reactivation_date >
                                             (admitted.snapshot_at AT TIME ZONE 'UTC')::date
                                          OR member_row.npi_reactivation_date <
                                             member_row.npi_deactivation_date)
                                    THEN member_row.npi_deactivation_date
                                    WHEN member_row.npi_reactivation_date IS NOT NULL
                                     AND member_row.npi_reactivation_date <=
                                        (admitted.snapshot_at AT TIME ZONE 'UTC')::date
                                    THEN member_row.npi_reactivation_date
                                    ELSE member_row.provider_enumeration_date
                                 END)::timestamp AT TIME ZONE 'UTC'"""

_OLD_TEMPORAL_VALIDATION = """                        OR member_row.provider_enumeration_date >
                            (admitted.snapshot_at AT TIME ZONE 'UTC')::date
                        OR member_row.last_update_date >
                            (admitted.snapshot_at AT TIME ZONE 'UTC')::date
                        OR member_row.npi_deactivation_date >
                            (admitted.snapshot_at AT TIME ZONE 'UTC')::date
                        OR member_row.npi_reactivation_date >
                            (admitted.snapshot_at AT TIME ZONE 'UTC')::date
                        OR (member_row.npi_reactivation_date IS NOT NULL AND
                            (member_row.npi_deactivation_date IS NULL OR
                             member_row.npi_reactivation_date <=
                                member_row.npi_deactivation_date))
                        OR (member_row.provider_enumeration_date IS NOT NULL
                            AND member_row.npi_deactivation_date IS NOT NULL
                            AND member_row.provider_enumeration_date >
                                member_row.npi_deactivation_date)
                        OR (member_row.provider_enumeration_date IS NOT NULL
                            AND member_row.npi_reactivation_date IS NOT NULL
                            AND member_row.provider_enumeration_date >
                                member_row.npi_reactivation_date)"""
_NEW_TEMPORAL_VALIDATION = """                        OR member_row.provider_enumeration_date >
                            (admitted.snapshot_at AT TIME ZONE 'UTC')::date
                        OR (member_row.provider_enumeration_date IS NOT NULL
                            AND member_row.npi_deactivation_date IS NOT NULL
                            AND member_row.npi_deactivation_date <=
                                (admitted.snapshot_at AT TIME ZONE 'UTC')::date
                            AND member_row.provider_enumeration_date >
                                member_row.npi_deactivation_date)
                        OR (member_row.provider_enumeration_date IS NOT NULL
                            AND member_row.npi_reactivation_date IS NOT NULL
                            AND member_row.npi_reactivation_date <=
                                (admitted.snapshot_at AT TIME ZONE 'UTC')::date
                            AND member_row.provider_enumeration_date >
                                member_row.npi_reactivation_date)"""

_OLD_EXCLUSION_EFFECTIVE_START = """                            WHEN (CASE WHEN member_row.npi_deactivation_date IS NOT NULL
                                           AND member_row.npi_reactivation_date IS NULL
                                      THEN member_row.npi_deactivation_date
                                      ELSE COALESCE(member_row.npi_reactivation_date,
                                                    member_row.provider_enumeration_date)
                                  END) IS NULL"""
_NEW_EXCLUSION_EFFECTIVE_START = """                            WHEN (CASE
                                      WHEN member_row.npi_deactivation_date IS NOT NULL
                                       AND member_row.npi_deactivation_date <=
                                          (admitted.snapshot_at AT TIME ZONE 'UTC')::date
                                       AND (member_row.npi_reactivation_date IS NULL
                                            OR member_row.npi_reactivation_date >
                                               (admitted.snapshot_at AT TIME ZONE 'UTC')::date
                                            OR member_row.npi_reactivation_date <
                                               member_row.npi_deactivation_date)
                                      THEN member_row.npi_deactivation_date
                                      WHEN member_row.npi_reactivation_date IS NOT NULL
                                       AND member_row.npi_reactivation_date <=
                                          (admitted.snapshot_at AT TIME ZONE 'UTC')::date
                                      THEN member_row.npi_reactivation_date
                                      ELSE member_row.provider_enumeration_date
                                  END) IS NULL"""

_UPGRADE_REPLACEMENTS = (
    (_OLD_MINIMUM_EFFECTIVE_DATE, _NEW_MINIMUM_EFFECTIVE_DATE),
    (_OLD_ENUMERATION_STATE, _NEW_ENUMERATION_STATE),
    (_OLD_COMMON_EFFECTIVE_START, _NEW_COMMON_EFFECTIVE_START),
    (_OLD_TEMPORAL_VALIDATION, _NEW_TEMPORAL_VALIDATION),
    (_OLD_EXCLUSION_EFFECTIVE_START, _NEW_EXCLUSION_EFFECTIVE_START),
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


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def _qf(schema: str, function: str) -> str:
    return f"{_q(schema)}.{_q(function)}"


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _replacement_sql(
    replacements: tuple[tuple[str, str], ...],
) -> str:
    statements: list[str] = []
    for old_fragment, new_fragment in replacements:
        statements.append(
            "IF pg_catalog.length(definition) - "
            "pg_catalog.length(pg_catalog.replace(definition, "
            f"{_literal(old_fragment)}, '')) <> "
            f"pg_catalog.length({_literal(old_fragment)}) "
            f"OR pg_catalog.strpos(definition, {_literal(new_fragment)}) <> 0 THEN\n"
            "    RAISE EXCEPTION 'nppes_lifecycle_validator_contract_unexpected' "
            "USING ERRCODE='55000';\n"
            "END IF;\n"
            "definition := pg_catalog.replace(definition, "
            f"{_literal(old_fragment)}, {_literal(new_fragment)});"
        )
    return "\n".join(statements)


def _rewrite_validator_sql(
    schema: str,
    current_name: str,
    target_name: str,
    replacements: tuple[tuple[str, str], ...],
) -> str:
    signature = f"{_qf(schema, current_name)}()"
    return f"""
    DO $migration$
    DECLARE
        definition text;
    BEGIN
        SELECT pg_catalog.pg_get_functiondef(
            pg_catalog.to_regprocedure({_literal(signature)})
        ) INTO definition;
        IF definition IS NULL THEN
            RAISE EXCEPTION 'nppes_lifecycle_validator_contract_unexpected'
                USING ERRCODE='55000';
        END IF;
        {_replacement_sql(replacements)}
        EXECUTE definition;
        ALTER FUNCTION {_qf(schema, current_name)}() RENAME TO {_q(target_name)};
    END;
    $migration$;
    """


def _require_empty_downgrade(schema: str) -> None:
    admission = _qt(schema, _ADMISSION)
    seal = _qt(schema, _ADMISSION_SEAL)
    member = _qt(schema, _MEMBER)
    op.execute(
        f"LOCK TABLE {admission}, {seal}, {member} "
        "IN SHARE ROW EXCLUSIVE MODE;"
    )
    op.execute(
        f"""
        DO $migration$
        BEGIN
            IF EXISTS (SELECT 1 FROM {admission})
               OR EXISTS (SELECT 1 FROM {seal})
               OR EXISTS (SELECT 1 FROM {member}) THEN
                RAISE EXCEPTION 'nppes_lifecycle_downgrade_requires_empty_admission'
                    USING ERRCODE='55000';
            END IF;
        END;
        $migration$;
        """
    )


def _rewrite_validator(
    schema: str,
    current_name: str,
    target_name: str,
    replacements: tuple[tuple[str, str], ...],
) -> None:
    op.execute(
        _rewrite_validator_sql(
            schema,
            current_name,
            target_name,
            replacements,
        )
    )
    op.execute(
        f"REVOKE ALL ON FUNCTION {_qf(schema, target_name)}() FROM PUBLIC;"
    )


def upgrade() -> None:
    """Adopt snapshot-effective lifecycle events without rewriting evidence."""

    _rewrite_validator(
        _schema(),
        _OLD_VALIDATOR,
        _NEW_VALIDATOR,
        _UPGRADE_REPLACEMENTS,
    )


def downgrade() -> None:
    """Restore the predecessor validator only before any admission exists."""

    schema = _schema()
    _require_empty_downgrade(schema)
    _rewrite_validator(
        schema,
        _NEW_VALIDATOR,
        _OLD_VALIDATOR,
        tuple(
            (new_fragment, old_fragment)
            for old_fragment, new_fragment in _UPGRADE_REPLACEMENTS
        ),
    )
