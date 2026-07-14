# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Precomputed Provider Directory doctor-profile serving artifacts."""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable


PROFILE_TABLE = "provider_directory_profile"
PROFILE_EVIDENCE_TABLE = "provider_directory_profile_evidence"
PROFILE_STAGE_PREFIX = "provider_directory_profile_stage"
PROFILE_EVIDENCE_STAGE_PREFIX = "provider_directory_profile_evidence_stage"
PROFILE_SPEC_PATH = (
    Path(__file__).resolve().parents[1]
    / "specs/provider_directory_profile_sources.json"
)
PROFILE_SQL_PATH = Path(__file__).resolve().parent / "sql"
PROFILE_SCHEMA_VERSION = 1
PROFILE_FACT_LIMIT = 100
PROFILE_FACT_EVIDENCE_LIMIT = 25
NPI_MIN = 1_000_000_000
NPI_MAX = 9_999_999_999
NPI_LUHN_PREFIX_DIGIT_SUM = 24

PROFILE_INDEX_SUFFIXES = ("generation_idx",)
PROFILE_EVIDENCE_INDEX_SUFFIXES = (
    "npi_idx",
    "npi_fact_idx",
    "source_idx",
    "endpoint_idx",
)


@lru_cache(maxsize=4)
def _sql_template(filename: str) -> str:
    """Load one immutable profile SQL template."""
    return (PROFILE_SQL_PATH / filename).read_text(encoding="utf-8")


def _render_sql_template(
    filename: str,
    replacements_by_token: dict[str, Any],
) -> str:
    """Substitute explicit tokens without interpreting SQL or JSON braces."""
    rendered_sql = _sql_template(filename)
    for token_name, replacement in replacements_by_token.items():
        token = "{{" + token_name + "}}"
        if token not in rendered_sql:
            raise RuntimeError(
                f"provider_directory_profile_sql_token_missing:{token_name}"
            )
        rendered_sql = rendered_sql.replace(token, str(replacement))
    unresolved_tokens = sorted(
        set(re.findall(r"\{\{[A-Z][A-Z0-9_]*\}\}", rendered_sql))
    )
    if unresolved_tokens:
        raise RuntimeError(
            "provider_directory_profile_sql_tokens_unresolved:"
            + ",".join(unresolved_tokens)
        )
    return rendered_sql


def quote_identifier(identifier: str) -> str:
    """Quote one PostgreSQL identifier without accepting SQL fragments."""
    return '"' + str(identifier).replace('"', '""') + '"'


def qualified_table(schema: str, table_name: str) -> str:
    """Return a safely quoted schema-qualified table reference."""
    return f"{quote_identifier(schema)}.{quote_identifier(table_name)}"


def is_valid_npi(value: Any) -> bool:
    """Return whether a value is a ten-digit NPI with a valid check digit."""
    value_text = str(value).strip()
    if (
        len(value_text) != 10
        or not value_text.isascii()
        or not value_text.isdigit()
    ):
        return False
    digits = [int(digit) for digit in value_text]
    digit_sum = NPI_LUHN_PREFIX_DIGIT_SUM + digits[-1]
    for position, digit in enumerate(digits[:-1], start=1):
        if position % 2:
            doubled = digit * 2
            digit_sum += doubled - 9 if doubled > 9 else doubled
        else:
            digit_sum += digit
    return digit_sum % 10 == 0


def valid_npi_sql(value_sql: str) -> str:
    """Build an inline PostgreSQL NPI range and Luhn predicate."""
    digit_terms: list[str] = []
    for position in range(1, 11):
        divisor = 10 ** (10 - position)
        digit_sql = f"((({value_sql}) / {divisor}) % 10)"
        if position < 10 and position % 2:
            digit_terms.append(
                f"(({digit_sql} * 2) - CASE WHEN {digit_sql} >= 5 THEN 9 ELSE 0 END)"
            )
        else:
            digit_terms.append(digit_sql)
    checksum_sql = "\n                + ".join(
        [str(NPI_LUHN_PREFIX_DIGIT_SUM), *digit_terms]
    )
    return f"""(
            ({value_sql}) BETWEEN {NPI_MIN} AND {NPI_MAX}
            AND MOD(
                {checksum_sql},
                10
            ) = 0
        )"""


def _bounded_identifier(value: str) -> str:
    if len(value) <= 63:
        return value
    digest = hashlib.sha1(value.encode("utf-8")).hexdigest()[:12]
    return f"{value[:50]}_{digest}"


def profile_stage_table_name(run_id: str | None = None) -> str:
    """Return a deterministic compact-profile stage name for one run."""
    token = run_id or f"{os.getpid()}:{time.time_ns()}"
    digest = hashlib.sha1(token.encode("utf-8")).hexdigest()[:16]
    return f"{PROFILE_STAGE_PREFIX}_{digest}"


def profile_evidence_stage_table_name(run_id: str | None = None) -> str:
    """Return a deterministic profile-evidence stage name for one run."""
    token = run_id or f"{os.getpid()}:{time.time_ns()}"
    digest = hashlib.sha1(token.encode("utf-8")).hexdigest()[:16]
    return f"{PROFILE_EVIDENCE_STAGE_PREFIX}_{digest}"


def profile_index_name(table_name: str, suffix: str) -> str:
    """Return a PostgreSQL-safe index name bounded to 63 characters."""
    return _bounded_identifier(f"{table_name}_{suffix}")


def load_profile_source_spec(path: Path | None = None) -> dict[str, Any]:
    """Load and validate the reviewed insurer profile-source contract."""
    spec_path = path or PROFILE_SPEC_PATH
    payload = json.loads(spec_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise RuntimeError("provider_directory_profile_source_spec_invalid")
    source_ids = payload.get("source_ids")
    entry_ids = payload.get("entry_ids")
    if (
        not isinstance(source_ids, list)
        or not source_ids
        or len(source_ids) != len(set(source_ids))
        or not all(
            isinstance(source_id, str)
            and source_id.startswith("pdfhir_")
            and len(source_id) > len("pdfhir_")
            for source_id in source_ids
        )
        or not isinstance(entry_ids, list)
        or not entry_ids
        or len(entry_ids) != len(set(entry_ids))
        or not all(isinstance(entry_id, str) and entry_id for entry_id in entry_ids)
    ):
        raise RuntimeError("provider_directory_profile_source_spec_invalid")
    return payload


def configured_profile_source_ids(path: Path | None = None) -> tuple[str, ...]:
    """Return reviewed source IDs in stable order."""
    return tuple(sorted(load_profile_source_spec(path)["source_ids"]))


def profile_table_sql(schema: str, table_name: str = PROFILE_TABLE) -> str:
    """Build the compact NPI profile serving-table definition."""
    return f"""
        CREATE UNLOGGED TABLE {qualified_table(schema, table_name)} (
            npi bigint PRIMARY KEY,
            profile_json jsonb NOT NULL,
            evidence_json jsonb NOT NULL,
            source_ids varchar[] NOT NULL,
            endpoint_ids varchar[] NOT NULL,
            dataset_ids varchar[] NOT NULL,
            source_count integer NOT NULL,
            independent_source_count integer NOT NULL,
            fact_count integer NOT NULL,
            generation_id varchar(64) NOT NULL,
            published_at timestamp without time zone NOT NULL
        );
    """


def profile_evidence_table_sql(
    schema: str,
    table_name: str = PROFILE_EVIDENCE_TABLE,
) -> str:
    """Build the normalized source-evidence serving-table definition."""
    return f"""
        CREATE UNLOGGED TABLE {qualified_table(schema, table_name)} (
            evidence_key char(32) PRIMARY KEY,
            npi bigint NOT NULL,
            fact_type varchar(64) NOT NULL,
            fact_key char(32) NOT NULL,
            value_json jsonb NOT NULL,
            source_id varchar(64) NOT NULL,
            endpoint_id varchar(64) NOT NULL,
            dataset_id varchar(96) NOT NULL,
            canonical_api_base text,
            source_org_name varchar(256),
            source_plan_name varchar(512),
            resource_type varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            role_resource_id varchar(256),
            active boolean,
            effective_start varchar(64),
            effective_end varchar(64),
            observed_at timestamp without time zone
        );
    """


def profile_evidence_columns() -> tuple[str, ...]:
    """Return evidence columns in copy and insert order."""
    return (
        "evidence_key",
        "npi",
        "fact_type",
        "fact_key",
        "value_json",
        "source_id",
        "endpoint_id",
        "dataset_id",
        "canonical_api_base",
        "source_org_name",
        "source_plan_name",
        "resource_type",
        "resource_id",
        "role_resource_id",
        "active",
        "effective_start",
        "effective_end",
        "observed_at",
    )


def profile_columns() -> tuple[str, ...]:
    """Return compact-profile columns in copy and insert order."""
    return (
        "npi",
        "profile_json",
        "evidence_json",
        "source_ids",
        "endpoint_ids",
        "dataset_ids",
        "source_count",
        "independent_source_count",
        "fact_count",
        "generation_id",
        "published_at",
    )


def profile_index_statements(
    schema: str,
    table_name: str,
    *,
    evidence: bool,
) -> tuple[str, ...]:
    """Build indexes for a compact profile or normalized evidence table."""
    table_ref = qualified_table(schema, table_name)
    if evidence:
        return (
            f"CREATE INDEX {quote_identifier(profile_index_name(table_name, 'npi_idx'))} ON {table_ref} (npi);",
            f"CREATE INDEX {quote_identifier(profile_index_name(table_name, 'npi_fact_idx'))} ON {table_ref} (npi, fact_type, fact_key);",
            f"CREATE INDEX {quote_identifier(profile_index_name(table_name, 'source_idx'))} ON {table_ref} (source_id, npi);",
            f"CREATE INDEX {quote_identifier(profile_index_name(table_name, 'endpoint_idx'))} ON {table_ref} (endpoint_id, npi);",
        )
    return (
        f"CREATE INDEX {quote_identifier(profile_index_name(table_name, 'generation_idx'))} ON {table_ref} (generation_id);",
    )


def profile_scope_source_ids_sql(source_ref: str) -> str:
    """Select reviewed aliases represented by the active immutable scope."""
    return f"""
        SELECT source.source_id
          FROM {source_ref} AS source
         WHERE source.endpoint_id IN (
                SELECT configured.endpoint_id
                  FROM {source_ref} AS configured
                 WHERE configured.source_id = ANY(CAST(:configured_source_ids AS varchar[]))
                   AND configured.endpoint_id IS NOT NULL
         )
         ORDER BY source.source_id;
    """


def copy_existing_evidence_sql(
    *,
    source_ref: str,
    target_ref: str,
) -> str:
    """Copy evidence for sources outside an incremental refresh."""
    columns = ", ".join(quote_identifier(column) for column in profile_evidence_columns())
    return f"""
        INSERT INTO {target_ref} ({columns})
        SELECT {columns}
          FROM {source_ref}
         WHERE source_id <> ALL(CAST(:source_ids AS varchar[]))
           AND {valid_npi_sql("npi")};
    """


def copy_unaffected_profiles_sql(
    *,
    profile_source_ref: str,
    evidence_source_ref: str,
    evidence_stage_ref: str,
    profile_stage_ref: str,
) -> str:
    """Copy profiles whose NPIs are unaffected by refreshed sources."""
    columns = ", ".join(quote_identifier(column) for column in profile_columns())
    return f"""
        INSERT INTO {profile_stage_ref} ({columns})
        WITH affected_npis AS MATERIALIZED (
            SELECT npi
              FROM {evidence_source_ref}
             WHERE source_id = ANY(CAST(:source_ids AS varchar[]))
            UNION
            SELECT npi
              FROM {evidence_stage_ref}
             WHERE source_id = ANY(CAST(:source_ids AS varchar[]))
        )
        SELECT {columns}
          FROM {profile_source_ref} AS profile
         WHERE {valid_npi_sql("profile.npi")}
           AND NOT EXISTS (
                SELECT 1
                  FROM affected_npis
                 WHERE affected_npis.npi = profile.npi
         );
    """


def profile_evidence_insert_sql(
    *,
    target_ref: str,
    source_ref: str,
    practitioner_ref: str,
    role_ref: str,
    organization_ref: str,
    service_ref: str,
    endpoint_ref: str | None = None,
) -> str:
    """Build immutable source evidence from scoped typed FHIR resources."""
    endpoint_ref = endpoint_ref or service_ref.replace(
        "provider_directory_healthcare_service",
        "provider_directory_endpoint",
    )
    return _render_sql_template(
        "provider_directory_profile_evidence.sql",
        {
            "TARGET_REF": target_ref,
            "SOURCE_REF": source_ref,
            "PRACTITIONER_REF": practitioner_ref,
            "ROLE_REF": role_ref,
            "ORGANIZATION_REF": organization_ref,
            "SERVICE_REF": service_ref,
            "ENDPOINT_REF": endpoint_ref,
            "VALID_NPI_SQL": valid_npi_sql("npi"),
        },
    )


def profile_insert_sql(
    *,
    evidence_ref: str,
    target_ref: str,
    old_evidence_ref: str | None,
    rebuild_all: bool,
) -> str:
    """Build compact and evidence-rich NPI profiles from normalized facts."""
    if rebuild_all or old_evidence_ref is None:
        affected_npis_sql = f"SELECT DISTINCT npi FROM {evidence_ref}"
    else:
        affected_npis_sql = f"""
            SELECT npi
              FROM {old_evidence_ref}
             WHERE source_id = ANY(CAST(:source_ids AS varchar[]))
            UNION
            SELECT npi
              FROM {evidence_ref}
             WHERE source_id = ANY(CAST(:source_ids AS varchar[]))
        """.strip()
    return _render_sql_template(
        "provider_directory_profile_aggregate.sql",
        {
            "AFFECTED_NPIS_SQL": affected_npis_sql,
            "EVIDENCE_REF": evidence_ref,
            "TARGET_REF": target_ref,
            "PROFILE_FACT_EVIDENCE_LIMIT": PROFILE_FACT_EVIDENCE_LIMIT,
            "PROFILE_FACT_LIMIT": PROFILE_FACT_LIMIT,
            "PROFILE_SCHEMA_VERSION": PROFILE_SCHEMA_VERSION,
        },
    )

def profile_source_dataset_pairs(
    datasets: Iterable[Any],
    selected_source_ids: Iterable[str],
) -> tuple[list[str], list[str]]:
    """Align selected source IDs with their immutable dataset IDs."""
    dataset_id_by_source_id = {
        str(dataset.source_id): str(dataset.dataset_id)
        for dataset in datasets
    }
    source_ids = sorted(set(selected_source_ids))
    missing_source_ids = [
        source_id
        for source_id in source_ids
        if source_id not in dataset_id_by_source_id
    ]
    if missing_source_ids:
        raise RuntimeError(
            "provider_directory_profile_dataset_missing:"
            + ",".join(missing_source_ids)
        )
    return source_ids, [dataset_id_by_source_id[source_id] for source_id in source_ids]
