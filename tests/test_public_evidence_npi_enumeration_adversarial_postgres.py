# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Isolated fail-closed PostgreSQL guards for NPI-enumeration rows."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

from public_evidence.source_release_primitives import CanonicalUtcInterval
from tests.public_evidence_npi_enumeration_adversarial_support import (
    coherent_adversarial_rows,
    coherent_link_owner_mismatch,
)
from tests.public_evidence_npi_enumeration_postgres_support import (
    TABLE_NAMES,
    insert_candidate,
    npi_candidate,
    npi_enumeration_schema,
    seed_owned_roots,
)
from tests.public_evidence_reference_roots_postgres_support import (
    insert_reference_row,
)
from tests.public_evidence_storage_postgres_support import (
    connect,
    insert_source_release,
    quoted,
)


async def _seed_adversarial_roots(
    connection: asyncpg.Connection,
    schema_name: str,
    persistence_candidate,
    source_root_by_field: dict[str, object] | None,
) -> None:
    if source_root_by_field is None:
        await seed_owned_roots(connection, schema_name, persistence_candidate)
        return
    await insert_source_release(connection, schema_name, "nppes_entity_address")
    async with connection.transaction():
        await connection.execute("SET LOCAL session_replication_role='replica'")
        await insert_reference_row(
            connection,
            schema_name,
            "public_evidence_source_record",
            source_root_by_field,
        )


async def _assert_rejected_without_rows(
    persistence_candidate,
    table_rows_by_name: dict[str, list[dict[str, object]]],
    *,
    error_pattern: str = "public_evidence_npi_record_invalid",
    use_replica_role: bool = False,
    source_root_by_field: dict[str, object] | None = None,
) -> None:
    async with npi_enumeration_schema() as (
        _engine,
        database_url,
        schema_name,
        _migration,
    ):
        connection = await connect(database_url)
        try:
            await _seed_adversarial_roots(
                connection,
                schema_name,
                persistence_candidate,
                source_root_by_field,
            )
            with pytest.raises(asyncpg.CheckViolationError, match=error_pattern):
                await insert_candidate(
                    connection,
                    schema_name,
                    persistence_candidate,
                    rows=table_rows_by_name,
                    use_replica_role=use_replica_role,
                )
            for table_name in TABLE_NAMES:
                assert (
                    await connection.fetchval(
                        f"SELECT count(*) FROM {quoted(schema_name)}.{quoted(table_name)}"
                    )
                    == 0
                )
        finally:
            await connection.close()


@pytest.mark.asyncio
async def test_always_validator_isolates_typed_owner_mismatch() -> None:
    candidate = npi_candidate()
    release_ref = candidate.record.release.source_release_ref
    replacement_suffix = "A" if not release_ref.endswith("A") else "B"
    await _assert_rejected_without_rows(
        candidate,
        coherent_adversarial_rows(
            candidate,
            typed_updates_by_field={
                "source_release_ref": release_ref[:-1] + replacement_suffix
            },
        ),
        use_replica_role=True,
    )


@pytest.mark.asyncio
async def test_validator_isolates_nonzero_single_link_ordinal() -> None:
    candidate = npi_candidate()
    await _assert_rejected_without_rows(
        candidate,
        coherent_adversarial_rows(
            candidate,
            link_updates_by_field={"source_record_ordinal": 1},
        ),
    )


@pytest.mark.asyncio
async def test_always_validator_isolates_link_owner_mismatch() -> None:
    candidate = npi_candidate()
    table_rows_by_name, source_root_by_field = coherent_link_owner_mismatch(candidate)
    await _assert_rejected_without_rows(
        candidate,
        table_rows_by_name,
        use_replica_role=True,
        source_root_by_field=source_root_by_field,
    )


@pytest.mark.asyncio
async def test_validator_isolates_wrong_but_well_formed_evidence_ref() -> None:
    candidate = npi_candidate()
    evidence_ref = candidate.record.evidence_ref
    replacement_suffix = "A" if not evidence_ref.endswith("A") else "B"
    await _assert_rejected_without_rows(
        candidate,
        coherent_adversarial_rows(
            candidate, row_evidence_ref=evidence_ref[:-1] + replacement_suffix
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field_name", "replacement"),
    (
        ("record_contract_sha256", "10" * 32),
        ("source_link_vector_sha256", "20" * 32),
        ("typed_row_sha256", "30" * 32),
        ("authority_state_sha256", "40" * 32),
    ),
)
async def test_validator_isolates_common_digest_pointer(
    field_name: str, replacement: str
) -> None:
    candidate = npi_candidate()
    await _assert_rejected_without_rows(
        candidate,
        coherent_adversarial_rows(
            candidate,
            common_updates_by_field={field_name: replacement},
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ("deactivated_null", "outside_release", "fractional"))
async def test_temporal_guards_reject_fully_rehashed_rows(case: str) -> None:
    candidate = npi_candidate(
        enumeration_state="deactivated" if case == "deactivated_null" else "active"
    )
    if case == "deactivated_null":
        interval = candidate.record.effective_interval
        record_updates_by_field = {
            "effective_interval": CanonicalUtcInterval(interval.start_at, None)
        }
        error_pattern = "public_evidence_npi_record_invalid"
    elif case == "outside_release":
        release_end = candidate.record.release.observed_interval.end_at
        parsed_end = datetime.fromisoformat(release_end.replace("Z", "+00:00"))
        record_updates_by_field = {
            "observed_at": (parsed_end + timedelta(seconds=1))
            .astimezone(timezone.utc)
            .strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        error_pattern = "public_evidence_npi_record_invalid"
    else:
        record_updates_by_field = {"observed_at": "2026-07-01T12:00:00.000001Z"}
        error_pattern = "public_evidence_record_shape_check"
    await _assert_rejected_without_rows(
        candidate,
        coherent_adversarial_rows(
            candidate,
            record_updates_by_field=record_updates_by_field,
        ),
        error_pattern=error_pattern,
    )
