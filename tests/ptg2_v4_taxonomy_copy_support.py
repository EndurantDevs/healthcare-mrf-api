# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real PostgreSQL compiler-input fixtures for V4 taxonomy lifecycle tests."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
import struct
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
)

from api.ptg2_code_filters import (
    INFERRED_PROVIDER_TAXONOMY_RULES,
    InferredProviderTaxonomyRule,
)
from process.ptg_parts import ptg2_shared_snapshot_publish as shared_publish
from process.ptg_parts import ptg2_v4_taxonomy_candidates as candidates
from process.ptg_parts.ptg2_v4_graph_compiler import (
    V4GraphNpiScopePreparation,
)


@dataclass(frozen=True)
class PreparedTaxonomyCopy:
    """Authenticated selected compiler COPY and its source evidence."""

    manifest: dict[str, Any]
    rules: tuple[InferredProviderTaxonomyRule, ...]
    copy_path: Path
    copy_bytes: bytes
    copy_sha256: str


def _quoted(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def compiler_rules() -> tuple[InferredProviderTaxonomyRule, ...]:
    return tuple(INFERRED_PROVIDER_TAXONOMY_RULES)


async def create_prepared_catalog(
    engine: AsyncEngine,
    schema_name: str,
) -> None:
    schema = _quoted(schema_name)
    row_cap = candidates.PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_CANDIDATES
    statements = (
        f"""
        CREATE TABLE {schema}.npi (
            npi bigint PRIMARY KEY,
            entity_type_code integer NOT NULL
        )
        """,
        f"""
        CREATE TABLE {schema}.npi_taxonomy (
            npi bigint NOT NULL,
            healthcare_provider_taxonomy_code varchar(10) NOT NULL,
            PRIMARY KEY (npi, healthcare_provider_taxonomy_code)
        )
        """,
        f"""
        INSERT INTO {schema}.npi (npi, entity_type_code)
        SELECT 1000000000 + value, 1
          FROM generate_series(0, {row_cap}) AS value
        """,
    )
    rules = compiler_rules()
    taxonomy_statement = sa.text(
        f"""
        INSERT INTO {schema}.npi_taxonomy (
            npi,
            healthcare_provider_taxonomy_code
        )
        SELECT 1000000000, code
          FROM unnest(CAST(:small_codes AS varchar[])) AS code
        UNION ALL
        SELECT 1000000000 + npi_key, code
          FROM generate_series(0, {row_cap}) AS npi_key
         CROSS JOIN unnest(CAST(:large_codes AS varchar[])) AS code
        """
    )
    async with engine.begin() as connection:
        for statement in statements:
            await connection.exec_driver_sql(statement)
        await connection.execute(
            taxonomy_statement,
            {
                "small_codes": tuple(rule.taxonomy_codes[0] for rule in rules[:5]),
                "large_codes": tuple(rule.taxonomy_codes[0] for rule in rules[5:]),
            },
        )


class ObservedScopeSession(AsyncSession):
    """Capture backend identity around production TEMP transaction phases."""

    observations: list[tuple[str, dict[str, Any]]] = []

    async def execute(
        self,
        statement: Any,
        params: Any = None,
        **kwargs: Any,
    ) -> Any:
        execution_result = await super().execute(statement, params, **kwargs)
        await self._record_scope_observation(str(statement))
        return execution_result

    async def _record_scope_observation(self, statement_text: str) -> None:
        phase = self._scope_phase(statement_text)
        if phase is None:
            return
        identity = (
            (
                await super().execute(
                    sa.text(
                        """
                    SELECT pg_backend_pid() AS backend_pid,
                           pg_my_temp_schema() AS temp_schema,
                           current_setting(
                               'transaction_isolation'
                           ) AS isolation,
                           current_setting(
                               'transaction_read_only'
                           ) AS read_only
                    """
                    )
                )
            )
            .mappings()
            .one()
        )
        self.observations.append((phase, dict(identity)))

    @staticmethod
    def _scope_phase(statement_text: str) -> str | None:
        if "CREATE TEMP TABLE" in statement_text:
            return "created"
        if "SET TRANSACTION ISOLATION LEVEL" in statement_text:
            return "prepared"
        return None


def configure_scope_session(engine: AsyncEngine, monkeypatch: Any) -> None:
    ObservedScopeSession.observations = []
    monkeypatch.setattr(shared_publish.db, "engine", engine)
    monkeypatch.setattr(
        shared_publish.db,
        "session_factory",
        async_sessionmaker(
            class_=ObservedScopeSession,
            expire_on_commit=False,
        ),
    )


def assert_scope_observations() -> None:
    assert len(ObservedScopeSession.observations) == 2
    observation_by_phase = dict(ObservedScopeSession.observations)
    created = observation_by_phase["created"]
    prepared = observation_by_phase["prepared"]
    assert created["backend_pid"] == prepared["backend_pid"]
    assert created["temp_schema"] == prepared["temp_schema"]
    assert int(prepared["temp_schema"]) > 0
    assert (created["isolation"], created["read_only"]) == (
        "read committed",
        "off",
    )
    assert (prepared["isolation"], prepared["read_only"]) == (
        "repeatable read",
        "on",
    )


def _binary_npi_scope_copy(row_count: int) -> bytes:
    payload = bytearray(b"PGCOPY\n\xff\r\n\x00")
    payload.extend(struct.pack(">ii", 0, 0))
    for npi_key in range(row_count):
        payload.extend(
            struct.pack(
                ">hIiIq",
                2,
                4,
                npi_key,
                8,
                1_000_000_000 + npi_key,
            )
        )
    payload.extend(struct.pack(">h", -1))
    return bytes(payload)


def npi_scope_preparation(
    work_directory: Path,
    *,
    sha256_override: str | None = None,
) -> V4GraphNpiScopePreparation:
    row_count = candidates.PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_CANDIDATES + 1
    scope_payload = _binary_npi_scope_copy(row_count)
    copy_path = work_directory / "npi-scope.copy"
    copy_path.write_bytes(scope_payload)
    copy_path.chmod(0o600)
    source_scope_directory = work_directory / "npi-scope-source"
    source_scope_directory.mkdir(mode=0o700)
    return V4GraphNpiScopePreparation(
        copy_path=copy_path,
        manifest={
            "row_count": row_count,
            "output_byte_count": len(scope_payload),
            "output_sha256": (
                sha256_override or hashlib.sha256(scope_payload).hexdigest()
            ),
        },
        graph_artifact_entries=(),
        source_scope_directory=source_scope_directory,
    )


async def prepare_real_compiler_input(
    engine: AsyncEngine,
    *,
    schema_name: str,
    work_directory: Path,
    monkeypatch: Any,
) -> dict[str, Any]:
    configure_scope_session(engine, monkeypatch)
    prepared = await shared_publish._prepare_v4_taxonomy_compiler_input(
        npi_scope_preparation(work_directory),
        schema_name=schema_name,
        work_directory=work_directory,
        progress_callback=None,
    )
    assert_scope_observations()
    return dict(prepared)


def _projection_from_prepared(
    prepared_rule: dict[str, Any],
    member_payload: bytes,
) -> dict[str, Any]:
    rule_digest = bytes.fromhex(prepared_rule["rule_digest"])
    member_count = int(prepared_rule["member_count"])
    is_observe = member_count > 1
    representation = (
        candidates.PTG2_V4_INFERRED_TAXONOMY_OBSERVE_REPRESENTATION
        if is_observe
        else candidates.PTG2_V4_INFERRED_TAXONOMY_DIRECT_REPRESENTATION
    )
    return {
        "rule_digest": rule_digest,
        "catalog_contract": (candidates.PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT),
        "catalog_digest": bytes.fromhex(prepared_rule["catalog_digest"]),
        "vector_format": candidates.PTG2_V4_INFERRED_TAXONOMY_VECTOR_FORMAT,
        "member_count": member_count,
        "member_digest": candidates.inferred_taxonomy_member_digest(
            rule_digest,
            member_count=member_count,
            payload=member_payload,
        ),
        "member_keys": member_payload,
        "representation": representation,
        "observe_reason": (
            candidates.PTG2_V4_INFERRED_TAXONOMY_CANDIDATE_CAP_REASON
            if is_observe
            else None
        ),
        "observe_count_lower_bound": member_count if is_observe else None,
        "pattern_count": 0,
        "pattern_member_count": 0,
        "pattern_member_bytes": 0,
        "pattern_member_digest": (
            candidates.inferred_taxonomy_pattern_member_digest(
                rule_digest,
                representation=representation,
                pattern_count=0,
                pattern_member_count=0,
                packed_pattern_payload=b"",
            )
        ),
        "pattern_member_payload": b"",
    }


def prepared_projection_rows(
    prepared: dict[str, Any],
    member_bytes: bytes,
) -> tuple[dict[str, Any], ...]:
    projections: list[dict[str, Any]] = []
    for prepared_rule in prepared["rules"]:
        member_offset = int(prepared_rule["member_offset_bytes"])
        member_end = member_offset + int(prepared_rule["member_byte_count"])
        projections.append(
            _projection_from_prepared(
                prepared_rule,
                member_bytes[member_offset:member_end],
            )
        )
    return tuple(
        sorted(
            projections,
            key=lambda projection: projection["rule_digest"],
        )
    )


def _binary_copy_field(value: Any, field_index: int) -> bytes:
    if value is None:
        return struct.pack(">i", -1)
    if field_index in {4, 10}:
        encoded = struct.pack(">i", int(value))
    elif field_index in {9, 11, 12}:
        encoded = struct.pack(">q", int(value))
    elif field_index in {0, 2, 5, 6, 13, 14}:
        encoded = bytes(value)
    else:
        encoded = str(value).encode("utf-8")
    return struct.pack(">i", len(encoded)) + encoded


def binary_compiler_copy(rows: tuple[dict[str, Any], ...]) -> bytes:
    payload = bytearray(b"PGCOPY\n\xff\r\n\x00")
    payload.extend(struct.pack(">ii", 0, 0))
    for projection in rows:
        payload.extend(struct.pack(">h", len(candidates._COMPILER_STAGE_COLUMNS)))
        for field_index, field_name in enumerate(candidates._COMPILER_STAGE_COLUMNS):
            payload.extend(
                _binary_copy_field(
                    projection.get(field_name),
                    field_index,
                )
            )
    payload.extend(struct.pack(">h", -1))
    return bytes(payload)


def assert_prepared_selection(
    prepared: dict[str, Any],
    projection_rows: tuple[dict[str, Any], ...],
) -> None:
    rejection_count = candidates.PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_CANDIDATES + 1
    assert (
        sorted(int(rule["member_count"]) for rule in prepared["rules"])
        == [1] * 5 + [rejection_count] * 5
    )
    assert (
        sum(
            projection["representation"] == "direct_v1"
            for projection in projection_rows
        )
        == 5
    )
    observe_projections = tuple(
        projection
        for projection in projection_rows
        if projection["representation"] == "observe_v1"
    )
    assert len(observe_projections) == 5
    assert {
        (
            projection["observe_reason"],
            projection["observe_count_lower_bound"],
            projection["member_count"],
        )
        for projection in observe_projections
    } == {("candidate_cap_exceeded", rejection_count, rejection_count)}


async def runtime_inventory(
    engine: AsyncEngine,
    schema_name: str,
) -> dict[str, int]:
    schema = _quoted(schema_name)
    inventory_sql = sa.text(
        """
        SELECT COUNT(*)::bigint AS relation_count,
               COALESCE(SUM(pg_total_relation_size(class.oid)), 0)::bigint
                   AS relation_bytes
         FROM pg_class AS class
          JOIN pg_namespace AS namespace
            ON namespace.oid = class.relnamespace
         WHERE namespace.nspname LIKE 'pg_temp_%'
           AND (
               class.relname LIKE 'ptg2_v4_taxonomy_%'
               OR class.relname LIKE 'ptg2_v4_npi_scope_input_%'
           )
        """
    )
    async with engine.connect() as connection:
        inventory = (await connection.execute(inventory_sql)).one()
        candidate_count = await connection.scalar(
            sa.text(
                f"SELECT COUNT(*) FROM {schema}." "ptg2_v4_inferred_taxonomy_candidate"
            )
        )
        root_count = await connection.scalar(
            sa.text(f"SELECT COUNT(*) FROM {schema}.ptg2_v4_snapshot_map_root")
        )
    return {
        "stage_relation_count": int(inventory.relation_count),
        "stage_relation_bytes": int(inventory.relation_bytes),
        "candidate_count": int(candidate_count or 0),
        "map_root_count": int(root_count or 0),
    }


async def current_stage_inventory(connection: Any) -> tuple[int, int]:
    inventory = (
        await connection.execute(
            sa.text(
                """
                SELECT COUNT(*)::bigint AS relation_count,
                       COALESCE(
                           SUM(pg_total_relation_size(class.oid)),
                           0
                       )::bigint AS relation_bytes
                  FROM pg_class AS class
                  JOIN pg_namespace AS namespace
                    ON namespace.oid = class.relnamespace
                 WHERE namespace.oid = pg_my_temp_schema()
                   AND class.relname LIKE 'ptg2_v4_taxonomy_%'
                """
            )
        )
    ).one()
    return int(inventory.relation_count), int(inventory.relation_bytes)
