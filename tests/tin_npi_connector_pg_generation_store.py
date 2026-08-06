# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable PostgreSQL proof for atomic connector generation sealing."""

from __future__ import annotations

import pytest

from process.tin_npi_connector_build import _reverse_rows_from_forward
from process.tin_npi_connector_generation import CompactTinNpiGeneration
from process.tin_npi_connector_generation_store import (
    TinNpiConnectorGenerationStoreError,
    load_and_seal_admitted_connector_generation,
)
from process.tin_npi_connector_publication import (
    ConnectorPublicationBundle,
    ConnectorPublicationLimits,
)
from process.tin_npi_connector_source import (
    canonical_source_ordinal_map_digest,
)
from tests.tin_npi_connector_pg_lifecycle_model import (
    ConnectorLifecycleScenario,
)


async def prove_store_load_seal_reuse(monkeypatch):
    scenario = await _committed_scenario(monkeypatch)
    try:
        bundle = _scenario_bundle(scenario)
        limits = _limits_for(bundle)

        first = await load_and_seal_admitted_connector_generation(
            scenario.connection,
            bundle,
            limits=limits,
            schema=scenario.session.schema,
        )
        first_counts = await _database_counts(scenario)

        second = await load_and_seal_admitted_connector_generation(
            scenario.connection,
            bundle,
            limits=limits,
            schema=scenario.session.schema,
        )

        assert first.reused is False
        assert second.reused is True
        assert second.generation_key == first.generation_key
        assert first_counts == (1, 1, 1, 1, 2, 3)
        assert await _database_counts(scenario) == first_counts
        await _assert_complete_generation(scenario, first.generation_key, bundle)
        assert await _current_pointer(scenario) == (0, None)
    finally:
        await _close_committed_scenario(scenario)


async def prove_store_atomic_rollback(monkeypatch):
    scenario = await _committed_scenario(monkeypatch)
    try:
        bundle = _scenario_bundle(scenario)
        connection = _CopyStatusFailureConnection(scenario.connection)

        with pytest.raises(
            TinNpiConnectorGenerationStoreError,
            match="connector generation COPY count is invalid",
        ):
            await load_and_seal_admitted_connector_generation(
                connection,
                bundle,
                limits=_limits_for(bundle),
                schema=scenario.session.schema,
            )

        assert await _database_counts(scenario) == (0, 0, 0, 0, 0, 0)
        assert await _current_pointer(scenario) == (0, None)
    finally:
        await _close_committed_scenario(scenario)


async def _committed_scenario(monkeypatch):
    scenario = await ConnectorLifecycleScenario.create(monkeypatch)
    await scenario.session.transaction.commit()
    assert not scenario.connection.is_in_transaction()
    return scenario


async def _close_committed_scenario(scenario):
    try:
        await scenario.connection.execute(
            f"DROP SCHEMA {scenario.quoted_schema} CASCADE"
        )
    finally:
        await scenario.connection.close()


def _scenario_bundle(scenario) -> ConnectorPublicationBundle:
    model = scenario.model
    ordered_source_ids = tuple(
        sorted(
            {dataset.source_id for dataset in model.source_vector.fhir_datasets},
            key=lambda source_id: source_id.encode("utf-8"),
        )
    )
    generation = CompactTinNpiGeneration(
        generation_id=model.generation_id.hex(),
        source_vector_id=model.source_vector.source_vector_id,
        source_ordinal_map=ordered_source_ids,
        source_ordinal_map_digest=canonical_source_ordinal_map_digest(
            ordered_source_ids
        ),
        scan_proofs=model.scan_proofs,
        scan_proof_digest=model.scan_proof_digest,
        lookup_digest=model.lookup_digest,
        evidence_rows=tuple(
            sorted(model.evidence_rows, key=lambda evidence: evidence.evidence_id)
        ),
        forward_rows=model.lookup_rows,
        reverse_rows=_reverse_rows_from_forward(model.lookup_rows),
    )
    return ConnectorPublicationBundle(model.source_vector, generation)


def _limits_for(bundle: ConnectorPublicationBundle) -> ConnectorPublicationLimits:
    counts = bundle.counts
    return ConnectorPublicationLimits(
        max_sources=counts.source_count,
        max_datasets=counts.dataset_count,
        max_token_policies=counts.token_policy_count,
        max_metadata_bytes=counts.metadata_byte_count,
        max_organizations=counts.organization_count,
        max_evidence_rows=counts.evidence_row_count,
        max_forward_rows=counts.forward_row_count,
        max_reverse_rows=counts.reverse_row_count,
        max_npi_edges=counts.npi_edge_count,
    )


async def _database_counts(scenario) -> tuple[int, ...]:
    table_names = (
        "tin_npi_connector_token_policy",
        "tin_npi_connector_identifier_policy",
        "tin_npi_connector_generation",
        "tin_npi_connector_generation_policy",
        "tin_npi_connector_lookup",
        "tin_npi_connector_evidence",
    )
    counts = []
    for table_name in table_names:
        counts.append(
            int(
                await scenario.connection.fetchval(
                    f"SELECT COUNT(*) FROM {scenario.quoted_schema}.{table_name}"
                )
            )
        )
    return tuple(counts)


async def _assert_complete_generation(scenario, generation_key, bundle):
    generation_record = await scenario.connection.fetchrow(
        f"""
        SELECT state, completed_at, generation_id, source_vector_id,
               source_count, source_dataset_count, token_policy_count,
               organization_count, matched_organization_count,
               evidence_count, forward_row_count, reverse_row_count,
               npi_edge_count, lookup_digest, scan_proof_digest
          FROM {scenario.quoted_schema}.tin_npi_connector_generation
         WHERE generation_key = $1
        """,
        generation_key,
    )
    counts = bundle.counts
    assert generation_record is not None
    assert generation_record["state"] == "complete"
    assert generation_record["completed_at"] is not None
    assert bytes(generation_record["generation_id"]).hex() == (
        bundle.generation.generation_id
    )
    assert bytes(generation_record["source_vector_id"]).hex() == (
        bundle.source_vector.source_vector_id
    )
    assert tuple(
        generation_record[field]
        for field in (
            "source_count",
            "source_dataset_count",
            "token_policy_count",
            "organization_count",
            "matched_organization_count",
            "evidence_count",
            "forward_row_count",
            "reverse_row_count",
            "npi_edge_count",
        )
    ) == (
        counts.source_count,
        counts.dataset_count,
        counts.token_policy_count,
        counts.organization_count,
        bundle.generation.matched_organization_count,
        counts.evidence_row_count,
        counts.forward_row_count,
        counts.reverse_row_count,
        counts.npi_edge_count,
    )
    assert bytes(generation_record["lookup_digest"]) == (
        bundle.generation.lookup_digest
    )
    assert bytes(generation_record["scan_proof_digest"]) == (
        bundle.generation.scan_proof_digest
    )


async def _current_pointer(scenario) -> tuple[int, int | None]:
    record = await scenario.connection.fetchrow(f"""
        SELECT pointer_version, generation_key
          FROM {scenario.quoted_schema}.tin_npi_connector_current
         WHERE pointer_key = 1
        """)
    assert record is not None
    return int(record["pointer_version"]), record["generation_key"]


class _CopyStatusFailureConnection:
    def __init__(self, delegate):
        self.delegate = delegate

    def transaction(self):
        return self.delegate.transaction()

    def is_in_transaction(self):
        return self.delegate.is_in_transaction()

    async def execute(self, sql, *arguments):
        return await self.delegate.execute(sql, *arguments)

    async def fetchval(self, sql, *arguments):
        return await self.delegate.fetchval(sql, *arguments)

    async def fetchrow(self, sql, *arguments):
        return await self.delegate.fetchrow(sql, *arguments)

    async def copy_records_to_table(
        self,
        table_name,
        *,
        schema_name,
        columns,
        records,
    ):
        status = await self.delegate.copy_records_to_table(
            table_name,
            schema_name=schema_name,
            columns=columns,
            records=records,
        )
        if table_name == "tin_npi_connector_evidence":
            return "COPY 0"
        return status
