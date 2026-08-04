# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded, redacted unit proofs for connector generation loading."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from process.tin_npi_connector_generation_store import (
    SealedConnectorGeneration,
    TinNpiConnectorGenerationStoreError,
    load_and_seal_admitted_connector_generation,
)
from process.tin_npi_connector_generation_store_copy import _copy_batches
from process.tin_npi_connector_generation_store_metadata import (
    expected_generation_metadata,
)
from process.tin_npi_connector_publication import (
    ConnectorPublicationBundle,
    ConnectorPublicationLimits,
    TinNpiConnectorPublicationError,
)
from tests.test_tin_npi_connector_generation import (
    _build_multi_source_generation,
)
from tests.test_tin_npi_connector_publication import _empty_bundle
from tests.tin_npi_connector_unit_support import TEST_HMAC_HEX


@pytest.mark.asyncio
async def test_store_loads_in_fk_order_and_maps_exact_source_ordinals(tmp_path):
    bundle = _multi_source_bundle(tmp_path)
    connection = _StoreConnection(bundle)

    result = await load_and_seal_admitted_connector_generation(
        connection,
        bundle,
        limits=_limits_for(bundle),
        schema="mrf",
    )

    assert result.reused is False
    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert [copy.table_name for copy in connection.copy_calls] == [
        "tin_npi_connector_generation_policy",
        "tin_npi_connector_lookup",
        "tin_npi_connector_evidence",
    ]
    evidence_copy = connection.copy_calls[-1]
    observed_ordinals = [record[6] for record in evidence_copy.records]
    expected_ordinals = [
        bundle.generation.source_ordinal_map.index(evidence.source_id)
        for evidence in bundle.generation.evidence_rows
    ]
    assert observed_ordinals == expected_ordinals
    assert set(observed_ordinals) == {0, 1}
    assert not connection.pointer_touched


@pytest.mark.asyncio
async def test_store_reuses_only_an_exact_complete_generation(tmp_path):
    bundle = _multi_source_bundle(tmp_path)
    connection = _StoreConnection(bundle, incumbent=True)

    result = await load_and_seal_admitted_connector_generation(
        connection,
        bundle,
        limits=_limits_for(bundle),
        schema="mrf",
    )

    assert result.reused is True
    assert result.generation_key == connection.generation_key
    assert connection.copy_calls == []
    assert connection.insert_attempts == 0
    assert connection.commits == 1
    assert any("FOR SHARE" in sql for sql, _arguments in connection.statements)


@pytest.mark.asyncio
async def test_store_rejects_incumbent_metadata_drift_without_identity_leak(tmp_path):
    bundle = _multi_source_bundle(tmp_path)
    connection = _StoreConnection(bundle, incumbent=True)
    connection.complete_record["lookup_digest"] = b"\xff" * 32

    with pytest.raises(
        TinNpiConnectorGenerationStoreError,
        match="connector generation reuse conflict",
    ) as captured_error:
        await load_and_seal_admitted_connector_generation(
            connection,
            bundle,
            limits=_limits_for(bundle),
            schema="mrf",
        )

    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert TEST_HMAC_HEX not in repr(captured_error.value)
    assert bundle.generation.generation_id not in repr(captured_error.value)


@pytest.mark.asyncio
async def test_store_rolls_back_when_copy_count_is_not_exact(tmp_path):
    bundle = _multi_source_bundle(tmp_path)
    connection = _StoreConnection(
        bundle,
        bad_copy_table="tin_npi_connector_evidence",
    )

    with pytest.raises(
        TinNpiConnectorGenerationStoreError,
        match="connector generation COPY count is invalid",
    ):
        await load_and_seal_admitted_connector_generation(
            connection,
            bundle,
            limits=_limits_for(bundle),
            schema="mrf",
        )

    assert connection.commits == 0
    assert connection.rollbacks == 1


@pytest.mark.asyncio
async def test_copy_transforms_and_writes_only_one_bounded_batch(tmp_path):
    bundle = _multi_source_bundle(tmp_path)
    record_builder = _RecordBuilder()
    connection = _BatchProbeConnection(bundle, record_builder)
    await _copy_batches(
        connection,
        "mrf",
        "synthetic_relation",
        ("synthetic_value",),
        tuple(range(257)),
        record_builder,
        128,
    )

    assert [len(call.records) for call in connection.copy_calls] == [128, 128, 1]


@pytest.mark.asyncio
@pytest.mark.parametrize("schema", ("MRF", "mrf;drop", "", 1))
async def test_store_rejects_invalid_schema_before_transaction(tmp_path, schema):
    bundle = _multi_source_bundle(tmp_path)
    connection = _StoreConnection(bundle)

    with pytest.raises(
        TinNpiConnectorGenerationStoreError,
        match="connector generation schema is invalid",
    ):
        await load_and_seal_admitted_connector_generation(
            connection,
            bundle,
            limits=_limits_for(bundle),
            schema=schema,
        )

    assert connection.transactions == 0


@pytest.mark.asyncio
async def test_store_rejects_vector_schema_mismatch_before_transaction(tmp_path):
    bundle = _multi_source_bundle(tmp_path)
    connection = _StoreConnection(bundle)

    with pytest.raises(
        TinNpiConnectorGenerationStoreError,
        match="connector generation source schema binding is invalid",
    ):
        await load_and_seal_admitted_connector_generation(
            connection,
            bundle,
            limits=_limits_for(bundle),
            schema="other",
        )

    assert connection.transactions == 0


@pytest.mark.asyncio
async def test_store_rejects_nested_transaction_before_mutation(tmp_path):
    bundle = _multi_source_bundle(tmp_path)
    connection = _StoreConnection(bundle, in_transaction=True)

    with pytest.raises(
        TinNpiConnectorGenerationStoreError,
        match="connector generation requires an idle connection",
    ):
        await load_and_seal_admitted_connector_generation(
            connection,
            bundle,
            limits=_limits_for(bundle),
            schema="mrf",
        )

    assert connection.transactions == 0
    assert connection.statements == []


@pytest.mark.asyncio
async def test_store_reapplies_zero_evidence_admission_before_transaction():
    bundle = _empty_bundle()
    connection = _StoreConnection(bundle)

    with pytest.raises(
        TinNpiConnectorPublicationError,
        match="zero evidence requires explicit admission",
    ):
        await load_and_seal_admitted_connector_generation(
            connection,
            bundle,
            limits=_limits_for(bundle),
            schema="mrf",
        )

    assert connection.transactions == 0


def test_sealed_result_repr_excludes_generation_digests(tmp_path):
    bundle = _multi_source_bundle(tmp_path)
    result = SealedConnectorGeneration(
        generation_key=17,
        generation_id=bundle.generation.generation_id,
        source_vector_id=bundle.source_vector.source_vector_id,
        counts=bundle.counts,
        reused=False,
    )

    rendered = repr(result)
    assert rendered == (
        "<sealed-connector-generation key=17 reused=false "
        "sources=2 evidence=3>"
    )
    assert result.generation_id not in rendered
    assert result.source_vector_id not in rendered
    assert TEST_HMAC_HEX not in rendered


def _multi_source_bundle(tmp_path) -> ConnectorPublicationBundle:
    generation, vector = _build_multi_source_generation(tmp_path)
    return ConnectorPublicationBundle(vector, generation)


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


@dataclass(frozen=True)
class _CopyCall:
    table_name: str
    schema_name: str
    columns: tuple[str, ...]
    records: tuple[tuple[object, ...], ...]


class _Transaction:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        self.connection.transactions += 1
        self.connection.in_transaction = True
        return self

    async def __aexit__(self, error_type, error, traceback):
        self.connection.in_transaction = False
        if error_type is None:
            self.connection.commits += 1
        else:
            self.connection.rollbacks += 1
        return False


class _StoreConnection:
    def __init__(
        self,
        bundle,
        *,
        incumbent=False,
        bad_copy_table=None,
        in_transaction=False,
    ):
        self.bundle = bundle
        self.generation_key = 41
        self.incumbent = incumbent
        self.bad_copy_table = bad_copy_table
        self.in_transaction = in_transaction
        self.inserted = False
        self.sealed = False
        self.transactions = 0
        self.commits = 0
        self.rollbacks = 0
        self.insert_attempts = 0
        self.copy_calls = []
        self.statements = []
        self.pointer_touched = False
        self.complete_record = self._complete_record()

    def transaction(self):
        return _Transaction(self)

    def is_in_transaction(self):
        return self.in_transaction

    async def execute(self, sql, *arguments):
        self.statements.append((sql, arguments))
        self.pointer_touched |= "tin_npi_connector_current" in sql
        if "SET state = 'complete'" in sql:
            self.sealed = True
            return "UPDATE 1"
        if sql.lstrip().startswith("INSERT"):
            return "INSERT 0 1"
        return "SELECT 1"

    async def fetchval(self, sql, *arguments):
        self.statements.append((sql, arguments))
        if "tin_npi_connector_generation" not in sql:
            raise AssertionError("unexpected scalar query")
        self.insert_attempts += 1
        self.inserted = True
        return self.generation_key

    async def fetchrow(self, sql, *arguments):
        self.statements.append((sql, arguments))
        if "tin_npi_connector_identifier_policy" in sql:
            policy = self.bundle.source_vector.identifier_policy
            return {
                "descriptor_canonical_json": policy.descriptor_canonical_json,
                "identifier_policy_sha256": bytes.fromhex(
                    policy.descriptor_sha256
                ),
            }
        if "tin_npi_connector_token_policy" in sql:
            policy_id = arguments[0]
            policy = next(
                candidate
                for candidate in self.bundle.source_vector.token_policies
                if candidate.token_policy_id == policy_id
            )
            return {
                "token_policy_descriptor_sha256": bytes.fromhex(
                    policy.token_policy_descriptor_sha256
                )
            }
        if "tin_npi_connector_generation" in sql:
            if self.incumbent or self.sealed:
                return dict(self.complete_record)
            return None
        raise AssertionError("unexpected row query")

    async def copy_records_to_table(
        self,
        table_name,
        *,
        schema_name,
        columns,
        records,
    ):
        copy_call = _CopyCall(
            table_name=table_name,
            schema_name=schema_name,
            columns=tuple(columns),
            records=tuple(records),
        )
        self.copy_calls.append(copy_call)
        if table_name == self.bad_copy_table:
            return "COPY 0"
        return f"COPY {len(copy_call.records)}"

    def _complete_record(self):
        record = expected_generation_metadata(self.bundle, self.bundle.counts)
        return {
            **record,
            "generation_key": self.generation_key,
            "state": "complete",
            "completed_at": object(),
            "failed_at": None,
            "retired_at": None,
            "gc_after": None,
        }


class _BatchProbeConnection(_StoreConnection):
    def __init__(self, bundle, record_builder):
        super().__init__(bundle)
        self.record_builder = record_builder

    async def copy_records_to_table(self, table_name, **kwargs):
        records = kwargs["records"]
        prior_count = sum(len(call.records) for call in self.copy_calls)
        assert self.record_builder.record_count == prior_count + len(records)
        return await super().copy_records_to_table(table_name, **kwargs)


class _RecordBuilder:
    def __init__(self):
        self.record_count = 0

    def __call__(self, source_row):
        self.record_count += 1
        return (source_row,)
