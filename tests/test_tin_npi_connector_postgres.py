# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable PostgreSQL lifecycle proof for the TIN-to-NPI connector."""

from __future__ import annotations

import asyncio
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import uuid

import pytest

from process import tin_npi_connector as connector
from process.provider_directory_source_summary import (
    ProviderDirectorySourceSummaryBinding,
    build_source_summary,
)


asyncpg = pytest.importorskip("asyncpg")

ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = ROOT / "alembic" / "versions" / "20260729110000_tin_npi_connector.py"
POSTGRES_DSN_ENV = "HLTHPRT_TIN_NPI_CONNECTOR_POSTGRES_DSN"
_TEST_DATABASE_PATTERN = re.compile(r"(?:^|[_-])test(?:[_-]|$)", re.IGNORECASE)


class _Capture:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: str) -> None:
        self.statements.append(statement)


def _load_migration():
    spec = importlib.util.spec_from_file_location(
        "tin_npi_connector_postgres_migration",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


async def _connection():
    dsn = os.getenv(POSTGRES_DSN_ENV)
    if not dsn:
        pytest.skip(f"set {POSTGRES_DSN_ENV} for the PostgreSQL proof")
    connection = await asyncpg.connect(dsn)
    database_name = str(await connection.fetchval("SELECT current_database()"))
    if _TEST_DATABASE_PATTERN.search(database_name) is None:
        await connection.close()
        pytest.fail(f"{POSTGRES_DSN_ENV} must target an explicit test database")
    return connection


async def _run_migration(migration, action: str, connection) -> list[str]:
    capture = _Capture()
    migration.op = capture
    getattr(migration, action)()
    for statement in capture.statements:
        await connection.execute(statement)
    return capture.statements


async def _create_provider_directory_fence_tables(connection, schema: str) -> None:
    quoted_schema = f'"{schema}"'
    await connection.execute(
        f"""
        CREATE TABLE {quoted_schema}.provider_directory_api_endpoint (
            endpoint_id varchar(64) PRIMARY KEY
        )
        """
    )
    await connection.execute(
        f"""
        CREATE TABLE {quoted_schema}.provider_directory_source (
            source_id varchar(64) PRIMARY KEY,
            endpoint_id varchar(64)
        )
        """
    )
    await connection.execute(
        f"""
        CREATE TABLE {quoted_schema}.provider_directory_endpoint_dataset (
            dataset_id varchar(96) PRIMARY KEY,
            endpoint_id varchar(64) NOT NULL,
            import_run_id varchar(64),
            acquisition_root_run_id varchar(64),
            previous_dataset_id varchar(96),
            dataset_hash varchar(64),
            status varchar(32) NOT NULL,
            is_current boolean NOT NULL,
            resource_count bigint NOT NULL,
            validated_at timestamp,
            published_at timestamp,
            superseded_at timestamp,
            publication_metadata_json jsonb
        )
        """
    )
    await connection.execute(
        f"""
        CREATE TABLE {quoted_schema}.provider_directory_dataset_resource (
            dataset_id varchar(96) NOT NULL
                REFERENCES
                    {quoted_schema}.provider_directory_endpoint_dataset (
                        dataset_id
                    ),
            resource_type varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            payload_hash varchar(64) NOT NULL,
            payload_json jsonb NOT NULL,
            PRIMARY KEY (dataset_id, resource_type, resource_id)
        )
        """
    )
    await connection.execute(
        f"""
        CREATE TABLE {quoted_schema}.ptg2_provider_tax_identity_manifest (
            snapshot_key bigint PRIMARY KEY,
            token_policy_id varchar(64) NOT NULL,
            token_policy_descriptor_sha256 bytea NOT NULL
        )
        """
    )


def _scan_proof(
    *,
    token_policy_id: str,
    identifier_rule: connector.FhirTinNpiIdentifierRule,
    evidence_rows: tuple[connector.FhirTinNpiEvidence, ...],
    source_summary_sha256: str,
    organization_resource_count: int,
    organization_resource_sha256: str,
    matched_organization_count: int,
    matched_evidence_counts: tuple[tuple[str, int], ...] | None = None,
) -> connector.FhirOrganizationScanProof:
    return connector.FhirOrganizationScanProof(
        source_id="source-a",
        endpoint_id="endpoint-a",
        dataset_id="dataset-a",
        source_summary_sha256=source_summary_sha256,
        identifier_rule_id=identifier_rule.rule_id,
        identifier_rule_sha256=identifier_rule.descriptor_sha256,
        organization_resource_count=organization_resource_count,
        organization_resource_sha256=organization_resource_sha256,
        state_counts=tuple(
            (
                state.value,
                (
                    matched_organization_count
                    if state is connector.FhirOrganizationEvidenceState.MATCHED
                    else (
                        organization_resource_count - matched_organization_count
                        if state
                        is connector.FhirOrganizationEvidenceState.MISSING_IDENTIFIERS
                        else 0
                    )
                ),
            )
            for state in sorted(
                connector.FHIR_ORGANIZATION_SCAN_TERMINAL_STATES,
                key=lambda candidate: candidate.value,
            )
        ),
        matched_evidence_counts=(
            matched_evidence_counts
            if matched_evidence_counts is not None
            else ((token_policy_id, len(evidence_rows)),)
        ),
        matched_evidence_sha256=(
            connector.canonical_fhir_evidence_set_digest(evidence_rows).hex()
        ),
    )


async def _expect_postgres_error(
    connection,
    marker: str,
    statement: str,
    *parameters,
) -> None:
    try:
        async with connection.transaction():
            await connection.execute(statement, *parameters)
    except asyncpg.PostgresError as exc:
        assert marker in str(exc)
    else:
        raise AssertionError(f"expected PostgreSQL error containing {marker!r}")


@pytest.mark.asyncio
async def test_connector_migration_upgrades_and_downgrades_only_when_empty(
    monkeypatch,
):
    connection = await _connection()
    transaction = connection.transaction()
    await transaction.start()
    schema = f"tin_npi_connector_test_{uuid.uuid4().hex}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    migration = _load_migration()
    try:
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        await _create_provider_directory_fence_tables(connection, schema)
        await _run_migration(migration, "upgrade", connection)
        downgrade_capture = _Capture()
        migration.op = downgrade_capture
        migration.downgrade()
        downgrade_fence = downgrade_capture.statements[0]
        token_policy = connector.TinTokenPolicyDescriptor.release_1(
            "ptg-tin-hmac-sha256-v1:release-1"
        )
        registry_transaction = connection.transaction()
        await registry_transaction.start()
        await connection.execute(
            f"""
            INSERT INTO "{schema}".tin_npi_connector_token_policy (
                token_policy_id,
                token_policy_descriptor_sha256
            ) VALUES ($1, $2)
            """,
            token_policy.token_policy_id,
            bytes.fromhex(token_policy.token_policy_descriptor_sha256),
        )
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_downgrade_requires_empty_inactive_foundation",
            downgrade_fence,
        )
        await registry_transaction.rollback()
        identifier_policy = connector.DEFAULT_FHIR_TIN_NPI_IDENTIFIER_POLICY
        registry_transaction = connection.transaction()
        await registry_transaction.start()
        await connection.execute(
            f"""
            INSERT INTO "{schema}".tin_npi_connector_identifier_policy (
                identifier_policy_id,
                descriptor_canonical_json,
                identifier_policy_sha256
            ) VALUES ($1, $2, $3)
            """,
            identifier_policy.policy_id,
            identifier_policy.descriptor_canonical_json,
            bytes.fromhex(identifier_policy.descriptor_sha256),
        )
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_downgrade_requires_empty_inactive_foundation",
            downgrade_fence,
        )
        await registry_transaction.rollback()
        await _run_migration(migration, "downgrade", connection)
        table_count = await connection.fetchval(
            """
            SELECT COUNT(*)
              FROM information_schema.tables
             WHERE table_schema = $1
               AND table_name LIKE 'tin_npi_connector_%'
            """,
            schema,
        )
        assert table_count == 0
    finally:
        await transaction.rollback()
        await connection.close()


@pytest.mark.asyncio
async def test_two_policy_record_parity_is_required_at_generation_seal(monkeypatch):
    connection = await _connection()
    transaction = connection.transaction()
    await transaction.start()
    schema = f"tin_npi_connector_test_{uuid.uuid4().hex}"
    quoted_schema = f'"{schema}"'
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    migration = _load_migration()
    try:
        await connection.execute(f"CREATE SCHEMA {quoted_schema}")
        await _create_provider_directory_fence_tables(connection, schema)
        organization_rows = (
            ("organization-a", "11" * 32),
            ("organization-b", "22" * 32),
        )
        organization_resource_sha256 = (
            connector.canonical_fhir_organization_identity_sha256(organization_rows)
        )
        source_summary = build_source_summary(
            binding=ProviderDirectorySourceSummaryBinding(
                dataset_id="dataset-a",
                endpoint_id="endpoint-a",
                acquisition_root_run_id="run-a",
                dataset_hash="ab" * 32,
            ),
            source_ids=("source-a",),
            selected_resources=("Organization",),
            count_by_resource={"Organization": len(organization_rows)},
            hash_by_resource={"Organization": organization_resource_sha256},
            count_by_field={"organization_resources": len(organization_rows)},
        )
        await connection.execute(
            f"""
            INSERT INTO {quoted_schema}.provider_directory_api_endpoint (
                endpoint_id
            ) VALUES ('endpoint-a')
            """
        )
        await connection.execute(
            f"""
            INSERT INTO {quoted_schema}.provider_directory_source (
                source_id,
                endpoint_id
            ) VALUES ('source-a', 'endpoint-a')
            """
        )
        await connection.execute(
            f"""
            INSERT INTO {quoted_schema}.provider_directory_endpoint_dataset (
                dataset_id,
                endpoint_id,
                acquisition_root_run_id,
                dataset_hash,
                status,
                is_current,
                resource_count,
                validated_at,
                published_at,
                publication_metadata_json
            ) VALUES (
                'dataset-a',
                'endpoint-a',
                'run-a',
                $1,
                'published',
                TRUE,
                2,
                timestamp '2026-07-27 00:00:00',
                timestamp '2026-07-27 00:01:00',
                $2::jsonb
            )
            """,
            "ab" * 32,
            json.dumps(
                {
                    "expected_resources": ["Organization"],
                    "selected_resources": ["Organization"],
                    "source_ids": ["source-a"],
                    "source_summary_v1": source_summary,
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
        )
        await connection.executemany(
            f"""
            INSERT INTO {quoted_schema}.provider_directory_dataset_resource (
                dataset_id,
                resource_type,
                resource_id,
                payload_hash,
                payload_json
            ) VALUES ('dataset-a', 'Organization', $1, $2, $3::jsonb)
            """,
            [
                (
                    resource_id,
                    payload_hash,
                    json.dumps(
                        {
                            "id": resource_id,
                            "resourceType": "Organization",
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                )
                for resource_id, payload_hash in organization_rows
            ],
        )
        relation_oid = int(
            await connection.fetchval(
                "SELECT to_regclass($1)::oid",
                f"{schema}.provider_directory_dataset_resource",
            )
        )
        await _run_migration(migration, "upgrade", connection)

        token_policies = (
            connector.TinTokenPolicyDescriptor.release_1(
                "ptg-tin-hmac-sha256-v1:release-1"
            ),
            connector.TinTokenPolicyDescriptor.release_1(
                "ptg-tin-hmac-sha256-v1:release-2"
            ),
        )
        await connection.executemany(
            f"""
            INSERT INTO {quoted_schema}.tin_npi_connector_token_policy (
                token_policy_id,
                token_policy_descriptor_sha256
            ) VALUES ($1, $2)
            """,
            [
                (
                    policy.token_policy_id,
                    bytes.fromhex(policy.token_policy_descriptor_sha256),
                )
                for policy in token_policies
            ],
        )
        identifier_rule = connector.FhirTinNpiIdentifierRule(
            rule_id="healthporta.test.source-a.endpoint-a.tax-as-ein.v1",
            source_id="source-a",
            endpoint_id="endpoint-a",
            npi_systems=("http://hl7.org/fhir/sid/us-npi",),
            npi_type_codings=(
                (
                    "http://terminology.hl7.org/CodeSystem/v2-0203",
                    "NPI",
                ),
            ),
            ein_systems=(),
            ein_type_codings=(
                (
                    "http://terminology.hl7.org/CodeSystem/v2-0203",
                    "TAX",
                ),
            ),
        )
        identifier_policy = connector.FhirTinNpiIdentifierPolicy(
            policy_id="healthporta.test.fhir-tax-as-ein.v2",
            rules=(identifier_rule,),
        )
        await connection.execute(
            f"""
            INSERT INTO {quoted_schema}.tin_npi_connector_identifier_policy (
                identifier_policy_id,
                descriptor_canonical_json,
                identifier_policy_sha256
            ) VALUES ($1, $2, $3)
            """,
            identifier_policy.policy_id,
            identifier_policy.descriptor_canonical_json,
            bytes.fromhex(identifier_policy.descriptor_sha256),
        )
        relation = connector.ConnectorRelationIdentity(
            schema=schema,
            relation="provider_directory_dataset_resource",
            relation_oid=relation_oid,
        )
        dataset = connector.FhirDatasetFenceIdentity(
            source_id="source-a",
            endpoint_id="endpoint-a",
            dataset_id="dataset-a",
            evidence_run_id="run-a",
            selected_resources=("Organization",),
            expected_resources=("Organization",),
            recorded_expected_resources=("Organization",),
            status="published",
            is_current=True,
            promote_on_cutover=False,
            dataset_hash="ab" * 32,
            resource_count=len(organization_rows),
            organization_resource_count=len(organization_rows),
            organization_resource_sha256=organization_resource_sha256,
            source_summary_sha256=source_summary["summary_sha256"],
            identifier_rule_id=identifier_rule.rule_id,
            identifier_rule_sha256=identifier_rule.descriptor_sha256,
            validated_at="2026-07-27 00:00:00",
        )
        tokens = {}
        for token_ordinal, (policy_ordinal, policy_name, resource_id) in enumerate(
            (
                (0, "release-1", "organization-a"),
                (0, "release-1", "organization-b"),
                (1, "release-2", "organization-a"),
                (1, "release-2", "organization-b"),
            ),
            start=1,
        ):
            token_hmac = bytes([token_ordinal * 16]) * 32
            tokens[(policy_name, resource_id)] = connector.TinTaxIdentityToken(
                token_policy_id=token_policies[policy_ordinal].token_policy_id,
                tin_id_128=token_hmac[:16],
                tin_hmac_sha256=token_hmac,
            )

        def evidence_row(
            *,
            token: connector.TinTaxIdentityToken,
            npi: int,
            resource_ordinal: int,
            evidence_as_of: str,
            payload_hash: str | None = None,
        ) -> connector.FhirTinNpiEvidence:
            resource_id, canonical_payload_hash = organization_rows[resource_ordinal]
            return connector.FhirTinNpiEvidence(
                token=token,
                npi=npi,
                source_id="source-a",
                source_endpoint_id="endpoint-a",
                source_dataset_id="dataset-a",
                source_record_hmac_sha256=bytes([resource_ordinal + 1]) * 32,
                source_record_identity_sha256=(
                    connector._fhir_organization_record_identity_sha256(
                        resource_id,
                        canonical_payload_hash,
                    )
                ),
                source_record_payload_hash=(
                    payload_hash if payload_hash is not None else canonical_payload_hash
                ),
                evidence_as_of=evidence_as_of,
                identifier_policy_id=identifier_policy.policy_id,
                identifier_policy_sha256=identifier_policy.descriptor_sha256,
                identifier_rule_id=identifier_rule.rule_id,
                identifier_rule_sha256=identifier_rule.descriptor_sha256,
            )

        async def seal_case(
            *,
            case_ordinal: int,
            evidence_rows: tuple[connector.FhirTinNpiEvidence, ...],
            should_seal: bool,
        ) -> int:
            evidence_as_of = f"2026-07-27T01:00:0{case_ordinal}.000000Z"
            assert all(row.evidence_as_of == evidence_as_of for row in evidence_rows)
            source_vector = connector.TinNpiConnectorSourceVector(
                fhir_datasets=(dataset,),
                input_relations=(relation,),
                token_policies=token_policies,
                evidence_as_of=evidence_as_of,
                identifier_policy=identifier_policy,
            )
            policy_evidence_counts = tuple(
                (
                    policy.token_policy_id,
                    sum(
                        row.token.token_policy_id == policy.token_policy_id
                        for row in evidence_rows
                    ),
                )
                for policy in token_policies
            )
            scan_proof = _scan_proof(
                token_policy_id=token_policies[0].token_policy_id,
                identifier_rule=identifier_rule,
                evidence_rows=evidence_rows,
                source_summary_sha256=source_summary["summary_sha256"],
                organization_resource_count=len(organization_rows),
                organization_resource_sha256=organization_resource_sha256,
                matched_organization_count=len(organization_rows),
                matched_evidence_counts=policy_evidence_counts,
            )
            scan_proofs = (scan_proof,)
            scan_proof_json = connector.canonical_fhir_organization_scan_proof_json(
                scan_proofs
            )
            scan_proof_digest = connector.canonical_fhir_organization_scan_proof_digest(
                scan_proofs
            )
            lookup_rows = connector._factor_forward_rows(
                evidence_rows,
                source_ordinal_map=("source-a",),
            )
            lookup_digest = connector._lookup_digest(lookup_rows)
            generation_id = bytes.fromhex(
                connector._generation_id(
                    source_vector_id=source_vector.source_vector_id,
                    scan_proof_digest=scan_proof_digest,
                    lookup_digest=lookup_digest,
                )
            )
            build_token = f"connector-two-policy-case-{case_ordinal}"
            generation_key = int(
                await connection.fetchval(
                    f"""
                    INSERT INTO {quoted_schema}.tin_npi_connector_generation (
                        generation_id,
                        source_vector_id,
                        source_vector_canonical_json,
                        schema_version,
                        lookup_schema_version,
                        lookup_contract_id,
                        generation_contract,
                        raw_policy,
                        projection_policy_id,
                        relationship_class,
                        site_resolution_contract_id,
                        source_record_identity_contract_id,
                        identifier_policy_id,
                        identifier_policy_sha256,
                        evidence_as_of,
                        source_ordinal_contract,
                        source_ordinal_map_canonical_json,
                        source_ordinal_map_digest,
                        scan_contract_id,
                        scan_proof_canonical_json,
                        scan_proof_digest,
                        source_count,
                        source_dataset_count,
                        source_relation_count,
                        token_policy_count,
                        lookup_digest,
                        organization_count,
                        matched_organization_count,
                        evidence_count,
                        forward_row_count,
                        reverse_row_count,
                        npi_edge_count,
                        build_token_sha256,
                        build_lease_expires_at,
                        state
                    ) VALUES (
                        $1, $2, $3, 3, 2,
                        'healthporta.tin-npi.compact-lookup.v2',
                        'tin_npi_connector_generation_v3',
                        'token_only_v1',
                        $4,
                        'same_organization_identifier',
                        $5,
                        $6,
                        $7,
                        $8,
                        $9,
                        'source_id_sorted_utf8_lsb0_bitmap_v1',
                        $10,
                        $11,
                        $12,
                        $13,
                        $14,
                        1, 1, 1, 2,
                        $15,
                        2, 2, $16, $17, $18, $19,
                        $20,
                        transaction_timestamp() + interval '1 hour',
                        'building'
                    )
                    RETURNING generation_key
                    """,
                    generation_id,
                    bytes.fromhex(source_vector.source_vector_id),
                    source_vector.canonical_json,
                    source_vector.projection_policy_id,
                    connector.TIN_NPI_SITE_RESOLUTION_CONTRACT_ID,
                    connector.FHIR_SOURCE_RECORD_HMAC_MESSAGE_FORMAT_ID,
                    identifier_policy.policy_id,
                    bytes.fromhex(identifier_policy.descriptor_sha256),
                    source_vector.evidence_as_of,
                    connector.canonical_source_ordinal_map_json(("source-a",)),
                    connector.canonical_source_ordinal_map_digest(("source-a",)),
                    connector.TIN_NPI_FHIR_ORGANIZATION_SCAN_CONTRACT_ID,
                    scan_proof_json,
                    scan_proof_digest,
                    lookup_digest,
                    len(evidence_rows),
                    len(lookup_rows),
                    len({npi for row in lookup_rows for npi in row.npis}),
                    sum(len(row.npis) for row in lookup_rows),
                    hashlib.sha256(build_token.encode()).digest(),
                )
            )
            await connection.execute(
                "SELECT set_config(" "'healthporta.tin_npi_build_token', $1, TRUE" ")",
                build_token,
            )
            await connection.executemany(
                f"""
                INSERT INTO
                    {quoted_schema}.tin_npi_connector_generation_policy (
                        generation_key,
                        token_policy_id
                    )
                VALUES ($1, $2)
                """,
                [(generation_key, policy.token_policy_id) for policy in token_policies],
            )
            await connection.executemany(
                f"""
                INSERT INTO {quoted_schema}.tin_npi_connector_lookup (
                    generation_key,
                    token_policy_id,
                    tin_id_128,
                    tin_hmac_sha256,
                    npis,
                    evidence_count,
                    source_bitmap,
                    npi_source_bitmap_matrix,
                    source_evidence_counts
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """,
                [
                    (
                        generation_key,
                        row.token.token_policy_id,
                        row.token.tin_id_128,
                        row.token.tin_hmac_sha256,
                        list(row.npis),
                        row.evidence_count,
                        row.source_bitmap,
                        row.npi_source_bitmap_matrix,
                        list(row.source_evidence_counts),
                    )
                    for row in lookup_rows
                ],
            )
            await connection.executemany(
                f"""
                INSERT INTO {quoted_schema}.tin_npi_connector_evidence (
                    generation_key,
                    evidence_id,
                    token_policy_id,
                    tin_id_128,
                    tin_hmac_sha256,
                    npi,
                    source_ordinal,
                    relationship_class,
                    source_record_hmac_sha256,
                    source_record_identity_sha256,
                    source_record_payload_sha256,
                    identifier_policy_sha256,
                    identifier_rule_id,
                    identifier_rule_sha256
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, 0,
                    $7, $8, $9, $10, $11, $12, $13
                )
                """,
                [
                    (
                        generation_key,
                        row.evidence_id,
                        row.token.token_policy_id,
                        row.token.tin_id_128,
                        row.token.tin_hmac_sha256,
                        row.npi,
                        row.relationship_class,
                        row.source_record_hmac_sha256,
                        row.source_record_identity_sha256,
                        bytes.fromhex(row.source_record_payload_hash),
                        bytes.fromhex(row.identifier_policy_sha256),
                        row.identifier_rule_id,
                        bytes.fromhex(row.identifier_rule_sha256),
                    )
                    for row in evidence_rows
                ],
            )
            if should_seal:
                await connection.execute(
                    f"""
                    UPDATE {quoted_schema}.tin_npi_connector_generation
                       SET state = 'complete'
                     WHERE generation_key = $1
                    """,
                    generation_key,
                )
                assert (
                    await connection.fetchval(
                        f"""
                        SELECT state
                          FROM {quoted_schema}.tin_npi_connector_generation
                         WHERE generation_key = $1
                        """,
                        generation_key,
                    )
                    == "complete"
                )
            else:
                await _expect_postgres_error(
                    connection,
                    "tin_npi_connector_generation_seal_mismatch",
                    f"""
                    UPDATE {quoted_schema}.tin_npi_connector_generation
                       SET state = 'complete'
                     WHERE generation_key = $1
                    """,
                    generation_key,
                )
            return generation_key

        case_specs = (
            (
                True,
                (
                    ("release-1", "organization-a", 1000000004, 0, None),
                    ("release-2", "organization-a", 1000000004, 0, None),
                    ("release-1", "organization-b", 1234567893, 1, None),
                    ("release-2", "organization-b", 1234567893, 1, None),
                ),
            ),
            (
                False,
                (
                    ("release-1", "organization-a", 1000000004, 0, None),
                    ("release-1", "organization-a", 1234567893, 0, None),
                    ("release-2", "organization-b", 1000000004, 1, None),
                    ("release-2", "organization-b", 1234567893, 1, None),
                ),
            ),
            (
                False,
                (
                    ("release-1", "organization-a", 1000000004, 0, None),
                    ("release-2", "organization-a", 1000000004, 0, "ff" * 32),
                    ("release-1", "organization-b", 1234567893, 1, None),
                    ("release-2", "organization-b", 1234567893, 1, None),
                ),
            ),
        )
        for case_ordinal, (should_seal, evidence_specs) in enumerate(case_specs):
            evidence_as_of = f"2026-07-27T01:00:0{case_ordinal}.000000Z"
            case_evidence = tuple(
                evidence_row(
                    token=tokens[(policy_name, resource_id)],
                    npi=npi,
                    resource_ordinal=resource_ordinal,
                    evidence_as_of=evidence_as_of,
                    payload_hash=payload_hash,
                )
                for (
                    policy_name,
                    resource_id,
                    npi,
                    resource_ordinal,
                    payload_hash,
                ) in evidence_specs
            )
            await seal_case(
                case_ordinal=case_ordinal,
                evidence_rows=case_evidence,
                should_seal=should_seal,
            )
    finally:
        await transaction.rollback()
        await connection.close()


@pytest.mark.asyncio
async def test_dataset_resource_guard_serializes_both_validation_race_orders(
    monkeypatch,
):
    admin_connection = await _connection()
    writer_connection = await _connection()
    validator_connection = await _connection()
    schema = f"tin_npi_connector_test_{uuid.uuid4().hex}"
    quoted_schema = f'"{schema}"'
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    migration = _load_migration()
    try:
        await admin_connection.execute(f"CREATE SCHEMA {quoted_schema}")
        await _create_provider_directory_fence_tables(
            admin_connection,
            schema,
        )
        await _run_migration(migration, "upgrade", admin_connection)
        await admin_connection.execute(
            f"""
            INSERT INTO {quoted_schema}.provider_directory_endpoint_dataset (
                dataset_id,
                endpoint_id,
                status,
                is_current,
                resource_count
            ) VALUES
                (
                    'dataset-validation-first',
                    'endpoint-a',
                    'acquiring',
                    false,
                    0
                ),
                (
                    'dataset-writer-first',
                    'endpoint-a',
                    'acquiring',
                    false,
                    0
                )
            """
        )

        validation_transaction = validator_connection.transaction()
        await validation_transaction.start()
        await validator_connection.execute(
            f"""
            UPDATE {quoted_schema}.provider_directory_endpoint_dataset
               SET status = 'validated'
             WHERE dataset_id = 'dataset-validation-first'
            """
        )
        blocked_writer = asyncio.create_task(
            writer_connection.execute(
                f"""
                INSERT INTO
                    {quoted_schema}.provider_directory_dataset_resource (
                        dataset_id,
                        resource_type,
                        resource_id,
                        payload_hash,
                        payload_json
                    ) VALUES (
                        'dataset-validation-first',
                        'Organization',
                        'organization-late',
                        $1,
                        '{{"id":"organization-late"}}'::jsonb
                    )
                """,
                "11" * 32,
            )
        )
        await asyncio.sleep(0.1)
        assert blocked_writer.done() is False
        await validation_transaction.commit()
        with pytest.raises(
            asyncpg.PostgresError,
            match="tin_npi_connector_dataset_resource_parent_immutable",
        ):
            await blocked_writer
        assert (
            await admin_connection.fetchval(
                f"""
                SELECT COUNT(*)
                  FROM {quoted_schema}.provider_directory_dataset_resource
                 WHERE dataset_id = 'dataset-validation-first'
                """
            )
            == 0
        )

        writer_transaction = writer_connection.transaction()
        await writer_transaction.start()
        await writer_connection.execute(
            f"""
            INSERT INTO {quoted_schema}.provider_directory_dataset_resource (
                dataset_id,
                resource_type,
                resource_id,
                payload_hash,
                payload_json
            ) VALUES (
                'dataset-writer-first',
                'Organization',
                'organization-early',
                $1,
                '{{"id":"organization-early"}}'::jsonb
            )
            """,
            "22" * 32,
        )
        blocked_validator = asyncio.create_task(
            validator_connection.execute(
                f"""
                UPDATE {quoted_schema}.provider_directory_endpoint_dataset
                   SET status = 'validated'
                 WHERE dataset_id = 'dataset-writer-first'
                """
            )
        )
        await asyncio.sleep(0.1)
        assert blocked_validator.done() is False
        await writer_transaction.commit()
        await blocked_validator
        assert (
            await admin_connection.fetchval(
                f"""
                SELECT COUNT(*)
                  FROM {quoted_schema}.provider_directory_dataset_resource
                 WHERE dataset_id = 'dataset-writer-first'
                """
            )
            == 1
        )
        assert (
            await admin_connection.fetchval(
                f"""
                SELECT status
                  FROM {quoted_schema}.provider_directory_endpoint_dataset
                 WHERE dataset_id = 'dataset-writer-first'
                """
            )
            == "validated"
        )
    finally:
        await writer_connection.close()
        await validator_connection.close()
        await admin_connection.execute(f"DROP SCHEMA IF EXISTS {quoted_schema} CASCADE")
        await admin_connection.close()


@pytest.mark.asyncio
async def test_connector_digest_build_cas_and_mutation_guards(monkeypatch):
    connection = await _connection()
    transaction = connection.transaction()
    await transaction.start()
    schema = f"tin_npi_connector_test_{uuid.uuid4().hex}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    migration = _load_migration()
    quoted_schema = f'"{schema}"'
    try:
        await connection.execute(f"CREATE SCHEMA {quoted_schema}")
        await _create_provider_directory_fence_tables(connection, schema)
        organization_rows = (
            ("organization-a", "11" * 32),
            ("organization-b", "22" * 32),
        )
        organization_resource_sha256 = (
            connector.canonical_fhir_organization_identity_sha256(organization_rows)
        )
        source_summary = build_source_summary(
            binding=ProviderDirectorySourceSummaryBinding(
                dataset_id="dataset-a",
                endpoint_id="endpoint-a",
                acquisition_root_run_id="run-a",
                dataset_hash="ab" * 32,
            ),
            source_ids=("source-a",),
            selected_resources=("Organization",),
            count_by_resource={"Organization": len(organization_rows)},
            hash_by_resource={
                "Organization": organization_resource_sha256,
            },
            count_by_field={
                "organization_resources": len(organization_rows),
            },
        )
        relation_oid = int(
            await connection.fetchval(
                "SELECT to_regclass($1)::oid",
                f"{schema}.provider_directory_dataset_resource",
            )
        )
        await connection.execute(
            f"""
            INSERT INTO {quoted_schema}.provider_directory_api_endpoint (
                endpoint_id
            ) VALUES ('endpoint-a')
            """
        )
        await connection.execute(
            f"""
            INSERT INTO {quoted_schema}.provider_directory_source (
                source_id,
                endpoint_id
            ) VALUES ('source-a', 'endpoint-a')
            """
        )
        await connection.execute(
            f"""
            INSERT INTO {quoted_schema}.provider_directory_endpoint_dataset (
                dataset_id,
                endpoint_id,
                acquisition_root_run_id,
                previous_dataset_id,
                dataset_hash,
                status,
                is_current,
                resource_count,
                validated_at,
                published_at,
                superseded_at,
                publication_metadata_json
            ) VALUES (
                'dataset-a',
                'endpoint-a',
                'run-a',
                NULL,
                $1,
                'published',
                TRUE,
                $3,
                timestamp '2026-07-27 00:00:00',
                timestamp '2026-07-27 00:01:00',
                NULL,
                $2::jsonb
            )
            """,
            "ab" * 32,
            json.dumps(
                {
                    "expected_resources": ["Organization"],
                    "selected_resources": ["Organization"],
                    "source_ids": ["source-a"],
                    "source_summary_v1": source_summary,
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
            len(organization_rows),
        )
        await connection.executemany(
            f"""
            INSERT INTO {quoted_schema}.provider_directory_dataset_resource (
                dataset_id,
                resource_type,
                resource_id,
                payload_hash,
                payload_json
            ) VALUES ('dataset-a', 'Organization', $1, $2, $3::jsonb)
            """,
            [
                (
                    resource_id,
                    payload_hash,
                    json.dumps(
                        {
                            "id": resource_id,
                            "resourceType": "Organization",
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                )
                for resource_id, payload_hash in organization_rows
            ],
        )
        await _run_migration(migration, "upgrade", connection)
        endpoint_dataset_guard_count = await connection.fetchval(
            f"""
            SELECT COUNT(*)
              FROM pg_catalog.pg_trigger AS trigger_row
             WHERE trigger_row.tgrelid =
                       '{quoted_schema}.provider_directory_endpoint_dataset'
                           ::regclass
               AND trigger_row.tgname =
                       'tin_npi_connector_endpoint_dataset_guard'
               AND trigger_row.tgtype = 31
               AND trigger_row.tgenabled = 'A'
               AND trigger_row.tgisinternal IS FALSE
               AND trigger_row.tgfoid =
                       '{quoted_schema}.guard_tin_npi_connector_endpoint_dataset()'
                           ::regprocedure
            """
        )
        assert endpoint_dataset_guard_count == 1
        guard_function_acl = await connection.fetchrow(
            """
            SELECT COUNT(*) AS function_count,
                   COUNT(*) FILTER (
                       WHERE function_acl.grantee = 0
                         AND function_acl.privilege_type = 'EXECUTE'
                   ) AS public_execute_count
              FROM pg_catalog.pg_proc AS function_row
              JOIN pg_catalog.pg_namespace AS function_namespace
                ON function_namespace.oid = function_row.pronamespace
              CROSS JOIN LATERAL pg_catalog.aclexplode(
                    COALESCE(
                        function_row.proacl,
                        pg_catalog.acldefault(
                            'f',
                            function_row.proowner
                        )
                    )
                   ) AS function_acl
             WHERE function_namespace.nspname = $1
               AND function_row.pronargs = 0
               AND function_row.proname = ANY($2::text[])
            """,
            schema,
            [
                "guard_tin_npi_connector_dataset_resource",
                "guard_tin_npi_connector_endpoint_dataset",
            ],
        )
        assert guard_function_acl["function_count"] == 2
        assert guard_function_acl["public_execute_count"] == 0
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_endpoint_dataset_insert_invalid",
            f"""
            INSERT INTO {quoted_schema}.provider_directory_endpoint_dataset (
                dataset_id,
                endpoint_id,
                status,
                is_current,
                resource_count,
                validated_at,
                published_at
            ) VALUES (
                'dataset-invalid-insert',
                'endpoint-a',
                'published',
                true,
                0,
                transaction_timestamp(),
                transaction_timestamp()
            )
            """,
        )
        await connection.execute(
            f"""
            INSERT INTO {quoted_schema}.provider_directory_endpoint_dataset (
                dataset_id,
                endpoint_id,
                status,
                is_current,
                resource_count
            ) VALUES (
                'dataset-invalid-transition',
                'endpoint-a',
                'acquiring',
                false,
                0
            )
            """
        )
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_endpoint_dataset_transition_invalid",
            f"""
            UPDATE {quoted_schema}.provider_directory_endpoint_dataset
               SET status = 'published',
                   is_current = true,
                   published_at = transaction_timestamp()
             WHERE dataset_id = 'dataset-invalid-transition'
            """,
        )
        await connection.execute(
            f"""
            DELETE FROM {quoted_schema}.provider_directory_endpoint_dataset
             WHERE dataset_id = 'dataset-invalid-transition'
            """
        )
        await connection.execute(
            f"""
            INSERT INTO {quoted_schema}.provider_directory_endpoint_dataset (
                dataset_id,
                endpoint_id,
                status,
                is_current,
                resource_count
            ) VALUES (
                'dataset-missing-validation-time',
                'endpoint-a',
                'acquiring',
                false,
                0
            )
            """
        )
        await connection.execute(
            f"""
            UPDATE {quoted_schema}.provider_directory_endpoint_dataset
               SET status = 'validated'
             WHERE dataset_id = 'dataset-missing-validation-time'
            """
        )
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_endpoint_dataset_transition_invalid",
            f"""
            UPDATE {quoted_schema}.provider_directory_endpoint_dataset
               SET status = 'published',
                   is_current = true,
                   published_at = transaction_timestamp()
             WHERE dataset_id = 'dataset-missing-validation-time'
            """,
        )
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_dataset_resource_parent_immutable",
            f"""
            INSERT INTO {quoted_schema}.provider_directory_dataset_resource (
                dataset_id,
                resource_type,
                resource_id,
                payload_hash,
                payload_json
            ) VALUES (
                'dataset-a',
                'Organization',
                'organization-3',
                $1,
                '{{"id":"organization-3"}}'::jsonb
            )
            """,
            "33" * 32,
        )
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_dataset_resource_parent_immutable",
            f"""
            UPDATE {quoted_schema}.provider_directory_dataset_resource
               SET payload_hash = $1
             WHERE dataset_id = 'dataset-a'
               AND resource_type = 'Organization'
               AND resource_id = 'organization-a'
            """,
            "44" * 32,
        )
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_dataset_resource_parent_immutable",
            f"""
            DELETE FROM {quoted_schema}.provider_directory_dataset_resource
             WHERE dataset_id = 'dataset-a'
               AND resource_type = 'Organization'
               AND resource_id = 'organization-a'
            """,
        )
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_dataset_resource_truncate_forbidden",
            f"TRUNCATE {quoted_schema}.provider_directory_dataset_resource",
        )
        await connection.execute("SET LOCAL session_replication_role = replica")
        try:
            await _expect_postgres_error(
                connection,
                "tin_npi_connector_dataset_resource_parent_immutable",
                f"""
                UPDATE {quoted_schema}.provider_directory_dataset_resource
                   SET payload_hash = $1
                 WHERE dataset_id = 'dataset-a'
                   AND resource_type = 'Organization'
                   AND resource_id = 'organization-a'
                """,
                "55" * 32,
            )
        finally:
            await connection.execute("SET LOCAL session_replication_role = origin")
        await connection.execute(
            f"""
            INSERT INTO {quoted_schema}.provider_directory_endpoint_dataset (
                dataset_id,
                endpoint_id,
                import_run_id,
                acquisition_root_run_id,
                previous_dataset_id,
                dataset_hash,
                status,
                is_current,
                resource_count,
                publication_metadata_json
            ) VALUES (
                'dataset-cleanup',
                'endpoint-a',
                'run-cleanup',
                'run-cleanup',
                'dataset-a',
                $1,
                'acquiring',
                false,
                1,
                '{{}}'::jsonb
            )
            """,
            "cd" * 32,
        )
        await connection.execute(
            f"""
            INSERT INTO {quoted_schema}.provider_directory_dataset_resource (
                dataset_id,
                resource_type,
                resource_id,
                payload_hash,
                payload_json
            ) VALUES (
                'dataset-cleanup',
                'Organization',
                'organization-cleanup',
                $1,
                '{{"id":"organization-cleanup"}}'::jsonb
            )
            """,
            "66" * 32,
        )
        await connection.execute(
            f"""
            UPDATE {quoted_schema}.provider_directory_dataset_resource
               SET payload_hash = $1
             WHERE dataset_id = 'dataset-cleanup'
            """,
            "77" * 32,
        )
        await connection.execute(
            f"""
            DELETE FROM {quoted_schema}.provider_directory_dataset_resource
             WHERE dataset_id = 'dataset-cleanup'
            """
        )
        await connection.execute(
            f"""
            INSERT INTO {quoted_schema}.provider_directory_dataset_resource (
                dataset_id,
                resource_type,
                resource_id,
                payload_hash,
                payload_json
            ) VALUES (
                'dataset-cleanup',
                'Organization',
                'organization-cleanup',
                $1,
                '{{"id":"organization-cleanup"}}'::jsonb
            )
            """,
            "88" * 32,
        )
        await connection.execute(
            f"""
            UPDATE {quoted_schema}.provider_directory_endpoint_dataset
               SET status = 'validated',
                   validated_at = transaction_timestamp()
             WHERE dataset_id = 'dataset-cleanup'
            """
        )
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_dataset_resource_parent_immutable",
            f"""
            DELETE FROM {quoted_schema}.provider_directory_dataset_resource
             WHERE dataset_id = 'dataset-cleanup'
            """,
        )
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_endpoint_dataset_transition_invalid",
            f"""
            UPDATE {quoted_schema}.provider_directory_endpoint_dataset
               SET status = 'published',
                   is_current = TRUE,
                   published_at =
                       transaction_timestamp() - interval '1 second'
             WHERE dataset_id = 'dataset-cleanup'
            """,
        )
        await connection.execute(
            f"""
            UPDATE {quoted_schema}.provider_directory_endpoint_dataset
               SET status = 'published',
                   is_current = TRUE,
                   published_at = transaction_timestamp()
             WHERE dataset_id = 'dataset-cleanup'
            """
        )
        await connection.execute(
            f"""
            UPDATE {quoted_schema}.provider_directory_endpoint_dataset
               SET status = 'superseded',
                   is_current = FALSE,
                   superseded_at = transaction_timestamp()
             WHERE dataset_id = 'dataset-cleanup'
            """
        )
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_dataset_resource_parent_immutable",
            f"""
            UPDATE {quoted_schema}.provider_directory_dataset_resource
               SET payload_hash = $1
             WHERE dataset_id = 'dataset-cleanup'
            """,
            "99" * 32,
        )
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_dataset_resource_parent_immutable",
            f"""
            INSERT INTO {quoted_schema}.provider_directory_dataset_resource (
                dataset_id,
                resource_type,
                resource_id,
                payload_hash,
                payload_json
            ) VALUES (
                'dataset-cleanup',
                'Organization',
                'organization-cleanup-2',
                $1,
                '{{"id":"organization-cleanup-2"}}'::jsonb
            )
            """,
            "aa" * 32,
        )
        await connection.execute(
            f"""
            DELETE FROM {quoted_schema}.provider_directory_dataset_resource
             WHERE dataset_id = 'dataset-cleanup'
            """
        )
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_endpoint_dataset_delete_forbidden",
            f"""
            DELETE FROM {quoted_schema}.provider_directory_endpoint_dataset
             WHERE dataset_id = 'dataset-cleanup'
            """,
        )
        await connection.execute(
            f"""
            INSERT INTO {quoted_schema}.provider_directory_endpoint_dataset (
                dataset_id,
                endpoint_id,
                status,
                is_current,
                resource_count
            ) VALUES (
                'dataset-verification-baseline',
                'endpoint-a',
                'verification_baseline',
                false,
                1
            )
            """
        )
        await connection.execute(
            f"""
            INSERT INTO {quoted_schema}.provider_directory_dataset_resource (
                dataset_id,
                resource_type,
                resource_id,
                payload_hash,
                payload_json
            ) VALUES (
                'dataset-verification-baseline',
                'Organization',
                'organization-baseline',
                $1,
                '{{"id":"organization-baseline"}}'::jsonb
            )
            """,
            "bb" * 32,
        )
        await connection.execute(
            f"""
            DELETE FROM {quoted_schema}.provider_directory_dataset_resource
             WHERE dataset_id = 'dataset-verification-baseline'
            """
        )

        token_policy_id = "ptg-tin-hmac-sha256-v1:release-1"
        token_policy = connector.TinTokenPolicyDescriptor.release_1(token_policy_id)
        await connection.execute(
            f"""
            INSERT INTO {quoted_schema}.ptg2_provider_tax_identity_manifest (
                snapshot_key,
                token_policy_id,
                token_policy_descriptor_sha256
            ) VALUES (1, $1, $2)
            """,
            token_policy_id,
            bytes.fromhex(token_policy.token_policy_descriptor_sha256),
        )
        identifier_rule = connector.FhirTinNpiIdentifierRule(
            rule_id="healthporta.test.source-a.endpoint-a.tax-as-ein.v1",
            source_id="source-a",
            endpoint_id="endpoint-a",
            npi_systems=("http://hl7.org/fhir/sid/us-npi",),
            npi_type_codings=(
                (
                    "http://terminology.hl7.org/CodeSystem/v2-0203",
                    "NPI",
                ),
            ),
            ein_systems=(),
            ein_type_codings=(
                (
                    "http://terminology.hl7.org/CodeSystem/v2-0203",
                    "TAX",
                ),
            ),
        )
        identifier_policy = connector.FhirTinNpiIdentifierPolicy(
            policy_id="healthporta.test.fhir-tax-as-ein.v2",
            rules=(identifier_rule,),
        )
        relation = connector.ConnectorRelationIdentity(
            schema=schema,
            relation="provider_directory_dataset_resource",
            relation_oid=relation_oid,
        )
        dataset = connector.FhirDatasetFenceIdentity(
            source_id="source-a",
            endpoint_id="endpoint-a",
            dataset_id="dataset-a",
            evidence_run_id="run-a",
            selected_resources=("Organization",),
            expected_resources=("Organization",),
            recorded_expected_resources=("Organization",),
            status="published",
            is_current=True,
            promote_on_cutover=False,
            dataset_hash="ab" * 32,
            resource_count=len(organization_rows),
            organization_resource_count=len(organization_rows),
            organization_resource_sha256=organization_resource_sha256,
            source_summary_sha256=source_summary["summary_sha256"],
            identifier_rule_id=identifier_rule.rule_id,
            identifier_rule_sha256=identifier_rule.descriptor_sha256,
            validated_at="2026-07-27 00:00:00",
        )
        source_vector = connector.TinNpiConnectorSourceVector(
            fhir_datasets=(dataset,),
            input_relations=(relation,),
            token_policies=(token_policy,),
            evidence_as_of="2026-07-27T00:00:00.000000Z",
            identifier_policy=identifier_policy,
        )
        token = connector.TinTaxIdentityToken(
            token_policy_id=token_policy_id,
            tin_id_128=bytes(range(16)),
            tin_hmac_sha256=bytes(range(32)),
        )
        lookup_row = connector.TinNpiLookupRow(
            token=token,
            relationship_class=connector.FHIR_SAME_ORGANIZATION_RELATIONSHIP,
            npis=(1000000004, 1234567893),
            evidence_count=2,
            source_ids=("source-a",),
            source_bitmap=b"\x01",
            npi_source_bitmap_matrix=b"\x01\x01",
            source_evidence_counts=(2,),
        )
        collision_token = connector.TinTaxIdentityToken(
            token_policy_id=token_policy_id,
            tin_id_128=token.tin_id_128,
            tin_hmac_sha256=token.tin_id_128 + b"\xff" * 16,
        )
        collision_row = connector.TinNpiLookupRow(
            token=collision_token,
            relationship_class=connector.FHIR_SAME_ORGANIZATION_RELATIONSHIP,
            npis=(1234567893,),
            evidence_count=1,
            source_ids=("source-a",),
            source_bitmap=b"\x01",
            npi_source_bitmap_matrix=b"\x01",
            source_evidence_counts=(1,),
        )
        evidence_rows = tuple(
            connector.FhirTinNpiEvidence(
                token=evidence_token,
                npi=npi,
                source_id="source-a",
                source_endpoint_id="endpoint-a",
                source_dataset_id="dataset-a",
                source_record_hmac_sha256=bytes([record_ordinal]) * 32,
                source_record_identity_sha256=(
                    connector._fhir_organization_record_identity_sha256(
                        resource_id,
                        payload_hash,
                    )
                ),
                source_record_payload_hash=payload_hash,
                evidence_as_of=source_vector.evidence_as_of,
                identifier_policy_id=identifier_policy.policy_id,
                identifier_policy_sha256=identifier_policy.descriptor_sha256,
                identifier_rule_id=identifier_rule.rule_id,
                identifier_rule_sha256=identifier_rule.descriptor_sha256,
            )
            for record_ordinal, (
                evidence_token,
                npi,
                (resource_id, payload_hash),
            ) in (
                (1, (token, 1000000004, organization_rows[0])),
                (1, (token, 1234567893, organization_rows[0])),
                (2, (collision_token, 1234567893, organization_rows[1])),
            )
        )
        scan_proof = _scan_proof(
            token_policy_id=token_policy_id,
            identifier_rule=identifier_rule,
            evidence_rows=evidence_rows,
            source_summary_sha256=source_summary["summary_sha256"],
            organization_resource_count=len(organization_rows),
            organization_resource_sha256=organization_resource_sha256,
            matched_organization_count=len(organization_rows),
        )
        scan_proofs = (scan_proof,)
        scan_proof_json = connector.canonical_fhir_organization_scan_proof_json(
            scan_proofs
        )
        scan_proof_digest = connector.canonical_fhir_organization_scan_proof_digest(
            scan_proofs
        )
        lookup_digest = connector._lookup_digest((lookup_row, collision_row))
        generation_id = bytes.fromhex(
            connector._generation_id(
                source_vector_id=source_vector.source_vector_id,
                scan_proof_digest=scan_proof_digest,
                lookup_digest=lookup_digest,
            )
        )
        build_token = "connector-build-proof-0001"
        build_token_sha256 = hashlib.sha256(build_token.encode()).digest()

        sql_descriptor = await connection.fetchval(
            f"""
            SELECT {quoted_schema}.
                   tin_npi_connector_token_policy_descriptor_sha256($1)
            """,
            token_policy_id,
        )
        assert bytes(sql_descriptor).hex() == (
            "a0c06f5494f80663686be6861038a880" "4d9509d0fdc2d2c8cc56c259e53d761c"
        )
        sql_identifier_rule_digest = await connection.fetchval(
            f"""
            SELECT {quoted_schema}.
                   tin_npi_connector_identifier_rule_sha256($1::jsonb)
            """,
            identifier_rule.descriptor_canonical_json,
        )
        assert bytes(sql_identifier_rule_digest) == bytes.fromhex(
            identifier_rule.descriptor_sha256
        )
        await connection.execute(
            f"""
            INSERT INTO {quoted_schema}.tin_npi_connector_token_policy (
                token_policy_id,
                token_policy_descriptor_sha256
            ) VALUES ($1, $2)
            """,
            token_policy_id,
            bytes.fromhex(token_policy.token_policy_descriptor_sha256),
        )
        identifier_descriptor_json = identifier_policy.descriptor_canonical_json
        assert await connection.fetchval(
            f"""
            SELECT {quoted_schema}.
                   tin_npi_connector_valid_identifier_policy($1, $2)
            """,
            identifier_descriptor_json,
            identifier_policy.policy_id,
        )
        sql_identifier_digest = await connection.fetchval(
            """
            SELECT sha256(
                convert_to(
                    'healthporta.tin-npi.fhir-identifier-policy.v2',
                    'UTF8'
                )
                || decode('00', 'hex')
                || convert_to($1, 'UTF8')
            )
            """,
            identifier_descriptor_json,
        )
        assert bytes(sql_identifier_digest) == bytes.fromhex(
            identifier_policy.descriptor_sha256
        )
        await connection.execute(
            f"""
            INSERT INTO {quoted_schema}.tin_npi_connector_identifier_policy (
                identifier_policy_id,
                descriptor_canonical_json,
                identifier_policy_sha256
            ) VALUES ($1, $2, $3)
            """,
            identifier_policy.policy_id,
            identifier_descriptor_json,
            bytes.fromhex(identifier_policy.descriptor_sha256),
        )
        generation_key = int(
            await connection.fetchval(
                f"""
                INSERT INTO {quoted_schema}.tin_npi_connector_generation (
                    generation_id,
                    source_vector_id,
                    source_vector_canonical_json,
                    schema_version,
                    lookup_schema_version,
                    lookup_contract_id,
                    generation_contract,
                    raw_policy,
                    projection_policy_id,
                    relationship_class,
                    site_resolution_contract_id,
                    source_record_identity_contract_id,
                    identifier_policy_id,
                    identifier_policy_sha256,
                    evidence_as_of,
                    source_ordinal_contract,
                    source_ordinal_map_canonical_json,
                    source_ordinal_map_digest,
                    scan_contract_id,
                    scan_proof_canonical_json,
                    scan_proof_digest,
                    source_count,
                    source_dataset_count,
                    source_relation_count,
                    token_policy_count,
                    lookup_digest,
                    organization_count,
                    matched_organization_count,
                    evidence_count,
                    forward_row_count,
                    reverse_row_count,
                    npi_edge_count,
                    build_token_sha256,
                    build_lease_expires_at,
                    state
                ) VALUES (
                    $1, $2, $3, 3, 2,
                    'healthporta.tin-npi.compact-lookup.v2',
                    'tin_npi_connector_generation_v3',
                    'token_only_v1',
                    $4,
                    'same_organization_identifier',
                    $5,
                    $6,
                    $7,
                    $8,
                    $9,
                    'source_id_sorted_utf8_lsb0_bitmap_v1',
                    $10,
                    $11,
                    $12,
                    $13,
                    $14,
                    1, 1, 1, 1,
                    $15,
                    2, 2, 3, 2, 2, 3,
                    $16,
                    transaction_timestamp() + interval '1 hour',
                    'building'
                )
                RETURNING generation_key
                """,
                generation_id,
                bytes.fromhex(source_vector.source_vector_id),
                source_vector.canonical_json,
                source_vector.projection_policy_id,
                connector.TIN_NPI_SITE_RESOLUTION_CONTRACT_ID,
                connector.FHIR_SOURCE_RECORD_HMAC_MESSAGE_FORMAT_ID,
                identifier_policy.policy_id,
                bytes.fromhex(identifier_policy.descriptor_sha256),
                source_vector.evidence_as_of,
                connector.canonical_source_ordinal_map_json(("source-a",)),
                connector.canonical_source_ordinal_map_digest(("source-a",)),
                connector.TIN_NPI_FHIR_ORGANIZATION_SCAN_CONTRACT_ID,
                scan_proof_json,
                scan_proof_digest,
                lookup_digest,
                build_token_sha256,
            )
        )
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_generation_must_start_building",
            f"""
            INSERT INTO {quoted_schema}.tin_npi_connector_generation
            OVERRIDING SYSTEM VALUE
            SELECT (
                jsonb_populate_record(
                    NULL::{quoted_schema}.tin_npi_connector_generation,
                    to_jsonb(candidate)
                    || jsonb_build_object(
                        'generation_key',
                        candidate.generation_key + 1000000000,
                        'evidence_as_of',
                        '2999-01-01T00:00:00.000000Z',
                        'created_at',
                        transaction_timestamp(),
                        'build_lease_expires_at',
                        transaction_timestamp() + interval '1 hour',
                        'state',
                        'building',
                        'completed_at',
                        NULL,
                        'failed_at',
                        NULL,
                        'retired_at',
                        NULL,
                        'gc_after',
                        NULL
                    )
                )
            ).*
              FROM {quoted_schema}.tin_npi_connector_generation AS candidate
             WHERE candidate.generation_key = $1
            """,
            generation_key,
        )
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_generation_not_loadable",
            f"""
            INSERT INTO {quoted_schema}.tin_npi_connector_generation_policy (
                generation_key,
                token_policy_id
            ) VALUES ($1, $2)
            """,
            generation_key,
            token_policy_id,
        )
        await connection.execute(
            "SELECT set_config(" "'healthporta.tin_npi_build_token', $1, TRUE" ")",
            build_token,
        )
        await connection.execute(
            f"""
            INSERT INTO {quoted_schema}.tin_npi_connector_generation_policy (
                generation_key,
                token_policy_id
            ) VALUES ($1, $2)
            """,
            generation_key,
            token_policy_id,
        )
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_lookup_payload_check",
            f"""
            INSERT INTO {quoted_schema}.tin_npi_connector_lookup (
                generation_key,
                token_policy_id,
                tin_id_128,
                tin_hmac_sha256,
                npis,
                evidence_count,
                source_bitmap,
                npi_source_bitmap_matrix,
                source_evidence_counts
            ) VALUES ($1, $2, $3, $4, $5, 2, $6, $7, $8)
            """,
            generation_key,
            token_policy_id,
            b"\xee" * 16,
            b"\xee" * 32,
            [1000000004, 1234567893],
            b"\x01",
            b"\x01\x00",
            [2],
        )
        await connection.execute(
            f"""
            INSERT INTO {quoted_schema}.tin_npi_connector_lookup (
                generation_key,
                token_policy_id,
                tin_id_128,
                tin_hmac_sha256,
                npis,
                evidence_count,
                source_bitmap,
                npi_source_bitmap_matrix,
                source_evidence_counts
            ) VALUES
                ($1, $2, $3, $4, $5, 2, $6, $11, $9),
                ($1, $2, $3, $7, $8, 1, $6, $12, $10)
            """,
            generation_key,
            token_policy_id,
            token.tin_id_128,
            token.tin_hmac_sha256,
            list(lookup_row.npis),
            lookup_row.source_bitmap,
            collision_token.tin_hmac_sha256,
            list(collision_row.npis),
            list(lookup_row.source_evidence_counts),
            list(collision_row.source_evidence_counts),
            lookup_row.npi_source_bitmap_matrix,
            collision_row.npi_source_bitmap_matrix,
        )
        for evidence_row in evidence_rows:
            sql_evidence_id = await connection.fetchval(
                f"""
                SELECT {quoted_schema}.
                       tin_npi_connector_evidence_id_sha256(
                           $1, $2, $3, $4, $5, $6, $7, $8, $9
                       )
                """,
                evidence_row.token.token_policy_id,
                evidence_row.token.tin_hmac_sha256,
                evidence_row.npi,
                evidence_row.relationship_class,
                evidence_row.source_record_hmac_sha256,
                evidence_row.source_record_identity_sha256,
                bytes.fromhex(evidence_row.source_record_payload_hash),
                bytes.fromhex(evidence_row.identifier_policy_sha256),
                bytes.fromhex(evidence_row.identifier_rule_sha256),
            )
            assert bytes(sql_evidence_id) == evidence_row.evidence_id
        assert evidence_rows[0].evidence_id.hex() == (
            "526e2237cf6f4e3c192672fffbeeda81" "a6f96f547a52f0dbb59b3417458c4359"
        )
        await connection.executemany(
            f"""
            INSERT INTO {quoted_schema}.tin_npi_connector_evidence (
                generation_key,
                evidence_id,
                token_policy_id,
                tin_id_128,
                tin_hmac_sha256,
                npi,
                source_ordinal,
                relationship_class,
                source_record_hmac_sha256,
                source_record_identity_sha256,
                source_record_payload_sha256,
                identifier_policy_sha256,
                identifier_rule_id,
                identifier_rule_sha256
            ) VALUES (
                $1, $2, $3, $4, $5, $6, 0, $7, $8, $9, $10, $11, $12, $13
            )
            """,
            [
                (
                    generation_key,
                    evidence_row.evidence_id,
                    evidence_row.token.token_policy_id,
                    evidence_row.token.tin_id_128,
                    evidence_row.token.tin_hmac_sha256,
                    evidence_row.npi,
                    evidence_row.relationship_class,
                    evidence_row.source_record_hmac_sha256,
                    evidence_row.source_record_identity_sha256,
                    bytes.fromhex(evidence_row.source_record_payload_hash),
                    bytes.fromhex(evidence_row.identifier_policy_sha256),
                    evidence_row.identifier_rule_id,
                    bytes.fromhex(evidence_row.identifier_rule_sha256),
                )
                for evidence_row in evidence_rows
            ],
        )
        candidate_count = await connection.fetchval(
            f"""
            SELECT COUNT(*)
              FROM {quoted_schema}.tin_npi_connector_lookup
             WHERE generation_key = $1
               AND token_policy_id = $2
               AND tin_id_128 = $3
            """,
            generation_key,
            token_policy_id,
            token.tin_id_128,
        )
        assert candidate_count == 2
        sql_lookup_digest = await connection.fetchval(
            f"""
            SELECT {quoted_schema}.
                   tin_npi_connector_lookup_set_sha256($1)
            """,
            generation_key,
        )
        assert bytes(sql_lookup_digest) == lookup_digest
        sql_evidence_digest = await connection.fetchval(
            f"""
            SELECT {quoted_schema}.
                   tin_npi_connector_evidence_set_sha256($1, 0)
            """,
            generation_key,
        )
        assert bytes(sql_evidence_digest) == (
            connector.canonical_fhir_evidence_set_digest(evidence_rows)
        )
        await connection.execute(
            f"""
            UPDATE {quoted_schema}.tin_npi_connector_generation
               SET state = 'complete'
             WHERE generation_key = $1
            """,
            generation_key,
        )
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_pointer_action_invalid",
            f"""
            UPDATE {quoted_schema}.tin_npi_connector_current
               SET pointer_version = 1,
                   generation_key = $1,
                   published_at = transaction_timestamp(),
                   updated_at = transaction_timestamp()
             WHERE pointer_key = 1
            """,
            generation_key,
        )
        await connection.execute(
            f"""
            ALTER TABLE {quoted_schema}.provider_directory_dataset_resource
            DISABLE TRIGGER
                tin_npi_connector_dataset_resource_insert_guard
            """
        )
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_dataset_resource_guard_changed",
            f"""
            SELECT {quoted_schema}.
                   publish_tin_npi_connector_generation(0, NULL, $1, $2)
            """,
            generation_key,
            bytes.fromhex(source_vector.source_vector_id),
        )
        await connection.execute(
            f"""
            ALTER TABLE {quoted_schema}.provider_directory_dataset_resource
            ENABLE ALWAYS TRIGGER
                tin_npi_connector_dataset_resource_insert_guard
            """
        )
        unexpected_policy = connector.TinTokenPolicyDescriptor.release_1(
            "ptg-tin-hmac-sha256-v1:unexpected"
        )
        await connection.execute(
            f"""
            INSERT INTO {quoted_schema}.ptg2_provider_tax_identity_manifest (
                snapshot_key,
                token_policy_id,
                token_policy_descriptor_sha256
            ) VALUES (2, $1, $2)
            """,
            unexpected_policy.token_policy_id,
            bytes.fromhex(unexpected_policy.token_policy_descriptor_sha256),
        )
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_token_policy_scope_changed",
            f"""
            SELECT {quoted_schema}.
                   publish_tin_npi_connector_generation(0, NULL, $1, $2)
            """,
            generation_key,
            bytes.fromhex(source_vector.source_vector_id),
        )
        await connection.execute(
            f"""
            DELETE FROM {quoted_schema}.ptg2_provider_tax_identity_manifest
             WHERE snapshot_key = 2
            """
        )
        await connection.execute(
            f"""
            INSERT INTO {quoted_schema}.provider_directory_source (
                source_id,
                endpoint_id
            ) VALUES ('source-b', 'endpoint-a')
            """
        )
        await connection.execute(
            f"""
            UPDATE {quoted_schema}.provider_directory_endpoint_dataset
               SET publication_metadata_json =
                   jsonb_set(
                       publication_metadata_json,
                       '{{source_ids}}',
                       '["source-a","source-b"]'::jsonb
                   )
             WHERE dataset_id = 'dataset-a'
            """
        )
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_fhir_source_scope_changed",
            f"""
            SELECT {quoted_schema}.
                   publish_tin_npi_connector_generation(0, NULL, $1, $2)
            """,
            generation_key,
            bytes.fromhex(source_vector.source_vector_id),
        )
        await connection.execute(
            f"""
            DELETE FROM {quoted_schema}.provider_directory_source
             WHERE source_id = 'source-b'
            """
        )
        await connection.execute(
            f"""
            UPDATE {quoted_schema}.provider_directory_endpoint_dataset
               SET publication_metadata_json =
                   jsonb_set(
                       publication_metadata_json,
                       '{{source_ids}}',
                       '["source-a"]'::jsonb
                   )
             WHERE dataset_id = 'dataset-a'
            """
        )
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_endpoint_dataset_transition_invalid",
            f"""
            UPDATE {quoted_schema}.provider_directory_endpoint_dataset
               SET dataset_hash = $1
             WHERE dataset_id = 'dataset-a'
            """,
            "ef" * 32,
        )
        await connection.execute(
            f"""
            UPDATE {quoted_schema}.provider_directory_endpoint_dataset
               SET publication_metadata_json =
                   jsonb_set(
                       publication_metadata_json,
                       '{{source_summary_v1,dataset_hash}}',
                       to_jsonb($1::text)
                   )
             WHERE dataset_id = 'dataset-a'
            """,
            "ef" * 32,
        )
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_fhir_dataset_changed",
            f"""
            SELECT {quoted_schema}.
                   publish_tin_npi_connector_generation(0, NULL, $1, $2)
            """,
            generation_key,
            bytes.fromhex(source_vector.source_vector_id),
        )
        await connection.execute(
            f"""
            UPDATE {quoted_schema}.provider_directory_endpoint_dataset
               SET publication_metadata_json =
                   jsonb_set(
                       publication_metadata_json,
                       '{{source_summary_v1,dataset_hash}}',
                       to_jsonb($1::text)
                   )
             WHERE dataset_id = 'dataset-a'
            """,
            "ab" * 32,
        )
        pointer_version = await connection.fetchval(
            f"""
            SELECT {quoted_schema}.
                   publish_tin_npi_connector_generation(0, NULL, $1, $2)
            """,
            generation_key,
            bytes.fromhex(source_vector.source_vector_id),
        )
        assert pointer_version == 1
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_pointer_cas_conflict",
            f"""
            SELECT {quoted_schema}.
                   publish_tin_npi_connector_generation(0, NULL, $1, $2)
            """,
            generation_key,
            bytes.fromhex(source_vector.source_vector_id),
        )
        later_source_vector = connector.TinNpiConnectorSourceVector(
            fhir_datasets=(dataset,),
            input_relations=(relation,),
            token_policies=(token_policy,),
            evidence_as_of="2026-07-28T00:00:00.000000Z",
            identifier_policy=identifier_policy,
        )
        later_scan_proof = _scan_proof(
            token_policy_id=token_policy_id,
            identifier_rule=identifier_rule,
            evidence_rows=(),
            source_summary_sha256=source_summary["summary_sha256"],
            organization_resource_count=len(organization_rows),
            organization_resource_sha256=organization_resource_sha256,
            matched_organization_count=0,
        )
        later_scan_proofs = (later_scan_proof,)
        later_scan_proof_json = connector.canonical_fhir_organization_scan_proof_json(
            later_scan_proofs
        )
        later_scan_proof_digest = (
            connector.canonical_fhir_organization_scan_proof_digest(later_scan_proofs)
        )
        later_lookup_digest = connector._lookup_digest(())
        later_generation_id = bytes.fromhex(
            connector._generation_id(
                source_vector_id=later_source_vector.source_vector_id,
                scan_proof_digest=later_scan_proof_digest,
                lookup_digest=later_lookup_digest,
            )
        )
        later_build_token = "connector-build-proof-0002"
        later_generation_key = int(
            await connection.fetchval(
                f"""
                INSERT INTO {quoted_schema}.tin_npi_connector_generation (
                    generation_id,
                    source_vector_id,
                    source_vector_canonical_json,
                    schema_version,
                    lookup_schema_version,
                    lookup_contract_id,
                    generation_contract,
                    raw_policy,
                    projection_policy_id,
                    relationship_class,
                    site_resolution_contract_id,
                    source_record_identity_contract_id,
                    identifier_policy_id,
                    identifier_policy_sha256,
                    evidence_as_of,
                    source_ordinal_contract,
                    source_ordinal_map_canonical_json,
                    source_ordinal_map_digest,
                    scan_contract_id,
                    scan_proof_canonical_json,
                    scan_proof_digest,
                    source_count,
                    source_dataset_count,
                    source_relation_count,
                    token_policy_count,
                    lookup_digest,
                    organization_count,
                    matched_organization_count,
                    evidence_count,
                    forward_row_count,
                    reverse_row_count,
                    npi_edge_count,
                    build_token_sha256,
                    build_lease_expires_at,
                    state
                )
                SELECT
                    $1,
                    $2,
                    $3,
                    schema_version,
                    lookup_schema_version,
                    lookup_contract_id,
                    generation_contract,
                    raw_policy,
                    projection_policy_id,
                    relationship_class,
                    site_resolution_contract_id,
                    source_record_identity_contract_id,
                    identifier_policy_id,
                    identifier_policy_sha256,
                    $4,
                    source_ordinal_contract,
                    source_ordinal_map_canonical_json,
                    source_ordinal_map_digest,
                    scan_contract_id,
                    $5,
                    $6,
                    source_count,
                    source_dataset_count,
                    source_relation_count,
                    token_policy_count,
                    $7,
                    2,
                    0,
                    0,
                    0,
                    0,
                    0,
                    $8,
                    clock_timestamp() + interval '1 hour',
                    'building'
                  FROM {quoted_schema}.tin_npi_connector_generation
                 WHERE generation_key = $9
                RETURNING generation_key
                """,
                later_generation_id,
                bytes.fromhex(later_source_vector.source_vector_id),
                later_source_vector.canonical_json,
                later_source_vector.evidence_as_of,
                later_scan_proof_json,
                later_scan_proof_digest,
                later_lookup_digest,
                hashlib.sha256(later_build_token.encode()).digest(),
                generation_key,
            )
        )
        await connection.execute(
            "SELECT set_config(" "'healthporta.tin_npi_build_token', $1, TRUE" ")",
            later_build_token,
        )
        await connection.execute(
            f"""
            INSERT INTO {quoted_schema}.tin_npi_connector_generation_policy (
                generation_key,
                token_policy_id
            ) VALUES ($1, $2)
            """,
            later_generation_key,
            token_policy_id,
        )
        await connection.execute(
            f"""
            UPDATE {quoted_schema}.tin_npi_connector_generation
               SET state = 'complete'
             WHERE generation_key = $1
            """,
            later_generation_key,
        )
        second_pointer_version = await connection.fetchval(
            f"""
            SELECT {quoted_schema}.
                   publish_tin_npi_connector_generation(1, $1, $2, $3)
            """,
            generation_key,
            later_generation_key,
            bytes.fromhex(later_source_vector.source_vector_id),
        )
        assert second_pointer_version == 2
        await connection.execute(
            f"""
            UPDATE {quoted_schema}.provider_directory_endpoint_dataset
               SET status = 'superseded',
                   is_current = FALSE,
                   superseded_at = transaction_timestamp()
             WHERE dataset_id = 'dataset-a'
            """
        )
        await connection.execute(
            f"""
            DELETE FROM {quoted_schema}.provider_directory_dataset_resource
             WHERE dataset_id = 'dataset-a'
            """
        )
        await connection.execute(
            f"""
            INSERT INTO {quoted_schema}.ptg2_provider_tax_identity_manifest (
                snapshot_key,
                token_policy_id,
                token_policy_descriptor_sha256
            ) VALUES (3, $1, $2)
            """,
            unexpected_policy.token_policy_id,
            bytes.fromhex(unexpected_policy.token_policy_descriptor_sha256),
        )
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_token_policy_scope_changed",
            f"""
            SELECT {quoted_schema}.
                   rollback_tin_npi_connector_generation(2, $1, $2)
            """,
            later_generation_key,
            generation_key,
        )
        await connection.execute(
            f"""
            DELETE FROM {quoted_schema}.ptg2_provider_tax_identity_manifest
             WHERE snapshot_key = 3
            """
        )
        rollback_pointer_version = await connection.fetchval(
            f"""
            SELECT {quoted_schema}.
                   rollback_tin_npi_connector_generation(2, $1, $2)
            """,
            later_generation_key,
            generation_key,
        )
        assert rollback_pointer_version == 3
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_generation_retire_forbidden",
            f"""
            UPDATE {quoted_schema}.tin_npi_connector_generation
               SET state = 'retired',
                   gc_after = transaction_timestamp() + interval '1 hour'
             WHERE generation_key = $1
            """,
            later_generation_key,
        )
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_generation_not_retirable",
            f"""
            SELECT {quoted_schema}.
                   retire_tin_npi_connector_generation(
                       $1,
                       clock_timestamp() + interval '23 hours'
                   )
            """,
            later_generation_key,
        )
        retired_generation_key = await connection.fetchval(
            f"""
            SELECT {quoted_schema}.
                   retire_tin_npi_connector_generation(
                       $1,
                       clock_timestamp() + interval '25 hours'
                   )
            """,
            later_generation_key,
        )
        assert retired_generation_key == later_generation_key
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_generation_delete_forbidden",
            f"""
            DELETE FROM {quoted_schema}.tin_npi_connector_generation
             WHERE generation_key = $1
            """,
            later_generation_key,
        )
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_child_immutable",
            f"""
            DELETE FROM {quoted_schema}.tin_npi_connector_evidence
             WHERE generation_key = $1
               AND evidence_id = $2
            """,
            generation_key,
            evidence_rows[0].evidence_id,
        )
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_child_immutable",
            f"""
            UPDATE {quoted_schema}.tin_npi_connector_lookup
               SET evidence_count = 4
             WHERE generation_key = $1
            """,
            generation_key,
        )
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_generation_not_loadable",
            f"""
            INSERT INTO {quoted_schema}.tin_npi_connector_lookup (
                generation_key,
                token_policy_id,
                tin_id_128,
                tin_hmac_sha256,
                npis,
                evidence_count,
                source_bitmap,
                npi_source_bitmap_matrix,
                source_evidence_counts
            ) VALUES ($1, $2, $3, $4, $5, 1, $6, $7, $8)
            """,
            generation_key,
            token_policy_id,
            b"\xcc" * 16,
            b"\xcc" * 32,
            [1000000004],
            b"\x01",
            b"\x01",
            [1],
        )
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_truncate_forbidden",
            (
                f"TRUNCATE {quoted_schema}.tin_npi_connector_evidence, "
                f"{quoted_schema}.tin_npi_connector_lookup"
            ),
        )
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_generation_retire_forbidden",
            f"""
            UPDATE {quoted_schema}.tin_npi_connector_generation
               SET state = 'retired',
                   gc_after = transaction_timestamp()
             WHERE generation_key = $1
            """,
            generation_key,
        )

        abandoned_dataset = connector.FhirDatasetFenceIdentity(
            source_id="source-a",
            endpoint_id="endpoint-a",
            dataset_id="dataset-a",
            evidence_run_id="run-abandoned",
            selected_resources=("Organization",),
            expected_resources=("Organization",),
            recorded_expected_resources=("Organization",),
            status="published",
            is_current=True,
            promote_on_cutover=False,
            dataset_hash="cd" * 32,
            resource_count=len(organization_rows),
            organization_resource_count=len(organization_rows),
            organization_resource_sha256=organization_resource_sha256,
            source_summary_sha256=source_summary["summary_sha256"],
            identifier_rule_id=identifier_rule.rule_id,
            identifier_rule_sha256=identifier_rule.descriptor_sha256,
            validated_at="2026-07-27 00:00:00",
        )
        abandoned_source_vector = connector.TinNpiConnectorSourceVector(
            fhir_datasets=(abandoned_dataset,),
            input_relations=(relation,),
            token_policies=(token_policy,),
            evidence_as_of="2026-07-27T00:00:00.000000Z",
            identifier_policy=identifier_policy,
        )
        abandoned_scan_proof_json = later_scan_proof_json
        abandoned_scan_proof_digest = later_scan_proof_digest
        empty_lookup_digest = connector._lookup_digest(())
        abandoned_generation_id = bytes.fromhex(
            connector._generation_id(
                source_vector_id=abandoned_source_vector.source_vector_id,
                scan_proof_digest=abandoned_scan_proof_digest,
                lookup_digest=empty_lookup_digest,
            )
        )
        abandoned_build_token = "lost-abandoned-build-token"
        abandoned_build_token_sha256 = hashlib.sha256(
            abandoned_build_token.encode()
        ).digest()
        abandoned_generation_insert = f"""
            INSERT INTO {quoted_schema}.tin_npi_connector_generation (
                generation_id,
                source_vector_id,
                source_vector_canonical_json,
                schema_version,
                lookup_schema_version,
                lookup_contract_id,
                generation_contract,
                raw_policy,
                projection_policy_id,
                relationship_class,
                site_resolution_contract_id,
                source_record_identity_contract_id,
                identifier_policy_id,
                identifier_policy_sha256,
                evidence_as_of,
                source_ordinal_contract,
                source_ordinal_map_canonical_json,
                source_ordinal_map_digest,
                scan_contract_id,
                scan_proof_canonical_json,
                scan_proof_digest,
                source_count,
                source_dataset_count,
                source_relation_count,
                token_policy_count,
                lookup_digest,
                organization_count,
                matched_organization_count,
                evidence_count,
                forward_row_count,
                reverse_row_count,
                npi_edge_count,
                build_token_sha256,
                build_lease_expires_at,
                state
            )
            SELECT
                $1,
                $2,
                $3,
                schema_version,
                lookup_schema_version,
                lookup_contract_id,
                generation_contract,
                raw_policy,
                projection_policy_id,
                relationship_class,
                site_resolution_contract_id,
                source_record_identity_contract_id,
                identifier_policy_id,
                identifier_policy_sha256,
                evidence_as_of,
                source_ordinal_contract,
                source_ordinal_map_canonical_json,
                source_ordinal_map_digest,
                scan_contract_id,
                $4,
                $5,
                source_count,
                source_dataset_count,
                source_relation_count,
                token_policy_count,
                $6,
                2,
                0,
                0,
                0,
                0,
                0,
                $7,
                clock_timestamp() + interval '2 seconds',
                'building'
              FROM {quoted_schema}.tin_npi_connector_generation
             WHERE generation_key = $8
            RETURNING generation_key
        """
        abandoned_generation_key = int(
            await connection.fetchval(
                abandoned_generation_insert,
                abandoned_generation_id,
                bytes.fromhex(abandoned_source_vector.source_vector_id),
                abandoned_source_vector.canonical_json,
                abandoned_scan_proof_json,
                abandoned_scan_proof_digest,
                empty_lookup_digest,
                abandoned_build_token_sha256,
                generation_key,
            )
        )
        await connection.execute(
            "SELECT set_config(" "'healthporta.tin_npi_build_token', $1, TRUE" ")",
            abandoned_build_token,
        )
        await connection.execute(
            f"""
            INSERT INTO {quoted_schema}.tin_npi_connector_generation_policy (
                generation_key,
                token_policy_id
            ) VALUES ($1, $2)
            """,
            abandoned_generation_key,
            token_policy_id,
        )
        abandoned_evidence_rows = evidence_rows[:2]
        await connection.execute(
            f"""
            INSERT INTO {quoted_schema}.tin_npi_connector_lookup (
                generation_key,
                token_policy_id,
                tin_id_128,
                tin_hmac_sha256,
                npis,
                evidence_count,
                source_bitmap,
                npi_source_bitmap_matrix,
                source_evidence_counts
            ) VALUES ($1, $2, $3, $4, $5, 2, $6, $7, $8)
            """,
            abandoned_generation_key,
            token_policy_id,
            token.tin_id_128,
            token.tin_hmac_sha256,
            [1000000004, 1234567893],
            b"\x01",
            b"\x01\x01",
            [2],
        )
        await connection.executemany(
            f"""
            INSERT INTO {quoted_schema}.tin_npi_connector_evidence (
                generation_key,
                evidence_id,
                token_policy_id,
                tin_id_128,
                tin_hmac_sha256,
                npi,
                source_ordinal,
                relationship_class,
                source_record_hmac_sha256,
                source_record_identity_sha256,
                source_record_payload_sha256,
                identifier_policy_sha256,
                identifier_rule_id,
                identifier_rule_sha256
            ) VALUES (
                $1, $2, $3, $4, $5, $6, 0, $7, $8, $9, $10, $11, $12, $13
            )
            """,
            [
                (
                    abandoned_generation_key,
                    evidence_row.evidence_id,
                    evidence_row.token.token_policy_id,
                    evidence_row.token.tin_id_128,
                    evidence_row.token.tin_hmac_sha256,
                    evidence_row.npi,
                    evidence_row.relationship_class,
                    evidence_row.source_record_hmac_sha256,
                    evidence_row.source_record_identity_sha256,
                    bytes.fromhex(evidence_row.source_record_payload_hash),
                    bytes.fromhex(evidence_row.identifier_policy_sha256),
                    evidence_row.identifier_rule_id,
                    bytes.fromhex(evidence_row.identifier_rule_sha256),
                )
                for evidence_row in abandoned_evidence_rows
            ],
        )
        await connection.execute(
            "SELECT set_config(" "'healthporta.tin_npi_build_token', '', TRUE" ")"
        )
        await connection.execute("SELECT pg_sleep(2.05)")
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_build_token_invalid",
            f"""
            UPDATE {quoted_schema}.tin_npi_connector_generation
               SET state = 'failed'
             WHERE generation_key = $1
            """,
            abandoned_generation_key,
        )
        abandoned_key = await connection.fetchval(
            f"""
            SELECT {quoted_schema}.
                   abandon_tin_npi_connector_generation($1)
            """,
            abandoned_generation_key,
        )
        assert abandoned_key == abandoned_generation_key
        abandoned_state = await connection.fetchrow(
            f"""
            SELECT state, failed_at
              FROM {quoted_schema}.tin_npi_connector_generation
             WHERE generation_key = $1
            """,
            abandoned_generation_key,
        )
        assert abandoned_state["state"] == "failed"
        assert abandoned_state["failed_at"] is not None
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_gc_batch_invalid",
            f"""
            SELECT *
              FROM {quoted_schema}.
                   gc_tin_npi_connector_generation($1, NULL)
            """,
            abandoned_generation_key,
        )
        first_gc_result = await connection.fetchrow(
            f"""
            SELECT *
              FROM {quoted_schema}.
                   gc_tin_npi_connector_generation($1, 1)
            """,
            abandoned_generation_key,
        )
        assert first_gc_result["deleted_evidence_rows"] == 1
        assert first_gc_result["deleted_lookup_rows"] == 0
        assert first_gc_result["generation_removed"] is False
        final_gc_result = await connection.fetchrow(
            f"""
            SELECT *
              FROM {quoted_schema}.
                   gc_tin_npi_connector_generation($1, 100)
            """,
            abandoned_generation_key,
        )
        assert final_gc_result["deleted_evidence_rows"] == 1
        assert final_gc_result["deleted_lookup_rows"] == 1
        assert final_gc_result["generation_removed"] is True
        assert (
            await connection.fetchval(
                f"""
                SELECT COUNT(*)
                  FROM {quoted_schema}.tin_npi_connector_generation
                 WHERE generation_key = $1
                """,
                abandoned_generation_key,
            )
            == 0
        )
        rebuilt_generation_key = int(
            await connection.fetchval(
                abandoned_generation_insert,
                abandoned_generation_id,
                bytes.fromhex(abandoned_source_vector.source_vector_id),
                abandoned_source_vector.canonical_json,
                abandoned_scan_proof_json,
                abandoned_scan_proof_digest,
                empty_lookup_digest,
                abandoned_build_token_sha256,
                generation_key,
            )
        )
        assert rebuilt_generation_key > abandoned_generation_key

        capture = _Capture()
        migration.op = capture
        migration.downgrade()
        await _expect_postgres_error(
            connection,
            "tin_npi_connector_downgrade_requires_empty_inactive_foundation",
            capture.statements[0],
        )
    finally:
        await transaction.rollback()
        await connection.close()
