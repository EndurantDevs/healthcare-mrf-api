# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepared PostgreSQL connector model used by lifecycle scenario slices."""

from __future__ import annotations

from dataclasses import dataclass, replace

from process import tin_npi_connector as connector
from process.tin_npi_connector_evidence import (
    _fhir_organization_record_identity_sha256 as record_identity_sha256,
)
from tests.tin_npi_connector_pg_generation_support import (
    build_generation_model,
    insert_evidence_rows,
    insert_generation,
    insert_generation_policies,
    insert_lookup_rows,
    set_build_token,
)
from tests.tin_npi_connector_postgres_support import (
    ORGANIZATION_ROWS,
    TransactionalSchema,
    build_identifier_policy,
    build_scan_proof,
    insert_published_directory,
)


TOKEN_POLICY_ID = "ptg-tin-hmac-sha256-v1:release-1"


@dataclass
class ConnectorLifecycleScenario:
    """One rollback-only schema with canonical connector model objects."""

    session: TransactionalSchema
    organization_digest: str
    source_summary: dict
    token_policy: connector.TinTokenPolicyDescriptor
    identifier_rule: connector.FhirTinNpiIdentifierRule
    identifier_policy: connector.FhirTinNpiIdentifierPolicy
    relation: connector.ConnectorRelationIdentity
    dataset: connector.FhirDatasetFenceIdentity
    source_vector: connector.TinNpiConnectorSourceVector
    token: connector.TinTaxIdentityToken
    collision_token: connector.TinTaxIdentityToken
    model: object
    build_token: str = "connector-build-proof-0001"
    generation_key: int | None = None

    @property
    def connection(self):
        return self.session.connection

    @property
    def quoted_schema(self):
        return self.session.quoted_schema

    @classmethod
    async def create(cls, monkeypatch):
        session = await TransactionalSchema.create(monkeypatch)
        organization_digest, source_summary = await insert_published_directory(
            session.connection,
            session.schema,
        )
        relation_oid = int(
            await session.connection.fetchval(
                "SELECT to_regclass($1)::oid",
                f"{session.schema}.provider_directory_dataset_resource",
            )
        )
        await session.upgrade()
        token_policy = connector.TinTokenPolicyDescriptor.release_1(TOKEN_POLICY_ID)
        identifier_rule, identifier_policy = build_identifier_policy()
        relation = connector.ConnectorRelationIdentity(
            schema=session.schema,
            relation="provider_directory_dataset_resource",
            relation_oid=relation_oid,
        )
        dataset = _build_dataset(
            organization_digest,
            source_summary,
            identifier_rule,
        )
        source_vector = _build_source_vector(
            dataset,
            relation,
            token_policy,
            identifier_policy,
        )
        token, collision_token = _build_tokens()
        model = _build_model(
            source_vector,
            token,
            collision_token,
            identifier_rule,
            identifier_policy,
            organization_digest,
            source_summary,
        )
        return cls(
            session=session,
            organization_digest=organization_digest,
            source_summary=source_summary,
            token_policy=token_policy,
            identifier_rule=identifier_rule,
            identifier_policy=identifier_policy,
            relation=relation,
            dataset=dataset,
            source_vector=source_vector,
            token=token,
            collision_token=collision_token,
            model=model,
        )

    async def register_model(self):
        await self.connection.execute(
            f"""
            INSERT INTO {self.quoted_schema}.ptg2_provider_tax_identity_manifest (
                snapshot_key,
                token_policy_id,
                token_policy_descriptor_sha256
            ) VALUES (1, $1, $2)
            """,
            TOKEN_POLICY_ID,
            bytes.fromhex(self.token_policy.token_policy_descriptor_sha256),
        )
        await self.connection.execute(
            f"""
            INSERT INTO {self.quoted_schema}.tin_npi_connector_token_policy (
                token_policy_id,
                token_policy_descriptor_sha256
            ) VALUES ($1, $2)
            """,
            TOKEN_POLICY_ID,
            bytes.fromhex(self.token_policy.token_policy_descriptor_sha256),
        )
        await self.connection.execute(
            f"""
            INSERT INTO {self.quoted_schema}.tin_npi_connector_identifier_policy (
                identifier_policy_id,
                descriptor_canonical_json,
                identifier_policy_sha256
            ) VALUES ($1, $2, $3)
            """,
            self.identifier_policy.policy_id,
            self.identifier_policy.descriptor_canonical_json,
            bytes.fromhex(self.identifier_policy.descriptor_sha256),
        )

    async def insert_build(self):
        self.generation_key = await insert_generation(
            self.connection,
            self.quoted_schema,
            self.model,
            self.build_token,
        )
        return self.generation_key

    async def load_build_children(self):
        assert self.generation_key is not None
        await set_build_token(self.connection, self.build_token)
        await insert_generation_policies(
            self.connection,
            self.quoted_schema,
            self.generation_key,
            (TOKEN_POLICY_ID,),
        )
        await insert_lookup_rows(
            self.connection,
            self.quoted_schema,
            self.generation_key,
            self.model.lookup_rows,
        )
        await insert_evidence_rows(
            self.connection,
            self.quoted_schema,
            self.generation_key,
            self.model.evidence_rows,
        )

    async def seal_build(self):
        assert self.generation_key is not None
        await self.connection.execute(
            f"""
            UPDATE {self.quoted_schema}.tin_npi_connector_generation
               SET state = 'complete'
             WHERE generation_key = $1
            """,
            self.generation_key,
        )

    def empty_model(self, evidence_as_of, *, dataset=None):
        selected_dataset = dataset or self.dataset
        source_vector = connector.TinNpiConnectorSourceVector(
            fhir_datasets=(selected_dataset,),
            input_relations=(self.relation,),
            token_policies=(self.token_policy,),
            evidence_as_of=evidence_as_of,
            identifier_policy=self.identifier_policy,
        )
        scan_proof = build_scan_proof(
            token_policy_id=TOKEN_POLICY_ID,
            identifier_rule=self.identifier_rule,
            evidence_rows=(),
            source_summary_sha256=self.source_summary["summary_sha256"],
            organization_resource_count=len(ORGANIZATION_ROWS),
            organization_resource_sha256=self.organization_digest,
            matched_organization_count=0,
        )
        return build_generation_model(
            source_vector,
            (scan_proof,),
            (),
            (),
            organization_count=2,
            matched_organization_count=0,
        )

    async def insert_empty_build(self, model, build_token):
        generation_key = await insert_generation(
            self.connection,
            self.quoted_schema,
            model,
            build_token,
        )
        await set_build_token(self.connection, build_token)
        await insert_generation_policies(
            self.connection,
            self.quoted_schema,
            generation_key,
            (TOKEN_POLICY_ID,),
        )
        return generation_key


def _build_dataset(organization_digest, source_summary, identifier_rule):
    return connector.FhirDatasetFenceIdentity(
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
        resource_count=len(ORGANIZATION_ROWS),
        organization_resource_count=len(ORGANIZATION_ROWS),
        organization_resource_sha256=organization_digest,
        source_summary_sha256=source_summary["summary_sha256"],
        identifier_rule_id=identifier_rule.rule_id,
        identifier_rule_sha256=identifier_rule.descriptor_sha256,
        validated_at="2026-07-27 00:00:00",
    )


def _build_source_vector(dataset, relation, token_policy, identifier_policy):
    return connector.TinNpiConnectorSourceVector(
        fhir_datasets=(dataset,),
        input_relations=(relation,),
        token_policies=(token_policy,),
        evidence_as_of="2026-07-27T00:00:00.000000Z",
        identifier_policy=identifier_policy,
    )


def _build_tokens():
    token = connector.TinTaxIdentityToken(
        token_policy_id=TOKEN_POLICY_ID,
        tin_id_128=bytes(range(16)),
        tin_hmac_sha256=bytes(range(32)),
    )
    collision_token = connector.TinTaxIdentityToken(
        token_policy_id=TOKEN_POLICY_ID,
        tin_id_128=token.tin_id_128,
        tin_hmac_sha256=token.tin_id_128 + b"\xff" * 16,
    )
    return token, collision_token


def _build_model(
    source_vector,
    token,
    collision_token,
    identifier_rule,
    identifier_policy,
    organization_digest,
    source_summary,
):
    lookup_rows = _build_lookup_rows(token, collision_token)
    evidence_rows = _build_evidence_rows(
        source_vector,
        token,
        collision_token,
        identifier_rule,
        identifier_policy,
    )
    scan_proof = build_scan_proof(
        token_policy_id=TOKEN_POLICY_ID,
        identifier_rule=identifier_rule,
        evidence_rows=evidence_rows,
        source_summary_sha256=source_summary["summary_sha256"],
        organization_resource_count=len(ORGANIZATION_ROWS),
        organization_resource_sha256=organization_digest,
        matched_organization_count=len(ORGANIZATION_ROWS),
    )
    return build_generation_model(
        source_vector,
        (scan_proof,),
        lookup_rows,
        evidence_rows,
        organization_count=2,
        matched_organization_count=2,
    )


def _build_lookup_rows(token, collision_token):
    primary_lookup = connector.TinNpiLookupRow(
        token=token,
        relationship_class=connector.FHIR_SAME_ORGANIZATION_RELATIONSHIP,
        npis=(1000000004, 1234567893),
        evidence_count=2,
        source_ids=("source-a",),
        source_bitmap=b"\x01",
        npi_source_bitmap_matrix=b"\x01\x01",
        source_evidence_counts=(2,),
    )
    collision_lookup = connector.TinNpiLookupRow(
        token=collision_token,
        relationship_class=connector.FHIR_SAME_ORGANIZATION_RELATIONSHIP,
        npis=(1234567893,),
        evidence_count=1,
        source_ids=("source-a",),
        source_bitmap=b"\x01",
        npi_source_bitmap_matrix=b"\x01",
        source_evidence_counts=(1,),
    )
    return primary_lookup, collision_lookup


def _build_evidence_rows(
    source_vector,
    token,
    collision_token,
    identifier_rule,
    identifier_policy,
):
    evidence_specs = (
        (1, token, 1000000004, ORGANIZATION_ROWS[0]),
        (1, token, 1234567893, ORGANIZATION_ROWS[0]),
        (2, collision_token, 1234567893, ORGANIZATION_ROWS[1]),
    )
    return tuple(
        _build_evidence(
            source_vector,
            identifier_rule,
            identifier_policy,
            evidence_spec,
        )
        for evidence_spec in evidence_specs
    )


def _build_evidence(source_vector, identifier_rule, identifier_policy, evidence_spec):
    record_ordinal, token, npi, resource_identity = evidence_spec
    resource_id, payload_hash = resource_identity
    return connector.FhirTinNpiEvidence(
        token=token,
        npi=npi,
        source_id="source-a",
        source_endpoint_id="endpoint-a",
        source_dataset_id="dataset-a",
        source_record_hmac_sha256=bytes([record_ordinal]) * 32,
        source_record_identity_sha256=(
            record_identity_sha256(
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


def abandoned_dataset(scenario):
    return replace(
        scenario.dataset,
        evidence_run_id="run-abandoned",
        dataset_hash="cd" * 32,
    )
