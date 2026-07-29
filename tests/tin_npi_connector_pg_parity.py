# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Two-policy same-record parity scenarios for PostgreSQL generation sealing."""

from __future__ import annotations

from dataclasses import dataclass

from process import tin_npi_connector as connector
from process.tin_npi_connector_evidence import (
    _fhir_organization_record_identity_sha256 as record_identity_sha256,
)
from process.tin_npi_connector_lookup import _factor_forward_rows
from tests.tin_npi_connector_pg_generation_support import (
    build_generation_model,
    insert_generation,
    load_generation_children,
    set_build_token,
)
from tests.tin_npi_connector_postgres_support import (
    ORGANIZATION_ROWS,
    TransactionalSchema,
    build_identifier_policy,
    build_scan_proof,
    expect_postgres_error,
    insert_published_directory,
)


TOKEN_POLICIES = (
    connector.TinTokenPolicyDescriptor.release_1("ptg-tin-hmac-sha256-v1:release-1"),
    connector.TinTokenPolicyDescriptor.release_1("ptg-tin-hmac-sha256-v1:release-2"),
)


@dataclass
class ParityScenario:
    """Prepared two-policy directory and connector registry."""

    session: TransactionalSchema
    organization_digest: str
    source_summary: dict
    identifier_rule: connector.FhirTinNpiIdentifierRule
    identifier_policy: connector.FhirTinNpiIdentifierPolicy
    dataset: connector.FhirDatasetFenceIdentity
    relation: connector.ConnectorRelationIdentity
    tokens_by_record: dict

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
        identifier_rule, identifier_policy = build_identifier_policy()
        await _register_policies(session, identifier_policy)
        dataset = _build_dataset(
            organization_digest,
            source_summary,
            identifier_rule,
        )
        relation = connector.ConnectorRelationIdentity(
            schema=session.schema,
            relation="provider_directory_dataset_resource",
            relation_oid=relation_oid,
        )
        return cls(
            session=session,
            organization_digest=organization_digest,
            source_summary=source_summary,
            identifier_rule=identifier_rule,
            identifier_policy=identifier_policy,
            dataset=dataset,
            relation=relation,
            tokens_by_record=_build_tokens(),
        )

    def evidence_row(
        self,
        *,
        policy_name,
        resource_id,
        npi,
        resource_ordinal,
        evidence_as_of,
        payload_hash=None,
    ):
        token = self.tokens_by_record[(policy_name, resource_id)]
        canonical_resource_id, canonical_payload_hash = ORGANIZATION_ROWS[
            resource_ordinal
        ]
        return connector.FhirTinNpiEvidence(
            token=token,
            npi=npi,
            source_id="source-a",
            source_endpoint_id="endpoint-a",
            source_dataset_id="dataset-a",
            source_record_hmac_sha256=bytes([resource_ordinal + 1]) * 32,
            source_record_identity_sha256=(
                record_identity_sha256(
                    canonical_resource_id,
                    canonical_payload_hash,
                )
            ),
            source_record_payload_hash=payload_hash or canonical_payload_hash,
            evidence_as_of=evidence_as_of,
            identifier_policy_id=self.identifier_policy.policy_id,
            identifier_policy_sha256=self.identifier_policy.descriptor_sha256,
            identifier_rule_id=self.identifier_rule.rule_id,
            identifier_rule_sha256=self.identifier_rule.descriptor_sha256,
        )

    async def seal_case(self, case_ordinal, evidence_rows, should_seal):
        model = self._generation_model(case_ordinal, evidence_rows)
        build_token = f"connector-two-policy-case-{case_ordinal}"
        generation_key = await insert_generation(
            self.connection,
            self.quoted_schema,
            model,
            build_token,
        )
        await set_build_token(self.connection, build_token)
        await load_generation_children(
            self.connection,
            self.quoted_schema,
            generation_key,
            model,
        )
        await self._seal_generation(generation_key, should_seal)

    def _generation_model(self, case_ordinal, evidence_rows):
        evidence_as_of = f"2026-07-27T01:00:0{case_ordinal}.000000Z"
        assert all(
            evidence.evidence_as_of == evidence_as_of for evidence in evidence_rows
        )
        source_vector = connector.TinNpiConnectorSourceVector(
            fhir_datasets=(self.dataset,),
            input_relations=(self.relation,),
            token_policies=TOKEN_POLICIES,
            evidence_as_of=evidence_as_of,
            identifier_policy=self.identifier_policy,
        )
        policy_counts = tuple(
            (
                policy.token_policy_id,
                sum(
                    evidence.token.token_policy_id == policy.token_policy_id
                    for evidence in evidence_rows
                ),
            )
            for policy in TOKEN_POLICIES
        )
        scan_proof = build_scan_proof(
            token_policy_id=TOKEN_POLICIES[0].token_policy_id,
            identifier_rule=self.identifier_rule,
            evidence_rows=evidence_rows,
            source_summary_sha256=self.source_summary["summary_sha256"],
            organization_resource_count=len(ORGANIZATION_ROWS),
            organization_resource_sha256=self.organization_digest,
            matched_organization_count=len(ORGANIZATION_ROWS),
            matched_evidence_counts=policy_counts,
        )
        lookup_rows = _factor_forward_rows(
            evidence_rows,
            source_ordinal_map=("source-a",),
        )
        return build_generation_model(
            source_vector,
            (scan_proof,),
            lookup_rows,
            evidence_rows,
            organization_count=2,
            matched_organization_count=2,
        )

    async def _seal_generation(self, generation_key, should_seal):
        update_statement = f"""
            UPDATE {self.quoted_schema}.tin_npi_connector_generation
               SET state = 'complete'
             WHERE generation_key = $1
        """
        if not should_seal:
            await expect_postgres_error(
                self.connection,
                "tin_npi_connector_generation_seal_mismatch",
                update_statement,
                generation_key,
            )
            return
        await self.connection.execute(update_statement, generation_key)
        generation_state = await self.connection.fetchval(
            f"""
            SELECT state
              FROM {self.quoted_schema}.tin_npi_connector_generation
             WHERE generation_key = $1
            """,
            generation_key,
        )
        assert generation_state == "complete"


async def _register_policies(session, identifier_policy):
    await session.connection.executemany(
        f"""
        INSERT INTO {session.quoted_schema}.tin_npi_connector_token_policy (
            token_policy_id,
            token_policy_descriptor_sha256
        ) VALUES ($1, $2)
        """,
        [
            (
                policy.token_policy_id,
                bytes.fromhex(policy.token_policy_descriptor_sha256),
            )
            for policy in TOKEN_POLICIES
        ],
    )
    await session.connection.execute(
        f"""
        INSERT INTO {session.quoted_schema}.tin_npi_connector_identifier_policy (
            identifier_policy_id,
            descriptor_canonical_json,
            identifier_policy_sha256
        ) VALUES ($1, $2, $3)
        """,
        identifier_policy.policy_id,
        identifier_policy.descriptor_canonical_json,
        bytes.fromhex(identifier_policy.descriptor_sha256),
    )


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


def _build_tokens():
    token_specs = (
        (0, "release-1", "organization-a"),
        (0, "release-1", "organization-b"),
        (1, "release-2", "organization-a"),
        (1, "release-2", "organization-b"),
    )
    tokens_by_record = {}
    for token_ordinal, token_spec in enumerate(token_specs, start=1):
        policy_ordinal, policy_name, resource_id = token_spec
        token_hmac = bytes([token_ordinal * 16]) * 32
        tokens_by_record[(policy_name, resource_id)] = connector.TinTaxIdentityToken(
            token_policy_id=TOKEN_POLICIES[policy_ordinal].token_policy_id,
            tin_id_128=token_hmac[:16],
            tin_hmac_sha256=token_hmac,
        )
    return tokens_by_record


CASE_SPECS = (
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


async def prove_two_policy_record_parity(monkeypatch):
    scenario = await ParityScenario.create(monkeypatch)
    try:
        for case_ordinal, (should_seal, evidence_specs) in enumerate(CASE_SPECS):
            evidence_as_of = f"2026-07-27T01:00:0{case_ordinal}.000000Z"
            evidence_rows = tuple(
                scenario.evidence_row(
                    policy_name=policy_name,
                    resource_id=resource_id,
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
            await scenario.seal_case(case_ordinal, evidence_rows, should_seal)
    finally:
        await scenario.session.close()
