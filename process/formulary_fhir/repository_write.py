# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Copy-on-write CoveragePlan and alias-version persistence."""

from __future__ import annotations

from dataclasses import dataclass

from db.models import db
from process.formulary_fhir.parser import resolve_alternative_references
from process.formulary_fhir.repository_shared import AliasVersionWrite
from process.formulary_fhir.repository_shared import PriorAliasState
from process.formulary_fhir.repository_shared import SOURCE_ID
from process.formulary_fhir.repository_shared import json_text
from process.formulary_fhir.repository_shared import medication_variant_hash
from process.formulary_fhir.repository_shared import membership_hash
from process.formulary_fhir.repository_shared import row_mapping
from process.formulary_fhir.repository_shared import stable_id
from process.formulary_fhir.repository_shared import table_name
from process.formulary_fhir.repository_shared import upstream_time
from process.formulary_fhir.types import CoveragePlanRecord, MedicationRecord


@dataclass(frozen=True)
class _PreparedAliasVersion:
    alias_version_id: str
    membership_hash: str
    medications_by_id: dict[str, MedicationRecord]
    variants_by_id: dict[str, str]


async def _insert_coverage_identity(plan: CoveragePlanRecord) -> None:
    await db.status(
        f"INSERT INTO {table_name('fhir_formulary_coverage_plan')} ("
        "public_id, source_id, upstream_list_id, canonical_identity) "
        "VALUES (:public_id, :source_id, :upstream_list_id, "
        ":canonical_identity) ON CONFLICT DO NOTHING;",
        public_id=plan.public_id,
        source_id=SOURCE_ID,
        upstream_list_id=plan.upstream_list_id,
        canonical_identity=plan.canonical_identity,
    )
    identity_row = await db.first(
        f"SELECT source_id, upstream_list_id, canonical_identity "
        f"FROM {table_name('fhir_formulary_coverage_plan')} "
        "WHERE public_id = :public_id;",
        public_id=plan.public_id,
    )
    identity_by_field = row_mapping(identity_row)
    if identity_by_field.get("canonical_identity") != plan.canonical_identity:
        raise RuntimeError("FHIR formulary public id collision")


async def _insert_coverage_version(
    coverage_version_id: str,
    plan: CoveragePlanRecord,
) -> None:
    await db.status(
        f"INSERT INTO {table_name('fhir_formulary_coverage_plan_version')} ("
        "coverage_version_id, public_id, upstream_version_id, "
        "upstream_last_updated, status, title, name, period_start, period_end, "
        "upstream_date, content_hash, metadata_json) VALUES ("
        ":coverage_version_id, :public_id, :upstream_version_id, "
        ":upstream_last_updated, :status, :title, :name, :period_start, "
        ":period_end, :upstream_date, :content_hash, "
        "CAST(:metadata_json AS jsonb)) "
        "ON CONFLICT (public_id, content_hash) DO NOTHING;",
        coverage_version_id=coverage_version_id,
        public_id=plan.public_id,
        upstream_version_id=plan.upstream_version_id,
        upstream_last_updated=upstream_time(plan.upstream_last_updated),
        status=plan.status,
        title=plan.title,
        name=plan.name,
        period_start=upstream_time(plan.period_start),
        period_end=upstream_time(plan.period_end),
        upstream_date=upstream_time(plan.upstream_date),
        content_hash=plan.content_hash,
        metadata_json=json_text(
            {
                "raw_identifiers": plan.raw_identifiers,
                "raw_extensions": plan.raw_extensions,
                "upstream_date": plan.upstream_date,
                "source_plan_identifiers": plan.source_plan_identifiers,
            }
        ),
    )


async def _link_coverage_version(
    dataset_id: str,
    plan: CoveragePlanRecord,
    coverage_version_id: str,
) -> None:
    await db.status(
        f"INSERT INTO {table_name('fhir_formulary_dataset_coverage_plan')} ("
        "dataset_id, public_id, coverage_version_id) VALUES ("
        ":dataset_id, :public_id, :coverage_version_id) "
        "ON CONFLICT (dataset_id, public_id) DO UPDATE SET "
        "coverage_version_id = EXCLUDED.coverage_version_id;",
        dataset_id=dataset_id,
        public_id=plan.public_id,
        coverage_version_id=coverage_version_id,
    )


async def _put_plan_aliases(plan: CoveragePlanRecord) -> dict[str, str]:
    aliases_by_identifier: dict[str, str] = {}
    for source_plan_identifier in plan.source_plan_identifiers:
        alias_id = stable_id("ffa_", plan.public_id, source_plan_identifier)
        await db.status(
            f"INSERT INTO {table_name('fhir_formulary_drug_plan_alias')} ("
            "alias_id, public_id, source_plan_identifier) VALUES ("
            ":alias_id, :public_id, :source_plan_identifier) "
            "ON CONFLICT (public_id, source_plan_identifier) DO NOTHING;",
            alias_id=alias_id,
            public_id=plan.public_id,
            source_plan_identifier=source_plan_identifier,
        )
        aliases_by_identifier[source_plan_identifier] = alias_id
    return aliases_by_identifier


def _prepare_alias_version(write: AliasVersionWrite) -> _PreparedAliasVersion:
    medications_by_id = {
        medication.upstream_medication_id: medication
        for medication in write.medications
    }
    if len(medications_by_id) != len(write.medications):
        raise RuntimeError("FHIR formulary alias returned duplicate medication ids")
    variants_by_id = (
        dict(write.prior.variants_by_medication_id)
        if write.acquisition_mode == "delta" and write.prior is not None
        else {}
    )
    variants_by_id.update(
        {
            medication_id: medication_variant_hash(medication)
            for medication_id, medication in medications_by_id.items()
        }
    )
    if len(variants_by_id) != write.expected_count:
        raise RuntimeError(
            "FHIR formulary exact count does not match unique alias membership"
        )
    computed_hash = membership_hash(variants_by_id)
    return _PreparedAliasVersion(
        alias_version_id=stable_id("ffav_", write.alias_id, computed_hash),
        membership_hash=computed_hash,
        medications_by_id=medications_by_id,
        variants_by_id=variants_by_id,
    )


async def _insert_alias_version(
    write: AliasVersionWrite,
    prepared: _PreparedAliasVersion,
) -> str:
    reused_version_id = (
        write.prior.alias_version_id
        if write.prior and write.acquisition_mode == "delta"
        else None
    )
    await db.status(
        f"INSERT INTO {table_name('fhir_formulary_drug_plan_alias_version')} ("
        "alias_version_id, alias_id, expected_count, membership_count, "
        "membership_hash, cutoff_at, acquisition_mode, "
        "reused_from_alias_version_id, summary_json) VALUES ("
        ":alias_version_id, :alias_id, :expected_count, :membership_count, "
        ":membership_hash, :cutoff_at, :acquisition_mode, "
        ":reused_from_alias_version_id, CAST(:summary_json AS jsonb)) "
        "ON CONFLICT (alias_id, membership_hash) DO NOTHING;",
        alias_version_id=prepared.alias_version_id,
        alias_id=write.alias_id,
        expected_count=write.expected_count,
        membership_count=len(prepared.variants_by_id),
        membership_hash=prepared.membership_hash,
        cutoff_at=write.cutoff_at,
        acquisition_mode=write.acquisition_mode,
        reused_from_alias_version_id=reused_version_id,
        summary_json=json_text(
            {
                "changed_medication_count": len(write.medications),
                "exact_count": write.expected_count,
            }
        ),
    )
    version_row = await db.first(
        f"SELECT alias_version_id FROM "
        f"{table_name('fhir_formulary_drug_plan_alias_version')} "
        "WHERE alias_id = :alias_id AND membership_hash = :membership_hash;",
        alias_id=write.alias_id,
        membership_hash=prepared.membership_hash,
    )
    alias_version_id = row_mapping(version_row).get("alias_version_id")
    if not alias_version_id:
        raise RuntimeError("FHIR formulary alias version insert failed")
    return str(alias_version_id)


async def _copy_prior_membership(
    alias_version_id: str,
    prior: PriorAliasState,
    changed_medication_ids: tuple[str, ...],
) -> None:
    copy_params_by_name = {
        "alias_version_id": alias_version_id,
        "prior_alias_version_id": prior.alias_version_id,
        "changed_ids": list(changed_medication_ids),
    }
    await db.status(
        f"INSERT INTO {table_name('fhir_formulary_alias_membership')} ("
        "alias_version_id, upstream_medication_id, medication_version_id, "
        "rxnorm_id, drug_tier, prior_authorization, step_therapy, "
        "quantity_limit, variant_hash) SELECT :alias_version_id, "
        "upstream_medication_id, medication_version_id, rxnorm_id, drug_tier, "
        "prior_authorization, step_therapy, quantity_limit, variant_hash FROM "
        f"{table_name('fhir_formulary_alias_membership')} "
        "WHERE alias_version_id = :prior_alias_version_id "
        "AND NOT (upstream_medication_id = ANY(:changed_ids));",
        **copy_params_by_name,
    )
    await db.status(
        f"INSERT INTO {table_name('fhir_formulary_alternative')} ("
        "alias_version_id, upstream_medication_id, raw_reference, "
        "corrected_reference, resolved_medication_id, resolved, rule_version, "
        "evidence_json) SELECT :alias_version_id, upstream_medication_id, "
        "raw_reference, corrected_reference, resolved_medication_id, resolved, "
        "rule_version, evidence_json "
        f"FROM {table_name('fhir_formulary_alternative')} "
        "WHERE alias_version_id = :prior_alias_version_id "
        "AND NOT (upstream_medication_id = ANY(:changed_ids));",
        **copy_params_by_name,
    )


async def _put_medication(medication: MedicationRecord) -> str:
    medication_version_id = stable_id(
        "ffm_",
        SOURCE_ID,
        medication.upstream_medication_id,
        medication.content_hash,
    )
    await db.status(
        f"INSERT INTO {table_name('fhir_formulary_medication')} ("
        "medication_version_id, source_id, upstream_medication_id, "
        "upstream_version_id, upstream_last_updated, status, drug_name, "
        "rxnorm_id, ndc11, codings_json, content_hash, metadata_json) VALUES ("
        ":medication_version_id, :source_id, :upstream_medication_id, "
        ":upstream_version_id, :upstream_last_updated, :status, :drug_name, "
        ":rxnorm_id, :ndc11, CAST(:codings_json AS jsonb), :content_hash, "
        "CAST(:metadata_json AS jsonb)) ON CONFLICT (source_id, "
        "upstream_medication_id, content_hash) DO NOTHING;",
        medication_version_id=medication_version_id,
        source_id=SOURCE_ID,
        upstream_medication_id=medication.upstream_medication_id,
        upstream_version_id=medication.upstream_version_id,
        upstream_last_updated=upstream_time(medication.upstream_last_updated),
        status=medication.status,
        drug_name=medication.drug_name,
        rxnorm_id=medication.rxnorm_id,
        ndc11=medication.ndc11,
        codings_json=json_text(
            [coding.__dict__ for coding in medication.codings]
        ),
        content_hash=medication.content_hash,
        metadata_json=json_text(
            {
                "raw_extensions": medication.raw_extensions,
                "source_plan_identifiers": medication.source_plan_identifiers,
            }
        ),
    )
    return medication_version_id


async def _insert_alternative_evidence(
    alias_version_id: str,
    medication_id: str,
    medication: MedicationRecord,
    known_medication_ids: set[str],
    *,
    apply_california_rule: bool,
) -> None:
    alternative_evidence_rows = resolve_alternative_references(
        medication.alternative_references,
        known_medication_ids=known_medication_ids,
        apply_california_rule=apply_california_rule,
    )
    for evidence in alternative_evidence_rows:
        await db.status(
            f"INSERT INTO {table_name('fhir_formulary_alternative')} ("
            "alias_version_id, upstream_medication_id, raw_reference, "
            "corrected_reference, resolved_medication_id, resolved, "
            "rule_version, evidence_json) VALUES ("
            ":alias_version_id, :upstream_medication_id, :raw_reference, "
            ":corrected_reference, :resolved_medication_id, :resolved, "
            ":rule_version, CAST(:evidence_json AS jsonb));",
            alias_version_id=alias_version_id,
            upstream_medication_id=medication_id,
            raw_reference=evidence.raw_reference,
            corrected_reference=evidence.corrected_reference,
            resolved_medication_id=evidence.resolved_medication_id,
            resolved=evidence.resolved,
            rule_version=evidence.rule_version,
            evidence_json=json_text({"same_source": True}),
        )


async def _insert_changed_membership(
    alias_version_id: str,
    prepared: _PreparedAliasVersion,
    *,
    apply_california_rule: bool,
) -> None:
    known_medication_ids = set(prepared.variants_by_id)
    for medication_id, medication in sorted(prepared.medications_by_id.items()):
        medication_version_id = await _put_medication(medication)
        await db.status(
            f"INSERT INTO {table_name('fhir_formulary_alias_membership')} ("
            "alias_version_id, upstream_medication_id, medication_version_id, "
            "rxnorm_id, drug_tier, prior_authorization, step_therapy, "
            "quantity_limit, variant_hash) VALUES ("
            ":alias_version_id, :upstream_medication_id, :medication_version_id, "
            ":rxnorm_id, :drug_tier, :prior_authorization, :step_therapy, "
            ":quantity_limit, :variant_hash);",
            alias_version_id=alias_version_id,
            upstream_medication_id=medication_id,
            medication_version_id=medication_version_id,
            rxnorm_id=medication.rxnorm_id,
            drug_tier=medication.drug_tier,
            prior_authorization=medication.prior_authorization,
            step_therapy=medication.step_therapy,
            quantity_limit=medication.quantity_limit,
            variant_hash=prepared.variants_by_id[medication_id],
        )
        await _insert_alternative_evidence(
            alias_version_id,
            medication_id,
            medication,
            known_medication_ids,
            apply_california_rule=apply_california_rule,
        )


async def _materialize_membership(
    alias_version_id: str,
    write: AliasVersionWrite,
    prepared: _PreparedAliasVersion,
) -> None:
    has_membership = bool(
        await db.scalar(
            f"SELECT EXISTS (SELECT 1 FROM "
            f"{table_name('fhir_formulary_alias_membership')} "
            "WHERE alias_version_id = :alias_version_id);",
            alias_version_id=alias_version_id,
        )
    )
    if has_membership:
        return
    if write.acquisition_mode == "delta" and write.prior is not None:
        await _copy_prior_membership(
            alias_version_id,
            write.prior,
            tuple(prepared.medications_by_id),
        )
    await _insert_changed_membership(
        alias_version_id,
        prepared,
        apply_california_rule=write.apply_california_rule,
    )


async def _link_alias_version(
    dataset_id: str,
    alias_id: str,
    alias_version_id: str,
) -> None:
    await db.status(
        f"INSERT INTO {table_name('fhir_formulary_dataset_alias')} ("
        "dataset_id, alias_id, alias_version_id) VALUES ("
        ":dataset_id, :alias_id, :alias_version_id) "
        "ON CONFLICT (dataset_id, alias_id) DO UPDATE SET "
        "alias_version_id = EXCLUDED.alias_version_id;",
        dataset_id=dataset_id,
        alias_id=alias_id,
        alias_version_id=alias_version_id,
    )


class FHIRFormularyWriteMixin:
    """Persist immutable CoveragePlan and alias-version content."""

    async def put_coverage_plan(
        self,
        *,
        dataset_id: str,
        plan: CoveragePlanRecord,
    ) -> dict[str, str]:
        """Store one CoveragePlan version and return its stable aliases."""

        coverage_version_id = stable_id(
            "ffcv_",
            plan.public_id,
            plan.content_hash,
        )
        async with db.transaction():
            await _insert_coverage_identity(plan)
            await _insert_coverage_version(coverage_version_id, plan)
            await _link_coverage_version(dataset_id, plan, coverage_version_id)
            return await _put_plan_aliases(plan)

    async def link_reused_alias(
        self,
        *,
        dataset_id: str,
        prior: PriorAliasState,
    ) -> None:
        """Link an unchanged prior alias version into a candidate dataset."""

        await _link_alias_version(
            dataset_id,
            prior.alias_id,
            prior.alias_version_id,
        )

    async def put_alias_version(self, write: AliasVersionWrite) -> str:
        """Persist and link one exactly counted copy-on-write alias version."""

        prepared = _prepare_alias_version(write)
        async with db.transaction():
            alias_version_id = await _insert_alias_version(write, prepared)
            await _materialize_membership(alias_version_id, write, prepared)
            await _link_alias_version(
                write.dataset_id,
                write.alias_id,
                alias_version_id,
            )
        return alias_version_id
