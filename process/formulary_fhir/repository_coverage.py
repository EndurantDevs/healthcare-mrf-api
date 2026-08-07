# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Source-qualified CoveragePlan identity and dataset-link writes."""

from __future__ import annotations

from typing import Any

from process.formulary_fhir.repository_checkpoint import require_alias
from process.formulary_fhir.repository_shared import AliasRef
from process.formulary_fhir.repository_shared import CoveragePlanWriteResult
from process.formulary_fhir.repository_shared import DatasetRef
from process.formulary_fhir.repository_shared import json_text
from process.formulary_fhir.repository_shared import lock_dataset
from process.formulary_fhir.repository_shared import row_mapping
from process.formulary_fhir.repository_shared import stable_id
from process.formulary_fhir.repository_shared import strict_hash
from process.formulary_fhir.repository_shared import table_name
from process.formulary_fhir.types import CoveragePlanRecord


async def _insert_identity(database: Any, source_id: str, plan: CoveragePlanRecord) -> None:
    await database.status(
        f"INSERT INTO {table_name('fhir_formulary_coverage_plan')} ("
        "public_id, source_id, upstream_list_id, canonical_identity) VALUES ("
        ":public_id, :source_id, :upstream_list_id, :canonical_identity) "
        "ON CONFLICT DO NOTHING;",
        public_id=plan.public_id,
        source_id=source_id,
        upstream_list_id=plan.upstream_list_id,
        canonical_identity=plan.canonical_identity,
    )
    identity_row = await database.first(
        f"SELECT source_id, public_id, upstream_list_id, canonical_identity "
        f"FROM {table_name('fhir_formulary_coverage_plan')} "
        "WHERE source_id = :source_id AND public_id = :public_id;",
        source_id=source_id,
        public_id=plan.public_id,
    )
    expected_identity_by_field = {
        "source_id": source_id,
        "public_id": plan.public_id,
        "upstream_list_id": plan.upstream_list_id,
        "canonical_identity": plan.canonical_identity,
    }
    if row_mapping(identity_row) != expected_identity_by_field:
        raise RuntimeError("FHIR formulary public identity collision")


async def _insert_version(
    database: Any,
    source_id: str,
    coverage_version_id: str,
    plan: CoveragePlanRecord,
) -> None:
    metadata_json = json_text(
        {
            "raw_identifiers": plan.raw_identifiers,
            "raw_extensions": plan.raw_extensions,
            "source_plan_identifiers": plan.source_plan_identifiers,
        }
    )
    await database.status(
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
        upstream_last_updated=plan.upstream_last_updated,
        status=plan.status,
        title=plan.title,
        name=plan.name,
        period_start=plan.period_start,
        period_end=plan.period_end,
        upstream_date=plan.upstream_date,
        content_hash=plan.content_hash,
        metadata_json=metadata_json,
    )
    version_row = await database.first(
        f"SELECT version.coverage_version_id, version.content_hash FROM "
        f"{table_name('fhir_formulary_coverage_plan_version')} AS version JOIN "
        f"{table_name('fhir_formulary_coverage_plan')} AS plan "
        "ON plan.public_id = version.public_id "
        "WHERE plan.source_id = :source_id AND version.public_id = :public_id "
        "AND version.content_hash = :content_hash;",
        source_id=source_id,
        public_id=plan.public_id,
        content_hash=plan.content_hash,
    )
    expected_version_by_field = {
        "coverage_version_id": coverage_version_id,
        "content_hash": plan.content_hash,
    }
    if row_mapping(version_row) != expected_version_by_field:
        raise RuntimeError("FHIR formulary coverage version collision")


async def _link_version(
    database: Any,
    source_id: str,
    dataset: DatasetRef,
    plan: CoveragePlanRecord,
    coverage_version_id: str,
) -> None:
    await database.status(
        f"INSERT INTO {table_name('fhir_formulary_dataset_coverage_plan')} ("
        "source_id, dataset_id, public_id, coverage_version_id) VALUES ("
        ":source_id, :dataset_id, :public_id, :coverage_version_id) "
        "ON CONFLICT DO NOTHING;",
        source_id=source_id,
        dataset_id=dataset.dataset_id,
        public_id=plan.public_id,
        coverage_version_id=coverage_version_id,
    )
    link_row = await database.first(
        f"SELECT source_id, coverage_version_id FROM "
        f"{table_name('fhir_formulary_dataset_coverage_plan')} "
        "WHERE source_id = :source_id AND dataset_id = :dataset_id "
        "AND public_id = :public_id;",
        source_id=source_id,
        dataset_id=dataset.dataset_id,
        public_id=plan.public_id,
    )
    expected_link_by_field = {
        "source_id": source_id,
        "coverage_version_id": coverage_version_id,
    }
    if row_mapping(link_row) != expected_link_by_field:
        raise RuntimeError("FHIR formulary dataset coverage link is inconsistent")


async def _put_aliases(
    database: Any,
    source_id: str,
    plan: CoveragePlanRecord,
) -> tuple[AliasRef, ...]:
    aliases: list[AliasRef] = []
    for source_plan_identifier in sorted(plan.source_plan_identifiers):
        alias = AliasRef(
            source_id=source_id,
            public_id=plan.public_id,
            alias_id=stable_id(
                "ffa_",
                source_id,
                plan.public_id,
                source_plan_identifier,
            ),
            source_plan_identifier=source_plan_identifier,
        )
        await database.status(
            f"INSERT INTO {table_name('fhir_formulary_drug_plan_alias')} ("
            "alias_id, source_id, public_id, source_plan_identifier) VALUES ("
            ":alias_id, :source_id, :public_id, :source_plan_identifier) "
            "ON CONFLICT DO NOTHING;",
            alias_id=alias.alias_id,
            source_id=source_id,
            public_id=alias.public_id,
            source_plan_identifier=alias.source_plan_identifier,
        )
        await require_alias(database, source_id, alias)
        aliases.append(alias)
    return tuple(aliases)


async def put_coverage_plan(
    database: Any,
    source_id: str,
    dataset: DatasetRef,
    plan: CoveragePlanRecord,
) -> CoveragePlanWriteResult:
    """Persist one exact CoveragePlan and its stable alias identities."""

    strict_hash(plan.content_hash, "coverage content hash")
    coverage_version_id = stable_id(
        "ffcv_",
        source_id,
        plan.public_id,
        plan.content_hash,
    )
    async with database.transaction():
        await lock_dataset(
            database,
            source_id,
            dataset,
            allowed_statuses={"building"},
        )
        await _insert_identity(database, source_id, plan)
        await _insert_version(database, source_id, coverage_version_id, plan)
        await _link_version(
            database,
            source_id,
            dataset,
            plan,
            coverage_version_id,
        )
        aliases = await _put_aliases(database, source_id, plan)
    return CoveragePlanWriteResult(dataset, coverage_version_id, aliases)


__all__ = ("put_coverage_plan",)
