# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Source-qualified SQL statements for current FHIR formulary pages."""

from sqlalchemy import and_, bindparam, case, select

from db.models import FHIRFormularyAlias
from db.models import FHIRFormularyAliasVersion
from db.models import FHIRFormularyCoveragePlan
from db.models import FHIRFormularyCoveragePlanVersion
from db.models import FHIRFormularyCurrent
from db.models import FHIRFormularyDataset
from db.models import FHIRFormularyDatasetAlias
from db.models import FHIRFormularyDatasetCoveragePlan
from db.models import FHIRFormularySourceArtifactSet
from db.models import FHIRFormularyUHCAdmissionReceipt
from process.formulary_fhir.uhc_source import UHC_FORMULARY_SOURCE_ID


current = FHIRFormularyCurrent.__table__
dataset = FHIRFormularyDataset.__table__
dataset_plan = FHIRFormularyDatasetCoveragePlan.__table__
plan = FHIRFormularyCoveragePlan.__table__
version = FHIRFormularyCoveragePlanVersion.__table__
dataset_alias = FHIRFormularyDatasetAlias.__table__
alias = FHIRFormularyAlias.__table__
alias_version = FHIRFormularyAliasVersion.__table__
artifact_set = FHIRFormularySourceArtifactSet.__table__
uhc_receipt = FHIRFormularyUHCAdmissionReceipt.__table__

CURRENT_DATASET_FROM = (
    current.join(
        dataset,
        and_(
            dataset.c.source_id == current.c.source_id,
            dataset.c.dataset_id == current.c.dataset_id,
        ),
    )
    .outerjoin(
        uhc_receipt,
        and_(
            uhc_receipt.c.source_id == dataset.c.source_id,
            uhc_receipt.c.candidate_dataset_id == dataset.c.dataset_id,
        ),
    )
    .outerjoin(
        artifact_set,
        and_(
            artifact_set.c.source_id == uhc_receipt.c.source_id,
            artifact_set.c.source_file_set_sha256
            == uhc_receipt.c.source_file_set_sha256,
        ),
    )
)
COVERAGE_COLUMNS = (
    case(
        (current.c.source_id == UHC_FORMULARY_SOURCE_ID, True),
        else_=False,
    ).label("coverage_required"),
    artifact_set.c.expected_file_count.label(
        "coverage_expected_artifact_count"
    ),
    uhc_receipt.c.expected_file_count.label(
        "coverage_receipt_expected_artifact_count"
    ),
    uhc_receipt.c.file_count.label("coverage_included_artifact_count"),
    uhc_receipt.c.excluded_file_count.label(
        "coverage_missing_artifact_count"
    ),
)
CURRENT_PLAN_FROM = (
    CURRENT_DATASET_FROM.join(
        dataset_plan,
        and_(
            dataset_plan.c.source_id == dataset.c.source_id,
            dataset_plan.c.dataset_id == dataset.c.dataset_id,
        ),
    )
    .join(
        plan,
        and_(
            plan.c.source_id == dataset_plan.c.source_id,
            plan.c.public_id == dataset_plan.c.public_id,
        ),
    )
    .join(
        version,
        and_(
            version.c.public_id == dataset_plan.c.public_id,
            version.c.coverage_version_id == dataset_plan.c.coverage_version_id,
        ),
    )
)
CURRENT_PLAN_PREDICATES = (
    dataset.c.status == "published",
    dataset.c.verified_at.is_not(None),
    dataset.c.failed_at.is_(None),
    dataset.c.error_json.is_(None),
    dataset.c.published_at == current.c.published_at,
    current.c.generation > 0,
    dataset.c.coverage_hash.is_not(None),
    dataset.c.membership_hash.is_not(None),
    dataset.c.publish_requested != dataset.c.seed_eligible,
    version.c.upstream_last_updated.is_not(None),
)
CURRENT_DATASET_PREDICATES = CURRENT_PLAN_PREDICATES[:-1]
DETAIL_COLUMNS = (
    plan.c.public_id.label("formulary_id"),
    version.c.status,
    version.c.title,
    version.c.name,
    version.c.period_start,
    version.c.period_end,
    version.c.upstream_last_updated.label("last_updated"),
    dataset.c.cutoff_at.label("as_of"),
    current.c.published_at,
    *COVERAGE_COLUMNS,
)

CATALOG_MARKER_STATEMENT = (
    select(
        dataset.c.dataset_id,
        plan.c.public_id.label("formulary_id"),
        current.c.published_at,
    )
    .select_from(CURRENT_PLAN_FROM)
    .where(*CURRENT_PLAN_PREDICATES)
    .order_by(plan.c.public_id)
)
CURRENT_DATASET_COUNTS_STATEMENT = (
    select(dataset.c.dataset_id, dataset.c.list_count, *COVERAGE_COLUMNS)
    .select_from(CURRENT_DATASET_FROM)
    .where(*CURRENT_DATASET_PREDICATES)
    .order_by(dataset.c.dataset_id)
)
FORMULARY_PAGE_STATEMENT = (
    select(*DETAIL_COLUMNS)
    .select_from(CURRENT_PLAN_FROM)
    .where(
        *CURRENT_PLAN_PREDICATES,
        plan.c.public_id > bindparam("last_id"),
    )
    .order_by(plan.c.public_id)
    .limit(bindparam("page_size"))
)
FORMULARY_CONTEXT_STATEMENT = (
    select(
        dataset.c.dataset_id,
        plan.c.public_id.label("formulary_id"),
        current.c.generation,
        current.c.published_at,
        *COVERAGE_COLUMNS,
    )
    .select_from(CURRENT_PLAN_FROM)
    .where(
        *CURRENT_PLAN_PREDICATES,
        plan.c.public_id == bindparam("public_id"),
    )
    .limit(2)
)
ALIAS_PAGE_FROM = (
    CURRENT_PLAN_FROM.join(
        dataset_alias,
        and_(
            dataset_alias.c.source_id == dataset.c.source_id,
            dataset_alias.c.dataset_id == dataset.c.dataset_id,
        ),
    )
    .join(
        alias,
        and_(
            alias.c.source_id == dataset_alias.c.source_id,
            alias.c.alias_id == dataset_alias.c.alias_id,
            alias.c.public_id == plan.c.public_id,
        ),
    )
    .join(
        alias_version,
        and_(
            alias_version.c.source_id == dataset_alias.c.source_id,
            alias_version.c.alias_id == dataset_alias.c.alias_id,
            alias_version.c.alias_version_id == dataset_alias.c.alias_version_id,
        ),
    )
)
ALIAS_PAGE_STATEMENT = (
    select(
        plan.c.public_id.label("formulary_id"),
        alias.c.alias_id,
        alias_version.c.membership_count.label("drug_count"),
    )
    .select_from(ALIAS_PAGE_FROM)
    .where(
        *CURRENT_PLAN_PREDICATES,
        plan.c.public_id == bindparam("public_id"),
        alias.c.alias_id > bindparam("last_id"),
    )
    .order_by(alias.c.alias_id)
    .limit(bindparam("page_size"))
)


__all__ = (
    "ALIAS_PAGE_FROM",
    "ALIAS_PAGE_STATEMENT",
    "CATALOG_MARKER_STATEMENT",
    "COVERAGE_COLUMNS",
    "CURRENT_DATASET_FROM",
    "CURRENT_DATASET_COUNTS_STATEMENT",
    "CURRENT_PLAN_PREDICATES",
    "DETAIL_COLUMNS",
    "FORMULARY_CONTEXT_STATEMENT",
    "FORMULARY_PAGE_STATEMENT",
    "alias",
    "alias_version",
    "current",
    "dataset",
    "plan",
)
