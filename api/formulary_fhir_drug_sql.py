# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Source-qualified SQL for current alias-scoped formulary medications."""

from sqlalchemy import and_, bindparam, select

from api.formulary_fhir_catalog_sql import ALIAS_PAGE_FROM
from api.formulary_fhir_catalog_sql import CURRENT_PLAN_PREDICATES
from api.formulary_fhir_catalog_sql import alias
from api.formulary_fhir_catalog_sql import alias_version
from api.formulary_fhir_catalog_sql import current
from api.formulary_fhir_catalog_sql import dataset
from api.formulary_fhir_catalog_sql import plan
from api.formulary_fhir_drug_values import FHIRFormularyDrugFilters
from db.models import FHIRFormularyAliasMembership
from db.models import FHIRFormularyAlternative
from db.models import FHIRFormularyMedication


membership = FHIRFormularyAliasMembership.__table__
medication = FHIRFormularyMedication.__table__
alternative = FHIRFormularyAlternative.__table__
target_membership = membership.alias("target_membership")

ALIAS_CONTEXT_STATEMENT = (
    select(
        alias.c.source_id,
        dataset.c.dataset_id,
        plan.c.public_id.label("formulary_id"),
        alias.c.alias_id,
        alias_version.c.alias_version_id,
        current.c.generation,
        current.c.published_at,
    )
    .select_from(ALIAS_PAGE_FROM)
    .where(
        *CURRENT_PLAN_PREDICATES,
        plan.c.public_id == bindparam("public_id"),
        alias.c.alias_id == bindparam("alias_id"),
    )
    .limit(2)
)

DRUG_FROM = ALIAS_PAGE_FROM.join(
    membership,
    and_(
        membership.c.source_id == alias_version.c.source_id,
        membership.c.alias_version_id == alias_version.c.alias_version_id,
    ),
).join(
    medication,
    and_(
        medication.c.source_id == membership.c.source_id,
        medication.c.upstream_medication_id
        == membership.c.upstream_medication_id,
        medication.c.medication_version_id == membership.c.medication_version_id,
    ),
)
DRUG_COLUMNS = (
    plan.c.public_id.label("formulary_id"),
    alias.c.alias_id,
    membership.c.upstream_medication_id,
    medication.c.medication_version_id.label("drug_id"),
    medication.c.status,
    medication.c.drug_name.label("name"),
    membership.c.rxnorm_id,
    medication.c.ndc11,
    medication.c.upstream_last_updated.label("last_updated"),
    membership.c.drug_tier.label("tier"),
    membership.c.prior_authorization,
    membership.c.step_therapy,
    membership.c.quantity_limit,
)


def drug_statement(
    filters: FHIRFormularyDrugFilters,
    *,
    exact_drug_id: str | None = None,
):
    """Build one current, alias-qualified medication page or detail query."""

    predicates = [
        *CURRENT_PLAN_PREDICATES,
        plan.c.public_id == bindparam("public_id"),
        alias.c.alias_id == bindparam("alias_id"),
    ]
    if exact_drug_id is None:
        predicates.append(medication.c.medication_version_id > bindparam("last_id"))
    else:
        predicates.append(
            medication.c.medication_version_id == bindparam("drug_id")
        )
    columns_by_filter = {
        "rxnorm_id": membership.c.rxnorm_id,
        "ndc11": medication.c.ndc11,
        "tier": membership.c.drug_tier,
        "prior_authorization": membership.c.prior_authorization,
        "step_therapy": membership.c.step_therapy,
        "quantity_limit": membership.c.quantity_limit,
    }
    for filter_name, filter_column in columns_by_filter.items():
        filter_value = getattr(filters, filter_name)
        if filter_value is not None:
            predicates.append(filter_column == bindparam(filter_name))
    statement = (
        select(*DRUG_COLUMNS)
        .select_from(DRUG_FROM)
        .where(*predicates)
        .order_by(medication.c.medication_version_id)
    )
    page_limit = 2 if exact_drug_id is not None else bindparam("page_size")
    return statement.limit(page_limit)


ALTERNATIVE_FROM = DRUG_FROM.join(
    alternative,
    and_(
        alternative.c.alias_version_id == membership.c.alias_version_id,
        alternative.c.upstream_medication_id
        == membership.c.upstream_medication_id,
    ),
).outerjoin(
    target_membership,
    and_(
        target_membership.c.alias_version_id == alternative.c.alias_version_id,
        target_membership.c.source_id == membership.c.source_id,
        target_membership.c.upstream_medication_id
        == alternative.c.resolved_medication_id,
    ),
)
ALTERNATIVE_STATEMENT = (
    select(
        membership.c.medication_version_id.label("owner_drug_id"),
        alternative.c.resolved,
        target_membership.c.medication_version_id.label("target_drug_id"),
    )
    .select_from(ALTERNATIVE_FROM)
    .where(
        *CURRENT_PLAN_PREDICATES,
        plan.c.public_id == bindparam("public_id"),
        alias.c.alias_id == bindparam("alias_id"),
        membership.c.medication_version_id.in_(
            bindparam("owner_drug_ids", expanding=True)
        ),
    )
    .order_by(
        membership.c.medication_version_id,
        alternative.c.raw_reference,
    )
    .limit(bindparam("alternative_limit"))
)


__all__ = (
    "ALIAS_CONTEXT_STATEMENT",
    "ALTERNATIVE_STATEMENT",
    "drug_statement",
)
