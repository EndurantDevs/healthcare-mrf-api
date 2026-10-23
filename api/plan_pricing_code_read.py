# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticated physical table bindings for complete procedure projections."""

from dataclasses import dataclass, replace

from sqlalchemy import literal, text
from sqlalchemy.dialects import postgresql

from api.plan_pricing_code_contract import BUILD_REVISION, CONTRACT, identity_digest
from api.plan_pricing_code_storage import attachment_name, locate
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable, projection_code_identity
from process import reference_family_archive as native


@dataclass(frozen=True)
class CodePricingCapability:
    identity: dict
    found: tuple
    binding_aliases: tuple[tuple[int, int], ...] = ()

    relations = {
        "plan_pricing_provider_membership": "plan_pricing_code_membership",
        "plan_pricing_provider_cell": "plan_pricing_code_provider_cell",
        "plan_pricing_provider_state": "plan_pricing_code_provider_state",
        "plan_pricing_rate_occurrence": "plan_pricing_code_occurrence",
        "plan_pricing_aggregate_pack": "plan_pricing_code_aggregate_pack",
        "plan_pricing_rate_profile": "plan_pricing_code_rate_profile",
    }

    @property
    def projection_id(self):
        """Return the digest of this exact authenticated capability identity."""
        return identity_digest(self.identity)

    def relation(self, name):
        """Expose existing read columns without copying or rewriting arbitrary SQL."""
        relation = self.relations[name]
        schema = native._quoted(self.found[0].schema_name)
        columns = "stored.*"
        if self.binding_aliases and name in {
            "plan_pricing_provider_membership",
            "plan_pricing_rate_occurrence",
            "plan_pricing_rate_profile",
        }:
            from api.plan_pricing_code_contract import SPEC

            model = next(model for model in SPEC.model_types if model.__tablename__ == relation)
            mapping = (
                "CASE stored.binding_ordinal "
                + " ".join(
                    f"WHEN {stored_ordinal} THEN {logical_ordinal}"
                    for stored_ordinal, logical_ordinal in self.binding_aliases
                )
                + " ELSE stored.binding_ordinal END AS binding_ordinal"
            )
            columns = ",".join(
                mapping if column.name == "binding_ordinal" else "stored." + native._quoted(column.name)
                for column in model.__table__.columns
            )
        constants = "CAST(:projection_id AS varchar(64)) AS projection_id"
        if name in {"plan_pricing_rate_occurrence", "plan_pricing_aggregate_pack", "plan_pricing_rate_profile"}:
            constants += ", CAST(:code_system AS varchar) AS code_system, CAST(:code AS varchar) AS code"
        predicate = ""
        if name == "plan_pricing_provider_cell":
            from api.plan_pricing_projection_v3_work import _normalized_taxonomy_codes

            inferred, codes = _normalized_taxonomy_codes((self.identity["code_system"], self.identity["code"]))
            if inferred:
                taxonomy_literals = ",".join(
                    str(literal(code).compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
                    for code in codes or ()
                )
                predicate = f" WHERE stored.entity_type_code=1 AND EXISTS(SELECT 1 FROM unnest(stored.taxonomy_codes) taxonomy_code WHERE upper(btrim(taxonomy_code))=ANY(ARRAY[{taxonomy_literals}]::varchar[]))"
        return f"(SELECT {columns}, {constants} FROM {schema}.{native._quoted(relation)} stored{predicate})"

    def selection(self, selection):
        """Bind the capability to this request without changing the release identity."""
        return replace(selection, pricing_projection_id=self.projection_id, pricing_projection_contract=CONTRACT)

    def contributors(self, bindings):
        """Preserve physical deduplication when a policy admits only a logical alias."""
        if bindings is None:
            return self
        admitted_ordinals = {binding.binding_ordinal for binding in bindings}
        if not admitted_ordinals.issubset({source["binding_ordinal"] for source in self.identity["sources"]}):
            raise PlanPricingProjectionUnavailable("code projection contributor coverage differs")
        by_representative = {}
        for source in self.identity["sources"]:
            if source["binding_ordinal"] in admitted_ordinals:
                by_representative.setdefault(source["materialized_binding_ordinal"], source["binding_ordinal"])
        return replace(self, binding_aliases=tuple(sorted(by_representative.items())))


async def resolve_code_pricing(session, selection, args):
    """Absence preserves ordinary routing; an invalid attached capability fails closed."""
    code_identity = projection_code_identity(args.get("code_system"), args.get("code"))
    if selection is None or code_identity is None:
        return None
    anchor_by_field = {
        "contract": CONTRACT,
        "build_revision": BUILD_REVISION,
        "plan_release_id": selection.plan_release_id,
        "serving_revision_id": selection.serving_revision_id,
        "binding_set_digest": selection.binding_set_digest,
        "code_system": code_identity[0],
        "code": code_identity[1],
    }
    if not await session.scalar(
        text("SELECT to_regclass(:relation) IS NOT NULL"),
        {"relation": "hp_snapshot_retention." + attachment_name(anchor_by_field)},
    ):
        return None
    from api.plan_pricing_code_build import resolve_inputs

    identity, _inputs = await resolve_inputs(session, selection, code_identity)
    found = await locate(session, identity)
    if found is None:
        raise PlanPricingProjectionUnavailable("the complete selected-code capability is unavailable")
    return CodePricingCapability(identity, found)
