# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticated physical table bindings for complete procedure projections."""

from dataclasses import dataclass, replace

from sqlalchemy import literal, text
from sqlalchemy.dialects import postgresql

from api.plan_pricing_code_contract import (
    BUILD_REVISION,
    CONTRACT,
    ZIP_BUILD_REVISION,
    ZIP_CONTRACT,
    canonical_radius,
    identity_digest,
    scope_selectors,
)
from api.plan_pricing_code_storage import attachment_name, locate
from api.plan_pricing_projection_contract import (
    PlanPricingProjectionUnavailable,
    PlanPricingProjectionUnsupported,
    _lock_provider_relations,
    projection_code_identity,
)
from process import reference_family_archive as native


@dataclass(frozen=True)
class CodePricingCapability:
    identity: dict
    found: tuple
    binding_aliases: tuple[tuple[int, int], ...] = ()
    api_read: bool = False

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
        if self.identity["contract"] == ZIP_CONTRACT and name in {
            "plan_pricing_provider_state",
            "plan_pricing_rate_occurrence",
        }:
            raise PlanPricingProjectionUnavailable("ZIP aggregates do not provide nationwide provider witnesses")
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
        return f"(SELECT {columns}, {constants} FROM ONLY {schema}.{native._quoted(relation)} stored{predicate})"

    def selection(self, selection):
        """Bind the capability to this request without changing the release identity."""
        return replace(
            selection, pricing_projection_id=self.projection_id, pricing_projection_contract=self.identity["contract"]
        )

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


async def resolve_code_pricing(session, selection, args, *, api_read=None):
    """Absence preserves ordinary routing; an invalid attached capability fails closed."""
    if api_read is not None and type(api_read) is not bool:
        raise PlanPricingProjectionUnavailable("code pricing API mode is invalid")
    options = {"api_read": True} if api_read else {}
    code_identity = projection_code_identity(args.get("code_system"), args.get("code"))
    if selection is None or code_identity is None:
        return None
    scope = _request_scope(args)
    if scope is not None:
        capability = await _resolve_capability(
            session, selection, code_identity, scope=scope, **({"api_read": None} if api_read is None else options)
        )
        if capability is not None:
            return capability
    return await _resolve_capability(session, selection, code_identity, **options)


def _request_scope(args):
    from api.plan_pricing_projection_read import projection_result_type, unsupported_projection_fields

    zip5 = str(args.get("zip5") or args.get("zip") or "").strip()
    if (
        not zip5
        or projection_result_type(args) != "rate_aggregates"
        or unsupported_projection_fields(args)
        or args.get("state")
        or args.get("city")
    ):
        return None
    try:
        return scope_selectors(
            {
                "scope_kind": "zip_radius",
                "zip5": zip5,
                "zip_radius_miles": canonical_radius(args.get("zip_radius_miles") or 0),
            }
        )
    except ValueError as error:
        raise PlanPricingProjectionUnsupported(str(error)) from error


async def _resolve_capability(session, selection, code_identity, *, scope=None, api_read=False):
    if api_read is not None and type(api_read) is not bool:
        raise PlanPricingProjectionUnavailable("code pricing API mode is invalid")
    anchor_by_field = {
        "contract": CONTRACT,
        "build_revision": BUILD_REVISION,
        "plan_release_id": selection.plan_release_id,
        "serving_revision_id": selection.serving_revision_id,
        "binding_set_digest": selection.binding_set_digest,
        "code_system": code_identity[0],
        "code": code_identity[1],
    }
    if scope is not None:
        anchor_by_field.update(contract=ZIP_CONTRACT, build_revision=ZIP_BUILD_REVISION, scope=scope)
    present, api_read = await _capability_attached(session, anchor_by_field, api_read)
    if not present:
        return None
    from api.plan_pricing_code_build import can_read_code_api, resolve_inputs

    if api_read is None:
        api_read = scope is not None and await can_read_code_api(session, selection)
    if scope is not None and not api_read:
        await _lock_provider_relations(session)
    options = {"api_read": True} if api_read else {}
    identity, _inputs = await resolve_inputs(
        session, selection, code_identity, **options, **({} if scope is None else {"scope": scope})
    )
    found = await locate(session, identity, **options)
    if found is None:
        raise PlanPricingProjectionUnavailable("the complete selected-code capability is unavailable")
    return CodePricingCapability(identity, found, api_read=api_read)


async def _capability_attached(session, anchor_by_field, api_read):
    """Keep discovery opaque until the original API read context is rechecked."""
    statement = text("SELECT to_regclass(:relation) IS NOT NULL")
    parameters_by_name = {"relation": "hp_snapshot_retention." + attachment_name(anchor_by_field)}
    if (
        api_read is False
        or api_read is None
        and (
            not callable(getattr(session, "get_transaction", None))
            or not callable(getattr(session, "in_transaction", None))
            or session.in_transaction() is not True
            or session.get_transaction() is None
        )
    ):
        return await session.scalar(statement, parameters_by_name), False
    from api.network_read_execution import _original_read_driver
    from api.plan_pricing_code_build import _same_api_input_session

    transaction = session.get_transaction()
    driver, backend_pid = await _original_read_driver(session)
    await _same_api_input_session(session, driver, backend_pid, transaction)
    buffered = await session.execute(statement, parameters_by_name)
    try:
        await _same_api_input_session(session, driver, backend_pid, transaction)
        return buffered.scalar_one(), api_read
    except BaseException as error:
        try:
            buffered.close()
        except BaseException as close_error:
            error.add_note("Attachment result cleanup failed: " + type(close_error).__name__)
        raise
