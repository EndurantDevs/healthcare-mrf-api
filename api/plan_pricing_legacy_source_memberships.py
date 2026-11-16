# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bound reverse provider memberships before reconstructing legacy rate cells."""

from itertools import chain

from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable, PlanPricingProjectionUnsupported
from api.ptg2_shared_blocks import PTG2SharedBlockError
from process.ptg_parts.ptg2_manifest_artifacts import ManifestReadLimitError, PTG2ManifestArtifactError


def _bounded_sets_by_npi(npis, members_by_npi, sets_by_group, maximum_members):
    sets_by_npi = {}
    retained_members = 0
    for npi in npis:
        selected_sets = set()
        for set_id in chain.from_iterable(sets_by_group.get(group, ()) for group in members_by_npi.get(npi, ())):
            if set_id in selected_sets:
                continue
            retained_members += 1
            if retained_members > maximum_members:
                raise PlanPricingProjectionUnsupported("aggregate projection exceeds its native membership bound")
            selected_sets.add(set_id)
        sets_by_npi[npi] = tuple(sorted(selected_sets))
    return sets_by_npi


async def _legacy_sets(session, serving, tables, npis, maximum_members):
    member_ids_by_npi = {npi: serving._ptg2_npi_member_id(npi) for npi in npis}
    groups_by_member = await serving._shared_graph_members_by_id(
        session,
        tables,
        "provider_npi_group",
        tuple(member_ids_by_npi.values()),
        max_members=maximum_members,
        max_projection_members=maximum_members,
    )
    groups_by_npi = {npi: groups_by_member.get(member_id, ()) for npi, member_id in member_ids_by_npi.items()}
    group_ids = tuple(sorted({group for groups in groups_by_npi.values() for group in groups}))
    sets_by_group = await serving._shared_graph_members_by_id(
        session,
        tables,
        "provider_inverted",
        group_ids,
        max_members=maximum_members,
        max_projection_members=maximum_members,
    )
    return _bounded_sets_by_npi(npis, groups_by_npi, sets_by_group, maximum_members)


async def bounded_source_provider_sets(session, tables, npis, maximum_members):
    """Admit both graph legs and final reverse expansion before retaining memberships."""
    from api import ptg2_serving as serving

    try:
        if not tables.uses_v4_graph:
            return await _legacy_sets(session, serving, tables, npis, maximum_members)
        keys_by_npi = await serving._v4_sets_by_npi(
            session, tables, npis, max_members=maximum_members, max_projection_members=maximum_members
        )
    except ManifestReadLimitError as error:
        raise PlanPricingProjectionUnsupported("aggregate projection exceeds its native membership bound") from error
    except (PTG2ManifestArtifactError, PTG2SharedBlockError) as error:
        if (
            not serving._is_v4_member_limit(error)
            and str(error) != "PTG2 shared graph selection exceeds max_projection_members"
        ):
            raise
        raise PlanPricingProjectionUnsupported("aggregate projection exceeds its native membership bound") from error
    set_keys = {key for keys in keys_by_npi.values() for key in keys}
    ids_by_key = await serving._provider_set_ids_for_keys(session, tables, set_keys)
    if set(ids_by_key) != set_keys:
        raise PlanPricingProjectionUnavailable("legacy aggregate provider-set membership is incomplete")
    return {npi: tuple(ids_by_key[key] for key in keys_by_npi.get(npi, ())) for npi in npis}
