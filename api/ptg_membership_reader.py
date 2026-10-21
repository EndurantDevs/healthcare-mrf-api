# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Intersect an explicitly covered code inside SQL before factor expansion."""

from sqlalchemy import text

from api.ptg2_v4_graph import _charge_v4_taxonomy_projection_work, v4_npi_keys_for_values
from api.ptg_membership_contract import encoded
from api.ptg_membership_source import source_identity
from api.ptg_membership_storage import locate
from process import reference_family_archive as native
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


def membership_statement(schema):
    schema = native._quoted(schema)
    return text(f"""
        WITH local_first AS MATERIALIZED (
            SELECT wanted.npi,first.member_key AS factor
              FROM unnest(CAST(:npis AS bigint[]),CAST(:keys AS bigint[])) wanted(npi,npi_key)
              JOIN {schema}.ptg_membership_first first ON first.owner_key=wanted.npi_key
             LIMIT :projection_limit
        ), priced_factors AS MATERIALIZED (
            SELECT DISTINCT second.owner_key AS factor,second.member_key AS provider_set_key
              FROM (SELECT DISTINCT factor FROM local_first) wanted
              JOIN {schema}.ptg_membership_second second ON second.owner_key=wanted.factor
              JOIN {schema}.ptg_membership_code code ON code.provider_set_key=second.member_key
                   AND code.code_key=ANY(CAST(:code_keys AS bigint[]))
             LIMIT :projection_limit
        ), matches AS MATERIALIZED (
            SELECT DISTINCT first.npi,priced.provider_set_key
              FROM local_first first JOIN priced_factors priced USING(factor)
             ORDER BY first.npi,priced.provider_set_key LIMIT :retained_limit
        )
        SELECT (SELECT count(*) FROM local_first) AS first_count,
               (SELECT count(*) FROM priced_factors) AS priced_count,
               COALESCE((SELECT jsonb_agg(jsonb_build_array(npi,provider_set_key) ORDER BY npi,provider_set_key)
                         FROM matches),'[]'::jsonb) AS pairs
    """)


async def indexed_memberships(session, tables, npis, request, state):
    """None means unindexed; an attached invalid candidate never falls back."""
    if not request.code_system or not request.code:
        return None
    if not state.membership_index_checked:
        _, _, identity = await source_identity(
            session,
            tables.source_key,
            tables.snapshot_id,
            request.code_system,
            request.code,
            tables=tables,
        )
        state.membership_index = await locate(session, identity)
        state.membership_index_checked = True
    found = state.membership_index
    if found is None:
        return None
    ownership, header, _owner, _view, _pin = found
    code_keys = tuple(sorted({int(row["code_key"]) for row in request.code_rows}))
    covered = {row["code_key"] for row in header["counts"]["code_coverage"]}
    if not set(code_keys).issubset(covered):
        raise PTG2ManifestArtifactError("membership index does not cover the complete requested physical code scope")
    _charge_v4_taxonomy_projection_work(batch_count=1)
    from api import ptg2_serving as serving

    keys = await v4_npi_keys_for_values(
        session,
        snapshot_key=serving._required_shared_snapshot_key(tables),
        npis=npis,
        schema_name=serving._payload_schema(tables),
    )
    remaining = request.forward_limits.maximum_retained_memberships - state.retained_memberships
    projection_remaining = request.forward_limits.maximum_projection_members - state.indexed_projection_members
    row = (
        (
            await session.execute(
                membership_statement(ownership.schema_name),
                {
                    "npis": list(keys),
                    "keys": list(keys.values()),
                    "code_keys": list(code_keys),
                    "projection_limit": max(projection_remaining, 0) + 1,
                    "retained_limit": max(remaining, 0) + 1,
                },
            )
        )
        .mappings()
        .one()
    )
    pairs = row["pairs"]
    projected = int(row["first_count"]) + int(row["priced_count"]) + len(pairs)
    if len(pairs) > remaining:
        raise serving.PTG2OnlineWorkBudgetExceeded("retained_memberships")
    if projected > projection_remaining:
        raise serving.PTG2OnlineWorkBudgetExceeded("projection_members")
    # Native index reads issue no CAS page fetches. Charge their complete bounded
    # intermediate/output projection and serialized bytes to the same request budget.
    byte_count = 16 * (int(row["first_count"]) + int(row["priced_count"]) + len(keys)) + len(encoded(pairs))
    _charge_v4_taxonomy_projection_work(member_count=projected, byte_count=byte_count)
    memberships = {npi: [] for npi in npis}
    for npi, provider_set_key in pairs:
        if npi not in memberships:
            raise PTG2ManifestArtifactError("membership index returned an out-of-scope NPI")
        memberships[npi].append(provider_set_key)
    code_sets = {key for values in memberships.values() for key in values}
    if len(state.code_sets | code_sets) > request.forward_limits.maximum_code_sets:
        raise serving.PTG2OnlineWorkBudgetExceeded("code_sets")
    state.indexed_projection_members += projected
    state.retained_memberships += len(pairs)
    state.code_sets.update(code_sets)
    return {npi: tuple(values) for npi, values in memberships.items()}
