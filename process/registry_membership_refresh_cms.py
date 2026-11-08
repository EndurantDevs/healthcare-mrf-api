# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve accepted CMS editions while retaining each original reviewed enrollment."""

import json
from dataclasses import replace

from process.network_address_projection import _identifier
from process.network_cms_registry_source_pair import _CMS_FIELDS
from process.network_fhir_membership_source import with_fhir_successor
from process.registry_cms_refresh_anchors import _capture_groups, _organizations, _original_identity, _scopes
from process.registry_refresh_anchor_contract import RegistryRefreshAnchorMismatch, RegistryRefreshAnchorUnavailable


async def _accepted_pair(session, driver, reader, source_schema, receipt_id, budget):
    size = await driver.fetchval(
        f"SELECT octet_length(notification_json) FROM {_identifier(reader.control_schema)}.registry_cms_source_notification WHERE source_schema=$1 AND receipt_id=$2",
        source_schema,
        receipt_id,
    )
    if size is None:
        raise RegistryRefreshAnchorUnavailable("registry_refresh_accepted_cms_unavailable")
    budget.reserve(({"raw_byte_count": size},))
    return (await reader.read(session, source_schema=source_schema, receipt_id=receipt_id)).pair


async def _original_pair(session, driver, reader, coordinates, bindings, budget):
    budget.reserve(({"raw_byte_count": 16384},))
    headers = await reader.matching_headers(session, coordinates)
    if not headers:
        raise RegistryRefreshAnchorUnavailable("registry_refresh_original_cms_unavailable")
    original = scopes = None
    for header in headers:
        pair = await _accepted_pair(session, driver, reader, coordinates.dataset_schema, header["receipt_id"], budget)
        if pair.proof.binding_coordinates != coordinates:
            raise RegistryRefreshAnchorMismatch("registry_refresh_original_cms_mismatch")
        actual = _scopes(pair, bindings, await _organizations(driver, pair.recipe.source_pin, bindings, budget))
        if original is not None and (_original_identity(original) != _original_identity(pair) or scopes != actual):
            raise RegistryRefreshAnchorUnavailable("registry_refresh_original_cms_ambiguous")
        original, scopes = pair, actual
    return original, scopes


async def _current_header(driver, source_schema):
    namespace = _identifier(source_schema)
    rows = await driver.fetch(
        f"""SELECT r.receipt_id,CASE WHEN octet_length((r.payload->'cms')::text)<=4096
          THEN (r.payload->'cms')::text END AS cms FROM {namespace}.provider_directory_cms_serving_receipt r
        JOIN {namespace}.entity_address_result_generation a
          ON r.address_lineage_id=a.local_lineage_id AND r.address_generation=a.local_generation
        JOIN {namespace}.provider_directory_profile_serving_generation p
          ON r.profile_generation_id=p.generation_id AND p.singleton_key='global'
        WHERE a.singleton AND {namespace}.cms_serving_snapshot_receipt_matches(r.payload) LIMIT 2"""
    )
    if len(rows) != 1:
        raise RegistryRefreshAnchorUnavailable("registry_refresh_current_cms_unavailable")
    authority = json.loads(rows[0]["cms"])
    if type(authority) is not dict or set(authority) != set(_CMS_FIELDS):
        raise RegistryRefreshAnchorUnavailable("registry_refresh_current_cms_unavailable")
    return rows[0]["receipt_id"], tuple(authority[name] for name in _CMS_FIELDS)


async def _current_pair(session, driver, reader, source_schema, budget):
    receipt_id, authority = await _current_header(driver, source_schema)
    await reader._connection(session, writing=False)
    headers = await driver.fetch(
        f"""SELECT receipt_id FROM {_identifier(reader.control_schema)}.registry_cms_source_notification
        WHERE source_schema=$1 AND notification_json::jsonb#>'{{pair,proof,cms_authority}}'=$2::jsonb
        ORDER BY receipt_id COLLATE "C" LIMIT 101""",
        source_schema,
        json.dumps(authority),
    )
    if not headers or len(headers) > 100:
        raise RegistryRefreshAnchorUnavailable("registry_refresh_current_cms_unavailable")
    pairs_by_pin, preferred, identity = {}, None, None
    for header in headers:
        pair = await _accepted_pair(session, driver, reader, source_schema, header["receipt_id"], budget)
        actual_identity = (_original_identity(pair), pair.recipe.source_pin.retained_epoch.content_sha256)
        if pair.proof.cms_authority != authority or (identity is not None and identity != actual_identity):
            raise RegistryRefreshAnchorUnavailable("registry_refresh_current_cms_ambiguous")
        identity = actual_identity
        pairs_by_pin[pair.recipe.source_pin.generation_id] = pair
        if header["receipt_id"] == receipt_id:
            preferred = pair
    if preferred is not None:
        return preferred
    if len(pairs_by_pin) != 1:
        raise RegistryRefreshAnchorUnavailable("registry_refresh_current_cms_ambiguous")
    return next(iter(pairs_by_pin.values()))


async def resolve_accepted_cms_recipes(session, driver, bindings, *, reader, read_budget):
    """Read the native accepted head, never choose by receipt timestamp or UUID."""
    scopes, recipes = {}, []
    current_by_schema = {}
    for coordinates, selected in _capture_groups(bindings).items():
        original, derived = await _original_pair(session, driver, reader, coordinates, selected, read_budget)
        if coordinates.dataset_schema not in current_by_schema:
            current_by_schema[coordinates.dataset_schema] = await _current_pair(
                session, driver, reader, coordinates.dataset_schema, read_budget
            )
        current = current_by_schema[coordinates.dataset_schema]
        current_scopes = _scopes(
            current, selected, await _organizations(driver, current.recipe.source_pin, selected, read_budget)
        )
        if derived != current_scopes:
            raise RegistryRefreshAnchorMismatch("registry_refresh_accepted_cms_continuity_mismatch")
        source = with_fhir_successor(original.recipe.source_pin, current.recipe.source_pin)
        recipes.append(replace(original.recipe, source_pin=source))
        scopes.update(derived)
    return scopes, tuple(recipes)
