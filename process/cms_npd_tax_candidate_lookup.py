# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded read-only PTG tax candidates for CMS NPI-only Organizations."""

from __future__ import annotations

import hmac
import re
from collections.abc import Iterable
from typing import Any

from sqlalchemy import text

from api.ptg2_v4_graph import lookup_v4_relation_member_prefixes, v4_npi_keys_for_values
from process.cms_npd_tax_candidate_report import (
    CmsNpiTaxCandidate,
)
from process.ptg_parts.ptg2_v4_snapshot_maps import PTG2_V4_SHARED_GENERATION
from process.tin_npi_connector_temporal import _normalize_npi

_SCHEMA = re.compile(r"[a-z][a-z0-9_]{0,62}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAX_NPIS = 256
_MAX_GROUPS_PER_NPI = 128
_GROUP_PREFIX = _MAX_GROUPS_PER_NPI + 1
_MAX_GROUPS = _MAX_NPIS * _GROUP_PREFIX


async def current_sealed_v4_tax_pin(session: Any, *, schema_name: str) -> tuple[str, int, str] | None:
    """Select only the globally current published V4 tax snapshot."""

    if type(schema_name) is not str or _SCHEMA.fullmatch(schema_name) is None:
        raise ValueError("CMS tax candidate schema is invalid")
    read_only = (await session.execute(text("SELECT current_setting('transaction_read_only')"))).scalar_one()
    if read_only != "on":
        raise ValueError("CMS tax candidate pin requires a read-only transaction")
    schema = f'"{schema_name}"'
    pin_query = await session.execute(
        text(f"""
            SELECT snapshot.snapshot_id, binding.snapshot_key,
                   manifest.content_digest,
                   layout.layout_manifest->'serving_index'->>'shared_snapshot_key'
                     AS layout_snapshot_key,
                   layout.layout_manifest #>>
                     '{{serving_index,provider_graph,provider_tax_identity,content_digest}}'
                     AS sealed_tax_digest
              FROM {schema}.ptg2_current_snapshot AS pointer
              JOIN {schema}.ptg2_snapshot AS snapshot
                ON snapshot.snapshot_id = pointer.snapshot_id
              JOIN {schema}.ptg2_v3_snapshot_binding AS binding
                ON binding.snapshot_id = snapshot.snapshot_id
              JOIN {schema}.ptg2_v3_snapshot_layout AS layout
                ON layout.snapshot_key = binding.snapshot_key
              JOIN {schema}.ptg2_v4_snapshot_map_root AS root
                ON root.snapshot_key = binding.snapshot_key
              JOIN {schema}.ptg2_provider_tax_identity_manifest AS manifest
                ON manifest.snapshot_key = binding.snapshot_key
             WHERE pointer.slot = 'current'
               AND snapshot.status = 'published'
               AND layout.state = 'sealed'
               AND layout.generation = :generation
               AND root.state = 'complete'
               AND manifest.contract = 'ptg2_provider_group_tax_identity_v1'
        """),
        {"generation": PTG2_V4_SHARED_GENERATION},
    )
    pin_row = pin_query.mappings().one_or_none()
    if pin_row is None:
        return None
    snapshot_id = pin_row["snapshot_id"]
    snapshot_key = pin_row["snapshot_key"]
    layout_snapshot_key = pin_row["layout_snapshot_key"]
    content_digest = pin_row["content_digest"]
    sealed_digest = pin_row["sealed_tax_digest"]
    if (
        type(snapshot_id) is not str
        or not snapshot_id
        or type(snapshot_key) is not int
        or snapshot_key <= 0
        or layout_snapshot_key != str(snapshot_key)
        or not isinstance(content_digest, (bytes, memoryview))
        or len(content_digest) != 32
        or type(sealed_digest) is not str
        or not hmac.compare_digest(bytes(content_digest).hex(), sealed_digest)
    ):
        raise ValueError("CMS tax candidate current snapshot pin is invalid")
    return snapshot_id, snapshot_key, sealed_digest


def _valid_npis(npis: Iterable[int]) -> tuple[int, ...]:
    if isinstance(npis, (str, bytes, bytearray)):
        raise ValueError("CMS tax candidate NPIs are invalid")
    values = tuple(npis)
    if (
        len(values) > _MAX_NPIS
        or any(type(npi) is not int for npi in values)
        or values != tuple(sorted(set(values)))
        or any(_normalize_npi(str(npi)) != npi for npi in values)
    ):
        raise ValueError("CMS tax candidate NPIs are invalid")
    return values


async def _sealed_tax_generation(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    manifest_sha256: str,
) -> None:
    manifest_query = await session.execute(
        text(f"""
            SELECT layout.generation,
                   manifest.content_digest,
                   layout.layout_manifest #>>
                       '{{serving_index,provider_graph,provider_tax_identity,content_digest}}'
                       AS sealed_tax_digest
              FROM {schema}.ptg2_v3_snapshot_layout AS layout
              JOIN {schema}.ptg2_provider_tax_identity_manifest AS manifest
                ON manifest.snapshot_key = layout.snapshot_key
             WHERE layout.snapshot_key = :snapshot_key
               AND layout.state = 'sealed'
               AND manifest.contract = 'ptg2_provider_group_tax_identity_v1'
        """),
        {"snapshot_key": snapshot_key},
    )
    manifest_row = manifest_query.mappings().one_or_none()
    if manifest_row is None:
        raise ValueError("CMS tax candidate snapshot is unavailable")
    generation = manifest_row["generation"]
    content_digest = manifest_row["content_digest"]
    sealed_digest = manifest_row["sealed_tax_digest"]
    if (
        generation != PTG2_V4_SHARED_GENERATION
        or not isinstance(content_digest, (bytes, memoryview))
        or len(content_digest) != 32
        or type(sealed_digest) is not str
        or not hmac.compare_digest(bytes(content_digest).hex(), manifest_sha256)
        or not hmac.compare_digest(sealed_digest, manifest_sha256)
    ):
        raise ValueError("CMS tax candidate snapshot manifest mismatch")


async def _group_keys_by_npi(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    npis: tuple[int, ...],
) -> dict[int, tuple[int, ...]]:
    if not npis:
        return {}
    npi_keys = await v4_npi_keys_for_values(
        session,
        snapshot_key=snapshot_key,
        npis=npis,
        schema_name=schema_name,
    )
    if set(npi_keys) - set(npis) or len(set(npi_keys.values())) != len(npi_keys):
        raise ValueError("CMS tax candidate NPI dictionary is invalid")
    graph = await lookup_v4_relation_member_prefixes(
        session,
        snapshot_key=snapshot_key,
        relation="npi_groups_exact",
        owner_keys=npi_keys.values(),
        schema_name=schema_name,
        limit_per_owner=_GROUP_PREFIX,
        max_members=_MAX_GROUPS,
    )
    if set(graph) != set(npi_keys.values()):
        raise ValueError("CMS tax candidate NPI graph is incomplete")
    return {npi: tuple(graph.get(npi_keys[npi], ())) if npi in npi_keys else () for npi in npis}


async def _tax_keys_by_group(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    group_keys: tuple[int, ...],
) -> dict[int, int | None]:
    if not group_keys:
        return {}
    selected_group_keys = set(group_keys)
    tax_query = await session.execute(
        text(f"""
            SELECT groups.provider_group_key,
                   identity.tax_identity_state, identity.tin_key
              FROM {schema}.ptg2_v3_provider_group AS groups
              JOIN {schema}.ptg2_provider_group_tax_identity AS identity
                ON identity.snapshot_key = groups.snapshot_key
               AND identity.provider_group_global_id_128 = groups.provider_group_global_id_128
             WHERE groups.snapshot_key = :snapshot_key
               AND groups.provider_group_key = ANY(CAST(:group_keys AS integer[]))
             ORDER BY groups.provider_group_key
        """),
        {"snapshot_key": snapshot_key, "group_keys": group_keys},
    )
    tax_key_by_group: dict[int, int | None] = {}
    for tax_row in tax_query.mappings():
        group_key = tax_row["provider_group_key"]
        state = tax_row["tax_identity_state"]
        tin_key = tax_row["tin_key"]
        if (
            type(group_key) is not int
            or group_key not in selected_group_keys
            or group_key in tax_key_by_group
            or state not in ("matched_ein", "missing", "malformed", "unsupported_type")
            or (state == "matched_ein") != (type(tin_key) is int and tin_key >= 0)
            or (state != "matched_ein" and tin_key is not None)
        ):
            raise ValueError("CMS tax candidate group tax evidence is invalid")
        tax_key_by_group[group_key] = tin_key
    if set(tax_key_by_group) != selected_group_keys:
        raise ValueError("CMS tax candidate group tax evidence is incomplete")
    return tax_key_by_group


def _npi_candidate(groups: tuple[int, ...], tax_key_by_group: dict[int, int | None]) -> CmsNpiTaxCandidate:
    if len(groups) == _GROUP_PREFIX:
        return CmsNpiTaxCandidate((), _GROUP_PREFIX, 0, degree_overflow=True)
    return CmsNpiTaxCandidate(
        tin_keys=tuple(sorted({tax_key_by_group[group] for group in groups if tax_key_by_group[group] is not None})),
        group_count=len(groups),
        groups_without_ein_match=sum(tax_key_by_group[group] is None for group in groups),
    )


def _validate_group_vectors(group_keys_by_npi: dict[int, tuple[int, ...]], selected_npis: tuple[int, ...]) -> None:
    if set(group_keys_by_npi) != set(selected_npis) or any(
        type(groups) is not tuple
        or len(groups) > _GROUP_PREFIX
        or groups != tuple(sorted(set(groups)))
        or any(type(group) is not int or group < 0 for group in groups)
        for groups in group_keys_by_npi.values()
    ):
        raise ValueError("CMS tax candidate NPI group evidence is invalid")


async def lookup_pinned_tax_candidates(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    manifest_sha256: str,
    npis: Iterable[int],
) -> dict[int, CmsNpiTaxCandidate]:
    """Return complete snapshot-local tax candidates, never confirmed bindings.

    The caller must provide a read-only transaction. Missing NPI/group/tax
    evidence yields explicit no-match counts; corrupt or drifting proof fails.
    """

    if (
        type(schema_name) is not str
        or _SCHEMA.fullmatch(schema_name) is None
        or type(snapshot_key) is not int
        or snapshot_key <= 0
        or type(manifest_sha256) is not str
        or _SHA256.fullmatch(manifest_sha256) is None
    ):
        raise ValueError("CMS tax candidate snapshot pin is invalid")
    selected_npis = _valid_npis(npis)
    schema = f'"{schema_name}"'
    read_only = (await session.execute(text("SELECT current_setting('transaction_read_only')"))).scalar_one()
    if read_only != "on":
        raise ValueError("CMS tax candidate lookup requires a read-only transaction")
    await session.execute(text("SET LOCAL statement_timeout = '15s'"))
    await session.execute(text("SET LOCAL lock_timeout = '1s'"))
    await _sealed_tax_generation(
        session,
        schema=schema,
        snapshot_key=snapshot_key,
        manifest_sha256=manifest_sha256,
    )
    group_keys_by_npi = await _group_keys_by_npi(
        session,
        schema_name=schema_name,
        snapshot_key=snapshot_key,
        npis=selected_npis,
    )
    _validate_group_vectors(group_keys_by_npi, selected_npis)
    group_keys = tuple(
        sorted({group for groups in group_keys_by_npi.values() if len(groups) < _GROUP_PREFIX for group in groups})
    )
    tax_key_by_group = await _tax_keys_by_group(
        session, schema=schema, snapshot_key=snapshot_key, group_keys=group_keys
    )
    await _sealed_tax_generation(
        session,
        schema=schema,
        snapshot_key=snapshot_key,
        manifest_sha256=manifest_sha256,
    )
    return {npi: _npi_candidate(group_keys_by_npi[npi], tax_key_by_group) for npi in selected_npis}
