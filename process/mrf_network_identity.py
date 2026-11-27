# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact ACA exchange network identities from provider-file plan links.

Provider files list, for each provider, the plans and tier that include it. Ingest
keys each provider row by the exact plan list one record gives for a tier and year
(a fragment). Finalization then groups plans whose provider lists are identical
into one network and rewrites the provider, tier and address-evidence rows to that
network's key. An issuer tier whose plans all share one provider list keeps the
historical ``(tier, issuer, year)`` key, so unaffected network IDs do not change.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass

from sqlalchemy import text

from process.ext.utils import return_checksum

# Fragment keys live above the int32 range; published network keys stay inside it.
_FRAGMENT_KEY_FLOOR = 1 << 56
_INT32_MIN = -(1 << 31)
_INT32_MAX = (1 << 31) - 1
_INSERT_BATCH_ROWS = 2000


class MRFNetworkIdentityError(RuntimeError):
    """The staged provider links cannot be published as unambiguous networks."""


def fragment_key(network_tier: str, issuer_id: int, year: int, plan_ids) -> int:
    """Key one provider record's exact plan list for a tier and year."""
    digest = hashlib.blake2b(
        json.dumps([network_tier, issuer_id, year, sorted(plan_ids)], separators=(",", ":")).encode(),
        digest_size=7,
    ).digest()
    return _FRAGMENT_KEY_FLOOR | int.from_bytes(digest, "big")


def legacy_network_key(network_tier: str, issuer_id: int, year: int) -> int:
    """Return the historical key shared by every plan of one issuer tier."""
    return return_checksum([network_tier, issuer_id, year])


def record_fragments(provider_plan_links):
    """Group one record's valid ``(plan_id, tier, issuer, year)`` links into fragments."""
    plan_ids_by_scope = defaultdict(set)
    for plan_id, network_tier, issuer_id, year in provider_plan_links:
        plan_ids_by_scope[(network_tier, issuer_id, year)].add(plan_id)
    return {
        fragment_key(network_tier, issuer_id, year, plan_ids): {
            "network_tier": network_tier,
            "issuer_id": issuer_id,
            "year": year,
            "plan_ids": tuple(sorted(plan_ids)),
        }
        for (network_tier, issuer_id, year), plan_ids in plan_ids_by_scope.items()
    }


@dataclass(frozen=True)
class NetworkAssignment:
    """Final network keys for every staged fragment and plan link."""

    fragment_rows: tuple  # (fragment, network_key, issuer_id, year)
    tier_rows: tuple  # (plan_id, network_key, network_tier, issuer_id, year)


def assign_networks(tier_links) -> NetworkAssignment:
    """Group plans with identical fragment sets into networks with stable keys.

    ``tier_links`` holds staged ``(plan_id, network_tier, issuer_id, year, fragment)``
    rows. Two plans of one issuer tier share a network exactly when the same
    provider records list them, so their provider sets are equal.
    """
    fragments_by_plan = defaultdict(set)
    fragments_by_scope = defaultdict(set)
    for plan_id, network_tier, issuer_id, year, fragment in tier_links:
        fragments_by_plan[(plan_id, network_tier, issuer_id, year)].add(fragment)
        fragments_by_scope[(network_tier, issuer_id, year)].add(fragment)
    plan_ids_by_network = defaultdict(list)
    for (plan_id, network_tier, issuer_id, year), fragment_set in fragments_by_plan.items():
        plan_ids_by_network[(network_tier, issuer_id, year, frozenset(fragment_set))].append(plan_id)

    fragment_rows = []
    tier_rows = []
    networks_by_key = {}
    for network, plan_ids in sorted(plan_ids_by_network.items(), key=lambda item: (item[0][:3], sorted(item[1]))):
        network_tier, issuer_id, year, fragment_set = network
        plan_ids = sorted(plan_ids)
        if fragment_set == fragments_by_scope[(network_tier, issuer_id, year)]:
            network_key = legacy_network_key(network_tier, issuer_id, year)
        else:
            network_key = return_checksum([network_tier, issuer_id, year, *plan_ids])
        if network_key in networks_by_key:
            raise MRFNetworkIdentityError(
                f"network key {network_key} is shared by {networks_by_key[network_key]} and "
                f"{(network_tier, issuer_id, year, plan_ids[0])}"
            )
        if not _INT32_MIN <= network_key <= _INT32_MAX:
            raise MRFNetworkIdentityError(f"network key {network_key} is outside the published int32 range")
        networks_by_key[network_key] = (network_tier, issuer_id, year, plan_ids[0])
        fragment_rows.extend((fragment, network_key, issuer_id, year) for fragment in sorted(fragment_set))
        tier_rows.extend((plan_id, network_key, network_tier, issuer_id, year) for plan_id in plan_ids)
    return NetworkAssignment(fragment_rows=tuple(fragment_rows), tier_rows=tuple(tier_rows))


def _columns(cls, replaced_by_name):
    """Return target and source column lists with some sources replaced."""
    names = [column.name for column in cls.__table__.columns]
    sources = [replaced_by_name.get(name, f"source.{name}") for name in names]
    return ", ".join(names), ", ".join(sources)


def _rewrite_statements(schema, cls, *, distinct_on, order_by, replaced_by_name):
    """Replace one staged table's fragment rows with one row per final network."""
    table = f"{schema}.{cls.__tablename__}"
    target_columns, source_columns = _columns(cls, replaced_by_name)
    return (
        "DROP TABLE IF EXISTS network_rewrite",
        f"CREATE TEMP TABLE network_rewrite ON COMMIT DROP AS SELECT DISTINCT ON ({distinct_on}) {source_columns} "
        f"FROM {table} source JOIN network_fragment_map fragment_map "
        "ON fragment_map.fragment = source.checksum_network "
        "AND fragment_map.issuer_id = source.issuer_id AND fragment_map.year = source.year "
        f"ORDER BY {distinct_on}, {order_by}",
        f"TRUNCATE {table}",
        f"INSERT INTO {table} ({target_columns}) SELECT {target_columns} FROM network_rewrite",
    )


_UNMAPPED_SQL = (
    "SELECT count(*) FROM {table} source WHERE NOT EXISTS (SELECT 1 FROM network_fragment_map m "
    "WHERE m.fragment = source.checksum_network AND m.issuer_id = source.issuer_id AND m.year = source.year)"
)
_EVIDENCE_IDENTITY = (
    "source.npi, source.type, source.checksum, source.issuer_id, source.year, "
    "fragment_map.network_key, source.import_id, source.source_url"
)


async def _insert_unnest(session, table, typed_columns, rows):
    """Insert rows with one array-bound statement per bounded batch."""
    names = [name for name, _type in typed_columns]
    arrays = ", ".join(f"CAST(:{name} AS {sql_type}[])" for name, sql_type in typed_columns)
    statement = text(f"INSERT INTO {table} ({', '.join(names)}) SELECT * FROM unnest({arrays})")
    for offset in range(0, len(rows), _INSERT_BATCH_ROWS):
        batch = rows[offset : offset + _INSERT_BATCH_ROWS]
        await session.execute(statement, {name: [row[i] for row in batch] for i, name in enumerate(names)})


async def _require_mapped(session, table, label):
    unmapped = (await session.execute(text(_UNMAPPED_SQL.format(table=table)))).scalar()
    if unmapped:
        raise MRFNetworkIdentityError(f"{unmapped} staged {label} rows reference no staged plan link")


_FRAGMENT_MAP_COLUMNS = (
    ("fragment", "bigint"),
    ("network_key", "bigint"),
    ("issuer_id", "integer"),
    ("year", "integer"),
)
_TIER_COLUMNS = (
    ("plan_id", "varchar"),
    ("checksum_network", "bigint"),
    ("network_tier", "varchar"),
    ("issuer_id", "integer"),
    ("year", "integer"),
)
_NETWORK_KEY_SOURCE = "fragment_map.network_key AS checksum_network"


def _provider_and_evidence_rewrites(schema, npi_cls, evidence_cls):
    """Return the provider and address-evidence rewrites, providers first."""
    provider_statements = _rewrite_statements(
        schema,
        npi_cls,
        distinct_on="source.npi, fragment_map.network_key",
        order_by="source.last_updated_on DESC NULLS LAST, source.checksum_network",
        replaced_by_name={"checksum_network": _NETWORK_KEY_SOURCE},
    )
    evidence_statements = _rewrite_statements(
        schema,
        evidence_cls,
        distinct_on=_EVIDENCE_IDENTITY,
        order_by="source.observed_at DESC NULLS LAST, source.evidence_checksum",
        replaced_by_name={
            "checksum_network": _NETWORK_KEY_SOURCE,
            "evidence_checksum": f"hashtextextended(concat_ws(':', {_EVIDENCE_IDENTITY}), 0) AS evidence_checksum",
            "source_record_id": (
                "concat_ws(':', source.npi, source.type, source.checksum, fragment_map.network_key) AS source_record_id"
            ),
        },
    )
    return provider_statements + evidence_statements


async def _staged_assignment(session, tier_table):
    """Assign networks to staged fragments, or return None when already published."""
    tier_links = (
        await session.execute(
            text(f"SELECT DISTINCT plan_id, network_tier, issuer_id, year, checksum_network FROM {tier_table}")
        )
    ).all()
    published_links = sum(1 for link in tier_links if _INT32_MIN <= link[4] <= _INT32_MAX)
    if tier_links and published_links == len(tier_links):
        # A retried finalization already rewrote this import; reassigning would change keys.
        return None, {"networks": len({link[4] for link in tier_links}), "plan_links": len(tier_links), "skipped": True}
    if published_links:
        raise MRFNetworkIdentityError(f"{published_links} staged plan links already carry published network keys")
    return assign_networks(tuple(tuple(link) for link in tier_links)), None


async def publish_network_identities(db, schema, *, npi_cls, tier_cls, evidence_cls) -> dict:
    """Rewrite staged fragment rows to final network keys in one transaction.

    Every staged provider and address-evidence row must reference a staged plan
    link; otherwise nothing is rewritten. Returns network, plan-link and provider
    row counts, or ``skipped`` when a retried finalization already published keys.
    """
    tier_table = f"{schema}.{tier_cls.__tablename__}"
    async with db.transaction() as session:
        assignment, skipped_summary = await _staged_assignment(session, tier_table)
        if assignment is None:
            return skipped_summary
        await session.execute(
            text(
                "CREATE TEMP TABLE network_fragment_map (fragment bigint NOT NULL, network_key bigint NOT NULL, "
                "issuer_id integer NOT NULL, year integer NOT NULL, PRIMARY KEY (fragment, network_key)) ON COMMIT DROP"
            )
        )
        await _insert_unnest(session, "network_fragment_map", _FRAGMENT_MAP_COLUMNS, assignment.fragment_rows)
        await _require_mapped(session, f"{schema}.{npi_cls.__tablename__}", "provider")
        await _require_mapped(session, f"{schema}.{evidence_cls.__tablename__}", "address evidence")
        await session.execute(text(f"TRUNCATE {tier_table}"))
        await _insert_unnest(session, tier_table, _TIER_COLUMNS, assignment.tier_rows)
        for statement in _provider_and_evidence_rewrites(schema, npi_cls, evidence_cls):
            await session.execute(text(statement))
        provider_rows = (await session.execute(text(f"SELECT count(*) FROM {schema}.{npi_cls.__tablename__}"))).scalar()
    return {
        "networks": len({fragment_row[1] for fragment_row in assignment.fragment_rows}),
        "plan_links": len(assignment.tier_rows),
        "provider_rows": provider_rows,
    }
