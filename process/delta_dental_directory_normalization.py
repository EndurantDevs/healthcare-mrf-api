# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Stage explicit positive Delta office memberships from one retained capture lineage."""

import hashlib
import json
import re
import sqlite3
from collections import Counter
from contextlib import closing, contextmanager
from dataclasses import dataclass

from process.delta_dental_directory_capture import MAX_PAGE_BYTES, NETWORK_LABELS, RESULT_CAP, FinderSearch
from process.delta_dental_directory_json import SOURCE_ID, _office_card, canonical_office_zip

_PLAN_NET = "http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/"
_SOURCE = f"urn:{SOURCE_ID}:"
_MAX_PLAN_BYTES = 32 * 1024 * 1024
_MAX_WINDOWS = 200_000
_MAX_CUMULATIVE_WINDOWS = 250_000
MAX_CUMULATIVE_LINEAGES = 16
_COUNT_FIELDS = (
    "observed_windows",
    "unobserved_windows",
    "capped_windows",
    "empty_windows",
    "publisher_rows",
    "windows_with_retained_gap",
    "unobserved_with_request_intent",
    "unobserved_without_retained_attempt",
    "unobserved_attempt_status_unknown",
    "unresolved_rows",
    "malformed_product_rows",
    "unreviewed_product_observations",
    "deduplicated_memberships",
)
_NETWORK_IDS = {
    "Delta Dental PPO": "ppo",
    "Delta Dental Premier": "premier",
    "Delta Dental Medicare Advantage": "medicare-advantage",
    "DeltaCare USA": "deltacare-usa",
    "Delta Dental Patient Direct": "patient-direct",
}
_STAGE = """
CREATE TEMP TABLE windows (
    selector TEXT, zip TEXT, radius REAL, PRIMARY KEY(selector, zip, radius)
) WITHOUT ROWID;
CREATE TEMP TABLE office (id TEXT PRIMARY KEY, office_number TEXT, address TEXT) WITHOUT ROWID;
CREATE TEMP TABLE relation (id TEXT PRIMARY KEY, npi TEXT, office_id TEXT) WITHOUT ROWID;
CREATE TEMP TABLE membership (id TEXT, product TEXT, PRIMARY KEY(id, product)) WITHOUT ROWID;
CREATE TEMP VIEW positive AS
SELECT relation.id, npi, office_id, office_number, address, membership.product
FROM relation JOIN office ON office_id = office.id JOIN membership ON relation.id = membership.id
WHERE npi IS NOT NULL AND address IS NOT NULL;
"""


def _unique_json(pairs):
    """Refuse duplicate wire fields before relying on their identity or product claims."""
    values_by_field = dict(pairs)
    if len(values_by_field) != len(pairs):
        raise ValueError("delta_retained_json_duplicate_field")
    return values_by_field


def _load_plan(connection, lineage, edition):
    """Select only the exact protected lineage, even when editions share cached pages."""
    definition = connection.execute(
        "SELECT definition FROM finder_plan WHERE id=? AND length(definition)<=?", (lineage, _MAX_PLAN_BYTES)
    ).fetchone()
    if definition is None:
        raise ValueError("delta_retained_lineage_missing")
    plan_edition, windows = json.loads(definition[0])
    if plan_edition != edition or type(windows) is not list or not 0 < len(windows) <= _MAX_WINDOWS:
        raise ValueError("delta_retained_edition_or_plan_invalid")
    searches = (FinderSearch(*window) for window in windows)
    connection.executemany(
        "INSERT OR IGNORE INTO windows VALUES (?, ?, ?)",
        ((search.selector, search.zip_code, search.radius_miles) for search in searches),
    )
    return definition[0].encode()


def _load_cumulative_plans(connection, lineages, edition, digest):
    """Index the exact retained cohort and visit each overlapping window once."""
    if connection.execute(
        "SELECT 1 FROM finder_plan WHERE json_extract(definition, '$[0]') IS NOT ? LIMIT 1", (edition,)
    ).fetchone():
        raise ValueError("delta_retained_edition_or_plan_invalid")
    for lineage in lineages:
        definition = _load_plan(connection, lineage, edition)
        digest.update(len(definition).to_bytes(8, "big"))
        digest.update(definition)
    planned_windows = connection.execute("SELECT count(*) FROM windows").fetchone()[0]
    if not 0 < planned_windows <= _MAX_CUMULATIVE_WINDOWS:
        raise ValueError("delta_retained_edition_or_plan_invalid")
    return planned_windows


def _structured_address(record):
    """Preserve publisher street and unit as independent structured address lines."""
    location = record["serviceOffice"]["location"]
    mapping = (
        ("streetAddress", "first"),
        ("streetAddressLine2", "second"),
        ("city", "city"),
        ("stateType", "state"),
        ("zipCode", "postalCode"),
    )
    address_by_field = {}
    for source_field, field in mapping:
        raw = location.get(source_field)
        if field == "second" and (raw is None or isinstance(raw, str) and not raw.strip()):
            continue
        if not isinstance(raw, str) or not raw.strip() or len(raw) > 512 or not raw.isprintable():
            return None
        address_by_field[field] = raw.strip()
    postal = canonical_office_zip(address_by_field["postalCode"])
    if postal is None:
        return None
    address_by_field["postalCode"] = postal
    lines = [address_by_field.pop("first")]
    if unit := address_by_field.pop("second", None):
        lines.append(unit)
    return json.dumps({"line": lines, **address_by_field, "country": "US"}, separators=(",", ":"))


def _stage_record(connection, provider_office, products, counts):
    """Admit exact relation identities and discard unresolved or unreviewed rows."""
    card, gaps = _office_card(provider_office)
    if not card or not all((card.address, card.office_id, card.provider_office_id, card.practitioner_npi)):
        counts["unresolved_rows"] += 1
        return
    address = _structured_address(provider_office)
    if (
        address is None
        or max(len(card.office_id), len(card.provider_office_id)) > 32
        or re.fullmatch(r"[12][0-9]{9}", card.practitioner_npi) is None
    ):
        counts["unresolved_rows"] += 1
        return
    office_identity = json.dumps((card.office_id, address), separators=(",", ":"))
    office_id = hashlib.sha256(office_identity.encode()).hexdigest()[:48]
    connection.execute(
        """INSERT INTO office VALUES (?, ?, ?) ON CONFLICT(id) DO UPDATE SET address=NULL
        WHERE office.office_number IS NOT excluded.office_number OR office.address IS NOT excluded.address""",
        (office_id, card.office_id, address),
    )
    connection.execute(
        """INSERT INTO relation VALUES (?, ?, ?) ON CONFLICT(id) DO UPDATE SET npi=NULL
        WHERE npi IS NOT excluded.npi OR office_id IS NOT excluded.office_id""",
        (card.provider_office_id, card.practitioner_npi, office_id),
    )
    if gaps:
        counts["malformed_product_rows"] += 1
        return
    labels = {product["productType"] for product in provider_office["products"]}
    counts["unreviewed_product_observations"] += len(labels - products)
    for label in labels & products:
        inserted = connection.execute(
            "INSERT OR IGNORE INTO membership VALUES (?, ?)", (card.provider_office_id, _NETWORK_IDS[label])
        )
        counts["deduplicated_memberships"] += int(inserted.rowcount == 0)


def _stage_response(connection, body, products, counts):
    """Index a bounded JSON response and mark actual empty or capped windows."""
    if not isinstance(body, bytes):
        raise ValueError("delta_retained_response_bounds_invalid")
    envelope = json.loads(body, object_pairs_hook=_unique_json)
    rows = envelope.get("providerResults") if type(envelope) is dict else None
    if type(rows) is not list or len(rows) > RESULT_CAP:
        raise ValueError("delta_retained_response_envelope_invalid")
    counts["observed_windows"] += 1
    counts["capped_windows"] += int(len(rows) == RESULT_CAP)
    counts["empty_windows"] += int(not rows)
    counts["publisher_rows"] += len(rows)
    for record in rows:
        _stage_record(connection, record, products, counts)


def _stage_windows(connection, products, counts, digest, stop_requested):
    """Process ordered selected windows with explicit transport gaps and identity conflicts."""
    has_attempt_ledger = (
        connection.execute("SELECT 1 FROM main.sqlite_schema WHERE type='table' AND name='finder_attempt'").fetchone()
        is not None
    )
    attempt_column = (
        """EXISTS (SELECT 1 FROM finder_attempt a WHERE a.selector=windows.selector
           AND a.zip_code=windows.zip AND a.radius=windows.radius AND a.page=1)"""
        if has_attempt_ledger
        else "NULL"
    )
    query = f"""
        SELECT windows.selector, zip, windows.radius, page, reason, {attempt_column},
               CASE WHEN length(body) BETWEEN 1 AND ? THEN body ELSE NULL END
        FROM windows
        LEFT JOIN finder_page
          ON windows.selector=finder_page.selector AND zip=finder_page.zip_code AND windows.radius=finder_page.radius
        LEFT JOIN finder_gap
          ON windows.selector=finder_gap.selector AND zip=finder_gap.zip_code AND windows.radius=finder_gap.radius
        ORDER BY windows.selector, zip, windows.radius, page
    """
    for selector, zip_code, radius, page, gap, has_intent, body in connection.execute(query, (MAX_PAGE_BYTES,)):
        if stop_requested is not None and stop_requested():
            raise RuntimeError("delta_observed_capture_interrupted")
        digest.update(json.dumps([selector, zip_code, radius, page, gap, has_intent]).encode())
        if page is None:
            counts["unobserved_windows"] += 1
            if has_intent:
                counts["unobserved_with_request_intent"] += 1
            elif gap is None:
                disposition = (
                    "unobserved_attempt_status_unknown" if has_intent is None else "unobserved_without_retained_attempt"
                )
                counts[disposition] += 1
        elif page == 1:
            _stage_response(connection, body, products, counts)
            digest.update(hashlib.sha256(body).digest())
        else:
            raise ValueError("delta_retained_legacy_pagination_unsupported")
        counts["windows_with_retained_gap"] += int(gap is not None)


def _resources(connection):
    """Yield the same sorted normalized resource protocol as ordinary source producers."""
    for office_id, office_number, address in connection.execute(
        "SELECT DISTINCT office_id, office_number, address FROM positive ORDER BY office_id"
    ):
        yield {
            "resourceType": "Location",
            "id": f"delta-office-{office_id}",
            "identifier": [{"system": _SOURCE + "office-id", "value": office_id}],
            "extension": [{"url": _SOURCE + "publisher-office-number", "valueString": office_number}],
            "address": json.loads(address),
        }
    for (product,) in connection.execute("SELECT DISTINCT product FROM positive ORDER BY product"):
        yield {
            "resourceType": "Organization",
            "id": f"delta-network-{product}",
            "meta": {"profile": [_PLAN_NET + "plannet-Network"]},
            "identifier": [{"system": _SOURCE + "product", "value": product}],
            "type": [
                {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/organization-type", "code": "ntwk"}]}
            ],
            "name": _PRODUCT_LABELS[product],
        }
    for (npi,) in connection.execute("SELECT DISTINCT npi FROM positive ORDER BY npi"):
        yield {
            "resourceType": "Practitioner",
            "id": f"delta-npi-{npi}",
            "identifier": [{"system": "http://hl7.org/fhir/sid/us-npi", "value": npi}],
        }
    for relation_id, npi, office_id, products in connection.execute(
        "SELECT id, npi, office_id, group_concat(product) FROM positive GROUP BY id, npi, office_id ORDER BY id"
    ):
        yield {
            "resourceType": "PractitionerRole",
            "id": f"delta-role-{relation_id}",
            "identifier": [{"system": _SOURCE + "provider-office-id", "value": relation_id}],
            "practitioner": {"reference": f"Practitioner/delta-npi-{npi}"},
            "location": [{"reference": f"Location/delta-office-{office_id}"}],
            "extension": [
                {
                    "url": _PLAN_NET + "network-reference",
                    "valueReference": {"reference": f"Organization/delta-network-{code}"},
                }
                for code in sorted(products.split(","))
            ],
        }


_PRODUCT_LABELS = {code: label for label, code in _NETWORK_IDS.items()}


@dataclass(frozen=True)
class DeltaObservedCapture:
    """A bounded source-stage resource stream with a positive-only receipt."""

    resource_counts: dict
    proof: dict
    _connection: sqlite3.Connection

    def iter_resources(self):
        """Stream ordered normalized resources before the capture scope closes."""
        return _resources(self._connection)


@contextmanager
def normalized_delta_capture(database, *, lineage, edition, reviewed_products, stop_requested=None):
    """Read one exact retained JSON lineage, holding only its disk-backed staging index."""
    lineages = (lineage,) if isinstance(lineage, str) else lineage
    if (
        not isinstance(edition, str)
        or not re.fullmatch(r"[a-f0-9]{64}", edition)
        or database.name != f"finder-{edition}.sqlite3"
        or type(lineages) is not tuple
        or not 0 < len(lineages) <= MAX_CUMULATIVE_LINEAGES
        or any(not isinstance(run, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", run) for run in lineages)
        or len(set(lineages)) != len(lineages)
        or type(reviewed_products) is not frozenset
        or not reviewed_products
        or not reviewed_products <= NETWORK_LABELS
    ):
        raise ValueError("delta_retained_publication_scope_invalid")
    with closing(
        sqlite3.connect(f"{database.resolve().as_uri()}?mode=ro", uri=True, check_same_thread=False)
    ) as connection:
        if stop_requested is not None:
            connection.set_progress_handler(lambda: int(stop_requested()), 10_000)
        connection.execute("PRAGMA temp_store=FILE")
        connection.executescript(_STAGE)
        connection.execute("BEGIN")
        digest = hashlib.sha256(json.dumps([edition, lineages, sorted(reviewed_products)]).encode())
        window_count = _load_cumulative_plans(connection, lineages, edition, digest)
        counts = Counter(dict.fromkeys(_COUNT_FIELDS, 0))
        _stage_windows(connection, reviewed_products, counts, digest, stop_requested)
        connection.commit()
        counts_by_family = dict(
            zip(
                ("Location", "Organization", "Practitioner", "PractitionerRole"),
                connection.execute(
                    "SELECT count(DISTINCT office_id), count(DISTINCT product), count(DISTINCT npi), count(DISTINCT id) FROM positive"
                ).fetchone(),
                strict=True,
            )
        )
        positive_count = connection.execute("SELECT count(*) FROM positive").fetchone()[0]
        indexed_count = connection.execute("SELECT count(*) FROM membership").fetchone()[0]
        proof_by_field = {
            "adapter_id": SOURCE_ID,
            "capture_sha256": digest.hexdigest(),
            "directory_complete": False,
            "normalized_output_only": True,
            "positive_observed_only": True,
            "planned_windows": window_count,
            **counts,
            "conflicting_memberships": indexed_count - positive_count,
            "positive_memberships": positive_count,
            "resource_counts": counts_by_family,
        }
        yield DeltaObservedCapture(counts_by_family, proof_by_field, connection)
