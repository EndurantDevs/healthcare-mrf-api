# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compare protected history data; dictionaries and hashes confer no writer authority."""

import re

from process import provider_directory_cms_history_conversion as history

ALLOCATION_CONTRACT = "cms-prepared-snapshot-allocation-data-v4"


def _require(condition, code):
    if not condition:
        raise ValueError("cms_history_record_" + code)


def _transaction(value):
    _require(
        type(value) is str and re.fullmatch(r"[1-9][0-9]{0,19}", value) is not None and int(value) < 2**64,
        "transaction_required",
    )


def _content(record, request):
    roots = set(history.ROOTS)
    _require(set(record["dataset_rows"]) == set(record["content"]) == roots, "roots_changed")
    for root in history.ROOTS:
        counts, content = record["dataset_rows"][root], record["content"][root]
        _require(
            type(counts) is dict
            and set(counts) == set(request["heaps"][root]["dataset_ids"])
            and all(type(count) is int and 0 <= count < 2**63 for count in counts.values()),
            "bounds_changed",
        )
        _require(
            type(content) is dict
            and set(content) == {"rows", "bytes", "sha256"}
            and all(type(content[key]) is int and 0 <= content[key] < 2**63 for key in ("rows", "bytes"))
            and content["rows"] == sum(counts.values())
            and type(content["sha256"]) is str
            and re.fullmatch(r"[0-9a-f]{64}", content["sha256"]) is not None,
            "content_changed",
        )


def _parents(request, parents):
    _require(type(parents) is dict and set(parents) == set(history.ROOTS), "parents_required")
    for root in history.ROOTS:
        planned, protected = request["heaps"][root]["parent"], parents[root]
        _require(
            planned["oid"] == protected["oid"]
            and planned["name"] == protected["relname"]
            and planned["schema_oid"] == protected["native"]["namespace"][0]
            and planned["owner_oid"] == protected["relowner"]
            and set(planned["indexes"].values()) == {entry["class"][0] for entry in protected["native"]["indexes"]},
            "parent_changed",
        )


def require_cms_history_record(record, *, storage, parents):
    """Validate an existing protected field's exact data shape and fixed plan coordinates.

    The issuer must authenticate Source/profile, the native identities and full
    history scans under continuous common custody. This comparison is not that
    issuer, a fence, or permission to convert; the original transaction is checked
    again by the consuming finisher.
    """
    _require(
        type(record) is dict and set(record) == {"transaction", "request", "dataset_rows", "content"}, "fields_changed"
    )
    _transaction(record["transaction"])
    plan = history.plan_cms_history_conversion(record["request"])
    request = plan["request"]
    _require(
        all(request[key] == storage[key] for key in ("database_oid", "catalog_schema", "catalog_schema_oid"))
        and request["owner_oid"] == storage["namespace_owner_oid"]
        and request["future_schema"] == storage["catalog_schema"]
        and request["future_schema_oid"] == storage["catalog_schema_oid"],
        "coordinates_changed",
    )
    _parents(request, parents)
    _require(type(record["dataset_rows"]) is dict and type(record["content"]) is dict, "content_required")
    _content(record, request)
    return plan
