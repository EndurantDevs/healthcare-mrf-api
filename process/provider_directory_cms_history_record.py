# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compare protected history data; dictionaries and hashes confer no writer authority."""

import re

from process import provider_directory_cms_history_conversion as history

ALLOCATION_CONTRACT = "cms-prepared-snapshot-allocation-data-v4"


def _require(condition, code):
    if not condition:
        raise ValueError("cms_history_record_" + code)


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


def require_cms_history_record(record, *, storage, parents):
    """Validate complete history content against its declared conversion names."""
    _require(type(record) is dict and set(record) == {"request", "dataset_rows", "content"}, "fields_changed")
    plan = history.plan_cms_history_conversion(record["request"])
    request = plan["request"]
    schema = storage["catalog_schema"] if type(storage) is dict else storage.catalog_schema
    _require(request["catalog_schema"] == request["future_schema"] == schema, "coordinates_changed")
    _require(type(parents) is dict and set(parents) == set(history.ROOTS), "parents_required")
    for root in history.ROOTS:
        planned, declared = request["heaps"][root]["parent"], parents[root]
        _require({key: planned[key] for key in ("schema", "name")} == declared, "parent_changed")
    _require(type(record["dataset_rows"]) is dict and type(record["content"]) is dict, "content_required")
    _content(record, request)
    return plan
