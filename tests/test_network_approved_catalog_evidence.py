# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure approved-map and native boundary checks; PostgreSQL proof is separate."""

import asyncio
import copy
import json

import pytest

from process import network_approved_catalog_evidence as export
from process import registry_record_store as store
from process.network_approved_membership_source import ApprovedMembershipSource

SOURCE = ApprovedMembershipSource(4, "a" * 64, 7)


def evidence(**changes):
    return {"network_id": 1, "expected_record_revision": 2, "pricing_refs": [], "benefit_refs": None, **changes}


def row(network_id=1, revision=3, value=None, present=False):
    return {"network_id": network_id, "record_revision": revision, "evidence_present": present, "evidence": value}


class NativeBoundary:
    """Interface-only stub; real syntax is checked in a separate native receipt."""

    def __init__(self):
        self.calls = []

    def parse_registry_network_evidence(self, raw):
        self.calls.append(raw)
        return json.dumps(json.loads(raw), sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


class Connection:
    def __init__(self, rows=None):
        self.rows = [row()] if rows is None else rows
        self.calls = []
        self.transaction = True
        self.isolation = "repeatable read"
        self.read_only = "on"
        self.generation = SOURCE.generation_id
        self.total_members = SOURCE.total_rows
        self.total_networks = len(self.rows)
        self.override = {}
        self.failure = None
        self.stream_failure = None
        self.stream_failure_after = 0
        self.stream_visits = 0
        self.stream_rows = None

    def is_in_transaction(self):
        return self.transaction

    async def fetchrow(self, sql, *parameters):
        self.calls.append((sql, parameters))
        assert "registry_revision_control" not in sql
        assert parameters[0] == SOURCE.approved_revision
        if "AS generation_id" in sql:
            assert "SELECT $1::bigint AS approved_revision,1 AS id" in sql
            return {
                "isolation": self.isolation,
                "approved_revision": SOURCE.approved_revision,
                "is_bounded": True,
                "invalid_documents": 0,
                "unresolved_rows": 0,
                "generation_id": self.generation,
                "total_rows": self.total_members,
            }
        if self.failure is not None:
            raise self.failure
        assert "registry_approved_record" in sql and "record_kind='network'" in sql
        assert parameters[4:] == (32768, 1048576, 4096, 16)
        references = sum(
            len(entry["evidence"].get(field) or [])
            for entry in self.rows
            if isinstance(entry["evidence"], dict)
            for field in ("pricing_refs", "benefit_refs")
        )
        return {
            "isolation": self.isolation,
            "read_only": self.read_only,
            "invalid_networks": 0,
            "total_networks": self.total_networks,
            "row_count": len(self.rows),
            "reference_count": references,
            "bounded": True,
            "rows_json": json.dumps(self.rows),
            **self.override,
        }

    def cursor(self, sql, *parameters, prefetch):
        self.calls.append((sql, parameters))
        assert prefetch == 16 and parameters[4] == 32768
        assert "jsonb_agg" not in sql and "ORDER BY network_id" in sql

        async def stream():
            for selected in self.rows if self.stream_rows is None else self.stream_rows:
                if self.stream_failure is not None and self.stream_visits == self.stream_failure_after:
                    raise self.stream_failure
                self.stream_visits += 1
                encoded = json.dumps(selected, ensure_ascii=False)
                is_stored_bounded = len(json.dumps(selected["evidence"], ensure_ascii=False).encode()) <= 32768
                yield {
                    "stored_bounded": is_stored_bounded,
                    "row_bytes": len(encoded.encode()) + 2 if is_stored_bounded else None,
                    "row_json": encoded if is_stored_bounded else None,
                }
            if self.stream_failure is not None and self.stream_visits == self.stream_failure_after:
                raise self.stream_failure

        return stream()


@pytest.fixture
def native_boundary(monkeypatch):
    boundary = NativeBoundary()
    monkeypatch.setattr(store, "_fast_module", lambda: boundary)
    return boundary


async def read(connection, **options):
    return await export.read_approved_network_catalog_evidence(
        connection, SOURCE, **{"control_schema": "sample_control", **options}
    )


@pytest.mark.asyncio
async def test_retained_author_is_distinct_from_immutable_controller_base_without_mutation(native_boundary):
    selected = row(value=evidence(), present=True)
    connection = Connection([selected])
    before = copy.deepcopy(selected)
    result = await read(connection)
    assert len(connection.calls) == 2
    assert result.approved_revision == 4 and result.approved_map_sha256 == "a" * 64
    assert result.records[0].authored_record_revision == 2
    assert result.records[0].approved_record_revision == 3
    assert result.records[0].evidence_origin == "retained"
    assert json.loads(result.request_bytes) == {"networks": [evidence(expected_record_revision=3)]}
    assert selected == before and len(native_boundary.calls) == 1


@pytest.mark.asyncio
async def test_absent_null_reviewed_none_and_empty_page_remain_distinct(native_boundary):
    connection = Connection(
        [
            row(),
            row(2, present=True),
            row(3, value=evidence(network_id=3, pricing_refs=[], benefit_refs=[]), present=True),
        ]
    )
    result = await read(connection)
    assert [entry.evidence_origin for entry in result.records] == ["absent", "null", "retained"]
    documents = json.loads(result.request_bytes)["networks"]
    assert documents[0]["pricing_refs"] is None and documents[1]["benefit_refs"] is None
    assert documents[2]["pricing_refs"] == documents[2]["benefit_refs"] == []
    assert len(native_boundary.calls) == 3
    empty = await read(Connection([]), offset=20)
    assert empty.records == () and empty.request_bytes is empty.request_sha256 is None


@pytest.mark.asyncio
async def test_explicit_selection_is_complete_sorted_and_never_truncated(native_boundary):
    connection = Connection([row(2), row(9)])
    result = await read(connection, network_ids=(2, 9))
    assert [entry.network_id for entry in result.records] == [2, 9]
    assert connection.calls[1][1][1] == [2, 9]
    with pytest.raises(export.ApprovedNetworkCatalogEvidenceError):
        await read(Connection([row(2)]), network_ids=(2, 9))


@pytest.mark.asyncio
async def test_one_and_five_thousand_networks_use_constant_native_set_statements(native_boundary):
    counts = []
    for size in (1, 5000):
        connection = Connection([row(index) for index in range(1, size + 1)])
        result = await read(connection)
        counts.append(len(connection.calls))
        assert len(result.records) == size and len(result.request_bytes) <= 1048576
    assert counts == [2, 2]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"limit": 0},
        {"limit": True},
        {"limit": 5001},
        {"offset": -1},
        {"offset": 2**63},
        {"network_ids": []},
        {"network_ids": ()},
        {"network_ids": (True,)},
        {"network_ids": (2, 1)},
        {"network_ids": (1, 1)},
        {"network_ids": (2**31,)},
        {"network_ids": (1, 2), "limit": 1},
        {"network_ids": (1,), "offset": 1},
        {"control_schema": 'invalid"schema'},
    ],
)
async def test_invalid_selection_refuses_before_query(native_boundary, changes):
    connection = Connection()
    with pytest.raises(export.ApprovedNetworkCatalogEvidenceError):
        await read(connection, **changes)
    assert connection.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "attribute,value",
    [("transaction", False), ("isolation", "read committed"), ("generation", "b" * 64), ("total_members", 8)],
)
async def test_actual_source_scope_requires_stable_transaction_and_complete_pin(native_boundary, attribute, value):
    connection = Connection()
    setattr(connection, attribute, value)
    with pytest.raises(export.ApprovedNetworkCatalogEvidenceError):
        await read(connection)
    assert len(connection.calls) <= 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "selected",
    [
        row(True),
        row(2**31),
        row(value=evidence(network_id=2), present=True),
        row(value=evidence(expected_record_revision=3), present=True),
        row(value=evidence(expected_record_revision=True), present=True),
        row(value=evidence(), present=False),
        row(value={**evidence(), "extra": None}, present=True),
    ],
)
async def test_malformed_retained_identity_base_or_document_refuses(native_boundary, selected):
    with pytest.raises(export.ApprovedNetworkCatalogEvidenceError) as failure:
        await read(Connection([selected]))
    assert str(failure.value) == "registry_approved_catalog_evidence_unavailable"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"bounded": False},
        {"invalid_networks": 1},
        {"invalid_networks": False},
        {"read_only": "off"},
        {"isolation": "read committed"},
        {"row_count": True},
        {"row_count": 5001},
        {"reference_count": 4097},
        {"reference_count": 1},
        {"rows_json": "x" * 1048577},
        {"rows_json": '[{"network_id":1,"network_id":2}]'},
    ],
)
async def test_preallocation_and_aggregate_failure_is_value_free(native_boundary, changes):
    connection = Connection()
    connection.override.update(changes)
    with pytest.raises(export.ApprovedNetworkCatalogEvidenceError):
        await read(connection)
    assert native_boundary.calls == [] or changes in ({"reference_count": 1}, {"bounded": False})


@pytest.mark.asyncio
async def test_missing_parser_refuses_even_unresolved_exports(monkeypatch):
    monkeypatch.setattr(store, "_fast_module", lambda: None)
    with pytest.raises(export.ApprovedNetworkCatalogEvidenceError):
        await read(Connection())


@pytest.mark.asyncio
async def test_cancellation_propagates_without_transaction_or_connection_ownership(native_boundary):
    connection = Connection()
    connection.failure = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await read(connection)
    assert connection.transaction and len(connection.calls) == 2 and native_boundary.calls == []


def test_sql_plan_bounds_native_payload_before_aggregate_output():
    sql = export._PAGE_SQL.format(namespace='"sample_control"')
    assert "FROM bounded" in sql and "registry_revision_control" not in sql
    assert "cardinality($2::integer[])" in sql and "record_revision::text" in sql
    assert "record_key::numeric<=2147483647" in sql


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ids,total,offset,limit",
    [
        ([1], 2, 0, 2),
        ([], 2, 0, 2),
        ([1, 2], 2, 0, 1),
        ([2], 2, 2, 1),
        ([], 3, 2, 2),
        ([2, 3], 3, 2, 2),
    ],
)
async def test_unselected_page_cardinality_refuses_before_reference_parsing(native_boundary, ids, total, offset, limit):
    connection = Connection([row(network_id) for network_id in ids])
    connection.total_networks = total
    with pytest.raises(export.ApprovedNetworkCatalogEvidenceError):
        await read(connection, offset=offset, limit=limit)
    assert len(connection.calls) == 2
    assert native_boundary.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ids,total,offset,limit",
    [([], 0, 0, 2), ([], 2, 2, 2), ([3], 3, 2, 2), ([2, 3], 5, 1, 2), ([1, 2], 5, 0, 2)],
)
async def test_exact_unselected_pages_preserve_source_and_request(native_boundary, ids, total, offset, limit):
    connection = Connection([row(network_id) for network_id in ids])
    connection.total_networks = total
    result = await read(connection, offset=offset, limit=limit)
    assert result.source is SOURCE and result.offset == offset and result.total_networks == total
    assert [record.network_id for record in result.records] == ids
    assert len(connection.calls) == 2 and len(native_boundary.calls) == len(ids)
    assert connection.calls[1][1][2:4] == (offset, limit)
    if ids:
        assert [entry["network_id"] for entry in json.loads(result.request_bytes)["networks"]] == ids
    else:
        assert result.request_bytes is result.request_sha256 is None


def oversized_rows(kind="references"):
    count, bindings = (257, 16) if kind == "references" else (300, 8)
    reference_by_field = {
        "healthporta_plan_id": "hpplan_" + "1" * 26,
        "plan_release_id": "hprelease_" + "2" * 26,
        "serving_revision_id": "hpserve_" + "3" * 26,
        "role": "in_network",
        "snapshot_id": "é" * 96,
    }
    return [
        row(
            identity,
            value=evidence(
                network_id=identity,
                pricing_refs=[{**reference_by_field, "ordinal": ordinal} for ordinal in range(bindings)],
            ),
            present=True,
        )
        for identity in range(1, count + 1)
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["references", "bytes"])
async def test_valid_oversize_refuses_only_after_complete_native_eof(native_boundary, kind):
    connection = Connection(oversized_rows(kind))
    connection.override.update(bounded=False, rows_json="[]")
    with pytest.raises(export.ApprovedNetworkCatalogEvidenceResourceLimit):
        await read(connection)
    assert len(connection.calls) == 3 and connection.stream_visits == len(connection.rows)
    assert len(native_boundary.calls) == len(connection.rows)
    if kind == "bytes":
        assert sum(len(entry["evidence"]["pricing_refs"]) for entry in connection.rows) < 4096


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "corruption", ["late_malformed", "late_base", "missing", "extra", "wrong_identity", "wrong_count"]
)
async def test_oversize_complete_identity_and_grammar_are_required(native_boundary, corruption):
    connection = Connection(oversized_rows())
    connection.override.update(bounded=False, rows_json="[]")
    if corruption == "late_malformed":
        connection.rows[-1]["evidence"]["unknown"] = None
    if corruption == "late_base":
        connection.rows[-1]["evidence"]["expected_record_revision"] = 3
    if corruption == "missing":
        connection.stream_rows = connection.rows[:-1]
    if corruption == "extra":
        connection.stream_rows = [*connection.rows, row(258)]
    if corruption == "wrong_identity":
        connection.rows[-1]["network_id"] = 256
    if corruption == "wrong_count":
        connection.override["reference_count"] = 4097
    with pytest.raises(export.ApprovedNetworkCatalogEvidenceError) as caught:
        await read(connection)
    assert type(caught.value) is export.ApprovedNetworkCatalogEvidenceError


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [asyncio.CancelledError(), RuntimeError("stream"), ValueError("stream")])
async def test_oversize_stream_failure_never_becomes_optional(native_boundary, failure):
    connection = Connection(oversized_rows())
    connection.override.update(bounded=False, rows_json="[]")
    connection.stream_failure, connection.stream_failure_after = failure, 250
    expected = export.ApprovedNetworkCatalogEvidenceError if isinstance(failure, ValueError) else type(failure)
    with pytest.raises(expected) as caught:
        await read(connection)
    if expected is not export.ApprovedNetworkCatalogEvidenceError:
        assert caught.value is failure
    assert connection.stream_visits == 250 and connection.transaction


def test_refused_cursor_bounds_documents_before_returning_rows():
    sql = export._REFUSED_PAGE_SQL.format(namespace='"sample_control"')
    assert "jsonb_agg" not in sql and "CASE WHEN stored_bounded THEN" in sql
    assert "octet_length(evidence::text)<=$5" in sql
    assert "ORDER BY network_id" in sql and "LIMIT $4" in sql


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["valid", "late_malformed", "cancelled"])
async def test_actual_optional_report_consumes_only_resource_refusal(native_boundary, outcome):
    from process import registry_pricing_release_binding_lookup as lookup
    from tests.test_registry_pricing_release_binding_lookup import REPORT

    connection = Connection(oversized_rows())
    connection.override.update(bounded=False, rows_json="[]")
    report = copy.deepcopy(REPORT)
    report["targets"] = [{**REPORT["targets"][0], "network_id": identity} for identity in range(1, 258)]
    if outcome == "late_malformed":
        connection.rows[-1]["evidence"]["unknown"] = None
    if outcome == "cancelled":
        connection.stream_failure = asyncio.CancelledError()
        connection.stream_failure_after = 250
    if outcome == "valid":
        extended_report = await lookup.append_pricing_binding_metadata(
            connection,
            report,
            SOURCE,
            control_schema="sample_control",
            max_report_bytes=16 * 1024 * 1024,
        )
        assert {key: extended_report[key] for key in report if key != "provenance"} == {
            key: report[key] for key in report if key != "provenance"
        }
        assert extended_report["provenance"]["pricing_binding_metadata"]["status"] == "unavailable"
        assert len(connection.calls) == 3 and connection.stream_visits == 257
    else:
        failure_type = asyncio.CancelledError if outcome == "cancelled" else export.ApprovedNetworkCatalogEvidenceError
        with pytest.raises(failure_type) as caught:
            await lookup.append_pricing_binding_metadata(
                connection,
                report,
                SOURCE,
                control_schema="sample_control",
                max_report_bytes=16 * 1024 * 1024,
            )
        assert type(caught.value) is failure_type
        if outcome == "cancelled":
            assert caught.value is connection.stream_failure


@pytest.mark.asyncio
async def test_oversize_single_document_stays_malformed_before_parser(native_boundary):
    connection = Connection(oversized_rows())
    connection.override.update(bounded=False, rows_json="[]")
    connection.rows[0]["evidence"]["unknown"] = "x" * 32769
    with pytest.raises(export.ApprovedNetworkCatalogEvidenceError) as caught:
        await read(connection)
    assert type(caught.value) is export.ApprovedNetworkCatalogEvidenceError
    assert native_boundary.calls == [] and connection.stream_visits == 1


@pytest.mark.asyncio
async def test_oversize_missing_native_parser_is_not_resource_only(monkeypatch):
    monkeypatch.setattr(store, "_fast_module", lambda: None)
    connection = Connection(oversized_rows())
    connection.override.update(bounded=False, rows_json="[]")
    with pytest.raises(export.ApprovedNetworkCatalogEvidenceError) as caught:
        await read(connection)
    assert type(caught.value) is export.ApprovedNetworkCatalogEvidenceError
    assert connection.stream_visits == 1
