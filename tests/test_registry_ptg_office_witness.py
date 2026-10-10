# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure witness orchestration; compiled codec cases run in the native family."""

import hashlib
import json
import struct
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import registry_ptg_office_capture as capture
from process import registry_ptg_office_witness as witness
from tests import test_registry_ptg_graph_reader as graph_fixture
from tests import test_registry_ptg_office_capture as office_fixture


def _scope():
    document = office_fixture._scope()
    document["evidence"]["graph_identity"] = graph_fixture._identity()
    return document


def _row(ordinal):
    row = office_fixture._row(ordinal, location=UUID(int=ordinal + 500))
    row["provider_witness_sha256"] = capture._digest(
        {
            "snapshot_key": 11,
            "dense_source_key": 0,
            "source_record_ordinal": ordinal - 1,
            "provider_group_ref": row["provider_group_ref"],
            "provider_system": "npi",
            "provider_id": row["provider_id"],
        }
    )
    row["evidence_id"] = capture._digest(
        [
            _scope()["coordinates"],
            row["office_evidence_json"]["source_scope"],
            row["binding_source_key"],
            "npi",
            row["provider_id"],
            row["location_id"],
            row["office_evidence_sha256"],
            row["provider_witness_sha256"],
            row["address_row_sha256"],
        ]
    )
    return row


class _OneResult(graph_fixture._Result):
    def one(self):
        assert len(self.rows) == 1
        return self.rows[0]


class _Session(graph_fixture._Session):
    def __init__(self, office_rows):
        super().__init__()
        self.office_rows = office_rows
        self.witness_calls = []
        self.failure = None

    async def execute(self, query, parameters=None):
        sql = str(query)
        if "WITH page AS MATERIALIZED" not in sql:
            return await super().execute(query, parameters)
        return self._witness_result(parameters)

    def _witness_result(self, parameters):
        after = parameters["after"]
        self.witness_calls.append(after)
        rows = [row for row in self.office_rows if row["ordinal"] > after][: parameters["page_rows"]]
        if self.failure == "last_page" and after >= 4096:
            rows = []
        if self.failure == "terminal" and not rows:
            raise RuntimeError("Missing terminal database page")
        count = len(rows)
        if self.failure == "short" and count:
            count -= 1
        if self.failure == "extra" and after >= len(self.office_rows):
            count = 1
        return _OneResult(
            rows=[
                {
                    "row_count": count,
                    "ordinal_count": count,
                    "first_ordinal": after + 1 if count else None,
                    "last_ordinal": after + count if count else None,
                    "scope_mismatch_count": 0,
                    "mismatch_count": 1 if self.failure == "unresolved" else 0,
                    "edge_count": 1 if count else 0,
                    "selected_edges": struct.pack(">II", 5, 5) if count else b"",
                }
            ]
        )


class _Driver:
    def __init__(self, rows, descriptor):
        self.rows, self.descriptor, self.calls = rows, descriptor, []
        self.closed = False
        self.manifest_changed = False

    def is_in_transaction(self):
        return not self.closed

    async def execute(self, sql):
        self.calls.append(sql)
        assert sql.startswith("LOCK TABLE ") and "registry_ptg_office_" in sql

    def _page(self, after, limit):
        return [row for row in self.rows if row["ordinal"] > after][:limit]

    async def fetchrow(self, sql, *parameters):
        self.calls.append(sql)
        if "sum(octet_length" in sql:
            rows = self._page(*parameters)
            return {"rows": len(rows), "bytes": sum(len(capture._canonical(row)) for row in rows)}
        if "count(DISTINCT source_record_key)" in sql:
            return {
                "rows": len(self.rows),
                "source_records": len({row["source_record_key"] for row in self.rows}),
                "offices": len({(row["provider_system"], row["provider_id"], row["location_id"]) for row in self.rows}),
                "first": 1,
                "last": len(self.rows),
                "invalid": 0,
            }
        raise AssertionError(sql)

    async def fetch(self, sql, *parameters):
        self.calls.append(sql)
        if "capture_manifest LIMIT 2" in sql:
            return [
                {
                    "id": 1,
                    "manifest_sha256": self.descriptor.manifest_sha256,
                    "manifest_json": {} if self.manifest_changed else json.loads(self.descriptor.manifest_json),
                }
            ]
        return self._page(*parameters)


def _prepared(office_rows):
    request = office_fixture._request(office_rows)
    scope = _scope()
    from process.network_serving_read import PinnedNetworkServingManifest

    serving = PinnedNetworkServingManifest(3, str(UUID(int=900)), "synthetic_serving", 1, {}, 3, "b" * 64, 45)
    digest = hashlib.sha256()
    identity = capture._retained_accounting(office_fixture._adoption_receipt(office_rows), office_rows, digest, None)
    accounting_by_field = {
        "input_row_count": len(office_rows),
        "canonical_input_sha256": request.canonical_input_sha256,
        "retained_site_rows_sha256": digest.hexdigest(),
        "retained_site_identity": identity,
    }
    command = capture.office_review_command(request, scope, serving, accounting_by_field)
    manifest_by_field = {
        "contract": "registry_ptg_office_capture.v1",
        "state": "prepared",
        "command": command,
        "command_sha256": capture._digest(command),
        "accounting": accounting_by_field,
    }
    custody_by_field = {"table_oid": 17, "manifest_table_oid": 18, "schema_oid": 19, "owner_oid": 20}
    descriptor = capture.RegistryPTGOfficeCaptureDescriptor(
        request.capture_id,
        "registry_ptg_office_" + request.capture_id.hex,
        capture._digest(manifest_by_field),
        capture._canonical(manifest_by_field),
        capture._canonical(custody_by_field),
    )
    return request, descriptor, serving, custody_by_field


@pytest.fixture
def arrange(monkeypatch):
    def setup(count=2, *, use_native_codecs=False):
        office_rows = [_row(ordinal) for ordinal in range(1, count + 1)]
        request, descriptor, serving, custody_by_field = _prepared(office_rows)
        session, driver = _Session(office_rows), _Driver(office_rows, descriptor)
        context = office_fixture._context(
            source_specification=SimpleNamespace(ptg_schema_name="synthetic_ptg", snapshot_id="synthetic_snapshot"),
            graph_identity=graph_fixture._identity(),
        )
        monkeypatch.setattr(capture, "_driver", AsyncMock(return_value=driver))
        monkeypatch.setattr(capture, "_scope", AsyncMock(return_value=_scope()))
        monkeypatch.setattr(capture, "_custody", AsyncMock(return_value=custody_by_field))
        monkeypatch.setattr(capture, "resolve_network_serving_manifest", AsyncMock(return_value=serving))
        monkeypatch.setattr(witness.source, "_physical_binding", AsyncMock(return_value=None))
        monkeypatch.setattr(witness.source, "_require_frozen_source", AsyncMock())
        monkeypatch.setattr(witness.source, "_source_state", AsyncMock(return_value=(graph_fixture._identity(), [{}])))

        async def adopt(driver, serving, batch, control):
            return office_fixture._adoption_receipt(batch)

        monkeypatch.setattr(capture, "_adopt_batch", adopt)
        if not use_native_codecs:
            monkeypatch.setattr(witness, "_verified_page", _synthetic_verified_page)
        return SimpleNamespace(
            rows=office_rows,
            request=request,
            descriptor=descriptor,
            context=context,
            session=session,
            driver=driver,
            custody_by_field=custody_by_field,
        )

    return setup


async def _synthetic_verified_page(session, driver, context, request, prepared, page, office_rows, limits):
    """Supply fixture bytes for orchestration only, without codec or graph proof."""
    limits[1]()
    canonical = b"".join(capture._canonical(office) + b"\n" for office in office_rows)
    return canonical, office_fixture._adoption_receipt(office_rows), b"synthetic_codec_boundary"


async def _verify(prepared, budget=None):
    return await witness.verify_registry_ptg_office_witness(
        prepared.session,
        prepared.context,
        prepared.request,
        prepared.descriptor,
        read_budget=budget if budget is not None else witness.graph.RegistryPTGGraphReadBudget(1048576),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["last_page", "terminal", "short", "extra", "unresolved"])
async def test_incomplete_or_extra_source_stream_never_returns_evidence(arrange, failure):
    prepared = arrange(4101 if failure == "last_page" else 2)
    prepared.session.failure = failure
    reasons_by_failure = {
        "last_page": "^registry_ptg_office_accounting_invalid$",
        "terminal": "^Missing terminal database page$",
        "short": "^registry_ptg_office_accounting_invalid$",
        "extra": "^registry_ptg_office_accounting_invalid$",
        "unresolved": "^registry_ptg_provider_unresolved$",
    }
    with pytest.raises((ValueError, RuntimeError), match=reasons_by_failure[failure]):
        await _verify(prepared)


@pytest.mark.asyncio
@pytest.mark.parametrize("replacement", ["schema", "manifest", "custody", "scope", "source", "serving", "transaction"])
async def test_context_or_relation_replacement_fails_closed(arrange, monkeypatch, replacement):
    prepared = arrange()
    if replacement == "schema":
        prepared.descriptor = replace(prepared.descriptor, schema_name="synthetic_ptg")
    if replacement == "manifest":
        prepared.driver.manifest_changed = True
    if replacement == "custody":
        monkeypatch.setattr(capture, "_custody", AsyncMock(side_effect=[prepared.custody_by_field, {"table_oid": 42}]))
    if replacement == "scope":
        scope = _scope()
        scope["file_versions"][0]["raw_sha256"] = "0" * 64
        monkeypatch.setattr(capture, "_scope", AsyncMock(side_effect=[_scope(), scope]))
    if replacement == "source":
        identity = graph_fixture._identity() | {"source_assignments_sha256": "f" * 64}
        monkeypatch.setattr(witness.source, "_source_state", AsyncMock(return_value=(identity, [{}])))
    if replacement == "serving":
        actual = capture.resolve_network_serving_manifest.return_value
        monkeypatch.setattr(
            capture,
            "resolve_network_serving_manifest",
            AsyncMock(side_effect=[actual, replace(actual, address_table_oid=99)]),
        )
    if replacement == "transaction":

        async def changed(*args):
            prepared.session.transaction = object()
            return prepared.custody_by_field

        monkeypatch.setattr(capture, "_custody", changed)
    reasons_by_replacement = {
        "schema": "^registry_ptg_office_descriptor_invalid$",
        "manifest": "^registry_ptg_office_manifest_changed$",
        "custody": "^registry_ptg_office_custody_changed$",
        "scope": "^registry_ptg_office_context_changed$",
        "source": "^registry_ptg_office_source_changed$",
        "serving": "^registry_ptg_office_context_changed$",
        "transaction": "^registry_ptg_office_transaction_changed$",
    }
    with pytest.raises(ValueError, match=reasons_by_replacement[replacement]):
        await _verify(prepared)


@pytest.mark.asyncio
@pytest.mark.parametrize("replacement", ["office", "address", "ordinal", "duplicate", "digest", "budget"])
async def test_full_input_and_closed_grain_are_revalidated(arrange, replacement):
    prepared = arrange()
    if replacement == "office":
        prepared.rows[0]["office_evidence_json"]["provider_id"] = "1234567890"
    if replacement == "address":
        prepared.rows[0]["address_row_sha256"] = "0" * 64
    if replacement == "ordinal":
        prepared.rows[0]["ordinal"] = 3
    if replacement == "duplicate":
        prepared.rows[1]["location_id"] = prepared.rows[0]["location_id"]
    if replacement == "digest":
        prepared.request = replace(prepared.request, canonical_input_sha256="0" * 64)
    invocation = _verify(prepared) if replacement != "budget" else _verify(prepared, object())
    reasons_by_replacement = {
        "office": "^registry_ptg_office_accounting_invalid$",
        "address": "^registry_ptg_office_accounting_invalid$",
        "ordinal": "^registry_ptg_office_accounting_invalid$",
        "duplicate": "^registry_ptg_office_accounting_invalid$",
        "digest": "^registry_ptg_office_context_changed$",
        "budget": "^registry_ptg_office_budget_invalid$",
    }
    with pytest.raises(ValueError, match=reasons_by_replacement[replacement]):
        await invocation
    if replacement == "duplicate":
        assert not prepared.session.witness_calls


def test_legacy_admission_still_refuses_and_no_legacy_namespace_is_fabricated():
    import inspect

    assert 'raise RegistryPTGCohortAuthorityError("registry_ptg_scope_unavailable")' in inspect.getsource(
        witness.source.require_registry_ptg_cohort_authority
    )
    assert "_office_relation(" not in inspect.getsource(witness)
    assert "expected_rows" not in inspect.getsource(witness)


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["client_id", "scope_id"])
async def test_server_selected_scope_client_cannot_be_replaced(arrange, field):
    prepared = arrange()
    supplied = "different_client" if field == "client_id" else UUID(int=900)
    prepared.context = replace(prepared.context, **{field: supplied})
    with pytest.raises(witness.RegistryPTGOfficeWitnessError, match="context_changed"):
        await _verify(prepared)


@pytest.mark.asyncio
async def test_payload_bounds_are_checked_before_office_fetch(arrange, monkeypatch):
    prepared = arrange()
    real_fetchrow = prepared.driver.fetchrow

    async def oversized(sql, *arguments):
        if "sum(octet_length" in sql:
            return {"rows": 1, "bytes": capture.MAX_INPUT_BYTES}
        return await real_fetchrow(sql, *arguments)

    monkeypatch.setattr(prepared.driver, "fetchrow", oversized)
    with pytest.raises(witness.RegistryPTGOfficeWitnessError, match="page_bounds"):
        await _verify(prepared)
    assert not prepared.session.witness_calls
