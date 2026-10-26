# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact retained-raw accounting is explicit and independent of native row equality."""

import asyncio
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import mrf_address_publication as address
from process import network_cms_registry_address_capture as native
from process import reference_family_archive as archive
from process import registry_aca_global_epoch as global_epoch


def _coverage():
    return {
        "contract": address.ADDRESS_COVERAGE_CONTRACT,
        "archive_name": "address_archive_v2",
        "archive_oid": 20,
        "tables": {
            name: {"rows": 3, "covered": 1, "unkeyable_null": 2, "keyable_null": 0, "uncovered_nonnull": 0}
            for name in address.REFERENCES
        },
    }


def test_explicit_partition_is_accepted_but_never_reinterprets_legacy_coverage():
    coverage = _coverage()
    address.require_address_coverage(coverage)
    global_epoch._coverage(coverage)
    for contract in (None, "mrf-address-coverage.indexed-set.v2"):
        legacy_map = {**coverage, "tables": {name: {"rows": 3, "uncovered": 2} for name in address.REFERENCES}}
        if contract is None:
            legacy_map.pop("contract")
        else:
            legacy_map["contract"] = contract
        with pytest.raises(RuntimeError, match="coverage is incomplete"):
            address.require_address_coverage(legacy_map)
        for counts in legacy_map["tables"].values():
            counts["uncovered"] = 0
        address.require_address_coverage(legacy_map)
        legacy_map["tables"]["mrf_address"]["unkeyable_null"] = 2
        with pytest.raises(RuntimeError, match="accounting is invalid"):
            address.require_address_coverage(legacy_map)


@pytest.mark.parametrize(
    "fault",
    [
        "missing_table",
        "extra_table",
        "missing_count",
        "extra_count",
        "bool",
        "negative",
        "overflow",
        "sum",
        "keyable",
        "uncovered",
        "downgrade",
    ],
)
def test_closed_accounting_refuses_tampered_counts_and_downgrades(fault):
    coverage = _coverage()
    _tamper_accounting(coverage, fault)
    with pytest.raises((ValueError, RuntimeError)):
        global_epoch._coverage(coverage)


def _tamper_accounting(coverage, fault):
    counts = coverage["tables"]["mrf_address"]
    if fault == "missing_table":
        del coverage["tables"]["mrf_address_evidence"]
        return
    if fault == "extra_table":
        coverage["tables"]["foreign"] = dict(counts)
        return
    if fault == "missing_count":
        del counts["covered"]
        return
    if fault == "extra_count":
        counts["extra"] = 0
        return
    if fault in {"bool", "negative", "overflow", "sum"}:
        counts["rows"] = {"bool": True, "negative": -1, "overflow": 2**63, "sum": 4}[fault]
        return
    if fault in {"keyable", "uncovered"}:
        counts["unkeyable_null"] -= 1
        counts["keyable_null" if fault == "keyable" else "uncovered_nonnull"] += 1
        return
    coverage["contract"] = "mrf-address-coverage.indexed-set.v2"


@pytest.mark.asyncio
@pytest.mark.parametrize("expected", [4, 3])
async def test_null_exception_stream_uses_canonical_city_fallback_and_exact_cardinality(monkeypatch, expected):
    rows = [
        ("10 Main St", None, "Austin", "invalid", "78701", "US"),
        ("10 Main St", None, "Austin", "TX", "invalid", "US"),
        ("10 Main St", None, "Austin", "TX", "78701", "US"),
        (None, None, "Austin", "TX", "78701", "US"),
    ]

    async def stream(*_args, **_kwargs):
        for row in rows:
            yield row

    cursor = Mock(side_effect=stream)
    driver = SimpleNamespace(cursor=cursor, fetchval=AsyncMock(return_value=5000))
    monkeypatch.setattr(native, "native_driver", AsyncMock(return_value=driver))
    operation = address._count_unkeyable_null_addresses(object(), '"stage"."mrf_address"', expected)
    if expected == len(rows):
        assert await operation == 2
    else:
        with pytest.raises(RuntimeError, match="accounting changed"):
            await operation
    query = cursor.call_args.args[0]
    assert query.endswith("WHERE address_key IS NULL") and "addr_key_v1" not in query
    assert cursor.call_args.kwargs == {"prefetch": 512}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["deadline", "cancel"])
async def test_null_exception_stream_propagates_interruption_and_closes_iteration(monkeypatch, failure):
    closures = []

    async def stream(*_args, **_kwargs):
        try:
            yield ("10 Main St", None, "Austin", "invalid", "78701", "US")
            if failure == "cancel":
                raise asyncio.CancelledError()
            await asyncio.sleep(1)
        finally:
            closures.append(True)

    driver = SimpleNamespace(cursor=Mock(side_effect=stream), fetchval=AsyncMock(return_value=1))
    monkeypatch.setattr(native, "native_driver", AsyncMock(return_value=driver))
    with pytest.raises(TimeoutError if failure == "deadline" else asyncio.CancelledError):
        await address._count_unkeyable_null_addresses(object(), '"stage"."mrf_address"', 2)
    assert closures == [True]


@pytest.mark.asyncio
async def test_accounted_clone_closure_recomputes_both_raw_partitions_and_keeps_scope_guards(monkeypatch):
    coverage = _coverage()
    counts = AsyncMock(return_value=deepcopy(coverage["tables"]))
    monkeypatch.setattr(address, "_capture_raw_address_counts", counts)
    session = SimpleNamespace(scalar=AsyncMock(return_value=False))
    qualified = lambda schema, name: f'"{schema}"."{name}"'
    await address.validate_canonical_closure(session, "mrf", "stage", qualified, address_coverage=coverage)
    counts.assert_awaited_once_with(session, "stage", qualified, '"stage"."mrf_canonical_address"')
    statements = [str(call.args[0]) for call in session.scalar.await_args_list]
    assert len(statements) == 3 and "source_bits & 16" in statements[0]
    assert "target.address_key=source.merged_into" in statements[1] and "paths.depth<64" in statements[2]
    counts.return_value["mrf_address"]["unkeyable_null"] += 1
    with pytest.raises(RuntimeError, match="accounting differs"):
        await address.validate_canonical_closure(session, "mrf", "stage", qualified, address_coverage=coverage)


def test_accounted_model_receipt_binds_complete_proof_and_refuses_legacy_downgrade():
    publication_map = {"attempt_id": "synthetic", "generation": {}, "native_address_coverage": _coverage()}
    receipt = archive._canonical_model_receipt(publication_map)
    assert archive._validate_mrf_auxiliary_receipt(receipt) == receipt
    assert receipt["contract"] == address.ACCOUNTED_MODEL_CLOSURE_CONTRACT
    changed = deepcopy(publication_map)
    changed["native_address_coverage"]["tables"]["mrf_address"]["rows"] += 1
    assert archive.mrf_publication_digest(changed) != receipt["publication_sha256"]
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="model receipt is invalid"):
        archive._validate_mrf_auxiliary_receipt({**receipt, "contract": address.MODEL_CLOSURE_CONTRACT})
    legacy = archive._canonical_model_receipt()
    assert archive._validate_mrf_auxiliary_receipt(legacy) == legacy


def test_accounted_manifest_binds_raw_totals_to_native_table_receipts():
    auxiliary = archive._canonical_model_receipt(
        {"attempt_id": "synthetic", "generation": {}, "native_address_coverage": _coverage()}
    )
    manifest_map = {"contract": archive.TYPED_MRF_CONTRACT, "auxiliary": auxiliary}
    receipts = tuple(archive.ReferenceTableReceipt(name, name, "a" * 64, 3) for name in address.REFERENCES)
    spec = SimpleNamespace(importer_id="mrf")
    assert archive._manifest_auxiliary(manifest_map, spec, receipts) == auxiliary
    changed = (replace(receipts[0], row_count=4), *receipts[1:])
    with pytest.raises(RuntimeError, match="raw row accounting differs"):
        archive._manifest_auxiliary(manifest_map, spec, changed)


@pytest.mark.parametrize("malformed", [None, "table", {"table_name": "mrf_address"}, {"row_count": 3}])
def test_accounted_manifest_rejects_malformed_tables_with_archive_error(malformed):
    auxiliary = archive._canonical_model_receipt(
        {"attempt_id": "synthetic", "generation": {}, "native_address_coverage": _coverage()}
    )
    spec = archive.reference_family_spec("mrf", canonical=True)
    tables = tuple(
        archive.ReferenceTableReceipt(model.__name__, model.__tablename__, "a" * 64, 3) for model in spec.model_types
    )
    metadata, digest = archive._source_metadata({"source_release": "synthetic"})
    manifest = archive.ReferenceFamilyManifest(
        "mrf",
        tables,
        metadata,
        digest,
        archive._schema_digest(tables),
        dependencies={"plan-attributes": "b" * 64},
        auxiliary=auxiliary,
    ).as_dict()
    assert archive.validate_reference_family_manifest(manifest).auxiliary == auxiliary
    manifest["tables"][0] = malformed
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="table receipt is invalid"):
        archive.validate_reference_family_manifest(manifest)


@pytest.mark.asyncio
async def test_source_clone_still_requires_full_model_equality_before_accounted_closure(monkeypatch):
    spec = archive.reference_family_spec("mrf", canonical=True)
    monkeypatch.setattr(archive, "_manifest_family_spec", lambda _manifest: spec)
    equality = AsyncMock(return_value=False)
    closure = AsyncMock()
    monkeypatch.setattr(archive, "_is_model_table_equal", equality)
    monkeypatch.setattr(archive, "validate_canonical_closure", closure)
    auxiliary = archive._canonical_model_receipt(
        {"attempt_id": "synthetic", "generation": {}, "native_address_coverage": _coverage()}
    )
    capture = SimpleNamespace(
        manifest=SimpleNamespace(as_dict=lambda: {}, auxiliary=auxiliary),
        schema_name="source",
        frozen_source_projection=None,
    )
    with pytest.raises(RuntimeError, match="clone differs"):
        await archive._validate_source_clone_sets(object(), capture, "stage")
    closure.assert_not_awaited()
    equality.return_value = True
    session = object()
    await archive._validate_source_clone_sets(session, capture, "stage")
    assert equality.await_count == len(spec.model_types) + 1
    assert closure.await_args.kwargs["address_coverage"] == auxiliary["address_coverage"]
