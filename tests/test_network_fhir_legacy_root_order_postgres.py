# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Historical Source roots remain distinct from independently replayed typed captures."""

import asyncio
import hashlib
import importlib
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import text

from process import network_fhir_source_witness_capture as witness
from process import network_fhir_typed_source as typed
from process.network_fhir_retained_capture_contract import RetainedFHIRCaptureError
from tests import source_witness_legacy_postgres_support as legacy
from tests.source_witness_capture_postgres_support import (
    _observed_source_descriptor,
    capture_arguments,
    source_modules,
    source_witness_case,
)
from tests.test_network_fhir_source_witness_capture_postgres import _assert_rolled_back, _capture
from tests.test_network_fhir_typed_source_postgres import (
    _assert_closed_capture_writes,
    _check_persistent_capture_reader,
    _produce_recipe_capture,
)

pytestmark = pytest.mark.asyncio


def _root_proof_modules():
    """Resolve the installed root verifier through the neutral producer facade."""
    catalog, producer = source_modules()
    return catalog, importlib.import_module(".fhir_source_membership_roots", producer.__package__)


def _locale_resources(original):
    return original() + [
        (
            "Location",
            identity,
            {
                "source_id": legacy.SOURCE,
                "resource_id": identity,
                "status": "inactive",
                "note": "café",
                "measure": 1.25,
            },
        )
        for identity in ("A-2", "A2", "B", "a.2", "a2", "b")
    ]


def _ordered_fixture_proof(context, contents, *args, **kwargs):
    resource_records, canonical_root, counts_by_family, _ = context.original(contents, *args, **kwargs)
    by_key = {(resource_record[1], resource_record[2]): resource_record for resource_record in resource_records}
    ordered_records = [by_key[key] for key in context.keys]
    encoded_triples = [
        json.dumps(list(resource_record[1:4]), separators=(",", ":")).encode() for resource_record in ordered_records
    ]
    root = hashlib.sha256(b"\n".join(encoded_triples)).hexdigest()
    hashes_by_family = {
        kind: hashlib.sha256(
            b"\n".join(
                encoded
                for resource_record, encoded in zip(ordered_records, encoded_triples, strict=True)
                if resource_record[1] == kind
            )
        ).hexdigest()
        for kind in counts_by_family
    }
    assert root != canonical_root
    context.proof_by_field.update(
        root=root, canonical_root=canonical_root, hashes=dict(hashes_by_family), counts=counts_by_family
    )
    if context.damage == "dataset_root":
        root = "f" * 64
    if context.damage == "family_root":
        hashes_by_family["Location"] = "e" * 64
    return resource_records, root, counts_by_family, hashes_by_family


def _install_historical_seed(monkeypatch, damage=None):
    """Use native source-column ordering before publication guards exist, independently of database locale."""
    from functools import partial

    original_seed = legacy._seed_original_publication
    context = SimpleNamespace(original=legacy.legacy._proof_rows, keys=[], proof_by_field={}, damage=damage)
    resources = partial(_locale_resources, legacy.legacy_resources)

    def seed(connection):
        connection.execute(
            text(
                "ALTER TABLE mrf.provider_directory_dataset_resource "
                'ALTER COLUMN resource_type TYPE varchar(64) COLLATE "en-US-x-icu", '
                'ALTER COLUMN resource_id TYPE varchar(256) COLLATE "en-US-x-icu"'
            )
        )
        context.keys.extend(
            tuple(resource_record)
            for resource_record in connection.execute(
                text(
                    "SELECT kind,identity FROM jsonb_to_recordset(CAST(:rows AS jsonb)) resource(kind text,identity text) "
                    'ORDER BY kind COLLATE "en-US-x-icu",identity COLLATE "en-US-x-icu"'
                ),
                {"rows": json.dumps([{"kind": kind, "identity": identity} for kind, identity, _ in resources()])},
            )
        )
        assert context.keys != sorted(context.keys)
        original_seed(connection)
        if damage == "payload":
            connection.execute(
                text(
                    "UPDATE mrf.provider_directory_dataset_resource SET payload_json=jsonb_set(payload_json::jsonb,'{measure}','2.5') WHERE resource_type='Location' AND resource_id='A2'"
                )
            )
        if damage == "missing":
            connection.execute(
                text(
                    "DELETE FROM mrf.provider_directory_dataset_resource WHERE resource_type='Location' AND resource_id='A2'"
                )
            )
        connection.commit()

    monkeypatch.setattr(legacy, "legacy_resources", resources)
    monkeypatch.setattr(legacy.legacy, "_proof_rows", partial(_ordered_fixture_proof, context))
    monkeypatch.setattr(legacy, "_seed_original_publication", seed)
    return context.proof_by_field


async def _header(case):
    return await case.fixture.admin.fetchval(
        'SELECT row_to_json(header)::text FROM "'
        + case.source_profile.schema_name
        + '".provider_directory_endpoint_dataset header WHERE dataset_id=$1',
        case.origin.dataset_id,
    )


async def _consume(case):
    async with case.source_export(source_profile=case.source_profile, origin=case.origin) as export:
        return [row for page in [page async for page in export["resources"]()] for row in page]


async def test_historical_source_closes_typed_capture_and_replays_without_old_witness_waiver(monkeypatch):
    recorded = _install_historical_seed(monkeypatch)
    catalog, roots = _root_proof_modules()

    original, passes = roots._historical_roots, []
    monkeypatch.setattr(roots, "_PAGE_ROWS", 2)

    async def observe(scope, origin):
        await original(scope, origin)
        passes.append(dict(scope.native_identity_by_field))

    monkeypatch.setattr(roots, "_historical_roots", observe)
    async with source_witness_case(monkeypatch, legacy=True) as case:
        before = await _header(case)
        assert case.origin.dataset_sha256 == recorded["root"] != recorded["canonical_root"]
        assert case.origin.resource_parent_oid != case.origin.resource_leaf_oid
        descriptor, _ = await _observed_source_descriptor(
            case.source_runtime, case.source_profile, case.origin, catalog
        )
        document, anchor = await _produce_recipe_capture(case, descriptor, (case.fixture.component.reader,))
        assert document["graph"]["origin"]["dataset_sha256"] == recorded["root"]
        assert dict(document["graph"]["origin"]["resource_hashes"]) == recorded["hashes"]
        names_by_kind = {kind: "historical_replay_" + kind + "_" + uuid4().hex for kind in typed._DDL}
        for kind, definition in typed._DDL.items():
            await case.reader.execute('CREATE TEMP TABLE "' + names_by_kind[kind] + '"(' + definition + ")")
        await case.reader.execute('CREATE INDEX ON "' + names_by_kind["addresses"] + '"(address_key)')
        try:
            await _check_persistent_capture_reader(case, document, anchor, descriptor, names_by_kind)
            await _assert_closed_capture_writes(case, document["schema_name"])
        finally:
            for name in names_by_kind.values():
                await case.reader.execute('DROP TABLE "' + name + '"')
        assert len(passes) == 2 and passes[0]["virtual_transaction"] != passes[1]["virtual_transaction"]
        arguments = capture_arguments(case)
        with pytest.raises(RetainedFHIRCaptureError):
            await _capture(case, witness, arguments)
        await _assert_rolled_back(case, arguments)
        assert len(passes) == 3 and await _header(case) == before


@pytest.mark.parametrize("damage", ["payload", "missing", "dataset_root", "family_root"])
async def test_genuine_historical_source_rejects_changed_rows_and_original_commitments(monkeypatch, damage):
    _install_historical_seed(monkeypatch, damage)
    async with source_witness_case(monkeypatch, legacy=True) as case:
        before = await _header(case)
        with pytest.raises(ValueError):
            await _consume(case)
        assert await _header(case) == before
        async with case.source_runtime.candidate_registry_reader() as reader:
            assert not reader.is_in_transaction()


async def test_historical_proof_cancellation_and_partial_export_close_source_snapshot(monkeypatch):
    _install_historical_seed(monkeypatch)
    _, roots = _root_proof_modules()

    async with source_witness_case(monkeypatch, legacy=True) as case:
        before = await _header(case)
        with pytest.raises(ValueError, match="catalog_source_stream_incomplete"):
            async with case.source_export(source_profile=case.source_profile, origin=case.origin) as export:
                iterator = export["resources"]()
                assert await anext(iterator)
                await iterator.aclose()
        started, original = asyncio.Event(), roots._collation_snapshot

        async def pause_after_collation(connection, origin):
            snapshot = await original(connection, origin)
            started.set()
            await asyncio.Future()
            return snapshot

        with monkeypatch.context() as patch:
            patch.setattr(roots, "_collation_snapshot", pause_after_collation)
            pending = asyncio.create_task(_consume(case))
            try:
                await asyncio.wait_for(started.wait(), 10)
                pending.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await pending
            finally:
                if not pending.done():
                    pending.cancel()
                    await asyncio.gather(pending, return_exceptions=True)
        resource_rows = await _consume(case)
        assert len(resource_rows) == sum(dict(case.origin.resource_counts).values())
        assert [
            (resource_row["resource_type"], resource_row["resource_id"]) for resource_row in resource_rows
        ] == sorted((resource_row["resource_type"], resource_row["resource_id"]) for resource_row in resource_rows)
        assert await _header(case) == before


async def test_modern_source_header_cannot_downgrade_to_historical_root_proof(monkeypatch):
    _, roots = _root_proof_modules()

    historical = AsyncMock(side_effect=AssertionError("modern publication reached historical proof"))
    monkeypatch.setattr(roots, "_historical_roots", historical)
    async with source_witness_case(monkeypatch) as case:
        before = await _header(case)
        downgraded = replace(
            case.origin,
            admission_profile=case.producer.OBSERVED_LEGACY_PROFILE,
            proof_sha256=None,
            descriptor_counts=None,
            admission_metadata_sha256=None,
            semantic_projection_as_of=None,
        )
        with pytest.raises(ValueError, match="membership_profile_unsupported"):
            async with case.source_export(source_profile=case.source_profile, origin=downgraded):
                pytest.fail("modern publication admitted as historical")
        historical.assert_not_awaited()
        assert await _header(case) == before
        assert len(await _consume(case)) == sum(dict(case.origin.resource_counts).values())
