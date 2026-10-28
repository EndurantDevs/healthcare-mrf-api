# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Standing policy remains explicit, command-bound and independent of native ACLs."""

import asyncio
import copy
import hashlib
import json
from contextlib import asynccontextmanager
from datetime import date
from unittest.mock import AsyncMock, Mock

import pytest

from db.registry_fhir_catalog import PROJECTOR_VERSION, SNAPSHOT_COLUMNS
from process import registry_fhir_catalog_grant as grants
from process import registry_fhir_catalog_publication as publication
from process.registry_fhir_catalog_validation import canonical_json, metadata_generation
from tests.test_registry_fhir_catalog_sources import descriptor, read_profile

COMPANY = "11111111-1111-4111-8111-111111111111"


def standing_grant(**changes):
    return grants.FHIRCatalogStandingGrant(
        **{
            "grant_id": "synthetic-catalog-policy",
            "version": 1,
            "control_schema": "catalog_control",
            "source_profiles": (read_profile(),),
            "company_crosswalks": (("alias-a", "owner-one", COMPANY),),
            **changes,
        }
    )


def snapshot(grant, descriptors=(), *, status="current", **changes):
    source_entries = [
        {
            "source_generation": hashlib.sha256(canonical_json(entry).encode()).hexdigest(),
            "source_id": entry["coordinates"]["source_id"],
            "status": status,
        }
        for entry in descriptors
    ]
    input_by_field = {
        "network_generation": 9,
        "manifest_sha256": "a" * 64,
        "metadata_approved_revision": 6,
        "membership_mapping_sha256": "b" * 64,
        "mapping_sha256": "c" * 64,
        "standing_grant_sha256": grant.sha256,
        "source_vector_json": sorted(
            source_entries, key=lambda entry: (entry["source_id"], entry["source_generation"])
        ),
        "projector_version": PROJECTOR_VERSION,
        "as_of": date(2026, 1, 1),
        **changes,
    }
    return {
        **input_by_field,
        "metadata_generation": metadata_generation(input_by_field),
        "schema_name": "catalog_snapshot",
        "custody_json": {},
        "accounting_json": {},
        "published_at": None,
    }


def command(grant, selected, *, previous=None):
    return {
        "operation": publication.OPERATION,
        "metadata_generation": selected["metadata_generation"],
        "expected_metadata_generation": previous,
        "expected_serving_generation": selected["network_generation"],
        "expected_approved_revision": selected["metadata_approved_revision"],
        "standing_grant_sha256": grant.sha256,
    }


def test_unattached_snapshot_requires_a_complete_null_directory_pair():
    grant = standing_grant()
    selected = snapshot(grant, network_generation=None, manifest_sha256=None)
    assert publication.validated_fhir_catalog_command(command(grant, selected))["expected_serving_generation"] is None
    for changes in ({"network_generation": None}, {"manifest_sha256": None}):
        with pytest.raises(ValueError, match="inputs_invalid"):
            snapshot(grant, **changes)


class CatalogConnection:
    def __init__(self, snapshots, descriptors=()):
        self.snapshots = {entry["metadata_generation"]: entry for entry in snapshots}
        self.descriptors = {hashlib.sha256(canonical_json(entry).encode()).hexdigest(): entry for entry in descriptors}
        self.queries = []
        self.active = False

    async def fetchrow(self, sql, generation):
        self.queries.append(sql)
        result = self.snapshots.get(generation)
        return None if result and "published_at IS NOT NULL" in sql and not result["published_at"] else result

    async def fetch(self, sql, generations, after):
        self.queries.append(sql)
        return [
            {
                "source_generation": generation,
                "source_id": entry["coordinates"]["source_id"],
                "document": canonical_json(entry),
            }
            for generation, entry in sorted(self.descriptors.items())
            if generation in generations and generation > after
        ]

    def is_in_transaction(self):
        return self.active

    @asynccontextmanager
    async def transaction(self, **options):
        assert options == {"isolation": "repeatable_read", "readonly": True}
        self.active = True
        try:
            yield
        finally:
            self.active = False


@pytest.fixture
def custody(monkeypatch):
    proof = AsyncMock()
    monkeypatch.setattr(grants, "verify_fhir_catalog_snapshot", proof)
    return proof


def test_grant_roundtrip_is_canonical_and_metadata_only():
    profile = grants.FHIRCatalogSourceReadProfile("second_source", "second_owner", ("second_reader",))
    first = standing_grant(source_profiles=(read_profile(), profile))
    second = standing_grant(source_profiles=(profile, read_profile()))
    assert first.sha256 == second.sha256
    assert grants.parse_fhir_catalog_standing_grant(json.dumps(first.document())).document() == first.document()
    assert first.crosswalk_for("alias-a") == (("owner-one", COMPANY),)
    assert first.crosswalk_for("alias-b") == ()
    assert first.sha256 != standing_grant(version=2, source_profiles=(profile, read_profile())).sha256
    assert first.document()["operation"] == "publish_fhir_catalog"


@pytest.mark.parametrize(
    "change",
    [
        "unknown",
        "wrong-operation",
        "duplicate-key",
        "bad-version",
        "bad-crosswalk",
        "duplicate-crosswalk",
        "duplicate-profile",
    ],
)
def test_grant_rejects_open_or_ambiguous_configuration(change):
    config = standing_grant().document()
    if change == "unknown":
        config["can_mutate_networks"] = True
    if change == "wrong-operation":
        config["operation"] = "publish_membership"
    if change == "bad-version":
        config["version"] = True
    if change == "bad-crosswalk":
        config["company_crosswalks"][0]["company_id"] = "not-a-company-id"
    if change == "duplicate-crosswalk":
        config["company_crosswalks"] *= 2
    if change == "duplicate-profile":
        config["source_profiles"] *= 2
    if change == "duplicate-key":
        config = json.dumps(config).replace('"version": 1', '"version": 1, "version": 2')
    with pytest.raises(grants.FHIRCatalogGrantError, match="grant_invalid"):
        grants.parse_fhir_catalog_standing_grant(config)


@pytest.mark.asyncio
async def test_authority_binds_full_protected_snapshot_and_exact_command(custody):
    grant, source_descriptor = standing_grant(), descriptor()
    selected = snapshot(grant, (source_descriptor,))
    writer = CatalogConnection((selected,), (source_descriptor,))
    runtime = grants.build_fhir_catalog_publication_runtime(grant, None)
    selected_command = command(grant, selected)
    receipt = await runtime.authorize_metadata(command=selected_command, snapshot=selected, connection=writer)
    assert receipt == {
        "authorized": True,
        "operation": publication.OPERATION,
        "command_sha256": hashlib.sha256(publication.publication_json(selected_command).encode()).hexdigest(),
        "standing_grant_sha256": grant.sha256,
    }
    custody.assert_awaited_once_with(writer, selected)
    assert all(column in writer.queries[0] for column in SNAPSHOT_COLUMNS)
    for field, value in (
        ("standing_grant_sha256", "f" * 64),
        ("expected_serving_generation", 10),
        ("expected_approved_revision", 7),
    ):
        with pytest.raises(grants.FHIRCatalogGrantError, match="changed"):
            await runtime.authorize_metadata(
                command={**selected_command, field: value}, snapshot=selected, connection=writer
            )


@pytest.mark.asyncio
async def test_profile_acl_observation_cannot_create_grant(custody):
    grant, source_descriptor = standing_grant(source_profiles=()), descriptor()
    selected = snapshot(grant, (source_descriptor,))
    writer = CatalogConnection((selected,), (source_descriptor,))
    runtime = grants.build_fhir_catalog_publication_runtime(grant, None)
    with pytest.raises(grants.FHIRCatalogGrantError, match="source_not_admitted"):
        await runtime.authorize_metadata(command=command(grant, selected), snapshot=selected, connection=writer)


@pytest.mark.asyncio
@pytest.mark.parametrize("history", ["missing", "unpublished", "different-source", "published"])
async def test_last_good_needs_exact_previous_published_custody_without_current_grant(custody, history):
    grant, source_descriptor = standing_grant(source_profiles=()), descriptor()
    prior = snapshot(grant, (source_descriptor,))
    prior["published_at"] = None if history == "unpublished" else "2026-01-01T00:00:00Z"
    if history == "different-source":
        prior = snapshot(grant, (), as_of=date(2025, 1, 1))
        prior["published_at"] = "2026-01-01T00:00:00Z"
    selected = snapshot(grant, (source_descriptor,), status="last_good", as_of=date(2026, 1, 2))
    writer = CatalogConnection((prior, selected), (source_descriptor,))
    runtime = grants.build_fhir_catalog_publication_runtime(grant, None)
    selected_command = command(grant, selected, previous=None if history == "missing" else prior["metadata_generation"])
    if history != "published":
        with pytest.raises(grants.FHIRCatalogGrantError, match="history_|snapshot_unavailable"):
            await runtime.authorize_metadata(command=selected_command, snapshot=selected, connection=writer)
        return
    assert (await runtime.authorize_metadata(command=selected_command, snapshot=selected, connection=writer))[
        "authorized"
    ] is True
    assert custody.await_count == 2
    assert any("published_at IS NOT NULL" in sql for sql in writer.queries)
    pool = Mock()
    runtime = grants.build_fhir_catalog_publication_runtime(grant, pool)
    async with runtime.source_fence(snapshot=selected, connection=writer):
        with pytest.raises(grants.FHIRCatalogGrantError, match="source_authorization_required"):
            await runtime.verify_sources(snapshot=selected, connection=writer)
        await runtime.authorize_metadata(command=selected_command, snapshot=selected, connection=writer)
        assert await runtime.verify_sources(snapshot=selected, connection=writer) is True
        with pytest.raises(grants.FHIRCatalogGrantError, match="source_context_required"):
            await asyncio.create_task(runtime.verify_sources(snapshot=selected, connection=writer))
        with pytest.raises(grants.FHIRCatalogGrantError, match="source_context_changed"):
            await runtime.verify_sources(snapshot={**selected, "metadata_generation": "f" * 64}, connection=writer)
    pool.acquire.assert_not_called()
    with pytest.raises(grants.FHIRCatalogGrantError, match="source_context_required"):
        await runtime.verify_sources(snapshot=selected, connection=writer)


@pytest.mark.asyncio
async def test_descriptor_tamper_is_refused_before_source_admission(custody):
    grant, source_descriptor = standing_grant(), descriptor()
    selected = snapshot(grant, (source_descriptor,))
    writer = CatalogConnection((selected,), (source_descriptor,))
    changed = copy.deepcopy(source_descriptor)
    changed["coordinates"]["producer_id"] = "unsupported-producer"
    writer.descriptors[next(iter(writer.descriptors))] = changed
    runtime = grants.build_fhir_catalog_publication_runtime(grant, None)
    with pytest.raises(ValueError):
        await runtime.authorize_metadata(command=command(grant, selected), snapshot=selected, connection=writer)


@pytest.mark.asyncio
async def test_current_source_control_cap_fails_before_reader_borrow(custody, monkeypatch):
    grant, source_descriptor = standing_grant(), descriptor()
    source_entries = [
        {"source_generation": f"{index:064x}", "source_id": f"source-{index:04}", "status": "current"}
        for index in range(1001)
    ]
    selected = snapshot(grant, (), source_vector_json=source_entries)
    writer, pool = CatalogConnection((selected,)), Mock()
    monkeypatch.setattr(
        grants,
        "_descriptors",
        AsyncMock(return_value={entry["source_generation"]: source_descriptor for entry in source_entries}),
    )
    runtime = grants.build_fhir_catalog_publication_runtime(grant, pool)
    with pytest.raises(grants.FHIRCatalogGrantError, match="current_source_limit"):
        await runtime.authorize_metadata(command=command(grant, selected), snapshot=selected, connection=writer)
    with pytest.raises(grants.FHIRCatalogGrantError, match="current_source_limit"):
        async with runtime.source_fence(snapshot=selected, connection=writer):
            pytest.fail("excessive source set entered publication")
    pool.acquire.assert_not_called()
