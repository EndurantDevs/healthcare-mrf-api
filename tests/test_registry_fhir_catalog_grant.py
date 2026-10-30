# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Standing policy remains explicit, command-bound and independent of native ACLs."""

import asyncio
import copy
import hashlib
import json
from contextlib import asynccontextmanager, nullcontext
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from db.registry_fhir_catalog import PROJECTOR_VERSION, SNAPSHOT_COLUMNS
from process import registry_fhir_catalog_grant as grants
from process import registry_fhir_catalog_publication as publication
from process.registry_fhir_catalog_permissions import _validate_column
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


@pytest.mark.parametrize("direct_role", [True, False])
@pytest.mark.parametrize(
    "role,table,column",
    [
        ("catalog_execution", "network_serving_control", "metadata_generation"),
        ("catalog_lifecycle", "registry_publication_request", "state"),
    ],
)
def test_publication_update_rights_cannot_be_granted_onward(direct_role, role, table, column):
    config = SimpleNamespace(execution_role="catalog_execution")
    rights_by_field = {
        "rolname": role,
        "direct_role": direct_role,
        "relname": table,
        "attname": column,
        "can_read": True,
        "can_insert": False,
        "can_update": True,
        "can_reference": False,
        "can_grant": False,
        "broad_write": False,
    }
    _validate_column(rights_by_field, config)
    with pytest.raises(ValueError, match="registry_fhir_catalog_permissions_invalid"):
        _validate_column({**rights_by_field, "can_grant": True}, config)


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


@pytest.mark.parametrize(
    "changes",
    [
        {"grant_id": "unprintable\npolicy"},
        {"retained_source_recipes": 1},
        {"observation_contract": "unknown-contract"},
        {"retained_sources": (object(),)},
        {"retained_source_uris": []},
        {"retained_source_uris": (("endpoint-only",),)},
        {"retained_source_uris": (("endpoint", "https://example.test/"),) * 2},
        {"source_profiles": [read_profile()]},
        {"source_profiles": (None,)},
        {"company_crosswalks": []},
        {"company_crosswalks": (("alias-a", "Organization/owner", COMPANY),)},
        {"company_crosswalks": (("registry-manual", "owner", COMPANY),)},
        {"company_crosswalks": (("alias-a", "owner", 1),)},
        {"company_crosswalks": (("alias-a", "owner", "00000000-0000-0000-0000-000000000000"),)},
    ],
)
def test_standing_grant_refuses_ambiguous_retained_authority_and_company_crosswalks(changes):
    with pytest.raises(grants.FHIRCatalogGrantError, match="grant_invalid"):
        standing_grant(**changes)


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_profiles", {}),
        ("company_crosswalks", [None]),
        ("company_crosswalks", [{"source_id": "alias-a", "company_id": COMPANY}]),
        ("retained_source_uris", {}),
        ("retained_source_uris", [None]),
    ],
)
def test_grant_parser_refuses_partial_authority_documents(field, value):
    document = standing_grant().document()
    document[field] = value
    with pytest.raises(grants.FHIRCatalogGrantError, match="grant_invalid"):
        grants.parse_fhir_catalog_standing_grant(document)


@pytest.mark.asyncio
@pytest.mark.parametrize("body_failure", [None, "error", "cancel"])
@pytest.mark.parametrize("rollback_fails", [False, True])
async def test_source_snapshot_closes_failed_or_canceled_reads_without_losing_original_error(
    body_failure, rollback_fails
):
    transaction = Mock(start=AsyncMock(), rollback=AsyncMock())
    reader = Mock(
        transaction=Mock(return_value=transaction),
        execute=AsyncMock(),
        is_in_transaction=Mock(return_value=True),
        terminate=Mock(),
    )
    cleanup_error = RuntimeError("rollback failed")
    if rollback_fails:
        transaction.rollback.side_effect = cleanup_error
    original = {"error": ValueError("read refused"), "cancel": asyncio.CancelledError("read canceled")}.get(
        body_failure
    )
    expected = original or (cleanup_error if rollback_fails else None)
    with pytest.raises(type(expected)) if expected is not None else nullcontext() as caught:
        async with grants._readonly_snapshot(reader):
            if original is not None:
                raise original
    if expected is not None:
        assert caught.value is expected
    transaction.start.assert_awaited_once()
    transaction.rollback.assert_awaited_once()
    assert reader.terminate.call_count == int(rollback_fails)


@pytest.mark.asyncio
async def test_canceling_an_active_source_read_waits_for_snapshot_rollback():
    transaction = Mock(start=AsyncMock(), rollback=AsyncMock())
    reader = Mock(
        transaction=Mock(return_value=transaction),
        execute=AsyncMock(),
        is_in_transaction=Mock(return_value=True),
        terminate=Mock(),
    )
    entered, pending = asyncio.Event(), asyncio.Event()

    async def read():
        async with grants._readonly_snapshot(reader):
            entered.set()
            await pending.wait()

    task = asyncio.create_task(read())
    try:
        await asyncio.wait_for(entered.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)
        transaction.rollback.assert_awaited_once()
        reader.terminate.assert_not_called()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    ["database", "descriptor_database", "replica", "login", "elevated", "permissions", "owner_path", "create"],
)
async def test_source_reader_census_refuses_wrong_database_or_authority(failure):
    identity_by_field = {
        "database_name": "isolated_catalog",
        "database_oid": 104,
        "system_identifier": "123456789",
        "postmaster_started": "2026-01-01T00:00:00Z",
        "recovering": False,
    }
    role_by_field = {
        "rolcanlogin": True,
        **dict.fromkeys(
            ("rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls", "elevated_membership"),
            False,
        ),
    }
    permissions = [{"admitted_role": True, "no_owner_path": True, "no_schema_create": True}]
    source_descriptor = descriptor()
    observed_identity_by_field = dict(identity_by_field)
    match failure:
        case "database":
            observed_identity_by_field["system_identifier"] = "other-system"
        case "descriptor_database":
            source_descriptor["database_oid"] += 1
        case "replica":
            observed_identity_by_field["recovering"] = True
        case "login":
            role_by_field["rolcanlogin"] = False
        case "elevated":
            role_by_field["elevated_membership"] = True
        case "permissions":
            permissions = []
        case _:
            permissions[0]["no_owner_path" if failure == "owner_path" else "no_schema_create"] = False
    reader = Mock(
        fetchrow=AsyncMock(side_effect=[observed_identity_by_field, role_by_field]),
        fetch=AsyncMock(return_value=permissions),
    )
    writer = Mock(fetchrow=AsyncMock(return_value=identity_by_field))
    reason = "database_changed" if failure in {"database", "descriptor_database", "replica"} else "reader_role_unsafe"
    with pytest.raises(grants.FHIRCatalogGrantError, match=reason):
        await grants._reader_identity(reader, writer, (source_descriptor,))


@pytest.mark.asyncio
async def test_changed_proposed_storage_refuses_before_descriptor_discovery(custody):
    grant = standing_grant()
    selected = snapshot(grant, (descriptor(),))
    writer = CatalogConnection((selected,), (descriptor(),))
    runtime = grants.build_fhir_catalog_publication_runtime(grant, None)
    with pytest.raises(grants.FHIRCatalogGrantError, match="snapshot_changed"):
        await runtime.authorize_metadata(
            command=command(grant, selected), snapshot={**selected, "schema_name": "other_snapshot"}, connection=writer
        )
    assert len(writer.queries) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("nested", ["writer_transaction", "source_fence"])
async def test_source_fence_refuses_nested_writer_or_existing_branded_scope(custody, nested):
    grant = standing_grant()
    selected = snapshot(grant)
    writer = CatalogConnection((selected,))
    runtime = grants.build_fhir_catalog_publication_runtime(grant, None)
    if nested == "writer_transaction":
        writer.active = True
        with pytest.raises(grants.FHIRCatalogGrantError, match="source_context_changed"):
            async with runtime.source_fence(snapshot=selected, connection=writer):
                pytest.fail("nested writer snapshot entered")
        assert writer.queries == []
    else:
        async with runtime.source_fence(snapshot=selected, connection=writer):
            with pytest.raises(grants.FHIRCatalogGrantError, match="source_context_changed"):
                async with runtime.source_fence(snapshot=selected, connection=writer):
                    pytest.fail("nested Source fence entered")
        assert grants._CONTEXT.get() is None


@pytest.mark.asyncio
async def test_source_fence_requires_authorization_even_when_no_source_reader_is_needed(custody):
    grant = standing_grant()
    selected = snapshot(grant)
    writer = CatalogConnection((selected,))
    runtime = grants.build_fhir_catalog_publication_runtime(grant, None)
    async with runtime.source_fence(snapshot=selected, connection=writer):
        with pytest.raises(grants.FHIRCatalogGrantError, match="source_authorization_required"):
            await runtime.verify_sources(snapshot=selected, connection=writer)
        await runtime.authorize_metadata(command=command(grant, selected), snapshot=selected, connection=writer)
        assert await runtime.verify_sources(snapshot=selected, connection=writer) is True
    assert grants._CONTEXT.get() is None


@pytest.mark.asyncio
async def test_observation_admission_needs_explicit_contract_even_with_exact_native_profile():
    from tests.test_registry_fhir_catalog_source_producer_grant import observed_descriptor

    source_descriptor = observed_descriptor()
    generation = grants.fhir_catalog_source_generation(source_descriptor)
    source_entries = [{"source_generation": generation, "source_id": "alias-a", "status": "current"}]
    descriptors_by_generation = {generation: source_descriptor}
    with pytest.raises(grants.FHIRCatalogGrantError, match="source_not_admitted"):
        grants._admitted_current(standing_grant(), source_entries, descriptors_by_generation)
    allowed = standing_grant(observation_contract=grants.CATALOG_OBSERVATION_CONTRACT)
    assert grants._admitted_current(allowed, source_entries, descriptors_by_generation) == ()


@pytest.mark.parametrize("document", ["", " ", "{}" + " " * grants.MAX_CONFIG_BYTES])
def test_standing_policy_text_bounds_refuse_before_configuration_can_be_used(document):
    with pytest.raises(grants.FHIRCatalogGrantError):
        grants.parse_fhir_catalog_standing_grant(document)


def _fixture_reader_identity(writer, monkeypatch):
    identity_by_field = {
        "database_name": "isolated_catalog",
        "database_oid": 104,
        "system_identifier": "123456789",
        "postmaster_started": "2026-01-01T00:00:00Z",
        "recovering": False,
    }
    role_by_field = {
        "rolcanlogin": True,
        **dict.fromkeys(
            ("rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls", "elevated_membership"),
            False,
        ),
    }
    original = writer.fetchrow

    async def fetchrow(query, *arguments):
        return identity_by_field if query == grants._IDENTITY_SQL else await original(query, *arguments)

    monkeypatch.setattr(writer, "fetchrow", fetchrow)
    return identity_by_field, role_by_field


@pytest.mark.asyncio
async def test_current_source_fence_keeps_original_reader_until_verified_writer_exit(custody, monkeypatch):
    grant, source_descriptor = standing_grant(), descriptor()
    selected = snapshot(grant, (source_descriptor,))
    writer = CatalogConnection((selected,), (source_descriptor,))
    identity, role = _fixture_reader_identity(writer, monkeypatch)
    transaction = Mock(start=AsyncMock(), rollback=AsyncMock())
    reader = Mock(
        fetchrow=AsyncMock(side_effect=[identity, role]),
        fetch=AsyncMock(return_value=[{"admitted_role": True, "no_owner_path": True, "no_schema_create": True}]),
        execute=AsyncMock(),
        transaction=Mock(return_value=transaction),
        is_in_transaction=Mock(return_value=True),
    )
    events = []

    @asynccontextmanager
    async def acquire():
        events.append("borrow")
        try:
            yield reader
        finally:
            events.append("release")

    @asynccontextmanager
    async def fence(actual_reader, descriptors):
        assert actual_reader is reader and descriptors == (source_descriptor,)
        events.append("fence")
        yield
        events.append("unfence")

    current = AsyncMock()
    monkeypatch.setattr(grants, "fhir_catalog_source_publication_fence", fence)
    monkeypatch.setattr(grants, "require_current_fhir_catalog_sources", current)
    runtime = grants.build_fhir_catalog_publication_runtime(grant, SimpleNamespace(acquire=acquire))
    async with runtime.source_fence(snapshot=selected, connection=writer):
        await runtime.authorize_metadata(command=command(grant, selected), snapshot=selected, connection=writer)
        assert await runtime.verify_sources(snapshot=selected, connection=writer) is True
        current.assert_awaited_once_with(reader, (source_descriptor,), lock=True)
        reader.is_in_transaction.return_value = False
        with pytest.raises(grants.FHIRCatalogGrantError, match="source_context_changed"):
            await runtime.verify_sources(snapshot=selected, connection=writer)
        reader.is_in_transaction.return_value = True
        assert events == ["borrow", "fence"]
    assert events == ["borrow", "fence", "unfence", "release"]
    transaction.rollback.assert_awaited_once()
    assert grants._CONTEXT.get() is None


@pytest.mark.parametrize("entry", [None, ("source",), ["source", "organization", COMPANY]])
def test_programmatic_crosswalks_require_complete_immutable_coordinate_triples(entry):
    with pytest.raises(grants.FHIRCatalogGrantError, match="invalid"):
        standing_grant(company_crosswalks=(entry,))


def test_crosswalk_field_bounds_do_not_override_total_policy_bytes():
    crosswalks = tuple(("s" * 120 + str(index), "o" * 60 + str(index), COMPANY) for index in range(1000, 6000))
    assert len(crosswalks) == grants.MAX_CROSSWALKS
    document = standing_grant().document()
    document["company_crosswalks"] = [
        {"source_id": source, "organization_id": organization, "company_id": company}
        for source, organization, company in crosswalks
    ]
    assert len(canonical_json(document).encode()) > grants.MAX_CONFIG_BYTES
    with pytest.raises(grants.FHIRCatalogGrantError, match="invalid"):
        standing_grant(company_crosswalks=crosswalks)


@pytest.mark.asyncio
async def test_invalid_fence_digest_refuses_before_snapshot_or_source_reads():
    writer = CatalogConnection(())
    runtime = grants.build_fhir_catalog_publication_runtime(standing_grant(), None)
    with pytest.raises(grants.FHIRCatalogGrantError, match="invalid"):
        async with runtime.source_fence(snapshot={"metadata_generation": "untyped-generation"}, connection=writer):
            pytest.fail("invalid digest entered a Source fence")
    assert writer.queries == []


@pytest.mark.parametrize("grant", [None, {}, SimpleNamespace()])
def test_observation_verifier_requires_a_real_standing_policy_before_reader_access(grant):
    with pytest.raises(grants.FHIRCatalogGrantError, match="invalid"):
        grants.build_fhir_catalog_observation_verifier(grant, None)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["policy", "storage"])
async def test_observation_seal_rejects_changed_policy_or_storage_before_source_entry(monkeypatch, changed):
    from tests.test_registry_fhir_catalog_source_producer_grant import verifier_case

    case = verifier_case(monkeypatch)
    callback = grants.build_fhir_catalog_observation_verifier(case.grant, case.pool, source_export=case.factory)
    actual_grant = standing_grant(version=2) if changed == "policy" else case.grant
    target = SimpleNamespace(schema_name="different_snapshot") if changed == "storage" else case.target
    with pytest.raises(grants.FHIRCatalogGrantError, match="policy_changed"):
        await callback(
            connection=case.connection, copy_target=target, snapshot=case.snapshot, standing_grant=actual_grant
        )
    grants._descriptors.assert_not_awaited()
    case.factory.assert_not_called()
    case.pool.acquire.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("history", ["removed", "unproved_last_good"])
async def test_observation_history_must_retain_exact_old_sources_before_replay(monkeypatch, history):
    from tests.test_registry_fhir_catalog_source_producer_grant import observed_descriptor, verifier_case, verify

    case = verifier_case(monkeypatch)
    if history == "removed":
        old = observed_descriptor(dataset="removed-dataset")
        prior = snapshot(case.grant, (old,))
        grants._prior_observations.return_value = (prior, {grants.fhir_catalog_source_generation(old): old})
    else:
        case.snapshot = snapshot(case.grant, tuple(case.descriptors.values()), status="last_good")
    reason = "observation_history_changed" if history == "removed" else "observation_history_required"
    with pytest.raises(grants.FHIRCatalogGrantError, match=reason):
        await verify(case)
    case.factory.assert_not_called()
    case.pool.acquire.assert_not_called()


@pytest.mark.asyncio
async def test_carried_observation_requires_its_protected_accounting_row_before_source_entry(monkeypatch):
    from tests.test_registry_fhir_catalog_source_producer_grant import verifier_case, verify

    accounting = grants._observation_accounting
    case = verifier_case(monkeypatch)
    prior = snapshot(case.grant, tuple(case.descriptors.values()), schema_name="previous_snapshot")
    grants._prior_observations.return_value = (prior, case.descriptors)
    monkeypatch.setattr(grants, "_observation_accounting", accounting)
    case.connection.fetchrow = AsyncMock(return_value=None)
    with pytest.raises(grants.FHIRCatalogGrantError, match="observation_accounting_changed"):
        await verify(case)
    assert "registry_fhir_catalog_source WHERE source_generation=$1" in case.connection.fetchrow.await_args.args[0]
    case.factory.assert_not_called()
    case.pool.acquire.assert_not_called()


@pytest.mark.asyncio
async def test_scoped_source_requires_native_reader_instead_of_source_export(monkeypatch):
    from tests.test_registry_fhir_catalog_source_producer_grant import scoped_observed_descriptor, verifier_case, verify

    selected, profile = scoped_observed_descriptor()
    case = verifier_case(monkeypatch, (selected,), (profile,))
    case.pool = None
    with pytest.raises(grants.FHIRCatalogGrantError, match="reader_required"):
        await verify(case)
    case.factory.assert_not_called()
