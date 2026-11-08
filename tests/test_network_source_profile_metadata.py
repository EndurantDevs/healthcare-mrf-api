# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""CMS metadata-only API wiring; protocol doubles provide no native custody proof."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import network_cms_registry_address_capture as clones
from process import network_cms_registry_source_pair as pairs
from process import network_fhir_membership_source as membership
from process import network_fhir_source_epoch as epoch
from process import network_read_identity as consumed
from process.network_compact_layout import CompactAddressRelation
from tests.test_network_cms_registry_source_pair import _retained_pair


class Connection:
    def __init__(self):
        self.active, self.modes, self.calls, self.events = True, ("repeatable read", "on"), [], []
        self.comment, self.payload, self.matches = None, None, True
        self.owner_oid = 31

    def is_in_transaction(self):
        return self.active

    async def fetchrow(self, query, *arguments):
        self.calls.append((query, arguments))
        if query.startswith("SELECT current_setting('transaction_isolation')"):
            return self.modes
        if query.startswith("SELECT * FROM pg_roles"):
            return {
                "oid": self.owner_oid,
                **dict.fromkeys(
                    ("rolcanlogin", "rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls"), False
                ),
            }
        assert "FROM" in query and "provider_directory_endpoint_dataset" in query
        self.events.append("data")
        return self.payload

    async def fetchval(self, query, *arguments):
        self.calls.append((query, arguments))
        if query == "SELECT pg_backend_pid()":
            return 321
        if query == "SHOW transaction_isolation":
            return self.modes[0]
        if query == "SHOW transaction_read_only":
            return self.modes[1]
        if query.startswith("SELECT obj_description"):
            return self.comment
        if query.startswith("SELECT oid FROM pg_roles"):
            return self.owner_oid
        if query.startswith("SELECT nspowner"):
            return self.owner_oid
        if query.startswith("SELECT EXISTS (SELECT 1 FROM"):
            self.events.append("data")
            return self.matches
        return False

    async def execute(self, query, *arguments):
        self.calls.append((query, arguments))
        assert query.startswith("LOCK TABLE")


def _relations(pin, owner_oid):
    return [
        {
            "relname": name,
            "oid": oid,
            "relkind": b"r",
            "relpersistence": b"p",
            "relowner": owner_oid,
            "nspowner": owner_oid,
            "schema_oid": pin.schema_oid,
            "relhastriggers": False,
            "relrowsecurity": False,
            "relforcerowsecurity": False,
            "inherited": False,
            "defaults": False,
            "indexes": json.dumps([{"owner_oid": owner_oid, "valid": True, "ready": True, "live": True}]),
        }
        for name, oid in pin.relation_oids
    ]


@pytest.mark.parametrize("api_read", [False, True])
async def test_epoch_catalog_keeps_all_checks_and_only_api_omits_lock(monkeypatch, api_read):
    retained = _retained_pair().recipe.source_pin.retained_epoch
    connection = Connection()
    catalog_rows = _relations(retained, connection.owner_oid)
    read = AsyncMock(return_value=catalog_rows)
    profiles = Mock()
    monkeypatch.setattr(epoch, "_catalog_relations", read)
    monkeypatch.setattr(epoch, "_require_index_profiles", profiles)
    actual = await epoch._catalog(
        connection,
        retained.schema_name,
        retained.owner_role,
        retained.runtime_roles,
        **({"api_read": True} if api_read else {}),
    )
    assert actual == (retained.schema_oid, retained.relation_oids, epoch._hash(catalog_rows))
    profiles.assert_called_once_with(catalog_rows, connection.owner_oid)
    assert sum(query.startswith("LOCK TABLE") for query, _ in connection.calls) == int(not api_read)
    assert any("has_parameter_privilege" in query and "is_grantable" in query for query, _ in connection.calls)


@pytest.mark.parametrize(
    "damage", ["missing", "owner", "schema_owner", "trigger", "rls", "inherit", "temporary", "index", "privileges"]
)
async def test_api_epoch_catalog_retains_native_custody_refusals(monkeypatch, damage):
    retained = _retained_pair().recipe.source_pin.retained_epoch
    connection = Connection()
    catalog_rows = _relations(retained, connection.owner_oid)
    changes_by_fault = {
        "owner": ("relowner", 999),
        "schema_owner": ("nspowner", 999),
        "trigger": ("relhastriggers", True),
        "rls": ("relrowsecurity", True),
        "inherit": ("inherited", True),
        "temporary": ("relpersistence", b"t"),
    }
    if damage in changes_by_fault:
        field, changed_field = changes_by_fault[damage]
        catalog_rows[0][field] = changed_field
    if damage == "missing":
        catalog_rows = catalog_rows[:-1]
    profiles = Mock(side_effect=epoch.FHIRSourceEpochError("bad index") if damage == "index" else None)
    monkeypatch.setattr(epoch, "_catalog_relations", AsyncMock(return_value=catalog_rows))
    monkeypatch.setattr(epoch, "_require_index_profiles", profiles)
    if damage == "privileges":
        previous = connection.fetchval

        async def unsafe(query, *arguments):
            return True if "WITH configured AS" in query else await previous(query, *arguments)

        connection.fetchval = unsafe
    with pytest.raises(epoch.FHIRSourceEpochError):
        await epoch._catalog(
            connection, retained.schema_name, retained.owner_role, retained.runtime_roles, api_read=True
        )
    assert not any(query.startswith("LOCK TABLE") for query, _ in connection.calls)


@pytest.mark.parametrize("api_read", [False, True])
async def test_epoch_requirement_preserves_comment_and_catalog_binding(monkeypatch, api_read):
    pair = _retained_pair()
    retained = pair.recipe.source_pin.retained_epoch
    connection = Connection()
    connection.comment = epoch._receipt_comment(retained)
    catalog = AsyncMock(return_value=(retained.schema_oid, retained.relation_oids, retained.catalog_sha256))
    content = AsyncMock(side_effect=AssertionError("metadata mode read full content"))
    monkeypatch.setattr(epoch, "_catalog", catalog)
    monkeypatch.setattr(epoch, "_content", content)
    assert (
        await epoch.require_retained_cms_fhir_source_epoch(
            connection,
            pair.proof.source_pin,
            retained,
            verify_content=False,
            **({"api_read": True} if api_read else {}),
        )
        == retained
    )
    assert catalog.await_args.kwargs == ({"api_read": True} if api_read else {})
    content.assert_not_awaited()


@pytest.mark.parametrize("api_read", [False, True])
async def test_clone_metadata_retains_ownership_role_trigger_index_checks(monkeypatch, api_read):
    pair, connection = _retained_pair(), Connection()
    catalog_rows = _relations(pair.address_ownership, connection.owner_oid)
    ownership, triggers, closed = AsyncMock(return_value=pair.address_ownership), AsyncMock(), AsyncMock()
    content = AsyncMock(side_effect=AssertionError("metadata mode called content capturer"))
    monkeypatch.setattr(clones, "verify_entity_address_archive_stage_ownership", ownership)
    monkeypatch.setattr(clones, "_catalog_relations", AsyncMock(return_value=catalog_rows))
    monkeypatch.setattr(clones, "_require_native_constraint_triggers", triggers)
    monkeypatch.setattr(clones, "_closed", closed)
    monkeypatch.setattr(clones, "capture_entity_address_archive_receipt", content)
    actual = await clones.address_clone_identity(
        clones.NativeReceiptSession(connection),
        pair.address_ownership,
        pair.owner_role,
        pair.runtime_roles,
        verify_content=False,
        **({"api_read": True} if api_read else {}),
    )
    assert actual == (None, epoch._hash(catalog_rows))
    ownership.assert_awaited_once()
    triggers.assert_awaited_once_with(connection, pair.address_ownership.schema_oid)
    closed.assert_awaited_once()
    content.assert_not_awaited()
    assert sum(query.startswith("LOCK TABLE") for query, _ in connection.calls) == int(not api_read)


@pytest.mark.parametrize("entry", ["epoch", "clone", "pair"])
@pytest.mark.parametrize("requested_content", [True, 0, None])
async def test_api_metadata_cannot_silently_replace_requested_content(entry, requested_content):
    pair, connection = _retained_pair(), Connection()
    with pytest.raises(ValueError):
        if entry == "epoch":
            await epoch.require_retained_cms_fhir_source_epoch(
                connection,
                pair.proof.source_pin,
                pair.recipe.source_pin.retained_epoch,
                verify_content=requested_content,
                api_read=True,
            )
        if entry == "clone":
            await clones.address_clone_identity(
                clones.NativeReceiptSession(connection),
                pair.address_ownership,
                pair.owner_role,
                pair.runtime_roles,
                verify_content=requested_content,
                api_read=True,
            )
        if entry == "pair":
            await pairs.require_registry_cms_source_pair_connection(
                connection, pair, verify_content=requested_content, api_read=True
            )
    assert not connection.calls


class GuardedRow(dict):
    def __init__(self, values, connection):
        super().__init__(values)
        self.connection = connection

    def __getitem__(self, key):
        assert self.connection.events[-1] == "guard"
        return super().__getitem__(key)


class GuardedMatch:
    def __init__(self, connection):
        self.connection = connection

    def __bool__(self):
        assert self.connection.events[-1] == "guard"
        return True


def _consumption_spy(monkeypatch, connection, retained, *, fail=False):
    async def guard(actual, relations, *, expected_backend_pid):
        assert actual is connection and expected_backend_pid == 321
        assert connection.events[-1] == "data"
        assert relations == (
            CompactAddressRelation(
                retained.schema_name,
                "provider_directory_endpoint_dataset",
                dict(retained.relation_oids)["provider_directory_endpoint_dataset"],
            ),
        )
        if fail:
            raise consumed.NetworkReadIdentityError("synthetic physical mismatch")
        connection.events.append("guard")

    checked = AsyncMock(side_effect=guard)
    monkeypatch.setattr(consumed, "verify_consumed_network_relations", checked)
    return checked


def _publication_row(pair):
    pin = pair.recipe.source_pin
    return {
        "endpoint_id": pin.endpoint_id,
        "dataset_hash": pin.dataset_sha256,
        "acquisition_root_run_id": pair.proof.cms_authority[4],
        "status": "validated",
        "published_at": None,
        "content_proof_admission_sha256": pair.proof.expected_admission_sha256,
        "publication_metadata_sha256": pair.proof.expected_metadata_sha256,
        "bindings": {
            **pairs.asdict(pair.proof.binding_coordinates),
            "source_key_kind": "organization_resource_id",
            "alias_scope": pin.alias_scope,
        },
        "semantic_as_of": pin.as_of,
    }


@pytest.mark.parametrize("fail", [False, True])
async def test_publication_metadata_dataset_read_is_guarded_before_any_field_use(monkeypatch, fail):
    pair, connection = _retained_pair(), Connection()
    connection.payload = GuardedRow(_publication_row(pair), connection)
    checked = _consumption_spy(monkeypatch, connection, pair.recipe.source_pin.retained_epoch, fail=fail)
    if fail:
        with pytest.raises(consumed.NetworkReadIdentityError):
            await pairs._retained_source_publication_time(connection, pair, api_read=True)
    else:
        assert await pairs._retained_source_publication_time(connection, pair, api_read=True) is None
    checked.assert_awaited_once()


@pytest.mark.parametrize("fail", [False, True])
async def test_api_epoch_admission_guard_precedes_boolean_use(monkeypatch, fail):
    pair, connection = _retained_pair(), Connection()
    retained = pair.recipe.source_pin.retained_epoch
    connection.matches = GuardedMatch(connection)
    catalog = AsyncMock(return_value=retained)
    monkeypatch.setattr(membership, "require_retained_cms_fhir_source_epoch", catalog)
    checked = _consumption_spy(monkeypatch, connection, retained, fail=fail)
    if fail:
        with pytest.raises(consumed.NetworkReadIdentityError):
            await membership._require_pinned_source(connection, pair.recipe.source_pin, api_read=True)
    else:
        assert await membership._require_pinned_source(connection, pair.recipe.source_pin, api_read=True) is retained
    assert catalog.await_args.kwargs == {"verify_content": False, "api_read": True}
    assert not any("WHERE false" in query or query.startswith("LOCK TABLE") for query, _ in connection.calls)
    checked.assert_awaited_once()


@pytest.mark.parametrize("kind", ["capture", "unretained", "successor"])
async def test_unassigned_generic_or_live_modes_fail_closed_without_lock_fallback(monkeypatch, kind):
    connection = Connection()
    pin = SimpleNamespace(
        retained_epoch=object() if kind != "unretained" else None,
        retained_capture=object() if kind == "capture" else None,
        retained_successor=object() if kind == "successor" else None,
    )
    verifier = AsyncMock(side_effect=AssertionError("unassigned mode reached epoch verifier"))
    monkeypatch.setattr(membership, "require_retained_cms_fhir_source_epoch", verifier)
    with pytest.raises(membership.FHIRMembershipSourceError):
        await membership._require_pinned_source(connection, pin, api_read=True)
    verifier.assert_not_awaited()
    assert not any(query.startswith("LOCK TABLE") for query, _ in connection.calls)


@pytest.mark.parametrize("entry", ["epoch", "clone", "pair_connection", "pair_resolver", "pin", "flat_pin"])
@pytest.mark.parametrize("modes", [("repeatable read", "off"), ("read committed", "on")])
async def test_api_metadata_requires_genuine_read_only_repeatable_transaction(monkeypatch, entry, modes):
    pair, connection = _retained_pair(), Connection()
    connection.modes = modes
    monkeypatch.setattr(pairs, "_normalize_receipt_session", AsyncMock())
    with pytest.raises(ValueError):
        if entry == "epoch":
            await epoch.require_retained_cms_fhir_source_epoch(
                connection,
                pair.proof.source_pin,
                pair.recipe.source_pin.retained_epoch,
                verify_content=False,
                api_read=True,
            )
        if entry == "clone":
            await clones.address_clone_identity(
                clones.NativeReceiptSession(connection),
                pair.address_ownership,
                pair.owner_role,
                pair.runtime_roles,
                verify_content=False,
                api_read=True,
            )
        if entry == "pair_connection":
            await pairs.require_registry_cms_source_pair_connection(
                connection, pair, verify_content=False, api_read=True
            )
        if entry == "pair_resolver":
            await pairs.resolve_committed_registry_cms_source_pair(
                clones.NativeReceiptSession(connection), pair.recipe, api_read=True
            )
        if entry == "pin":
            await membership._require_pinned_source(connection, pair.recipe.source_pin, api_read=True)
        if entry == "flat_pin":
            await membership._require_flat_pinned_source(connection, pair.recipe.source_pin, None, api_read=True)
    assert not any(query.startswith("LOCK TABLE") for query, _ in connection.calls)


@pytest.mark.parametrize("api_read", [False, True])
@pytest.mark.parametrize("resolver", [False, True])
async def test_pair_mode_propagates_without_changing_default_child_options(monkeypatch, api_read, resolver):
    pair, connection = _retained_pair(), Connection()
    connection.comment = pairs._pair_comment(pair) + pairs._json(pair.as_dict())
    connection.payload = GuardedRow(_publication_row(pair), connection) if api_read else _publication_row(pair)
    monkeypatch.setattr(pairs, "_normalize_receipt_session", AsyncMock())
    source = AsyncMock(return_value=pair.recipe.source_pin.retained_epoch)
    clone = AsyncMock(return_value=(None, pair.address_catalog_sha256))
    monkeypatch.setattr(pairs, "require_retained_cms_fhir_source_epoch", source)
    monkeypatch.setattr(pairs, "address_clone_identity", clone)
    monkeypatch.setattr(
        pairs, "_read_committed_pair", AsyncMock(return_value=(pair, pair.address_ownership.schema_oid))
    )
    monkeypatch.setattr(pairs, "_publication", AsyncMock(return_value=json.loads(pair.cms_receipt_payload)))
    checked = _consumption_spy(monkeypatch, connection, pair.recipe.source_pin.retained_epoch)
    options = {"api_read": True} if api_read else {}
    if resolver:
        actual = await pairs.resolve_committed_registry_cms_source_pair(
            clones.NativeReceiptSession(connection), pair.recipe, **options
        )
        assert actual.pair == pair and actual.source_published_at is None
        assert checked.await_count == int(api_read)
    else:
        assert (
            await pairs.require_registry_cms_source_pair_connection(connection, pair, verify_content=False, **options)
            == pair
        )
        checked.assert_not_awaited()
    assert source.await_args.kwargs == clone.await_args.kwargs == {"verify_content": False, **options}


@pytest.mark.parametrize("entry", ["epoch", "clone", "pair_connection", "pair_resolver", "pin", "flat_pin"])
@pytest.mark.parametrize("invalid", [0, 1, None, "true"])
async def test_api_flag_is_strict_and_never_an_authority_marker(entry, invalid):
    pair, connection = _retained_pair(), Connection()
    with pytest.raises(ValueError):
        if entry == "epoch":
            await epoch.require_retained_cms_fhir_source_epoch(
                connection,
                pair.proof.source_pin,
                pair.recipe.source_pin.retained_epoch,
                verify_content=False,
                api_read=invalid,
            )
        if entry == "clone":
            await clones.address_clone_identity(
                clones.NativeReceiptSession(connection),
                pair.address_ownership,
                pair.owner_role,
                pair.runtime_roles,
                verify_content=False,
                api_read=invalid,
            )
        if entry == "pair_connection":
            await pairs.require_registry_cms_source_pair_connection(
                connection, pair, verify_content=False, api_read=invalid
            )
        if entry == "pair_resolver":
            await pairs.resolve_committed_registry_cms_source_pair(
                clones.NativeReceiptSession(connection), pair.recipe, api_read=invalid
            )
        if entry == "pin":
            await membership._require_pinned_source(connection, pair.recipe.source_pin, api_read=invalid)
        if entry == "flat_pin":
            await membership._require_flat_pinned_source(connection, pair.recipe.source_pin, None, api_read=invalid)
    assert not connection.calls
