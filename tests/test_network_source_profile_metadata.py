# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""CMS source admission for API reads by name; protocol doubles provide no native custody proof."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import asyncpg
import pytest

from process import network_cms_registry_address_capture as clones
from process import network_cms_registry_source_pair as pairs
from process import network_fhir_membership_source as membership
from process import network_fhir_source_epoch as epoch
from process import network_read_identity as consumed
from process.network_compact_layout import CompactAddressRelation
from tests.test_network_cms_registry_source_pair import _retained_pair, _witness_pair

DATASET_TABLE = "provider_directory_endpoint_dataset"


class Connection:
    def __init__(self):
        self.active, self.modes, self.calls, self.events = True, ("repeatable read", "on"), [], []
        self.comment, self.payload, self.matches = None, None, True
        self.owner_oid = 31
        # A copied database gives every table an identifier that no pin recorded.
        self.oid_by_table = {name: 900000 + index for index, name in enumerate(sorted(epoch._TABLES))}

    def is_in_transaction(self):
        return self.active

    async def fetch(self, query, *arguments):
        self.calls.append((query, arguments))
        assert all(check in query for check in ("relrowsecurity", "relispartition", "pg_inherits", "pg_rewrite"))
        return [{"relname": name, "oid": oid} for name, oid in self.oid_by_table.items() if name in arguments[1]]

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
        assert "FROM" in query and DATASET_TABLE in query
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
        if query.startswith("SELECT to_regclass"):
            return self.oid_by_table.get(arguments[0].rpartition(".")[2])
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


async def test_epoch_catalog_keeps_all_checks_under_its_lock(monkeypatch):
    retained = _retained_pair().recipe.source_pin.retained_epoch
    connection = Connection()
    catalog_rows = _relations(retained, connection.owner_oid)
    profiles = Mock()
    monkeypatch.setattr(epoch, "_catalog_relations", AsyncMock(return_value=catalog_rows))
    monkeypatch.setattr(epoch, "_require_index_profiles", profiles)
    actual = await epoch._catalog(connection, retained.schema_name, retained.owner_role, retained.runtime_roles)
    assert actual == (retained.schema_oid, retained.relation_oids, epoch._hash(catalog_rows))
    profiles.assert_called_once_with(catalog_rows, connection.owner_oid)
    assert sum(query.startswith("LOCK TABLE") for query, _ in connection.calls) == 1
    assert any("has_parameter_privilege" in query and "is_grantable" in query for query, _ in connection.calls)


async def test_epoch_requirement_preserves_comment_and_catalog_binding(monkeypatch):
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
            connection, pair.proof.source_pin, retained, verify_content=False
        )
        == retained
    )
    assert catalog.await_args.kwargs == {}
    content.assert_not_awaited()


async def test_clone_metadata_retains_ownership_role_trigger_index_checks(monkeypatch):
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
    )
    assert actual == (None, epoch._hash(catalog_rows))
    ownership.assert_awaited_once()
    triggers.assert_awaited_once_with(connection, pair.address_ownership.schema_oid)
    closed.assert_awaited_once()
    content.assert_not_awaited()
    assert sum(query.startswith("LOCK TABLE") for query, _ in connection.calls) == 1


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
            CompactAddressRelation(retained.schema_name, DATASET_TABLE, connection.oid_by_table[DATASET_TABLE]),
        )
        if fail:
            raise consumed.NetworkReadIdentityError("synthetic physical mismatch")
        connection.events.append("guard")

    checked = AsyncMock(side_effect=guard)
    monkeypatch.setattr(consumed, "verify_consumed_network_relations", checked)
    return checked


def _recorded_identifiers(pair):
    retained = pair.recipe.source_pin.retained_epoch
    return {
        retained.schema_oid,
        pair.address_ownership.schema_oid,
        pair.recipe.source_pin.resource_table_oid,
        *(oid for _, oid in retained.relation_oids),
        *(oid for _, oid in pair.address_ownership.relation_oids),
    }


def _assert_no_recorded_oid_sent(connection, pair):
    recorded = _recorded_identifiers(pair)
    assert not recorded & {321, *connection.oid_by_table.values()}
    integers = [argument for _, arguments in connection.calls for argument in arguments if type(argument) is int]
    assert not recorded & set(integers)
    assert not any("WHERE false" in query for query, _ in connection.calls)
    assert all(
        query.endswith(" IN ACCESS SHARE MODE") for query, _ in connection.calls if query.startswith("LOCK TABLE")
    )


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
    _assert_no_recorded_oid_sent(connection, pair)


@pytest.mark.parametrize("fail", [False, True])
async def test_api_admission_names_current_epoch_tables_and_guards_before_boolean_use(monkeypatch, fail):
    pair, connection = _retained_pair(), Connection()
    pin = pair.recipe.source_pin
    connection.matches = GuardedMatch(connection)
    custody = AsyncMock(side_effect=AssertionError("API admission compared recorded custody"))
    monkeypatch.setattr(membership, "require_retained_cms_fhir_source_epoch", custody)
    checked = _consumption_spy(monkeypatch, connection, pin.retained_epoch, fail=fail)
    if fail:
        with pytest.raises(consumed.NetworkReadIdentityError):
            await membership._require_pinned_source(connection, pin, api_read=True)
    else:
        assert await membership._require_pinned_source(connection, pin, api_read=True) == tuple(
            CompactAddressRelation(pin.read_schema_name, name, oid) for name, oid in connection.oid_by_table.items()
        )
    checked.assert_awaited_once()
    _assert_no_recorded_oid_sent(connection, pair)
    # The share lock on every epoch table precedes the row filtering check, so the check cannot go stale.
    queries = [query for query, _ in connection.calls]
    lock = next(index for index, query in enumerate(queries) if query.startswith("LOCK TABLE"))
    assert lock < next(index for index, query in enumerate(queries) if "relrowsecurity" in query)
    assert all(f'"{name}"' in queries[lock] for name in epoch._TABLES)


async def test_api_admission_refuses_when_the_epoch_tables_cannot_be_locked(monkeypatch):
    pair, connection = _retained_pair(), Connection()

    async def unavailable(query, *arguments):
        raise asyncpg.LockNotAvailableError("synthetic lock timeout")

    connection.execute = unavailable
    monkeypatch.setattr(consumed, "verify_consumed_network_relations", AsyncMock())
    with pytest.raises(membership.FHIRMembershipSourceError):
        await membership._require_pinned_source(connection, pair.recipe.source_pin, api_read=True)
    assert connection.events == [] and not any("relrowsecurity" in query for query, _ in connection.calls)


async def test_api_admission_refuses_absent_or_row_filtered_epoch_table(monkeypatch):
    pair, connection = _retained_pair(), Connection()
    # The catalog query returns only plain permanent tables without row filtering.
    del connection.oid_by_table["provider_directory_dataset_resource"]
    checked = AsyncMock()
    monkeypatch.setattr(consumed, "verify_consumed_network_relations", checked)
    with pytest.raises(membership.FHIRMembershipSourceError):
        await membership._require_pinned_source(connection, pair.recipe.source_pin, api_read=True)
    assert connection.events == []
    checked.assert_not_awaited()


async def test_api_admission_of_a_live_schema_pin_compares_no_recorded_table(monkeypatch):
    pair, connection = _retained_pair(), Connection()
    pin = pair.proof.source_pin
    connection.fetch = AsyncMock(side_effect=AssertionError("a live schema has no epoch tables"))
    checked = AsyncMock()
    monkeypatch.setattr(consumed, "verify_consumed_network_relations", checked)
    assert await membership._require_pinned_source(connection, pin, {"producer_id": "example"}, api_read=True) == ()
    query, arguments = next(call for call in connection.calls if call[0].startswith("SELECT EXISTS"))
    assert "published_at IS NOT NULL" in query and "relkind='r'" in query and "oid=$" not in query
    assert arguments[0] == pin.schema_name and arguments[-1] == '{"producer_id": "example"}'
    checked.assert_awaited_once_with(connection, (), expected_backend_pid=321)
    _assert_no_recorded_oid_sent(connection, pair)


@pytest.mark.parametrize("kind", ["capture", "witness", "successor", "supplied_read"])
async def test_api_admission_refuses_generic_successor_and_supplied_read_modes(kind):
    connection = Connection()
    pin = SimpleNamespace(
        retained_epoch=object(),
        retained_capture=object() if kind == "capture" else None,
        source_witness_capture=object() if kind == "witness" else None,
        retained_successor=object() if kind == "successor" else None,
    )
    with pytest.raises(membership.FHIRMembershipSourceError):
        await membership._require_pinned_source(
            connection, pin, None, object() if kind == "supplied_read" else None, api_read=True
        )
    assert not connection.calls


@pytest.mark.parametrize("entry", ["pair_resolver", "pin"])
@pytest.mark.parametrize("modes", [("repeatable read", "off"), ("read committed", "on")])
async def test_api_reads_require_genuine_read_only_repeatable_transaction(entry, modes):
    pair, connection = _retained_pair(), Connection()
    connection.modes = modes
    with pytest.raises(ValueError):
        if entry == "pair_resolver":
            await pairs.resolve_committed_registry_cms_source_pair(
                clones.NativeReceiptSession(connection), pair.recipe, api_read=True
            )
        if entry == "pin":
            await membership._require_pinned_source(connection, pair.recipe.source_pin, api_read=True)
    assert connection.events == [] and not any(query.startswith("LOCK TABLE") for query, _ in connection.calls)


@pytest.mark.parametrize("entry", ["pair_resolver", "pin"])
@pytest.mark.parametrize("invalid", [0, 1, None, "true"])
async def test_api_flag_is_strict_and_never_an_authority_marker(entry, invalid):
    pair, connection = _retained_pair(), Connection()
    with pytest.raises(ValueError):
        if entry == "pair_resolver":
            await pairs.resolve_committed_registry_cms_source_pair(
                clones.NativeReceiptSession(connection), pair.recipe, api_read=invalid
            )
        if entry == "pin":
            await membership._require_pinned_source(connection, pair.recipe.source_pin, api_read=invalid)
    assert not connection.calls


def _pair_resolver_doubles(monkeypatch, pair, connection, *, schema_oid):
    doubles = SimpleNamespace(
        source=AsyncMock(return_value=pair.recipe.source_pin.retained_epoch),
        clone=AsyncMock(return_value=(None, pair.address_catalog_sha256)),
        witness=AsyncMock(),
        publication=AsyncMock(return_value=json.loads(pair.cms_receipt_payload)),
    )
    monkeypatch.setattr(pairs, "require_retained_cms_fhir_source_epoch", doubles.source)
    monkeypatch.setattr(pairs, "address_clone_identity", doubles.clone)
    monkeypatch.setattr(pairs, "require_registry_cms_address_copy_witness", doubles.witness)
    monkeypatch.setattr(pairs, "_publication", doubles.publication)
    monkeypatch.setattr(pairs, "_read_committed_pair", AsyncMock(return_value=(pair, schema_oid)))
    doubles.checked = _consumption_spy(monkeypatch, connection, pair.recipe.source_pin.retained_epoch)
    return doubles


@pytest.mark.parametrize("copied", [False, True])
async def test_api_pair_resolver_reads_by_name_and_compares_no_physical_identity(monkeypatch, copied):
    pair, connection = _witness_pair(), Connection()
    connection.payload = GuardedRow(_publication_row(pair), connection)
    doubles = _pair_resolver_doubles(
        monkeypatch, pair, connection, schema_oid=pair.address_ownership.schema_oid + (700000 if copied else 0)
    )
    actual = await pairs.resolve_committed_registry_cms_source_pair(
        clones.NativeReceiptSession(connection), pair.recipe, api_read=True
    )
    assert actual.pair == pair and actual.source_published_at is None
    for custody in (doubles.source, doubles.clone, doubles.witness):
        custody.assert_not_awaited()
    doubles.publication.assert_awaited_once()
    doubles.checked.assert_awaited_once()
    _assert_no_recorded_oid_sent(connection, pair)
    doubles.publication.return_value = {"another": "receipt"}
    with pytest.raises(ValueError, match="^registry_cms_source_pair_invalid$"):
        await pairs.resolve_committed_registry_cms_source_pair(
            clones.NativeReceiptSession(connection), pair.recipe, api_read=True
        )


@pytest.mark.parametrize("copied", [False, True])
async def test_pair_resolver_outside_api_reads_keeps_every_custody_check(monkeypatch, copied):
    pair, connection = _witness_pair(), Connection()
    connection.payload = _publication_row(pair)
    doubles = _pair_resolver_doubles(
        monkeypatch, pair, connection, schema_oid=pair.address_ownership.schema_oid + int(copied)
    )
    operation = pairs.resolve_committed_registry_cms_source_pair(clones.NativeReceiptSession(connection), pair.recipe)
    if copied:
        with pytest.raises(ValueError, match="^registry_cms_source_pair_invalid$"):
            await operation
        doubles.source.assert_not_awaited()
        return
    assert (await operation).pair == pair
    assert doubles.source.await_args.kwargs == doubles.clone.await_args.kwargs == {"verify_content": False}
    doubles.witness.assert_awaited_once()
    doubles.checked.assert_not_awaited()
