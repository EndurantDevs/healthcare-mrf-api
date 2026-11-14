# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""CMS source admission for API reads by name; protocol doubles provide no native custody proof."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import pytest

from process import network_cms_registry_address_capture as clones
from process import network_cms_registry_source_pair as pairs
from process import network_fhir_membership_source as membership
from process import network_fhir_source_epoch as epoch
from process.network_compact_layout import CompactAddressRelation
from tests.test_network_cms_registry_source_pair import _retained_pair, _witness_pair

DATASET_TABLE = "provider_directory_endpoint_dataset"


class Connection:
    def __init__(self):
        self.active, self.modes, self.calls, self.events = True, ("repeatable read", "on"), [], []
        self.comment, self.payload, self.matches = None, None, True
        self.owner_oid = 31
        self.tables = set(epoch._TABLES)

    def is_in_transaction(self):
        return self.active

    async def fetch(self, query, *arguments):
        self.calls.append((query, arguments))
        assert all(check in query for check in ("relrowsecurity", "relispartition", "pg_inherits", "pg_rewrite"))
        assert "SELECT c.relname::text FROM" in query
        return [{"relname": name} for name in sorted(self.tables) if name in arguments[1]]

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


async def test_clone_metadata_retains_ownership_role_trigger_index_checks(monkeypatch):
    pair, connection = _retained_pair(), Connection()
    catalog_rows = _relations(pair.address_ownership, connection.owner_oid)
    ownership, triggers = AsyncMock(return_value=pair.address_ownership), AsyncMock()
    content = AsyncMock(side_effect=AssertionError("metadata mode called content capturer"))
    monkeypatch.setattr(clones, "verify_entity_address_archive_stage_ownership", ownership)
    monkeypatch.setattr(clones, "_catalog_relations", AsyncMock(return_value=catalog_rows))
    monkeypatch.setattr(clones, "_require_native_constraint_triggers", triggers)
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
    content.assert_not_awaited()
    assert sum(query.startswith("LOCK TABLE") for query, _ in connection.calls) == 1


def _recorded_identifiers(pair):
    return {
        pair.address_ownership.schema_oid,
        *(oid for _, oid in pair.address_ownership.relation_oids),
    }


def _assert_no_recorded_oid_sent(connection, pair):
    recorded = _recorded_identifiers(pair)
    integers = [argument for _, arguments in connection.calls for argument in arguments if type(argument) is int]
    assert not recorded & set(integers)
    assert not any("pg_backend_pid" in query or "to_regclass" in query for query, _ in connection.calls)
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


@pytest.mark.parametrize(
    "damage",
    [
        None,
        "endpoint_id",
        "dataset_hash",
        "acquisition_root_run_id",
        "status",
        "content_proof_admission_sha256",
        "publication_metadata_sha256",
        "bindings",
        "semantic_as_of",
        "missing",
    ],
)
async def test_publication_metadata_keeps_complete_logical_source_checks(damage):
    pair, connection = _retained_pair(), Connection()
    connection.payload = _publication_row(pair)
    if damage == "missing":
        connection.payload = None
    elif damage == "bindings":
        connection.payload[damage] = {}
    elif damage is not None:
        connection.payload[damage] = "changed"
    if damage is not None:
        with pytest.raises(ValueError, match="registry_cms_source_pair_invalid"):
            await pairs._retained_source_publication_time(connection, pair, api_read=True)
    else:
        assert await pairs._retained_source_publication_time(connection, pair, api_read=True) is None
    _assert_no_recorded_oid_sent(connection, pair)


@pytest.mark.parametrize("matches", [False, True])
async def test_api_admission_keeps_named_tables_and_dataset_metadata(monkeypatch, matches):
    pair, connection = _retained_pair(), Connection()
    pin = pair.recipe.source_pin
    connection.matches = matches
    custody = AsyncMock(side_effect=AssertionError("API admission compared recorded custody"))
    monkeypatch.setattr(membership, "require_retained_cms_fhir_source_epoch", custody)
    if not matches:
        with pytest.raises(membership.FHIRMembershipSourceError):
            await membership._require_pinned_source(connection, pin, api_read=True)
    else:
        assert await membership._require_pinned_source(connection, pin, api_read=True) == tuple(
            CompactAddressRelation(pin.read_schema_name, name) for name in sorted(connection.tables)
        )
    custody.assert_not_awaited()
    _assert_no_recorded_oid_sent(connection, pair)
    queries = [query for query, _ in connection.calls]
    lock = next(index for index, query in enumerate(queries) if query.startswith("LOCK TABLE"))
    assert lock < next(index for index, query in enumerate(queries) if "relrowsecurity" in query)
    assert all(f'"{name}"' in queries[lock] for name in epoch._TABLES)


async def test_api_admission_refuses_when_the_epoch_tables_cannot_be_locked(monkeypatch):
    pair, connection = _retained_pair(), Connection()

    async def unavailable(query, *arguments):
        raise asyncpg.LockNotAvailableError("synthetic lock timeout")

    connection.execute = unavailable
    with pytest.raises(membership.FHIRMembershipSourceError):
        await membership._require_pinned_source(connection, pair.recipe.source_pin, api_read=True)
    assert connection.events == [] and not any("relrowsecurity" in query for query, _ in connection.calls)


async def test_api_admission_refuses_absent_or_row_filtered_epoch_table(monkeypatch):
    pair, connection = _retained_pair(), Connection()
    # The catalog query returns only plain permanent tables without row filtering.
    connection.tables.remove("provider_directory_dataset_resource")
    with pytest.raises(membership.FHIRMembershipSourceError):
        await membership._require_pinned_source(connection, pair.recipe.source_pin, api_read=True)
    assert connection.events == []


async def test_api_admission_of_a_live_schema_pin_compares_no_recorded_table(monkeypatch):
    pair, connection = _retained_pair(), Connection()
    pin = pair.proof.source_pin
    connection.fetch = AsyncMock(side_effect=AssertionError("a live schema has no epoch tables"))
    assert await membership._require_pinned_source(connection, pin, {"producer_id": "example"}, api_read=True) == ()
    query, arguments = next(call for call in connection.calls if call[0].startswith("SELECT EXISTS"))
    assert "published_at IS NOT NULL" in query and "relkind='r'" in query and "oid=$" not in query
    assert arguments[0] == pin.schema_name and arguments[-1] == '{"producer_id": "example"}'
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
    return doubles


@pytest.mark.parametrize("copied", [False, True])
async def test_api_pair_resolver_reads_by_name_and_compares_no_physical_identity(monkeypatch, copied):
    pair, connection = _witness_pair(), Connection()
    connection.payload = _publication_row(pair)
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
