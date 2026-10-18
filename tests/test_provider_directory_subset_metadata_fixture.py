# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Record-only tests for the fixed metadata privilege setup."""

from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace

import pytest

from process import provider_directory_source_callable_custody as custody
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.provider_directory_source_control_privileges import METADATA_FUNCTIONS
from tests import provider_directory_fhir_subset_abandonment_pg_support as fixture
from tests.test_acquisition_validator_argument_types_migration import _current_functions, _migration
from tests.test_provider_directory_source_callable_custody import _entry

_METADATA_SIGNATURES = (
    *(
        signature
        for signature in METADATA_FUNCTIONS
        if signature
        in {
            "provider_directory_subset_canonical_sha256(jsonb)",
            "provider_directory_subset_completion_proof_pair_valid(jsonb,text)",
            "provider_directory_subset_completion_proof_shape_valid(jsonb,text,bigint)",
        }
    ),
    "provider_directory_subset_replay_evidence_shape_valid(jsonb,text,jsonb,text)",
    "provider_directory_subset_coverage_shape_valid(jsonb,jsonb,text,text)",
)


def _functions(*, optional=False):
    from tests.test_source_validator_argument_types_migration import _load, _parse_function

    current_by_signature = _current_functions()
    subset = _load("20260808190000_provider_directory_subset_completion_proof.py")
    historical_by_name = dict(
        _parse_function(sql)
        for sql in (
            subset._replay_shape_valid_function_sql("snapshot_schema"),
            subset._coverage_shape_valid_function_sql("snapshot_schema", reviewed_root_policy_aware=True),
        )
    )
    entries = []
    selected = (*_METADATA_SIGNATURES, *((fixture._OPTIONAL_METADATA_SIGNATURE,) if optional else ()))
    for index, signature in enumerate(selected):
        name = signature.split("(")[0]
        if signature in fixture._HISTORICAL_METADATA_CONTRACTS:
            contract = fixture._HISTORICAL_METADATA_CONTRACTS[signature]
            entry = _entry("provider_directory_subset_completion_proof_shape_valid(jsonb,text,bigint)", owner=900)
            entry["definition"].update(
                proname=name,
                pronargs=len(contract[0]),
                proargnames=[argument for argument, _ in contract[0]],
                proargtypes=[custody._TYPE_OIDS[kind] for _, kind in contract[0]],
            )
            body = historical_by_name[name][-1]
        else:
            key = next(key for key in custody._CONTRACTS if key.split("(")[0] == name)
            entry = _entry(key, owner=900)
            body = next(
                attributes[-1] for (candidate, _), attributes in current_by_signature.items() if candidate == name
            )
        for caller, old, new, _ in _migration()._calls("snapshot_schema"):
            if caller == name:
                body = body.replace(old, new)
        entry["definition"].update(oid=str(501 + index), prosrc=body)
        entry["signature"] = '"snapshot_schema"."' + name + '"(' + signature.split("(", 1)[1]
        entry["access"] = [
            [role, role == 900, False, role == 900, role == 900, role == 900] for role in (101, 102, 103, 104, 900)
        ]
        entries.append(entry)
    return entries


class MetadataConnection:
    """Record SQL edges without granting native authority."""

    def __init__(self):
        self.entries = _functions()
        self.commands = []
        self.identities = 0
        self.drift = None
        self.policy = GenericSourcePolicy("snapshot_schema", "source_owner", "source_publisher", ("reader",), ())

    def is_in_transaction(self):
        return True

    @asynccontextmanager
    async def transaction(self):
        before = deepcopy(self.entries)
        try:
            yield
        except BaseException:
            self.entries = before
            raise

    async def fetchrow(self, sql, *arguments):
        assert "session_user::text" in sql
        self.identities += 1
        backend = 12 if self.drift == "backend" and self.identities > 1 else 11
        return dict(session="administrator", current="administrator", backend=backend, xid="42", native_admin=True)

    async def fetchval(self, sql, *arguments):
        if "to_regprocedure" in sql and fixture._OPTIONAL_METADATA_SIGNATURE.split("(")[0] in arguments[0]:
            return next(
                (int(entry["definition"]["oid"]) for entry in self.entries if entry["signature"] == arguments[0]), None
            )
        if "pg_roles" in sql:
            return 101 if arguments[0] == "source_owner" else 900
        return 201 if "to_regnamespace" in sql else 501

    async def fetch(self, sql, *arguments):
        if "owner_usage" in sql:
            return [dict(oid=role, owner_usage=role in (101, 102, 104, 900)) for role in (101, 102, 103, 104, 900)]
        assert sql == custody._FUNCTION_SQL
        return deepcopy(self.entries)

    async def execute(self, sql):
        self.commands.append(sql)
        if not sql.startswith("GRANT EXECUTE ON FUNCTION"):
            return "OK"
        for entry in self.entries:
            if entry["signature"] not in sql:
                continue
            acl_values = [101, 900, "EXECUTE", False]
            if acl_values not in entry["acl"]:
                entry["acl"].append(acl_values)
            entry["definition"]["proacl"] = ["native grant representation"]
            for row in entry["access"]:
                row[-1] = row[-1] or row[0] in (101, 102, 104)
            if self.drift == "definition":
                entry["definition"]["oid"] = "999"
        return "GRANT"


@pytest.fixture
def connection():
    return MetadataConnection()


async def test_fixed_metadata_grants_are_narrow_and_repeatable(connection):
    assert _METADATA_SIGNATURES == fixture._METADATA_SIGNATURES and len(_METADATA_SIGNATURES) == 5
    before = deepcopy(connection.entries)
    await fixture._grant_metadata_controls(connection, connection.policy)
    await fixture._grant_metadata_controls(connection, connection.policy)
    assert len(connection.commands) == 2 * len(_METADATA_SIGNATURES)
    assert all("WITH GRANT OPTION" not in sql and "PUBLIC" not in sql for sql in connection.commands)
    for previous, current in zip(before, connection.entries, strict=True):
        assert current["acl"] == [*previous["acl"], [101, 900, "EXECUTE", False]]
        assert current["definition"]["proowner"] == previous["definition"]["proowner"]
        assert current["definition"]["prosrc"] == previous["definition"]["prosrc"]
        assert current["access"][2][-1] is False


@pytest.mark.parametrize("fault", ["missing", "duplicate", "body", "signature", "public", "grantable", "foreign"])
async def test_invalid_catalog_refuses_before_grant(connection, fault):
    entry = connection.entries[0]
    if fault == "missing":
        connection.entries.pop()
    elif fault == "duplicate":
        connection.entries.append(deepcopy(entry))
    elif fault == "body":
        entry["definition"]["prosrc"] += "-- changed"
    elif fault == "signature":
        entry["signature"] += ",text"
    else:
        entry["acl"].append([0 if fault == "public" else 105, 900, "EXECUTE", fault == "grantable"])
    with pytest.raises((AssertionError, RuntimeError, ValueError)):
        await fixture._grant_metadata_controls(connection, connection.policy)
    assert connection.commands == []


@pytest.mark.parametrize("fault", ["backend", "definition"])
async def test_post_grant_drift_rolls_back(connection, fault):
    before = deepcopy(connection.entries)
    connection.drift = fault
    with pytest.raises(AssertionError):
        await fixture._grant_metadata_controls(connection, connection.policy)
    assert connection.entries == before


async def test_checkpoint_enrollment_grants_metadata(connection, monkeypatch):
    async def close_relation(*arguments):
        assert arguments[0] is connection

    monkeypatch.setattr(fixture, "_close_relation_grants", close_relation)
    scenario = SimpleNamespace(connection=connection, schema="snapshot_schema")
    component = SimpleNamespace(owner="source_owner", policy=connection.policy)
    await fixture._enroll_checkpoint_custody(scenario, component)
    grants = [sql for sql in connection.commands if sql.startswith("GRANT EXECUTE")]
    for entry in connection.entries:
        assert any(entry["signature"] in sql and 'TO "source_owner"' in sql for sql in grants)


async def test_cancelled_grant_preserves_error_and_rolls_back(connection, monkeypatch):
    from asyncio import CancelledError

    before = deepcopy(connection.entries)
    execute = connection.execute
    failure = CancelledError("cancel metadata setup")

    async def cancel_grant(sql):
        await execute(sql)
        raise failure

    monkeypatch.setattr(connection, "execute", cancel_grant)
    with pytest.raises(CancelledError) as caught:
        await fixture._grant_metadata_controls(connection, connection.policy)
    assert caught.value is failure
    assert connection.entries == before


@pytest.mark.parametrize("failed", [False, True])
@pytest.mark.parametrize("retire_after", [False, True])
async def test_historical_migration_restores_exact_hooks_then_retires_after_success(
    connection, monkeypatch, failed, retire_after
):
    """A failed migration propagates for its caller rollback; success rechecks current custody."""
    events = []
    scenario = SimpleNamespace(connection=connection, schema="snapshot_schema")
    saved = ({"tgname": "saved", "definition": "saved exact definition"},)
    proof = object()

    async def restore(actual, guards):
        assert actual is scenario and guards is saved
        events.append("restore")

    async def capture(actual, policy, *, legacy):
        assert actual is connection and policy is connection.policy and legacy is True
        events.append("capture")
        return proof

    async def retire(actual, policy, observed):
        assert actual is connection and policy is connection.policy and observed is proof
        events.append("retire")

    monkeypatch.setattr(fixture, "_restore_saved_bulk_guards", restore)
    monkeypatch.setattr(fixture, "capture_checkpoint_controls", capture)
    monkeypatch.setattr(fixture, "retire_bulk_abandonment_hooks", retire)
    token = fixture._HISTORICAL_CHECKPOINT_CATALOG.set((scenario, connection.policy, saved))
    error = ValueError("migration refused")
    try:
        try:
            async with fixture.historical_checkpoint_catalog(scenario, retire_after=retire_after):
                events.append("migration")
                if failed:
                    raise error
        except ValueError as caught:
            assert failed and caught is error
    finally:
        fixture._HISTORICAL_CHECKPOINT_CATALOG.reset(token)
    assert events == ["restore", "migration"] + (["capture", "retire"] if retire_after and not failed else [])


async def test_historical_migration_without_current_writer_keeps_original_catalog(monkeypatch):
    from unittest.mock import AsyncMock

    restore = AsyncMock()
    monkeypatch.setattr(fixture, "_restore_saved_bulk_guards", restore)
    assert fixture._HISTORICAL_CHECKPOINT_CATALOG.get() is None
    async with fixture.historical_checkpoint_catalog(object()):
        assert fixture._HISTORICAL_CHECKPOINT_CATALOG.get() is None
    restore.assert_not_awaited()


@pytest.mark.parametrize("terminal_window", [False, True])
@pytest.mark.parametrize("damage", [None, "body", "signature", "public", "security"])
async def test_historical_shape_metadata_requires_exact_constructor(connection, terminal_window, damage):
    from tests.test_source_validator_argument_types_migration import _load, _parse_function

    subset = _load("20260808190000_provider_directory_subset_completion_proof.py")
    _, attributes = _parse_function(
        subset._proof_shape_valid_function_sql(
            "snapshot_schema",
            reviewed_subset_profile_aware=True,
            reviewed_subset_terminal_window_profile_aware=terminal_window,
        )
    )
    body = attributes[-1]
    assert custody._body_digest(body, "snapshot_schema") in fixture._HISTORICAL_SHAPE_DIGESTS
    entry = next(
        entry
        for entry in connection.entries
        if entry["definition"]["proname"] == "provider_directory_subset_completion_proof_shape_valid"
    )
    entry["definition"]["prosrc"] = body + ("-- changed" if damage == "body" else "")
    if damage == "signature":
        entry["signature"] += ",text"
    elif damage == "public":
        entry["acl"].append([0, 900, "EXECUTE", False])
    elif damage == "security":
        entry["definition"]["prosecdef"] = False
    if damage:
        with pytest.raises((AssertionError, ValueError)):
            await fixture._grant_metadata_controls(connection, connection.policy)
        assert connection.commands == []
    else:
        await fixture._grant_metadata_controls(connection, connection.policy)
        assert len(connection.commands) == 5


@pytest.mark.parametrize("signature", fixture._HISTORICAL_METADATA_CONTRACTS)
@pytest.mark.parametrize("damage", [None, "body", "type", "public"])
async def test_historical_header_dependencies_require_exact_custody(connection, signature, damage):
    entry = next(
        entry
        for entry in connection.entries
        if entry["signature"].endswith(signature.split("(", 1)[1])
        and entry["definition"]["proname"] == signature.split("(")[0]
    )
    contract = fixture._HISTORICAL_METADATA_CONTRACTS[signature]
    assert custody._body_digest(entry["definition"]["prosrc"], "snapshot_schema") == contract[-1]
    if damage == "body":
        entry["definition"]["prosrc"] += "-- changed"
    elif damage == "type":
        entry["definition"]["proargtypes"][0] = custody._TYPE_OIDS["text"]
    elif damage == "public":
        entry["acl"].append([0, 900, "EXECUTE", False])
    if damage:
        with pytest.raises((AssertionError, ValueError)):
            await fixture._grant_metadata_controls(connection, connection.policy)
        assert connection.commands == []
    else:
        await fixture._grant_metadata_controls(connection, connection.policy)
        assert len(connection.commands) == 5


@pytest.mark.parametrize("damage", [None, "body", "public"])
async def test_optional_admission_callable_requires_exact_custody(connection, damage):
    connection.entries = _functions(optional=True)
    entry = connection.entries[-1]
    assert entry["definition"]["proname"] == fixture._OPTIONAL_METADATA_SIGNATURE.split("(")[0]
    if damage == "body":
        entry["definition"]["prosrc"] += "-- changed"
    elif damage == "public":
        entry["acl"].append([0, 900, "EXECUTE", False])
    if damage:
        with pytest.raises((AssertionError, ValueError)):
            await fixture._grant_metadata_controls(connection, connection.policy)
        assert connection.commands == []
    else:
        await fixture._grant_metadata_controls(connection, connection.policy)
        assert len(connection.commands) == 6
