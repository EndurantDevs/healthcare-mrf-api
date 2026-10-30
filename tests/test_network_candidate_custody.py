# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic custody boundaries; native privilege acceptance is separate."""

import copy
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from uuid import UUID

import pytest

from process import network_candidate_custody as custody
from process import network_membership_publication
from process.network_membership_copy import MembershipCopyTarget

pytestmark = pytest.mark.asyncio
FINAL_PROOF = custody._final_proof
TARGET = MembershipCopyTarget(*(str(UUID(int=i)) for i in range(1, 5)), "network_candidate_" + UUID(int=4).hex)


def _candidate_catalog():
    return {
        "namespace": {"oid": 30, "nspowner": 31},
        "heaps": {"network_membership": 40, "provider_location_binding": 41, "entity_address_unified": 42},
        "relations": [
            {"oid": 40 + i, "relname": name, "relkind": b"r", "relowner": 31}
            for i, name in enumerate(("network_membership", "provider_location_binding", "entity_address_unified"))
        ],
    }


def _control_catalog():
    return dict(
        schema_oid=10,
        owner_oid=11,
        rolname="control_owner",
        schema_owner_oid=11,
        schema_owner_role="control_owner",
        candidate_owner_oid=11,
        candidate_owner_role="control_owner",
        database_oid=12,
        database_name="synthetic",
        candidate_table_oid=13,
        seal_table_oid=14,
        owned=True,
        closed=True,
        **{
            k: False
            for k in ("rolcanlogin", "rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls")
        },
    )


def _operator_identity():
    """Return the fixed non-elevated synthetic operator identity."""
    return dict(
        session="operator",
        current="operator",
        backend=20,
        xid="21",
        role_oid=22,
        rolcanlogin=True,
        rolsuper=False,
        rolcreatedb=False,
        rolcreaterole=False,
        rolreplication=False,
        rolbypassrls=False,
    )


class Connection:
    def __init__(self):
        """Create mutable synthetic custody state with closed native defaults."""
        self.active = False
        self.calls = []
        self.isolation = "repeatable read"
        self.control = _control_catalog()
        self.actor = _operator_identity()
        self.candidate = {key: getattr(TARGET, key) for key in custody._STABLE[:5]}
        self.candidate.update(
            schema_revision=1,
            source_generations={"custom_membership": "a" * 64},
            approved_custom_revision=1,
            expected_head=0,
            expected_rows=2,
            accepted_rows=2,
            index_ready=True,
            source_recipes_json=[],
            validation_json={
                "writer_closure": {},
                "address_source": {"schema_name": "source", "table_name": "addresses", "generation_id": "a" * 64},
            },
            created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            state="ready",
        )
        self.catalog = _candidate_catalog()
        self.candidate["validation_json"]["writer_closure"] = custody._receipt(
            TARGET,
            {"owner_role": "loading_owner", "loader_roles": [], "reader_roles": ["execution", "reader"]},
            self.catalog,
        )
        self.seal = None
        self.exists = True
        self.can_set = True
        self.forbidden_set = set()
        self.memberships = set()
        self.current_role = "operator"
        self.metadata_lock_roles = []
        self.nonreaders = set()
        self.enrolled_readers = {"execution", "reader"}
        self.forbidden = set()
        self.q_forbidden = set()
        self.heap_safe = True
        self.address_safe = True
        self.address_checks = 0
        self.address_censuses = []
        self.safe = True
        self.inherit = True
        self.noninherited = set()
        self.index = "b" * 64
        self.proofs = 0
        self.unknown_commit = False

    def is_in_transaction(self):
        return self.active

    @asynccontextmanager
    async def transaction(self, **options):
        assert not self.active
        before = copy.deepcopy((self.catalog, self.seal, self.enrolled_readers))
        self.active = True
        try:
            yield
        except BaseException:
            self.catalog, self.seal, self.enrolled_readers = before
            raise
        finally:
            self.active = False
        if self.unknown_commit:
            self.unknown_commit = False
            raise RuntimeError("commit acknowledgement lost")

    async def fetchrow(self, sql, *args):
        self.calls.append((sql, args))
        if sql == custody._CONTROL_SQL:
            return copy.deepcopy(self.control)
        if sql == custody._IDENTITY_SQL:
            return copy.deepcopy(self.actor)
        if "network_membership_candidate WHERE" in sql:
            if sql.endswith(" FOR UPDATE"):
                self.metadata_lock_roles.append(self.current_role)
            return copy.deepcopy(self.candidate)
        if "receipt_json::text" in sql:
            return copy.deepcopy(self.seal)
        raise AssertionError(sql)

    async def fetch(self, sql, *args):
        self.calls.append((sql, args))
        assert sql == custody._READER_SQL
        return [{"name": role} for role in sorted(args[0]) if role not in self.nonreaders]

    async def fetchval(self, sql, *args):
        self.calls.append((sql, args))
        if sql.startswith("WITH target AS ("):
            address = self.candidate["validation_json"]["address_source"]
            assert args[:2] == (address["schema_name"], address["table_name"])
            assert type(args[2]) is list and 1 <= len(args[2]) <= 128
            self.address_censuses.append(tuple(args[2]))
            self.address_checks += 1
            return self.address_safe
        if "SELECT NOT EXISTS(SELECT FROM unnest($1::text[]) configured(name)" in sql:
            return set(args[0]) <= self.enrolled_readers
        if sql == custody._CENSUS_SQL:
            return not (set(args[0]) & self.forbidden)
        if sql == custody._Q_REACH_SQL:
            return not (set(args[0]) & self.q_forbidden)
        if "FROM pg_class c WHERE c.oid=ANY" in sql:
            return self.heap_safe
        if "to_regclass" in sql:
            return 14 if self.exists else None
        if sql == "SHOW transaction_isolation":
            return self.isolation
        if "SELECT rolname FROM pg_roles" in sql:
            return "control_owner" if args[0] == 11 else "loading_owner"
        if "pg_has_role" in sql and "USAGE" in sql:
            return self.inherit and not set(args).intersection(self.noninherited)
        if "pg_has_role" in sql and "MEMBER" in sql:
            return bool(set(args).intersection(self.memberships))
        if "pg_has_role" in sql or "has_database_privilege" in sql:
            return self.can_set and not set(args).intersection(self.forbidden_set)
        if "quote_ident(current_user)" in sql:
            return '"operator"'
        if "SELECT oid::bigint" in sql:
            return 11
        raise AssertionError(sql)

    async def execute(self, sql, *args):
        self.calls.append((sql, args))
        if sql.startswith("SET LOCAL ROLE"):
            self.current_role = sql.rsplit('"', 2)[1]
        if sql.startswith("GRANT SELECT ON") and ".network_candidate_custody " in sql:
            self.enrolled_readers.add(sql.rsplit('"', 2)[1])
        if sql.startswith("ALTER SCHEMA"):
            self.catalog["namespace"]["nspowner"] = 11
        if sql.startswith("ALTER TABLE"):
            for row in self.catalog["relations"]:
                if '"' + row["relname"] + '"' in sql:
                    row["relowner"] = 11
        if sql.startswith("INSERT INTO"):
            assert self.seal is None
            self.seal = {"seal_sha256": args[1], "receipt_json": args[2]}


@pytest.fixture
def boundary(monkeypatch):
    connection = Connection()

    async def catalog(conn, target, candidate):
        assert conn is connection and target == TARGET
        return copy.deepcopy(conn.catalog)

    async def closed(conn, roles, catalog):
        custody._require(
            conn.safe
            and catalog["namespace"]["nspowner"] == 11
            and all(row["relowner"] == 11 for row in catalog["relations"])
        )

    async def proof(conn, *args):
        conn.proofs += 1

    async def index(conn, *args):
        return conn.index

    async def no_op(*args):
        return None

    monkeypatch.setattr(custody, "_catalog", catalog)
    monkeypatch.setattr(custody, "_check_closed", closed)
    monkeypatch.setattr(custody, "_final_proof", proof)
    monkeypatch.setattr(custody, "_index_digest", index)
    monkeypatch.setattr(custody, "_remove_grants", no_op)
    monkeypatch.setattr(custody, "_remove_default_grants", no_op)
    monkeypatch.setattr(
        network_membership_publication,
        "_publication_readiness",
        lambda candidate: (None, None, None, {"owner_role": "loading_owner"}),
    )
    return connection


async def is_authorized(connection, target):
    assert target == TARGET
    return True


async def seal(connection, authorize=is_authorized, **options):
    return await custody.seal_network_candidate_custody(
        connection,
        TARGET,
        control_schema="control",
        runtime_roles=("execution", "reader"),
        authorize=authorize,
        **options,
    )


@pytest.mark.parametrize("field", ["schema_oid", "relation_oids", "index_oids", "index_table_oids"])
async def test_initial_seal_refuses_prepared_identity_drift_before_independent_source_proof(boundary, field):
    from process.network_membership_writer_closure import NetworkWriterClosureError

    retained = boundary.candidate["validation_json"]["writer_closure"]
    if field == "schema_oid":
        retained[field] += 1
    elif field == "relation_oids":
        retained[field]["network_membership"] += 1
    else:
        retained[field]["unexpected_index"] = 40
    with pytest.raises(NetworkWriterClosureError, match="changed"):
        await seal(boundary)
    assert boundary.proofs == 0 and boundary.seal is None


async def test_ordinary_seal_refuses_ptg_even_without_contribution_metadata(boundary):
    from process.registry_source_recipe_store import canonical_registry_source_recipes, registry_source_recipes_sha256
    from tests.test_registry_publication_worker import _configured_recipe

    recipes = canonical_registry_source_recipes((_configured_recipe("ptg"),))
    boundary.candidate["source_recipes_json"] = recipes
    boundary.candidate["source_generations"]["registry_source_recipes"] = registry_source_recipes_sha256(recipes)
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await seal(boundary)
    assert boundary.seal is None and boundary.proofs == 0


async def test_q_census_uses_control_owner_without_changing_candidate_census(boundary):
    boundary.control["schema_owner_oid"] = 17
    await custody._census_closed(boundary, boundary.control, boundary.catalog, ("execution", "reader"))
    assert (custody._Q_REACH_SQL, (["execution", "reader"], 17)) in boundary.calls
    assert (custody._CENSUS_SQL, (["execution", "reader"], 11, 30, [40, 41, 42])) in boundary.calls


@pytest.mark.parametrize("role", ["execution", "reader"])
async def test_every_actor_with_q_reach_refuses_before_candidate_mutation(boundary, role):
    before = copy.deepcopy(boundary.catalog)
    boundary.q_forbidden.add(role)
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await seal(boundary)
    assert boundary.catalog == before and boundary.seal is None and boundary.proofs == 0


async def test_q_reach_is_rechecked_after_transfer_before_storing_seal(boundary, monkeypatch):
    original = custody._transfer
    before = copy.deepcopy(boundary.catalog)

    async def transfer_then_change_reach(*arguments):
        await original(*arguments)
        boundary.q_forbidden.add("reader")

    monkeypatch.setattr(custody, "_transfer", transfer_then_change_reach)
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await seal(boundary)
    assert boundary.catalog == before and boundary.seal is None
    assert not any(sql.startswith("INSERT INTO") for sql, _ in boundary.calls)


async def test_q_reach_is_rechecked_after_final_authorization(boundary):
    before = copy.deepcopy(boundary.catalog)
    calls = []

    async def is_authorized_then_change_reach(connection, target):
        calls.append(target)
        if len(calls) == 2:
            connection.q_forbidden.add("reader")
        return True

    with pytest.raises(custody.NetworkCandidateCustodyError):
        await seal(boundary, authorize=is_authorized_then_change_reach)
    assert len(calls) == 2 and boundary.catalog == before and boundary.seal is None


@pytest.mark.parametrize("operation", ["seal", "verify"])
async def test_existing_seal_refuses_later_q_reach(boundary, operation):
    await seal(boundary)
    before = copy.deepcopy(boundary.seal)
    boundary.q_forbidden.add("reader")
    with pytest.raises(custody.NetworkCandidateCustodyError):
        if operation == "seal":
            await seal(boundary)
        else:
            await verify(boundary)
    assert boundary.seal == before


async def test_pre_enrolled_seal_never_grants_and_rechecks_existing_seal(boundary):
    receipt = await seal(boundary, pre_enrolled_only=True)
    assert not any("GRANT SELECT ON" in sql and ".network_candidate_custody " in sql for sql, _ in boundary.calls)
    assert await seal(boundary, pre_enrolled_only=True) == receipt
    boundary.enrolled_readers.remove("execution")
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await seal(boundary, pre_enrolled_only=True)
    assert boundary.enrolled_readers == {"reader"}


async def test_ordinary_seal_keeps_enrollment_and_missing_pre_enrollment_rolls_back(boundary):
    boundary.enrolled_readers.clear()
    before = copy.deepcopy(boundary.catalog)
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await seal(boundary, pre_enrolled_only=True)
    assert boundary.seal is None and boundary.catalog == before and boundary.enrolled_readers == set()
    await seal(boundary)
    assert boundary.enrolled_readers == {"execution", "reader"}


@pytest.mark.parametrize("pre_enrolled_only", [None, 0, 1, "true"])
async def test_custody_enrollment_mode_is_internal_boolean(boundary, pre_enrolled_only):
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await seal(boundary, pre_enrolled_only=pre_enrolled_only)
    assert not boundary.calls


async def verify(connection, **options):
    connection.active = True
    try:
        return await custody.verify_network_candidate_custody(connection, TARGET, control_schema="control", **options)
    finally:
        connection.active = False


async def test_seal_transfers_only_registered_candidate_and_replay_does_not_revalidate(boundary):
    connection = boundary
    before = copy.deepcopy(connection.candidate)
    result = await seal(connection)
    assert connection.candidate == before
    assert result["receipt"]["closure"]["owner_role"] == "control_owner"
    assert connection.proofs == 1
    connection.calls.clear()
    assert await seal(connection) == result
    assert connection.proofs == 1
    assert not any(sql.startswith(("ALTER", "INSERT", "GRANT", "REVOKE")) for sql, _ in connection.calls)
    connection.candidate["state"] = "published"
    assert await verify(connection) == result
    assert all(f'FROM "{TARGET.schema_name}".' not in sql for sql, _ in connection.calls)
    assert any("ACCESS SHARE MODE NOWAIT" in sql for sql, _ in connection.calls)


async def test_transfer_retains_namespace_access_until_final_owner_closes_grants(boundary, monkeypatch):
    async def remove_grants(connection, namespace, catalog, owner_oid):
        connection.calls.append(("close_grants", (connection.current_role,)))

    monkeypatch.setattr(custody, "_remove_grants", remove_grants)
    await seal(boundary)
    statements = [sql for sql, _ in boundary.calls]
    bridge = statements.index(f'GRANT USAGE ON SCHEMA "{TARGET.schema_name}" TO "operator"')
    transfers = [index for index, sql in enumerate(statements) if sql.startswith("ALTER TABLE")]
    assert statements[bridge - 1] == 'SET LOCAL ROLE "control_owner"'
    assert statements[bridge + 1] == 'SET LOCAL ROLE "operator"'
    assert bridge < min(transfers) <= max(transfers) < statements.index("close_grants")
    assert boundary.calls[statements.index("close_grants")][1] == ("control_owner",)
    assert boundary.current_role == "operator"


@pytest.mark.parametrize(
    "part", ["scope", "validation", "source", "created", "oid", "index", "acl", "control", "seal", "owner"]
)
async def test_changed_identity_or_evidence_never_uses_legacy_fallback(boundary, part):
    connection = boundary
    await seal(connection)
    match part:
        case "scope":
            connection.candidate["producer_id"] = str(UUID(int=77))
        case "validation":
            connection.candidate["validation_json"]["extra"] = True
        case "source":
            connection.candidate["source_generations"]["custom_membership"] = "c" * 64
        case "created":
            connection.candidate["created_at"] = datetime(2026, 2, 1, tzinfo=timezone.utc)
        case "oid":
            connection.catalog["heaps"]["network_membership"] = 99
        case "index":
            connection.index = "c" * 64
        case "acl":
            connection.safe = False
        case "control":
            connection.control["database_oid"] += 1
        case "seal":
            connection.seal["seal_sha256"] = "0" * 64
        case "owner":
            connection.control["owned"] = False
    with pytest.raises((ValueError, KeyError)):
        await verify(connection, required=False)


@pytest.mark.parametrize("missing", ["table", "row"])
async def test_optional_absence_preserves_legacy_read_committed(boundary, missing):
    connection = boundary
    connection.isolation = "read committed"
    if missing == "table":
        connection.exists = False
    assert await verify(connection, required=False) is None
    with pytest.raises(ValueError):
        await verify(connection)


@pytest.mark.parametrize(
    "drift", ["set_role", "native_set", "authority", "final_actor", "uncommitted", "published", "full_proof"]
)
async def test_actor_authority_and_final_failure_leave_no_seal_or_transfer(boundary, drift, monkeypatch):
    connection = boundary
    before = copy.deepcopy(connection.catalog)
    authorization_calls = []

    async def is_authorized(conn, target):
        authorization_calls.append(None)
        if drift == "final_actor" and len(authorization_calls) == 2:
            conn.actor["backend"] += 1
        return drift != "authority"

    match drift:
        case "set_role":
            connection.actor["current"] = "other"
        case "native_set":
            connection.can_set = False
        case "uncommitted":
            connection.active = True
        case "published":
            connection.candidate["state"] = "published"
        case "full_proof":

            async def fail(*args):
                raise RuntimeError("malformed family")

            monkeypatch.setattr(custody, "_final_proof", fail)
    with pytest.raises((ValueError, RuntimeError)):
        await seal(connection, is_authorized)
    assert connection.seal is None and connection.catalog == before


async def test_existing_seal_still_requires_original_operator_authority(boundary):
    await seal(boundary)
    boundary.can_set = False
    with pytest.raises(ValueError):
        await seal(boundary)


async def test_sealed_verification_requires_repeatable_read(boundary):
    await seal(boundary)
    boundary.isolation = "read committed"
    with pytest.raises(ValueError):
        await verify(boundary, required=False)


async def test_safe_set_only_operator_restores_role_and_replays(boundary):
    boundary.noninherited = {11}
    result = await seal(boundary)
    assert await seal(boundary) == result
    assert any('SET LOCAL ROLE "control_owner"' == sql for sql, _ in boundary.calls)
    assert any('SET LOCAL ROLE "operator"' == sql for sql, _ in boundary.calls)


async def test_set_only_loading_owner_refuses_before_native_transfer(boundary):
    boundary.inherit = False
    before = copy.deepcopy(boundary.catalog)
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await seal(boundary)
    assert boundary.catalog == before and boundary.seal is None
    assert any('SET LOCAL ROLE "loading_owner"' == sql for sql, _ in boundary.calls)
    assert boundary.current_role == "operator"
    assert not any(sql.startswith("ALTER ") for sql, _ in boundary.calls)


async def test_complete_128_role_census_is_not_truncated(boundary):
    roles = tuple(f"runtime_{number:03d}" for number in range(128))
    result = await custody.seal_network_candidate_custody(
        boundary, TARGET, control_schema="control", runtime_roles=roles, authorize=is_authorized
    )
    assert result["receipt"]["closure"]["reader_roles"] == list(roles)
    assert len([sql for sql, _ in boundary.calls if sql.startswith('GRANT SELECT ON "control".')]) == 128
    assert await verify(boundary) == result

    assert boundary.address_censuses and all(census == roles for census in boundary.address_censuses)


async def test_census_above_128_is_refused_before_sql(boundary):
    with pytest.raises(ValueError):
        await custody.seal_network_candidate_custody(
            boundary,
            TARGET,
            control_schema="control",
            runtime_roles=tuple(f"runtime_{i}" for i in range(129)),
            authorize=is_authorized,
        )
    assert boundary.calls == []


async def test_lost_seal_commit_is_recovered_by_exact_replay(boundary):
    boundary.unknown_commit = True
    with pytest.raises(RuntimeError, match="acknowledgement lost"):
        await seal(boundary)
    assert boundary.seal is not None
    result = await seal(boundary)
    assert result["seal_sha256"] == boundary.seal["seal_sha256"] and boundary.proofs == 1


def _mock_final_source_proofs(monkeypatch, calls):
    from process import network_source_contribution_custody as source_custody
    from process import registry_source_office_custody as office_custody

    async def contributions(*_arguments, **_options):
        calls.append("source")

    async def offices(*_arguments, **_options):
        calls.append("offices")

    monkeypatch.setattr(source_custody, "verify_independent_source_contributions", contributions)
    monkeypatch.setattr(office_custody, "verify_independent_initial_source_offices", offices)


@pytest.mark.parametrize("failure", ["relationships", "retention", "provider", "index"])
async def test_final_custodian_uses_full_existing_verification_and_propagates_failure(boundary, monkeypatch, failure):
    boundary.candidate["validation_json"]["address_source"] = {
        "schema_name": "source",
        "table_name": "addresses",
        "generation_id": "a" * 64,
    }
    monkeypatch.setattr(
        network_membership_publication,
        "_publication_readiness",
        lambda candidate: (None, None, {"index_definition_sha256": "b" * 64}, {}),
    )
    calls = []
    _mock_final_source_proofs(monkeypatch, calls)

    async def matching(*args):
        calls.append("relationships")
        if failure == "relationships":
            raise RuntimeError("relationships refused")

    async def retention(*args):
        calls.append("retention")
        if failure == "retention":
            raise RuntimeError("retention refused")

    async def provider(*args):
        calls.append("provider")
        if failure == "provider":
            raise RuntimeError("provider refused")

    async def semantics(*args, **kwargs):
        assert kwargs == {"replay": True}

    monkeypatch.setattr(custody, "_matching_inputs", matching)
    monkeypatch.setattr(custody, "verify_registry_source_retention_rows", retention)
    monkeypatch.setattr(network_membership_publication, "_verify_provider_custody", provider)
    monkeypatch.setattr(custody, "_source_semantics", semantics)
    if failure == "index":
        boundary.index = "0" * 64
    with pytest.raises((RuntimeError, custody.NetworkCandidateCustodyError)):
        await FINAL_PROOF(boundary, TARGET, boundary.candidate, "control")
    assert calls[0] == "retention"
    if failure != "retention":
        assert calls[1:3] == ["source", "offices"]
        assert "relationships" in calls


async def test_empty_legacy_table_does_not_require_new_protected_owner(boundary):
    boundary.control["owned"] = False
    boundary.isolation = "read committed"
    assert await verify(boundary, required=False) is None
    with pytest.raises(ValueError):
        await seal(boundary)


async def test_native_heap_behavior_drift_is_terminal_even_with_unchanged_oids(boundary):
    await seal(boundary)
    boundary.heap_safe = False
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await verify(boundary, required=False)


async def test_lifecycle_and_authority_census_never_creates_candidate_read_access(boundary):
    boundary.nonreaders = {"lifecycle", "api", "authority"}
    roles = ("loading_owner", "execution", "reader", "lifecycle", "api", "authority")
    result = await custody.seal_network_candidate_custody(
        boundary, TARGET, control_schema="control", runtime_roles=roles, authorize=is_authorized
    )
    assert result["receipt"]["runtime_roles"] == sorted(roles)
    assert result["receipt"]["closure"]["reader_roles"] == ["execution", "loading_owner", "reader"]
    fact_grants = [sql for sql, _ in boundary.calls if sql.startswith('GRANT SELECT ON "' + TARGET.schema_name + '".')]
    assert len(fact_grants) == 9
    select_grants = [sql for sql, _ in boundary.calls if sql.startswith("GRANT SELECT")]
    assert not any('TO "' + role + '"' in sql for sql in select_grants for role in boundary.nonreaders)
    assert not any('TO "' + role + '"' in sql for sql in fact_grants for role in boundary.nonreaders)
    assert await verify(boundary) == result
    boundary.forbidden = {"lifecycle"}
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await verify(boundary)


async def test_large_census_protects_nonreaders_without_granting_them_reads(boundary):
    roles = tuple(f"runtime_{i:03d}" for i in range(100))
    boundary.nonreaders = set(roles[2:])
    result = await custody.seal_network_candidate_custody(
        boundary, TARGET, control_schema="control", runtime_roles=roles, authorize=is_authorized
    )
    assert result["receipt"]["runtime_roles"] == list(roles)
    assert result["receipt"]["closure"]["reader_roles"] == list(roles[:2])
    boundary.forbidden = {roles[-1]}
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await verify(boundary, required=False)


async def test_seal_replay_requires_exact_forbidden_census_even_if_reader_subset_matches(boundary):
    boundary.nonreaders = {"lifecycle"}
    await custody.seal_network_candidate_custody(
        boundary,
        TARGET,
        control_schema="control",
        runtime_roles=("execution", "reader", "lifecycle"),
        authorize=is_authorized,
    )
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await seal(boundary)


def _event_assertion(kind):
    checkpoint_by_name = {"event_id": str(UUID(int=7)), "attempt_id": str(UUID(int=8))}
    if kind == "cms":
        coordinates_by_name = dict(
            source_system="fhir",
            source_id="cms-npd",
            dataset_schema="source_schema",
            dataset_id="dataset",
            producer_id="producer",
            edition_id="edition",
        )
        checkpoint_by_name.update(
            notification_sha256="a" * 64,
            pair_sha256="b" * 64,
            recipe_sha256="c" * 64,
            coordinates=coordinates_by_name,
            replaced_coordinates=coordinates_by_name,
        )
    elif kind == "aca":
        checkpoint_by_name.update(
            header={"capture_id": str(UUID(int=9)), "recipe_sha256": "a" * 64, "notification_sha256": "b" * 64},
            reader_identity_sha256="c" * 64,
        )
    else:
        checkpoint_by_name.update(
            header={"capture_id": str(UUID(int=9)), "approval_sha256": "a" * 64, "hold_sha256": "b" * 64},
            reader_identity_sha256="c" * 64,
        )
    return {"revision": 1, "kind": kind, "checkpoint": checkpoint_by_name}


@pytest.mark.parametrize("kind", ["cms", "ptg", "aca"])
async def test_revision_two_seal_binds_assertion_and_atomic_checkpoint_without_rehashing(boundary, kind):
    connection = boundary
    assertion = _event_assertion(kind)
    report = connection.candidate["validation_json"]
    report[custody.SOURCE_EVENT_ASSERTION] = assertion
    actual = await seal(connection)
    assert actual["receipt"]["revision"] == 2
    assert custody._SOURCE_CHECKPOINTS[kind] not in report
    report[custody._SOURCE_CHECKPOINTS[kind]] = copy.deepcopy(assertion["checkpoint"])
    assert await verify(connection) == actual
    connection.candidate["state"] = "published"
    assert await verify(connection) == actual
    assert connection.proofs == 1
    del report[custody._SOURCE_CHECKPOINTS[kind]]
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await verify(connection)


@pytest.mark.parametrize("kind", ["cms", "ptg", "aca"])
@pytest.mark.parametrize(
    "damage", ["checkpoint", "other_checkpoint", "assertion", "extra_json", "missing_assertion", "bool"]
)
async def test_revision_two_rejects_every_change_except_exact_redundant_checkpoint(boundary, kind, damage):
    connection = boundary
    assertion = _event_assertion(kind)
    report = connection.candidate["validation_json"]
    report[custody.SOURCE_EVENT_ASSERTION] = assertion
    await seal(connection)
    match damage:
        case "checkpoint":
            report[custody._SOURCE_CHECKPOINTS[kind]] = {**assertion["checkpoint"], "attempt_id": str(UUID(int=99))}
        case "other_checkpoint":
            report[custody._SOURCE_CHECKPOINTS["ptg" if kind == "cms" else "cms"]] = assertion["checkpoint"]
        case "assertion":
            assertion["checkpoint"]["event_id"] = str(UUID(int=99))
        case "extra_json":
            report["unrelated"] = True
        case "missing_assertion":
            del report[custody.SOURCE_EVENT_ASSERTION]
        case _:
            report[custody._SOURCE_CHECKPOINTS[kind]] = {**assertion["checkpoint"], "attempt_id": True}
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await verify(connection)


@pytest.mark.parametrize(
    "damage",
    [
        "revision",
        "bool_revision",
        "kind",
        "extra",
        "checkpoint_type",
        "checkpoint_extra",
        "uuid",
        "sha",
        "coordinates",
        "completed",
    ],
)
async def test_initial_assertion_shape_is_closed_and_completed_checkpoint_refuses_before_transfer(boundary, damage):
    connection = boundary
    assertion = _event_assertion("cms")
    report = connection.candidate["validation_json"]
    report[custody.SOURCE_EVENT_ASSERTION] = assertion
    match damage:
        case "revision":
            assertion["revision"] = 2
        case "bool_revision":
            assertion["revision"] = True
        case "kind":
            assertion["kind"] = "other"
        case "extra":
            assertion["extra"] = True
        case "checkpoint_type":
            assertion["checkpoint"] = []
        case "checkpoint_extra":
            assertion["checkpoint"]["extra"] = True
        case "uuid":
            assertion["checkpoint"]["attempt_id"] = "invalid"
        case "sha":
            assertion["checkpoint"]["notification_sha256"] = "invalid"
        case "coordinates":
            assertion["checkpoint"]["coordinates"]["extra"] = True
        case _:
            report["cms_source_notification"] = copy.deepcopy(assertion["checkpoint"])
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await seal(connection)
    assert connection.seal is None and connection.catalog["namespace"]["nspowner"] == 31


async def test_revision_one_hash_still_includes_every_checkpoint_and_assertion_byte(boundary):
    connection = boundary
    actual = await seal(connection)
    assert actual["receipt"]["revision"] == 1
    report = connection.candidate["validation_json"]
    assertion = _event_assertion("cms")
    report[custody.SOURCE_EVENT_ASSERTION] = assertion
    report["cms_source_notification"] = copy.deepcopy(assertion["checkpoint"])
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await verify(connection)


@pytest.mark.parametrize("kind", ["cms", "ptg", "aca"])
async def test_revision_two_unknown_commit_reuses_exact_ready_assertion_and_seal(boundary, kind):
    connection = boundary
    connection.candidate["validation_json"][custody.SOURCE_EVENT_ASSERTION] = _event_assertion(kind)
    connection.unknown_commit = True
    with pytest.raises(RuntimeError, match="acknowledgement"):
        await seal(connection)
    retained = copy.deepcopy(connection.seal)
    assert (await seal(connection))["receipt"]["revision"] == 2
    assert connection.seal == retained and connection.proofs == 1


async def test_initial_cms_assertion_is_closed_and_preserves_exact_initial_mode(boundary):
    assertion = _event_assertion("cms")
    assertion["checkpoint"].update(replaced_coordinates=None, mode="initial", base_manifest=None)
    boundary.candidate["validation_json"][custody.SOURCE_EVENT_ASSERTION] = assertion
    assert (await seal(boundary))["receipt"]["revision"] == 2


@pytest.mark.parametrize("damage", ["missing", "extra", "digest"])
async def test_ptg_assertion_requires_closed_native_header(boundary, damage):
    assertion = _event_assertion("ptg")
    header = assertion["checkpoint"]["header"]
    if damage == "missing":
        del header["hold_sha256"]
    elif damage == "extra":
        header["extra"] = True
    else:
        header["approval_sha256"] = "invalid"
    boundary.candidate["validation_json"][custody.SOURCE_EVENT_ASSERTION] = assertion
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await seal(boundary)
    assert boundary.seal is None and boundary.catalog["namespace"]["nspowner"] == 31


@pytest.mark.parametrize("field", ["recipe_sha256", "notification_sha256", "capture_id", "extra"])
async def test_aca_assertion_rejects_changed_or_unclosed_native_header(boundary, field):
    assertion = _event_assertion("aca")
    assertion["checkpoint"]["header"][field] = "invalid"
    boundary.candidate["validation_json"][custody.SOURCE_EVENT_ASSERTION] = assertion
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await seal(boundary)
    assert boundary.seal is None


def _split_control(connection):
    connection.control.update(
        schema_owner_oid=15,
        schema_owner_role="schema_owner",
        candidate_owner_oid=15,
        candidate_owner_role="schema_owner",
    )
    connection.forbidden_set.add(15)
    connection.noninherited.add(15)


@pytest.mark.parametrize("kind", [None, "cms", "ptg", "aca"])
async def test_split_seal_binds_owners_and_original_metadata_locker(boundary, kind):
    _split_control(boundary)
    report = boundary.candidate["validation_json"]
    if kind is not None:
        assertion = _event_assertion(kind)
        report[custody.SOURCE_EVENT_ASSERTION] = assertion
    sealed = await seal(boundary)
    assert sealed["receipt"]["revision"] == 3
    assert sealed["receipt"]["control"] == {
        "database_oid": 12,
        "database_name": "synthetic",
        "schema_oid": 10,
        "owner_oid": 11,
        "owner_role": "control_owner",
        "candidate_table_oid": 13,
        "seal_table_oid": 14,
        "schema_owner_oid": 15,
        "schema_owner_role": "schema_owner",
        "candidate_owner_oid": 15,
        "candidate_owner_role": "schema_owner",
    }
    assert await seal(boundary) == sealed
    assert boundary.metadata_lock_roles == ["operator", "operator"]
    assert not any(sql == 'SET LOCAL ROLE "schema_owner"' for sql, _ in boundary.calls)
    if kind is not None:
        report[custody._SOURCE_CHECKPOINTS[kind]] = copy.deepcopy(assertion["checkpoint"])
    boundary.candidate["state"] = "published"
    assert await verify(boundary) == sealed
    assert boundary.proofs == 1
    if kind is not None:
        del report[custody._SOURCE_CHECKPOINTS[kind]]
        with pytest.raises(custody.NetworkCandidateCustodyError):
            await verify(boundary)


async def test_split_owner_refuses_ordinary_operator_that_can_assume_schema_owner(boundary):
    _split_control(boundary)
    boundary.forbidden_set.clear()
    before = copy.deepcopy(boundary.catalog)
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await seal(boundary)
    assert boundary.catalog == before and boundary.seal is None


async def test_split_owner_refuses_inherited_schema_owner_even_without_set_privilege(boundary):
    _split_control(boundary)
    boundary.noninherited.clear()
    before = copy.deepcopy(boundary.catalog)
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await seal(boundary)
    assert boundary.catalog == before and boundary.seal is None


async def test_split_owner_refuses_membership_only_schema_owner_path(boundary):
    _split_control(boundary)
    boundary.memberships.add(15)
    before = copy.deepcopy(boundary.catalog)
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await seal(boundary)
    assert boundary.catalog == before and boundary.seal is None


async def test_split_owner_preserves_original_superuser_maintenance_path(boundary):
    _split_control(boundary)
    boundary.actor["rolsuper"] = True
    boundary.forbidden_set.clear()
    assert (await seal(boundary))["receipt"]["revision"] == 3


@pytest.mark.parametrize("revision", [1, 2])
async def test_old_seals_never_reinterpret_single_owner_after_ledger_transfer(boundary, revision):
    if revision == 2:
        boundary.candidate["validation_json"][custody.SOURCE_EVENT_ASSERTION] = _event_assertion("cms")
    sealed = await seal(boundary)
    assert sealed["receipt"]["revision"] == revision
    _split_control(boundary)
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await verify(boundary)


@pytest.mark.parametrize(
    "field",
    [
        "schema_owner_oid",
        "schema_owner_role",
        "candidate_owner_oid",
        "candidate_owner_role",
        "owner_oid",
        "rolname",
        "candidate_table_oid",
        "seal_table_oid",
    ],
)
async def test_split_seal_rejects_owner_or_relation_identity_drift(boundary, field):
    _split_control(boundary)
    await seal(boundary)
    original = boundary.control[field]
    boundary.control[field] = original + 1 if type(original) is int else original + "_changed"
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await verify(boundary)


async def test_retained_address_guard_refusal_preserves_synthetic_candidate(boundary):
    before = copy.deepcopy(boundary.catalog)
    boundary.address_safe = False
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await seal(boundary)
    assert boundary.address_checks == 1
    assert boundary.catalog == before and boundary.seal is None and boundary.proofs == 0
    assert not any(sql.startswith("INSERT INTO") for sql, _ in boundary.calls)


async def test_retained_address_guard_is_rechecked_for_seal_and_replay(boundary):
    receipt = await seal(boundary)
    assert boundary.address_checks >= 2
    checked = boundary.address_checks
    assert await verify(boundary) == receipt
    assert boundary.address_checks == checked + 1
    boundary.address_safe = False
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await verify(boundary)
    assert boundary.seal["seal_sha256"] == receipt["seal_sha256"]
