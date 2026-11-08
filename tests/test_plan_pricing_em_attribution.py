# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure custody and replay contracts; genuine PostgreSQL qualification is separate."""

from dataclasses import FrozenInstanceError, asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import plan_pricing_em_attribution as attribution
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from api.plan_release_serving import PlanReleaseServingSelection, PlanReleaseSnapshotBinding
from process.network_custom_address_source import NetworkCustomAddressSourceError


def _binding(ordinal=3, source="source-a"):
    return PlanReleaseSnapshotBinding(ordinal, "snapshot-" + source, source, "plan-a", "group", "in_network", True)


def _selection(*bindings):
    return PlanReleaseServingSelection(
        "hpserve_" + "1" * 26,
        "hprelease_" + "2" * 26,
        "hpplan_" + "3" * 26,
        None,
        "2026-01",
        "published",
        "a" * 64,
        bindings or (_binding(), _binding(17, "source-b")),
    )


def _relations():
    common_by_field = {
        "relowner": 7,
        "relpersistence": b"p",
        "relrowsecurity": False,
        "relforcerowsecurity": False,
        "relhastriggers": False,
        "relhasrules": False,
    }
    relations = [common_by_field | {"relname": "rates", "oid": 11, "filenode": 21, "relkind": b"r"}]
    for name, oid, filenode, keys in [("rates_lookup", 12, 22, "2 3 1"), ("rates_pkey", 13, 23, "1 2 3")]:
        relations.append(
            common_by_field
            | {
                "relname": name,
                "oid": oid,
                "filenode": filenode,
                "relkind": b"i",
                "indrelid": 11,
                "indisvalid": True,
                "indisready": True,
                "indislive": True,
                "no_predicate": True,
                "no_expression": True,
                "amname": "btree",
                "indnatts": 3,
                "indnkeyatts": 3,
                "indkey": keys,
                "indisprimary": name == "rates_pkey",
                "indisunique": name == "rates_pkey",
            }
        )
    return relations


def _catalog():
    return {
        "namespace": {"oid": 5, "nspowner": 7},
        "relations": _relations(),
        "heaps": {"rates": 11},
        "shape": {"columns": [], "constraints": []},
    }


def _receipt(selection=None, **changes):
    selection = selection or _selection()
    receipt = attribution.EMAttributionReceipt(
        *attribution._identity(selection),
        "artifact_owner",
        ("artifact_reader",),
        5,
        7,
        11,
        21,
        (("rates_lookup", 12, 22), ("rates_pkey", 13, 23)),
        attribution._catalog_digest(_catalog()),
        12,
        2,
        (3, 17),
        tuple((index, 2, "38") for index in range(6)),
        "c" * 64,
        "",
    )
    receipt = replace(receipt, **changes)
    return replace(receipt, receipt_sha256=attribution._receipt_digest(receipt))


def _role(**changes):
    return {
        "oid": 7,
        **{
            flag: False
            for flag in ("rolcanlogin", "rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls")
        },
        **changes,
    }


class _Savepoint:
    def __init__(self, outcomes):
        self.outcomes = outcomes

    async def __aenter__(self):
        self.outcomes.append("entered")

    async def __aexit__(self, kind, error, traceback):
        self.outcomes.append("rollback" if kind else "released")


class _Session:
    def __init__(self):
        self.outcomes = []
        self.execute = AsyncMock()

    def begin_nested(self):
        return _Savepoint(self.outcomes)


def _resolver_setup(monkeypatch, selection=None, receipt=None):
    selection = selection or _selection()
    receipt = receipt or _receipt(selection)
    driver = SimpleNamespace(
        execute=AsyncMock(), fetchrow=AsyncMock(return_value=_role()), fetchval=AsyncMock(return_value='"publisher"')
    )
    monkeypatch.setattr(attribution, "_lock_preparation", AsyncMock())
    monkeypatch.setattr(attribution, "_driver", AsyncMock(return_value=driver))
    monkeypatch.setattr(attribution.builder, "_selected_release", AsyncMock(return_value=selection))
    monkeypatch.setattr(
        attribution, "_namespace", AsyncMock(return_value={"receipt": attribution._canonical(receipt.as_dict())})
    )
    monkeypatch.setattr(attribution, "_catalog", AsyncMock(return_value=_catalog()))
    monkeypatch.setattr(attribution, "_closed", AsyncMock())
    monkeypatch.setattr(attribution, "_check_roles", AsyncMock())
    monkeypatch.setattr(attribution, "_content", AsyncMock(side_effect=AssertionError("Reader must not rehash rows")))
    return driver, selection, receipt


def test_full_recipe_retains_duplicates_without_double_price_contribution():
    first = _binding()
    duplicate = replace(first, binding_ordinal=19)
    selection = _selection(first, duplicate, _binding(17, "source-b"))
    assert selection.in_network_bindings == (first, selection.bindings[2])
    assert [binding.binding_ordinal for binding in selection.in_network_bindings] == [3, 17]
    identity = attribution._identity(selection)
    assert '"binding_ordinal":19' in identity[0]
    changed = replace(selection, bindings=(first, replace(duplicate, required=False), selection.bindings[2]))
    assert changed.in_network_bindings == selection.in_network_bindings
    assert attribution._identity(changed) != identity
    assert attribution._identity(replace(selection, pricing_projection_id="f" * 64)) == identity
    assert attribution._identity(replace(selection, serving_revision_id="hpserve_" + "4" * 26)) != identity
    assert attribution._identity(replace(selection, binding_set_digest="b" * 64)) != identity


def test_receipt_is_immutable_closed_and_canonical():
    receipt = _receipt()
    assert attribution._decode(attribution._canonical(receipt.as_dict())) == receipt
    assert receipt.rates_relation == f'"{receipt.schema_name}"."rates"'
    with pytest.raises(FrozenInstanceError):
        receipt.row_count = 1
    with pytest.raises((ValueError, TypeError)):
        attribution._decode(attribution._canonical(receipt.as_dict() | {"extra": True}))
    with pytest.raises(ValueError):
        attribution._decode('{"contract":"x","contract":"y"}')
    with pytest.raises(ValueError):
        attribution._decode("x" * (attribution._MAX_RECEIPT_BYTES + 1))
    with pytest.raises(ValueError):
        attribution._decode(attribution._canonical(asdict(receipt)))


@pytest.mark.parametrize(
    "changes",
    [
        {"row_count": 0},
        {"row_count": True},
        {"row_count": attribution.builder.MAX_EM_ATTRIBUTED_RATE_ROWS + 1},
        {"provider_count": attribution.builder.MAX_EM_RATE_ROWS + 1},
        {"binding_ordinals": (19,)},
        {"binding_ordinals": (17, 3)},
        {"code_accounting": ((0, 12, "1"),)},
        {"code_accounting": tuple((index, 2, "0") for index in range(6))},
        {"code_accounting": tuple((index, 3, "38") for index in range(6))},
        {"table_filenode": 0},
        {"index_identities": ((),)},
        {"runtime_roles": ("artifact_owner",)},
    ],
)
def test_accounting_and_native_identity_refuse_invalid_receipts(changes):
    with pytest.raises((ValueError, TypeError)):
        attribution._validate_receipt(_receipt(**changes))


@pytest.mark.parametrize(
    "field,change",
    [
        ("relpersistence", b"u"),
        ("relhastriggers", True),
        ("relhasrules", True),
        ("relrowsecurity", True),
        ("indisvalid", False),
        ("indisready", False),
        ("indislive", False),
        ("indkey", "2 1 3"),
        ("indnatts", 4),
        ("indisunique", True),
        ("no_predicate", False),
        ("no_expression", False),
        ("filenode", 0),
    ],
)
def test_native_relation_shapes_are_closed(field, change):
    relations = _relations()
    relations[1][field] = change
    with pytest.raises(ValueError):
        attribution._validate_relations(relations)


@pytest.mark.asyncio
async def test_reader_revalidates_current_full_selection_and_custody(monkeypatch):
    driver, selection, receipt = _resolver_setup(monkeypatch)
    session = object()
    assert await attribution.resolve_em_attribution(session, selection) == receipt
    assert driver.execute.await_args.args == (f"LOCK TABLE {receipt.rates_relation} IN ACCESS SHARE MODE",)
    attribution._closed.assert_awaited_once_with(driver, 5, 7, ("artifact_reader",), None)
    attribution._content.assert_not_awaited()
    attribution.builder._selected_release.assert_awaited_once_with(
        session, selection.plan_release_id, selection.serving_revision_id
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", ["absent", "malformed", "stale", "filtered", "filenode", "index", "privilege", "owner"]
)
async def test_reader_refuses_missing_stale_or_open_artifacts(monkeypatch, fault):
    driver, selection, receipt = _resolver_setup(monkeypatch)
    if fault in {"absent", "malformed"}:
        attribution._namespace.return_value = None if fault == "absent" else {"receipt": "{}"}
    if fault == "stale":
        attribution.builder._selected_release.return_value = replace(selection, binding_set_digest="e" * 64)
    if fault == "filtered":
        selection = replace(selection, bindings=(selection.bindings[0],))
    if fault in {"filenode", "index"}:
        catalog = _catalog()
        catalog["relations"][0 if fault == "filenode" else 1]["filenode"] += 1
        attribution._catalog.return_value = catalog
    if fault == "privilege":
        attribution._closed.side_effect = NetworkCustomAddressSourceError("private privilege detail")
    if fault == "owner":
        driver.fetchrow.return_value = _role(rolcanlogin=True)
    with pytest.raises(PlanPricingProjectionUnavailable, match="^E&M attribution is unavailable$"):
        await attribution.resolve_em_attribution(object(), selection)
    attribution._content.assert_not_awaited()


@pytest.mark.asyncio
async def test_replay_checks_full_recipe_and_never_rebuilds(monkeypatch):
    _, selection, receipt = _resolver_setup(monkeypatch)
    monkeypatch.setattr(
        attribution.builder, "_create_stage_tables", AsyncMock(side_effect=AssertionError("no rebuild"))
    )
    session = _Session()
    assert (
        await attribution.prepare_em_attribution_in_session(
            session,
            plan_release_id=selection.plan_release_id,
            serving_revision_id=selection.serving_revision_id,
            owner_role="artifact_owner",
            runtime_roles=("artifact_reader",),
        )
        == receipt
    )
    assert session.outcomes == ["entered", "released"]
    attribution.builder._create_stage_tables.assert_not_awaited()
    for roles in [("other_owner", ("artifact_reader",)), ("artifact_owner", ("other_reader",))]:
        with pytest.raises(PlanPricingProjectionUnavailable):
            await attribution.prepare_em_attribution_in_session(
                session,
                plan_release_id=selection.plan_release_id,
                serving_revision_id=selection.serving_revision_id,
                owner_role=roles[0],
                runtime_roles=roles[1],
            )
    attribution._namespace.return_value = {"receipt": None}
    with pytest.raises(PlanPricingProjectionUnavailable):
        await attribution.prepare_em_attribution_in_session(
            session,
            plan_release_id=selection.plan_release_id,
            serving_revision_id=selection.serving_revision_id,
            owner_role="artifact_owner",
            runtime_roles=("artifact_reader",),
        )


@pytest.mark.asyncio
async def test_prepare_reuses_full_builder_and_rollback_savepoint(monkeypatch):
    first = _binding()
    selection = _selection(first, replace(first, binding_ordinal=19), _binding(17, "source-b"))
    _, _, receipt = _resolver_setup(monkeypatch, selection)
    attribution._namespace.return_value = None
    monkeypatch.setattr(attribution, "_check_roles", AsyncMock())
    monkeypatch.setattr(attribution.builder, "_create_stage_tables", AsyncMock())
    projections = [object(), object()]
    monkeypatch.setattr(attribution.builder, "_binding_projections", AsyncMock(return_value=projections))
    monkeypatch.setattr(attribution.builder, "_stage_em_rates", AsyncMock())
    monkeypatch.setattr(attribution, "_publish", AsyncMock(return_value=receipt))
    session = _Session()
    arguments_by_name = dict(
        plan_release_id=selection.plan_release_id,
        serving_revision_id=selection.serving_revision_id,
        owner_role="artifact_owner",
        runtime_roles=("artifact_reader",),
    )
    assert await attribution.prepare_em_attribution_in_session(session, **arguments_by_name) == receipt
    attribution.builder._binding_projections.assert_awaited_once_with(session, selection)
    assert session.execute.await_args.args[0].text == attribution.builder._CREATE_RATE_STAGE_SQL
    assert attribution.builder._stage_em_rates.await_args.args[2] is projections
    attribution.builder._stage_em_rates.side_effect = ValueError("private staging detail")
    attribution._publish.reset_mock()
    with pytest.raises(PlanPricingProjectionUnavailable, match="^E&M attribution preparation is unavailable$"):
        await attribution.prepare_em_attribution_in_session(session, **arguments_by_name)
    assert session.outcomes[-1] == "rollback"
    attribution._publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_native_copy_constraints_and_accounting_are_set_based():
    connection = SimpleNamespace(execute=AsyncMock(), fetchval=AsyncMock(return_value=False))
    await attribution._create_rates(connection, "em_attribution_" + "a" * 40)
    statements = [call.args[0] for call in connection.execute.await_args_list]
    assert len(statements) == 4
    assert "PRIMARY KEY(binding_ordinal,npi,code_index)" in statements[1]
    assert "code_index BETWEEN 0 AND 5" in statements[1] and "'NaN','Infinity','-Infinity'" in statements[1]
    assert (
        "INSERT INTO" in statements[2]
        and "SELECT binding_ordinal,npi,code_index,minimum_rate,maximum_rate,rate_count" in statements[2]
    )
    assert "FROM plan_pricing_em_distance_rate_stage" in statements[2]
    assert "(npi,code_index,binding_ordinal)" in statements[3]
    assert connection.fetchval.await_args.args[0].count("EXCEPT ALL") == 2
    connection.fetchval.return_value = True
    with pytest.raises(ValueError, match="copied prices"):
        await attribution._create_rates(connection, "em_attribution_" + "a" * 40)


@pytest.mark.asyncio
async def test_transaction_requirement_uses_actual_session_driver():
    driver = SimpleNamespace(is_in_transaction=lambda: True, fetchval=AsyncMock(return_value="repeatable read"))
    session = SimpleNamespace(
        in_transaction=lambda: True,
        connection=AsyncMock(
            return_value=SimpleNamespace(
                get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver))
            )
        ),
    )
    assert await attribution._driver(session) is driver
    driver.fetchval.return_value = "read committed"
    with pytest.raises(ValueError, match="repeatable-read"):
        await attribution._driver(session)
    session.in_transaction = lambda: False
    with pytest.raises(ValueError, match="repeatable-read"):
        await attribution._driver(session)


@pytest.mark.asyncio
async def test_operator_scope_restores_role_even_when_verification_fails(monkeypatch):
    receipt = _receipt()
    connection = SimpleNamespace(execute=AsyncMock(), fetchval=AsyncMock(return_value='"publisher"'))
    monkeypatch.setattr(attribution, "_verify", AsyncMock(side_effect=ValueError("custody changed")))
    with pytest.raises(ValueError, match="custody changed"):
        await attribution._owner_verify(connection, receipt, receipt.identity_json)
    assert [call.args[0] for call in connection.execute.await_args_list] == [
        'SET LOCAL ROLE "artifact_owner"',
        'SET LOCAL ROLE "publisher"',
    ]


@pytest.mark.asyncio
async def test_publication_hashes_native_chunks_and_restores_operator(monkeypatch):
    selection = _selection()
    connection = SimpleNamespace(execute=AsyncMock(), fetchval=AsyncMock(side_effect=[7, '"publisher"', "'receipt'"]))
    monkeypatch.setattr(attribution, "_create_rates", AsyncMock())
    accounting = (12, 2, (3, 17), tuple((index, 2, "38") for index in range(6)))
    monkeypatch.setattr(attribution, "_accounting", AsyncMock(return_value=accounting))
    monkeypatch.setattr(attribution, "_content", AsyncMock(return_value=(12, "c" * 64)))
    monkeypatch.setattr(attribution, "_catalog", AsyncMock(return_value=_catalog()))
    monkeypatch.setattr(attribution, "_transfer_and_grant", AsyncMock())

    async def verify_native(_connection, receipt, identity_json):
        attribution._validate_receipt(receipt)
        assert receipt.identity_json == identity_json
        return receipt

    monkeypatch.setattr(attribution, "_verify", verify_native)
    receipt = await attribution._publish(
        connection, selection, attribution._identity(selection), "artifact_owner", ("artifact_reader",)
    )
    assert receipt == _receipt(selection)
    attribution._content.assert_awaited_once_with(connection, receipt.schema_name, "rates")
    assert connection.execute.await_args_list[0].args == ('SET LOCAL ROLE "artifact_owner"',)
    assert "COMMENT ON SCHEMA" in connection.execute.await_args_list[1].args[0]
    assert connection.execute.await_args_list[2].args == ('SET LOCAL ROLE "publisher"',)
    attribution._content.return_value = (11, "c" * 64)
    attribution._transfer_and_grant.reset_mock()
    with pytest.raises(ValueError, match="content accounting"):
        await attribution._publish(
            connection, selection, attribution._identity(selection), "artifact_owner", ("artifact_reader",)
        )
    attribution._transfer_and_grant.assert_not_awaited()


@pytest.mark.asyncio
async def test_native_columns_and_constraints_require_exact_shape():
    columns = [
        {
            "attnum": index,
            "attname": name,
            "type": kind,
            "attnotnull": True,
            "attidentity": "",
            "attgenerated": "",
            "attcollation": 0,
            "default_expr": None,
        }
        for index, (name, kind) in enumerate(attribution._COLUMNS, 1)
    ]
    constraints = [
        {
            "conname": name,
            "contype": kind,
            "conkey": "{" + keys.replace(" ", ",") + "}",
            "convalidated": True,
            "condeferrable": False,
            "condeferred": False,
            "definition": "native check",
        }
        for name, (kind, keys) in attribution._CONSTRAINT_KEYS.items()
    ]
    connection = SimpleNamespace(fetch=AsyncMock(side_effect=[columns, constraints]))
    shape = await attribution._table_shape(connection, 11)
    assert len(shape["columns"]) == 6 and len(shape["constraints"]) == 7
    assert "AND contype<>'n'" in connection.fetch.await_args.args[0]
    for invalid_columns, invalid_constraints in [
        (columns[:-1], constraints),
        ([columns[0] | {"attidentity": "a"}, *columns[1:]], constraints),
        ([columns[0] | {"attgenerated": "s"}, *columns[1:]], constraints),
        (columns, constraints[:-1]),
        (columns, [constraints[0] | {"convalidated": False}, *constraints[1:]]),
    ]:
        connection.fetch.side_effect = [invalid_columns, invalid_constraints]
        with pytest.raises(ValueError):
            await attribution._table_shape(connection, 11)


@pytest.mark.asyncio
async def test_prefix_collision_refuses_replay_without_repair(monkeypatch):
    old_receipt = _receipt()
    selection = replace(_selection(), binding_set_digest="d" * 64)
    driver, _, _ = _resolver_setup(monkeypatch, selection, old_receipt)
    monkeypatch.setattr(attribution.builder, "_create_stage_tables", AsyncMock())
    with pytest.raises(PlanPricingProjectionUnavailable):
        await attribution.prepare_em_attribution_in_session(
            _Session(),
            plan_release_id=selection.plan_release_id,
            serving_revision_id=selection.serving_revision_id,
            owner_role="artifact_owner",
            runtime_roles=("artifact_reader",),
        )
    attribution.builder._create_stage_tables.assert_not_awaited()
    assert not any("COMMENT" in call.args[0] or "CREATE" in call.args[0] for call in driver.execute.await_args_list)


def test_check_column_order_and_exact_capacity_are_preserved():
    assert attribution._constraint_columns({"contype": "c", "conkey": "{5,4}"}) == "4 5"
    assert attribution._constraint_columns({"contype": "c", "conkey": "{4,5}"}) == "4 5"
    assert attribution._constraint_columns({"contype": "p", "conkey": "{3,2,1}"}) == "3 2 1"
    row_count = attribution.builder.MAX_EM_ATTRIBUTED_RATE_ROWS
    receipt = _receipt(
        row_count=row_count,
        provider_count=attribution.builder.MAX_EM_RATE_ROWS,
        code_accounting=tuple((index, row_count // 6, str(row_count)) for index in range(6)),
    )
    attribution._validate_receipt(receipt)


@pytest.mark.parametrize("market", ["", "group"])
def test_legacy_blank_market_is_preserved_in_full_recipe(market):
    selection = _selection(replace(_binding(), plan_market_type=market), _binding(17, "source-b"))
    receipt = _receipt(selection)
    assert attribution._decode(attribution._canonical(receipt.as_dict())) == receipt
    assert '"plan_market_type":"' + market + '"' in receipt.identity_json
    assert attribution._identity(selection) != attribution._identity(
        replace(
            selection, bindings=(replace(selection.bindings[0], plan_market_type="individual"), selection.bindings[1])
        )
    )
    with pytest.raises(ValueError, match="binding identity"):
        attribution._identity(_selection(replace(_binding(), plan_market_type=None)))


@pytest.mark.asyncio
@pytest.mark.parametrize("has_overflow", [False, True, None])
async def test_provider_code_count_guard_precedes_publication_accounting(has_overflow):
    maximum = 2**63 - 1
    connection = SimpleNamespace(
        fetchval=AsyncMock(return_value=has_overflow),
        fetchrow=AsyncMock(return_value={"row_count": 12, "provider_count": 1, "ordinals": [3, 17]}),
        fetch=AsyncMock(
            return_value=[{"code_index": index, "row_count": 2, "count": str(maximum)} for index in range(6)]
        ),
    )
    if has_overflow is False:
        accounting = await attribution._accounting(connection, "em_attribution_" + "a" * 40)
        assert accounting[3] == tuple((index, 2, str(maximum)) for index in range(6))
    else:
        with pytest.raises(ValueError, match="signed bigint"):
            await attribution._accounting(connection, "em_attribution_" + "a" * 40)
        connection.fetchrow.assert_not_awaited()
        connection.fetch.assert_not_awaited()
    query = connection.fetchval.await_args.args[0]
    assert "GROUP BY npi,code_index" in query
    assert "sum(rate_count::numeric)>9223372036854775807" in query
    assert "ORDER BY npi,code_index LIMIT 1" in query


@pytest.mark.asyncio
@pytest.mark.parametrize("is_locked", [True, False, None, 1])
async def test_preparation_lock_uses_full_identity_and_never_waits(is_locked):
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=is_locked))
    identity_sha256 = attribution._identity(_selection())[1]
    if is_locked is True:
        await attribution._lock_preparation(connection, identity_sha256)
    else:
        with pytest.raises(ValueError, match="fresh transaction"):
            await attribution._lock_preparation(connection, identity_sha256)
    connection.fetchval.assert_awaited_once_with(
        "SELECT pg_try_advisory_xact_lock(hashtextextended($1::text,0))", identity_sha256
    )
    connection.fetchval.reset_mock()
    with pytest.raises(ValueError, match="lock identity"):
        await attribution._lock_preparation(connection, identity_sha256[:40])
    connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
async def test_busy_prepare_rolls_back_before_namespace_or_builder(monkeypatch):
    driver, selection, _ = _resolver_setup(monkeypatch)
    monkeypatch.setattr(attribution, "_lock_preparation", AsyncMock(side_effect=ValueError("busy")))
    monkeypatch.setattr(attribution.builder, "_create_stage_tables", AsyncMock())
    session = _Session()
    with pytest.raises(PlanPricingProjectionUnavailable, match="^E&M attribution preparation is unavailable$"):
        await attribution.prepare_em_attribution_in_session(
            session,
            plan_release_id=selection.plan_release_id,
            serving_revision_id=selection.serving_revision_id,
            owner_role="artifact_owner",
            runtime_roles=("artifact_reader",),
        )
    attribution._lock_preparation.assert_awaited_once_with(driver, attribution._identity(selection)[1])
    attribution._namespace.assert_not_awaited()
    attribution._check_roles.assert_not_awaited()
    attribution.builder._create_stage_tables.assert_not_awaited()
    assert session.outcomes == ["entered", "rollback"]


@pytest.mark.asyncio
async def test_prepare_lock_precedes_exact_replay_and_preserves_identity(monkeypatch):
    driver, selection, receipt = _resolver_setup(monkeypatch)
    events = []

    async def locked(_connection, identity_sha256):
        assert _connection is driver and identity_sha256 == receipt.identity_sha256
        events.append("lock")

    async def namespace(_connection, schema_name):
        assert events[0] == "lock" and schema_name == receipt.schema_name
        events.append("namespace")
        return {"receipt": attribution._canonical(receipt.as_dict())}

    monkeypatch.setattr(attribution, "_lock_preparation", locked)
    monkeypatch.setattr(attribution, "_namespace", namespace)
    assert (
        await attribution.prepare_em_attribution_in_session(
            _Session(),
            plan_release_id=selection.plan_release_id,
            serving_revision_id=selection.serving_revision_id,
            owner_role="artifact_owner",
            runtime_roles=("artifact_reader",),
        )
        == receipt
    )
    assert events == ["lock", "namespace", "namespace"]


class _BuildTransaction:
    def __init__(self, session):
        self.session = session
        self.outcomes = []

    async def __aenter__(self):
        self.outcomes.append("begin")
        return self.session

    async def __aexit__(self, kind, error, traceback):
        self.outcomes.append("rollback" if kind else "commit")


@pytest.mark.asyncio
@pytest.mark.parametrize("has_failure", [False, True])
async def test_public_build_owns_rr_transaction_and_json_receipt(monkeypatch, has_failure):
    receipt = _receipt()
    session = SimpleNamespace(execute=AsyncMock())
    transaction = _BuildTransaction(session)
    transaction_factory = lambda: transaction
    monkeypatch.setattr(attribution.db, "transaction", transaction_factory)

    async def prepare(_session, **parameters_by_name):
        assert _session is session
        assert session.execute.await_args.args[0].text == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"
        assert parameters_by_name == {
            "plan_release_id": _selection().plan_release_id,
            "serving_revision_id": _selection().serving_revision_id,
            "owner_role": "artifact_owner",
            "runtime_roles": ("artifact_reader",),
        }
        if has_failure:
            raise PlanPricingProjectionUnavailable("E&M attribution preparation is unavailable")
        return receipt

    monkeypatch.setattr(attribution, "prepare_em_attribution_in_session", prepare)
    parameters_by_name = dict(
        plan_release_id=_selection().plan_release_id,
        serving_revision_id=_selection().serving_revision_id,
        owner_role="artifact_owner",
        runtime_roles=["artifact_reader"],
    )
    if has_failure:
        with pytest.raises(PlanPricingProjectionUnavailable):
            await attribution.build_plan_pricing_em_attribution(**parameters_by_name)
    else:
        document_by_field = await attribution.build_plan_pricing_em_attribution(**parameters_by_name)
        assert attribution._decode(attribution._canonical(document_by_field)) == receipt
    assert transaction.outcomes == ["begin", "rollback" if has_failure else "commit"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "runtime_roles",
    [
        None,
        "artifact_reader",
        ("artifact_reader",),
        [],
        ["artifact_reader"] * 2,
        ["z_reader", "a_reader"],
        ["artifact_owner"],
        [1],
        ["reader"] * 33,
    ],
)
async def test_public_build_refuses_noncanonical_json_roles_before_database(monkeypatch, runtime_roles):
    transaction = AsyncMock(side_effect=AssertionError("no database before validation"))
    monkeypatch.setattr(attribution.db, "transaction", transaction)
    with pytest.raises(ValueError):
        await attribution.build_plan_pricing_em_attribution(
            plan_release_id=_selection().plan_release_id,
            serving_revision_id=_selection().serving_revision_id,
            owner_role="artifact_owner",
            runtime_roles=runtime_roles,
        )
    transaction.assert_not_called()
