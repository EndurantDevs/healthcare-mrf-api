# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure boundaries for adoption validation; SQL execution requires native proof."""

import asyncio
import importlib.util
import json
import re
from types import SimpleNamespace

import pytest

from process import provider_directory_rooted_graph_history_validation as history
from process.fhir_request_failure_policy import FHIR_REQUEST_FAILURE_POLICY_ID
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.provider_directory_rooted_graph_store_identity import _canonical_json


def _policy():
    return GenericSourcePolicy("sample", "sample_owner", "sample_publisher", ("sample_reader",))


def _family():
    names = [history._HEADER, *(name for root in history._ROOTS for name in (root, root + "_legacy"))]
    root_by_name = {name: dict(oid=index + 100, parent_oid=None) for index, name in enumerate(names)}
    control_by_name = {
        name: {"catalog": {"classes": [dict(relname=name, oid=index + 200)]}}
        for index, name in enumerate(("provider_directory_source", "provider_directory_api_endpoint"))
    }
    header_by_name = {
        name: dict(name=name, oid=index + 300, catalog={"classes": [name]}, columns=["dataset_id"])
        for index, name in enumerate(
            (
                "provider_directory_endpoint_dataset",
                "provider_directory_uhc_flex_practitioner_dataset",
                "provider_directory_rooted_graph_dataset",
            )
        )
    }
    payloads = [
        dict(
            relname="provider_directory_dataset_resource",
            oid=400,
            relkind="p",
            parents=[],
            direct_acl_closed=True,
            readers_closed=True,
            native_checks=True,
        )
    ]
    return dict(
        schema="sample",
        roots=root_by_name,
        relations={name: {} for name in names},
        members=[],
        acquisitions={root: {} for root in history._ROOTS},
        control_inputs=control_by_name,
        source_inputs=dict(headers=header_by_name, family=payloads),
    )


def _candidate(family, *, attached=False):
    acquisition = "pdrga_" + "1" * 48
    for root, prefix in history._PREFIXES.items():
        family["acquisitions"][root][acquisition] = "sealed" if attached else "building"
        member_by_field = dict(
            name=prefix + acquisition[6:],
            root=root,
            oid=500 + len(family["members"]),
            parent_oid=family["roots"][root]["oid"] if attached else None,
            acquisition_id=acquisition,
            phase="sealed" if attached else "indexed",
            bounds=[acquisition] if attached else None,
        )
        family["members"].append(member_by_field)
        family["relations"][member_by_field["name"]] = {}


@pytest.mark.parametrize("attached", [False, True])
def test_complete_exact_members_use_only(attached):
    family = _family()
    _candidate(family, attached=attached)
    selected = history._relation_members(_policy(), family)
    assert len(selected) == 10
    for root in history._ROOTS:
        sql = history._selection("sample", selected, root)
        assert sql.count("FROM ONLY ") == 3
        assert sql.count(root + '_legacy"') == 1
        assert family["members"][history._ROOTS.index(root)]["name"] in sql


@pytest.mark.parametrize(
    "change,code",
    [
        (lambda family: family.update(schema="other"), "schema"),
        (lambda family: family["members"][0].update(oid=True), "native_identity"),
        (lambda family: family["members"][0].update(name="unissued"), "member"),
        (lambda family: family["members"][0].update(parent_oid=999), "parent"),
        (lambda family: family["members"][0].update(phase="sealed"), "phase"),
        (lambda family: family["members"][0].update(oid=100), "oid_reused"),
    ],
)
def test_member_drift_refuses(change, code):
    family = _family()
    _candidate(family)
    change(family)
    with pytest.raises(ValueError, match="rooted_graph_history_" + code):
        history._relation_members(_policy(), family)


def test_source_inputs_are_closed():
    family = _family()
    assert history._read_members(family)["provider_directory_dataset_resource"] == 400
    family["source_inputs"]["family"][0]["readers_closed"] = False
    with pytest.raises(ValueError, match="source_custody"):
        history._read_members(family)


def test_canonical_batch_matches_house_codec(monkeypatch):
    payloads = [dict(value=1.0, unicode="é"), dict(value=-0.0), dict(value=1e-7)]
    texts = [_canonical_json(payload) for payload in payloads]
    calls = []
    original = history._canonical_json

    def observed(value):
        calls.append(value)
        return original(value)

    monkeypatch.setattr(history, "_canonical_json", observed)
    history._decode_batch(texts, canonical=True)
    assert calls == [payloads]
    assert texts == ['{"unicode":"é","value":1.0}', '{"value":-0.0}', '{"value":1e-07}']


@pytest.mark.parametrize("text", ['{"a":1,"a":2}', '{ "a":1}', '{"a":1e-7}', '{"a":NaN}'])
def test_noncanonical_payload_refuses(text):
    with pytest.raises(ValueError, match="json_invalid"):
        history._decode_batch([text], canonical=True)


def test_missing_keeps_original_http_encoding():
    text = ' { "issue": [{"code":"not-found", "severity":"error"}], "resourceType":"OperationOutcome" } '
    history._decode_batch([text], canonical=False)
    with pytest.raises(ValueError, match="json_invalid"):
        history._decode_batch([text], canonical=True)


@pytest.mark.parametrize("text", ['{"x":1,"x":1}', '{"x":1e-7}', '{"x":Infinity}'])
def test_missing_strict_decoder_refuses(text):
    with pytest.raises(ValueError, match="json_invalid"):
        history._decode_batch([text], canonical=False)


class NativeBoundary:
    """Only the existing public driver boundary; no native authority is simulated."""

    def __init__(self, pages=(), error=None, close_error=None):
        self.pages = iter(pages)
        self.error = error
        self.close_error = close_error
        self.commands = []

    async def execute(self, sql, **options):
        self.commands.append((sql, options))
        if sql.startswith("CLOSE ") and self.close_error:
            raise self.close_error

    async def fetch(self, sql):
        self.commands.append((sql, {}))
        if self.error:
            raise self.error
        return next(self.pages, [])


@pytest.mark.asyncio
async def test_cursor_bounds_bytes_and_rows(monkeypatch):
    text = _canonical_json({"value": "a" * (history._BATCH_BYTES // 8 - 12)})
    rows = [dict(payload=text, bytes=len(text.encode())) for _ in range(17)]
    driver = NativeBoundary([rows[:16], rows[16:]])
    batches = []
    monkeypatch.setattr(history, "_decode_batch", lambda texts, **_options: batches.append(list(texts)))
    receipt = await history._canonical_cursor(driver, "SELECT payload", canonical=True)
    assert receipt == dict(rows=17, batches=3)
    assert [len(batch) for batch in batches] == [7, 7, 3]
    assert all(sum(len(value.encode()) + 1 for value in batch) + 2 <= history._BATCH_BYTES for batch in batches)
    assert driver.commands[0][0].startswith('DECLARE "pdrg_history_')
    assert driver.commands[-1][0].startswith('CLOSE "pdrg_history_')
    assert driver.commands[-1][1] == {"timeout": 10}


@pytest.mark.asyncio
async def test_cursor_accepts_maximum_single_resource():
    maximum = history.contract.PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RESOURCE_JSON_BYTES
    text = '"' + "a" * (maximum - 2) + '"'
    driver = NativeBoundary([[dict(payload=text, bytes=maximum)]])
    assert await history._canonical_cursor(driver, "SELECT payload", canonical=True) == dict(rows=1, batches=1)


@pytest.mark.asyncio
async def test_cursor_row_bound(monkeypatch):
    driver = NativeBoundary([[dict(payload="{}", bytes=2)] * 16, [dict(payload="{}", bytes=2)]])
    batches = []
    monkeypatch.setattr(history, "_decode_batch", lambda texts, **_options: batches.append(list(texts)))
    assert await history._canonical_cursor(driver, "SELECT payload", canonical=True) == dict(rows=17, batches=2)
    assert list(map(len, batches)) == [16, 1]


@pytest.mark.asyncio
async def test_cursor_refuses_oversize_and_closes():
    driver = NativeBoundary([[dict(payload="{}", bytes=65537)]])
    with pytest.raises(ValueError, match="payload_size"):
        await history._canonical_cursor(driver, "SELECT payload", canonical=False)
    assert driver.commands[-1][0].startswith("CLOSE ")


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [asyncio.CancelledError(), ValueError("original")])
async def test_failed_fetch_preserves_error(error):
    driver = NativeBoundary(error=error, close_error=RuntimeError("aborted"))
    with pytest.raises(type(error)) as caught:
        await history._canonical_cursor(driver, "SELECT payload", canonical=True)
    assert caught.value is error
    assert [command.split()[0] for command, _options in driver.commands] == ["DECLARE", "FETCH", "CLOSE"]


@pytest.mark.asyncio
async def test_successful_fetch_propagates_close_failure():
    driver = NativeBoundary(close_error=RuntimeError("close"))
    with pytest.raises(RuntimeError, match="close"):
        await history._canonical_cursor(driver, "SELECT payload", canonical=True)


def test_render_preserves_history_and_protocols():
    sql, members = history._render(_policy(), _family())
    assert len(members) == 7
    assert FHIR_REQUEST_FAILURE_POLICY_ID in sql
    assert "__" not in sql
    assert "clock_timestamp(" not in sql and "transaction_timestamp(" not in sql
    assert "r.attempt>w.attempt_count" in sql
    assert "all_resources<>selected_resources" in sql
    assert "used_payload_bytes IS DISTINCT FROM actual_bytes" in sql
    assert "EXCEPT SELECT" in sql and "reference_shape" in sql
    for resource_type, definitions in history.contract.PROVIDER_DIRECTORY_ROOTED_GRAPH_REFERENCE_FIELD_CONTRACT:
        for name, cardinality, target in definitions:
            assert f"('{resource_type}','{name}',{str(cardinality == 'repeated').lower()},'{target}')" in sql
    for contract in ("resource-set", "edge-set", "terminal-set", "terminal-record", "root"):
        assert "healthporta.provider-directory.rooted-graph-" + contract in sql


def test_sql_is_one_read_only_balanced_statement():
    sql, _members = history._render(_policy(), _family())
    masked = re.sub(r"'(?:''|[^'])*'|--[^\n]*", lambda match: " " * len(match[0]), sql)
    depth = 0
    for character in masked:
        depth += (character == "(") - (character == ")")
        assert depth >= 0
    assert depth == 0 and masked.count(";") == 1 and masked.rstrip().endswith(";")
    assert (
        re.search(r"\b(INSERT|UPDATE|DELETE|ALTER|CREATE|DROP|TRUNCATE|GRANT|REVOKE|LOCK|CALL|DO|COPY)\b", masked, re.I)
        is None
    )
    assert "pg_catalog.pg_locks" in sql and "read_relations" in sql


def _migration(filename):
    path = history._SQL.parents[2] / "alembic" / "versions" / filename
    specification = importlib.util.spec_from_file_location("_history_" + path.stem, path)
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def test_retained_lineage_has_no_current_alias():
    original = _migration("20260811020000_provider_directory_rooted_graph_acquisition.py")
    guard = original._acquisition_guard_sql("sample")
    creation, after_creation = guard.split("IF TG_OP = 'INSERT' THEN", 1)[1].split("RETURN NEW;", 1)
    assert "root_source.endpoint_id = NEW.root_endpoint_id" in creation
    assert "acquisition_source.endpoint_id =" in creation
    assert "root_source.endpoint_id" not in after_creation
    assert "acquisition_source.endpoint_id" not in after_creation
    latest = _migration("20260909120000_fhir_request_failure_budget.py")._coverage_lineage_sql("sample")
    assert "root_parent.status IN ('published', 'superseded')" in latest
    assert "root_header.source_id = candidate.root_source_id" in latest
    assert "root_header.endpoint_id = candidate.root_endpoint_id" in latest
    assert "root_source.endpoint_id" not in latest and "acquisition_source.endpoint_id" not in latest
    sql, _members = history._render(_policy(), _family())
    assert "rs.endpoint_id" not in sql and "s.endpoint_id" not in sql
    assert "ON re.endpoint_id=a.root_endpoint_id" in sql
    assert "p.status IN ('published','superseded')" in sql
    assert "h.source_id=a.root_source_id" in sql and "h.endpoint_id=a.root_endpoint_id" in sql
    assert f"a.acquisition_source_id='{original._ROOTED_SOURCE_ID}'" in sql
    assert f"a.acquisition_endpoint_id='{original._ROOTED_ENDPOINT_ID}'" in sql


@pytest.mark.asyncio
async def test_runner_identity_and_census(monkeypatch):
    receipt_by_field = dict(invalid=[], resources=0, missing_responses=0)
    state = SimpleNamespace(transaction=True, identities=iter([(1, "2", "admin", "admin")] * 2))
    driver = NativeBoundary()
    driver.is_in_transaction = lambda: state.transaction

    async def identity(_sql):
        return next(state.identities)

    async def aggregate(_sql):
        return json.dumps(receipt_by_field)

    driver.fetchrow, driver.fetchval = identity, aggregate
    assert await history.validate_rooted_graph_history(driver, _policy(), _family()) == {
        **receipt_by_field,
        "payload_batches": 0,
        "missing_batches": 0,
    }
    state.transaction = False
    with pytest.raises(ValueError, match="transaction"):
        await history.validate_rooted_graph_history(driver, _policy(), _family())


@pytest.mark.asyncio
async def test_invalid_set_never_opens_cursor():
    driver = NativeBoundary()
    driver.is_in_transaction = lambda: True

    async def identity(_sql):
        return (1, "2", "admin", "admin")

    async def aggregate(_sql):
        return dict(invalid=["budget"])

    driver.fetchrow, driver.fetchval = identity, aggregate
    with pytest.raises(ValueError, match="set_invalid"):
        await history.validate_rooted_graph_history(driver, _policy(), _family())
    assert driver.commands == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "final_identity", [(2, "2", "admin", "admin"), (1, "3", "admin", "admin"), (1, "2", "admin", "owner")]
)
async def test_context_drift_refuses(final_identity):
    driver = NativeBoundary()
    identities = iter([(1, "2", "admin", "admin"), final_identity])
    driver.is_in_transaction = lambda: True

    async def identity(_sql):
        return next(identities)

    async def aggregate(_sql):
        return dict(invalid=[], resources=0, missing_responses=0)

    driver.fetchrow, driver.fetchval = identity, aggregate
    with pytest.raises(ValueError, match="context"):
        await history.validate_rooted_graph_history(driver, _policy(), _family())
