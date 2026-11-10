# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep active collection replays and retries bound to admitted input."""

from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sanic.exceptions import SanicException
from sqlalchemy.exc import IntegrityError

from api import control, control_imports
from process.ptg_parts.cigna_toc_collection import COLLECTION_PROOF_PARAM
from tests.test_cigna_toc_collection import _subject
from tests.test_cigna_toc_dispatch import _request


def _changed_params(params, change):
    changed = deepcopy(params)
    if change == "month":
        changed["import_month"] = "2026-12"
        changed["toc_urls"] = [url.replace("2026-11", "2026-12") for url in changed["toc_urls"]]
        proof = changed[COLLECTION_PROOF_PARAM]
        for field in ("physical_index_url", "selected_file_name", "last_updated_on"):
            proof[field] = proof[field].replace("2026-11", "2026-12")
        for member in proof["collection"]["members"]:
            member["name"] = member["name"].replace("2026-11", "2026-12")
        return changed
    if change == "archive":
        changed[COLLECTION_PROOF_PARAM]["collection"]["archive_sha256"] = "a" * 64
    elif change == "signed_url":
        changed["toc_urls"][0] += "&Signature=renewed"
    elif change in {"withdrawal", "source_withdrawal"}:
        changed.pop(COLLECTION_PROOF_PARAM)
        changed["toc_urls"] = ["https://rates.example.test/index.json"]
        if change == "source_withdrawal":
            changed.pop("source_file_import_id")
    else:
        changed.update(
            {
                "filter": {"file_url_contains": ["other-core.json.gz"]},
                "plans": {"plan_ids": ["987654321"]},
                "market": {"plan_market_types": ["individual"]},
                "market_alias": {"plan_market_type": "individual"},
                "network_alias": {"source_network_name": "different-network"},
                "max_items": {"max_items": 10},
                "source_key": {"source_key": "other-source"},
            }.get(change, {})
        )
    return changed


def _install_active_run(monkeypatch, params, lookup):
    active_run_map = {
        "run_id": "existing-run",
        "importer": "ptg",
        "status": "running",
        "params": control_imports._params_for_import_run_response("ptg", params),
    }
    reads = []

    class Connection:
        async def scalar(self, statement):
            assert "import_run.params" in str(statement)
            assert "existing-run" in statement.compile().params.values()
            reads.append("private-proof")
            return deepcopy(params)

    @asynccontextmanager
    async def acquire():
        yield Connection()

    monkeypatch.setattr(control_imports.db, "acquire", acquire)
    for name in (
        "require_source_attempt_capabilities",
        "guard_source_attempt",
        "acquire_ptg_admission_lock",
        "recheck_frozen_binding_on_connection",
    ):
        monkeypatch.setattr(control_imports, name, AsyncMock())
    monkeypatch.setattr(control_imports, "is_ptg_wave_owned_run", AsyncMock(return_value=False))
    monkeypatch.setattr(
        control_imports,
        "_active_idempotency_run",
        AsyncMock(return_value=active_run_map if lookup == "idempotency" else None),
    )
    monkeypatch.setattr(control_imports, "_active_ptg_source_file_replay", AsyncMock(return_value=active_run_map))
    enqueue = AsyncMock(side_effect=AssertionError("an active replay cannot enqueue work"))
    monkeypatch.setattr(control_imports, "_enqueue_import_start", enqueue)
    if lookup == "integrity":
        monkeypatch.setattr(
            control_imports,
            "_admit_import_row",
            AsyncMock(side_effect=IntegrityError("insert", {}, Exception("duplicate"))),
        )
        monkeypatch.setattr(
            control_imports, "find_active_run_by_idempotency_key", AsyncMock(return_value=active_run_map)
        )
    return active_run_map, reads, enqueue


@pytest.mark.parametrize("lookup", ["source", "idempotency", "integrity"])
@pytest.mark.parametrize(
    "change",
    [
        None,
        "signed_url",
        "archive",
        "filter",
        "plans",
        "market",
        "market_alias",
        "network_alias",
        "max_items",
        "source_key",
        "withdrawal",
    ],
)
async def test_create_collection_replay_checks_private_proof_and_scope(tmp_path, monkeypatch, lookup, change):
    params = _request(_subject(tmp_path).proof)
    active_run_map, reads, enqueue = _install_active_run(monkeypatch, params, lookup)
    payload = {"importer": "ptg", "params": _changed_params(params, change), "idempotency_key": "same-request"}
    if change in {None, "signed_url"}:
        replay, created = await control_imports.create_import_run(payload)
        assert replay == active_run_map and created is False
        assert COLLECTION_PROOF_PARAM not in replay["params"]
    else:
        with pytest.raises(SanicException, match="admitted proof or scope") as failure:
            await control_imports.create_import_run(payload)
        assert failure.value.status_code == 409
    assert reads == ["private-proof"]
    enqueue.assert_not_awaited()


@pytest.mark.parametrize(
    "change",
    [None, "signed_url", "filter", "plans", "market", "market_alias", "network_alias", "max_items", "source_key"],
)
async def test_public_collection_retry_preserves_admitted_selectors(tmp_path, monkeypatch, change):
    params = _request(_subject(tmp_path).proof)
    public_run_map = {"importer": "ptg", "params": control_imports._params_for_import_run_response("ptg", params)}
    monkeypatch.setattr(control_imports, "get_import_run", AsyncMock(return_value=public_run_map))
    monkeypatch.setattr(
        control_imports.npi_execution, "read_control_run", AsyncMock(return_value=SimpleNamespace(params=params))
    )
    monkeypatch.setattr(control_imports, "require_not_wave_owned_run", AsyncMock())
    create = AsyncMock(return_value=({"status": "queued"}, True))
    monkeypatch.setattr(control_imports, "create_import_run", create)
    changed = _changed_params(params, change)
    overrides_by_name = {name: value for name, value in changed.items() if value != params.get(name)}
    if change in {None, "signed_url"}:
        assert await control_imports.retry_import_run("existing-run", {"retry_params": overrides_by_name}) == (
            {"status": "queued"},
            True,
        )
        child_params_by_name = create.await_args.args[0]["params"]
        assert child_params_by_name[COLLECTION_PROOF_PARAM] == params[COLLECTION_PROOF_PARAM]
        assert (
            child_params_by_name["plan_ids"] == params["plan_ids"]
            and child_params_by_name["file_url_contains"] == params["file_url_contains"]
        )
    else:
        with pytest.raises(SanicException) as failure:
            await control_imports.retry_import_run("existing-run", {"retry_params": overrides_by_name})
        assert failure.value.status_code == 409
        create.assert_not_awaited()
    assert COLLECTION_PROOF_PARAM not in public_run_map["params"]


async def test_collection_conflict_keeps_control_api_409(monkeypatch):
    monkeypatch.setattr(control, "_require_control_auth", lambda _request: None)
    monkeypatch.setattr(control, "_validated_control_import_payload", lambda payload: payload)
    monkeypatch.setattr(
        control, "create_import_run", AsyncMock(side_effect=SanicException("scope conflict", status_code=409))
    )
    with pytest.raises(SanicException) as failure:
        await control.control_create_import(SimpleNamespace(json={"importer": "ptg"}))
    assert failure.value.status_code == 409


async def test_replay_checks_the_durable_source_alias(tmp_path, monkeypatch):
    params = _request(_subject(tmp_path).proof)
    active_run_map, _, enqueue = _install_active_run(monkeypatch, params, "idempotency")
    active_run_map["source_file_import_id"] = "different-durable-attempt"
    with pytest.raises(ValueError, match="identity views conflict"):
        await control_imports.create_import_run({"importer": "ptg", "params": params, "idempotency_key": "same"})
    enqueue.assert_not_awaited()


async def test_replay_requires_the_original_private_params(tmp_path, monkeypatch):
    params = _request(_subject(tmp_path).proof)
    _, _, enqueue = _install_active_run(monkeypatch, params, "idempotency")

    @asynccontextmanager
    async def acquire():
        yield SimpleNamespace(scalar=AsyncMock(return_value=None))

    monkeypatch.setattr(control_imports.db, "acquire", acquire)
    with pytest.raises(ValueError, match="lacks retained archive proof"):
        await control_imports.create_import_run({"importer": "ptg", "params": params, "idempotency_key": "same"})
    enqueue.assert_not_awaited()


def test_collection_retry_can_adjust_execution_parameters(tmp_path):
    params = _request(_subject(tmp_path).proof)
    child_params_by_name = control_imports._retry_child_params(
        {"importer": "ptg", "params": params}, "parent", {"workers": 2}
    )
    assert child_params_by_name == {**params, "workers": 2}


@pytest.mark.parametrize(
    "lookup",
    [
        "idempotency",
        "active-importer",
        "wave-public-idempotency",
        "wave-locked-idempotency",
        "wave-public-importer",
        "wave-locked-importer",
    ],
)
async def test_withdrawn_source_identity_cannot_bypass_collection_replay(tmp_path, monkeypatch, lookup):
    params = _request(_subject(tmp_path).proof)
    active_run_map, reads, enqueue = _install_active_run(monkeypatch, params, "source")
    requested_params = _changed_params(params, "withdrawal")
    requested_params.pop("source_file_import_id")
    requested_params.pop("import_id")
    monkeypatch.setattr(
        control_imports,
        "find_active_run_by_idempotency_key",
        AsyncMock(return_value=active_run_map if lookup in {"idempotency", "wave-public-idempotency"} else None),
    )
    monkeypatch.setattr(
        control_imports,
        "find_earliest_active_run_by_importer",
        AsyncMock(return_value=active_run_map if lookup in {"active-importer", "wave-public-importer"} else None),
    )
    monkeypatch.setattr(
        control_imports,
        "_active_idempotency_run",
        AsyncMock(return_value=active_run_map if lookup == "wave-locked-idempotency" else None),
    )
    monkeypatch.setattr(
        control_imports,
        "_active_importer_runs",
        AsyncMock(return_value=[active_run_map] if lookup == "wave-locked-importer" else []),
    )
    monkeypatch.setattr(control_imports, "require_no_capacity_owning_wave", AsyncMock())
    control_request_by_name = {
        "importer": "ptg",
        "params": requested_params,
        "idempotency_key": "same" if "idempotency" in lookup else None,
    }
    with pytest.raises(SanicException) as failure:
        if lookup.startswith("wave-"):
            await control_imports._admit_wave_fenced_import_run(control_request_by_name)
        else:
            await control_imports.create_import_run(control_request_by_name)
    assert failure.value.status_code == 409
    assert reads == ["private-proof"]
    enqueue.assert_not_awaited()


def test_collection_retry_accepts_equivalent_selector_aliases(tmp_path):
    params_by_name = {
        **_request(_subject(tmp_path).proof),
        "plan_market_types": ["individual"],
        "source_network_names": ["one-network"],
    }
    overrides_by_name = {
        "plan_market_types": None,
        "plan_market_type": "individual",
        "source_network_names": None,
        "source_network_name": "one-network",
    }
    assert control_imports._retry_child_params(
        {"importer": "ptg", "params": params_by_name}, "parent", overrides_by_name
    ) == {
        **params_by_name,
        **overrides_by_name,
    }


async def test_ordinary_replay_avoids_private_collection_reads(tmp_path, monkeypatch):
    params_by_name = {"toc_urls": ["https://rates.example.test/ordinary-index.json"]}
    active_run_map, reads, enqueue = _install_active_run(monkeypatch, params_by_name, "source")
    monkeypatch.setattr(control_imports, "find_active_run_by_idempotency_key", AsyncMock(return_value=active_run_map))
    assert await control_imports.create_import_run(
        {"importer": "ptg", "params": params_by_name, "idempotency_key": "same"}
    ) == (active_run_map, False)
    assert reads == []
    enqueue.assert_not_awaited()


async def test_ordinary_admission_without_params_keeps_legacy_contract(tmp_path, monkeypatch):
    params = _request(_subject(tmp_path).proof)
    _, events, inserted, _ = _install_retained_attempt(monkeypatch, params, "succeeded")
    assert (
        await control_imports._admit_wave_fenced_import_run(
            {"run_id": "legacy-run", "importer": "ptg", "idempotency_key": None}
        )
        is None
    )
    assert events == ["acquire_ptg_admission_lock", "require_no_capacity_owning_wave", "insert"]
    assert len(inserted) == 1


def _install_retained_attempt(monkeypatch, params, status):
    """Keep real admission SQL while isolating all storage and queue effects."""
    retained_run_by_field = {
        "run_id": "retained-run",
        "importer": "ptg",
        "source_file_import_id": params["source_file_import_id"],
        "import_id": params["import_id"],
        "status": status,
        "params": deepcopy(params),
        "metrics": {},
    }
    events, inserted = [], []

    class Connection:
        async def all(self, statement):
            values = statement.compile().params
            if "status_1" in values:
                return []
            assert set(values) == {"importer_1", "source_file_import_id_1"} and values["importer_1"] == "ptg"
            events.append("retained-inputs")
            is_source_match = values["source_file_import_id_1"] == retained_run_by_field["source_file_import_id"]
            return [deepcopy(retained_run_by_field)] if is_source_match else []

        async def status(self, statement):
            events.append("insert")
            inserted.append(statement.compile().params)

    @asynccontextmanager
    async def acquire():
        yield Connection()

    monkeypatch.setattr(control_imports.db, "acquire", acquire)
    monkeypatch.setattr(control_imports.db, "execute", AsyncMock(side_effect=AssertionError("unmocked database read")))
    monkeypatch.setattr(control_imports, "find_active_run_by_idempotency_key", AsyncMock(return_value=None))
    monkeypatch.setattr(control_imports, "find_earliest_active_run_by_importer", AsyncMock(return_value=None))
    for name in (
        "require_source_attempt_capabilities",
        "guard_source_attempt",
        "acquire_ptg_admission_lock",
        "require_no_capacity_owning_wave",
        "insert_or_compare_frozen_binding",
        "record_source_attempt_event",
    ):

        async def record(*_args, _name=name, **_kwargs):
            events.append(_name)

        monkeypatch.setattr(control_imports, name, AsyncMock(side_effect=record))
    enqueue = AsyncMock(return_value={})
    monkeypatch.setattr(control_imports, "_enqueue_import_start", enqueue)
    monkeypatch.setattr(
        control_imports,
        "_persist_enqueue_result",
        AsyncMock(side_effect=lambda _run_id, _importer, run: control_imports.normalize_run(run)),
    )
    monkeypatch.setattr(control_imports, "enqueue_status_event", Mock())
    monkeypatch.setattr(control_imports, "_write_run_live_progress", Mock())
    return retained_run_by_field, events, inserted, enqueue


@pytest.mark.parametrize(("status", "retry_of"), [("failed", "retained-run"), ("succeeded", None)])
@pytest.mark.parametrize("change", ["archive", "filter", "withdrawal", "source_withdrawal", "month"])
async def test_terminal_collection_attempt_rejects_changed_shared_admission(
    tmp_path, monkeypatch, status, retry_of, change
):
    params = _request(_subject(tmp_path).proof)
    retained_run_by_field, events, inserted, enqueue = _install_retained_attempt(monkeypatch, params, status)
    with pytest.raises(SanicException, match="admitted proof or scope") as failure:
        await control_imports.create_import_run(
            {
                "importer": "ptg",
                "params": _changed_params(params, change),
                "retry_of_run_id": retry_of,
                "idempotency_key": "new-control-run",
            }
        )
    assert failure.value.status_code == 409
    assert events == (
        []
        if change == "source_withdrawal"
        else [
            "require_source_attempt_capabilities",
            "guard_source_attempt",
        ]
    ) + [
        "acquire_ptg_admission_lock",
        "require_no_capacity_owning_wave",
        "retained-inputs",
    ]
    assert not inserted and retained_run_by_field["params"] == params
    enqueue.assert_not_awaited()


@pytest.mark.parametrize(("status", "retry_of"), [("failed", "retained-run"), ("succeeded", None)])
async def test_terminal_collection_attempt_preserves_execution_changes(tmp_path, monkeypatch, status, retry_of):
    params = _request(_subject(tmp_path).proof)
    _, events, inserted, enqueue = _install_retained_attempt(monkeypatch, params, status)
    requested_params_by_name = {**_changed_params(params, "signed_url"), "workers": 2}
    run, created = await control_imports.create_import_run(
        {
            "importer": "ptg",
            "params": requested_params_by_name,
            "retry_of_run_id": retry_of,
            "idempotency_key": "new-control-run",
        }
    )
    assert created and COLLECTION_PROOF_PARAM not in run["params"]
    assert inserted[0]["params"] == requested_params_by_name
    assert inserted[0]["retry_of_run_id"] == retry_of
    assert events.index("retained-inputs") < events.index("insert_or_compare_frozen_binding") < events.index("insert")
    assert enqueue.await_args.args[0]["params"] == requested_params_by_name


@pytest.mark.parametrize("change", ["archive", "filter", "month"])
async def test_new_collection_attempt_can_change_inputs(tmp_path, monkeypatch, change):
    params = _request(_subject(tmp_path).proof)
    _, events, inserted, enqueue = _install_retained_attempt(monkeypatch, params, "succeeded")
    requested_params_by_name = {
        **_changed_params(params, change),
        "import_id": "new-source-attempt",
        "source_file_import_id": "new-source-attempt",
    }
    run, created = await control_imports.create_import_run(
        {"importer": "ptg", "params": requested_params_by_name, "idempotency_key": "new-control-run"}
    )
    assert created and COLLECTION_PROOF_PARAM not in run["params"]
    assert "retained-inputs" in events and inserted[0]["params"] == requested_params_by_name
    assert enqueue.await_args.args[0]["params"] == requested_params_by_name


async def test_ordinary_terminal_attempt_keeps_legacy_selectors(tmp_path, monkeypatch):
    params = _changed_params(_request(_subject(tmp_path).proof), "withdrawal")
    _, _, inserted, _ = _install_retained_attempt(monkeypatch, params, "succeeded")
    requested_params_by_name = _changed_params(params, "filter")
    _, created = await control_imports.create_import_run(
        {"importer": "ptg", "params": requested_params_by_name, "idempotency_key": "new-control-run"}
    )
    assert created and inserted[0]["params"] == requested_params_by_name


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("params", None, "lacks retained archive proof"),
        ("import_id", "other-attempt", "identity views conflict"),
    ],
)
async def test_terminal_collection_checks_retained_evidence(tmp_path, monkeypatch, field, value, message):
    params = _request(_subject(tmp_path).proof)
    retained_run_by_field, _, inserted, enqueue = _install_retained_attempt(monkeypatch, params, "failed")
    retained_run_by_field[field] = value
    with pytest.raises(ValueError, match=message):
        await control_imports.create_import_run(
            {"importer": "ptg", "params": params, "idempotency_key": "new-control-run"}
        )
    assert not inserted
    enqueue.assert_not_awaited()


@pytest.mark.parametrize("legacy_id", [None, 71, "unrelated-attempt"])
async def test_unrelated_legacy_attempt_keeps_admission(tmp_path, monkeypatch, legacy_id):
    params = _request(_subject(tmp_path).proof)
    _, events, inserted, _ = _install_retained_attempt(monkeypatch, params, "succeeded")
    requested_params_by_name = {"toc_urls": ["https://rates.example.test/index.json"], "import_id": legacy_id}
    _, created = await control_imports.create_import_run(
        {"importer": "ptg", "params": requested_params_by_name, "idempotency_key": "new-control-run"}
    )
    assert created and inserted[0]["params"] == requested_params_by_name
    assert ("retained-inputs" in events) == (legacy_id is not None)
