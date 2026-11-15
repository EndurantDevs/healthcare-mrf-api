# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named native counter DATA semantics, revision CAS and failed-registration rollback."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_native_inputs as inputs
from process import provider_directory_cms_native_registration as registration
from tests.test_cms_native_input_registration import NativeProtocol


async def test_named_counter_reads_preserve_scope():
    """The current registration readers return declared names without issuing identity authority."""
    native = NativeProtocol()
    ledger = await registration._native_registration_ledger(native, "sample")
    function = await registration._native_registration_function(native, "sample", ledger)
    assert ledger == {"schema_name": "sample", "table_name": "cms_native_input_revision"}
    assert function == {
        "schema_name": "sample",
        "function_name": "cms_native_input_advance",
        "prosrc": inputs._advance_body("sample"),
    }
    assert all(kind != "execute" for kind, *_ in native.calls)
    assert all("pg_roles" not in sql and "has_function_privilege" not in sql for _, sql, _ in native.calls)


@pytest.mark.parametrize(
    "change",
    ["ledger_absent", "ledger_shape", "function_absent", "function_shape", "counter_step", "counter_stamp"],
)
async def test_named_counter_refuses_semantic_drift(change):
    native = NativeProtocol()
    if change == "ledger_absent":
        native.ledger = None
    elif change == "ledger_shape":
        native.ledger["safe"] = False
    elif change == "function_absent":
        native.function = None
    elif change == "function_shape":
        native.function["safe"] = False
    else:
        native.function["prosrc"] = native.function["prosrc"].replace(
            "revision=revision+1" if change == "counter_step" else "mutation_xid=pg_current_xact_id()",
            "revision=revision+2" if change == "counter_step" else "mutation_xid=NULL",
        )
        assert native.function["prosrc"] != inputs._advance_body("sample")
    with pytest.raises(RuntimeError, match="native_revision_(ledger|guard)_unavailable"):
        ledger = await registration._native_registration_ledger(native, "sample")
        await registration._native_registration_function(native, "sample", ledger)
    assert all(kind != "execute" for kind, *_ in native.calls)


@pytest.mark.parametrize("change", ["missing", "enabled", "step", "stamp", "scope"])
async def test_ledger_guards_keep_data_invariants(change):
    native = NativeProtocol()
    if change == "missing":
        native.ledger_guards.pop()
    elif change == "enabled":
        native.ledger_guards[0]["enabled"] = "O"
    else:
        previous, replacement = {
            "step": ("NEW.revision=OLD.revision+1", "NEW.revision=OLD.revision+2"),
            "stamp": ("NEW.mutation_xid=pg_current_xact_id()", "NEW.mutation_xid IS NULL"),
            "scope": ("NEW.schema_name=OLD.schema_name", "TRUE"),
        }[change]
        body = native.ledger_guards[0]["prosrc"]
        assert previous in body
        native.ledger_guards[0]["prosrc"] = body.replace(previous, replacement)
    with pytest.raises(RuntimeError, match="^cms_address_native_revision_ledger_unavailable$"):
        await registration._native_registration_ledger(native, "sample")
    assert all(kind != "execute" for kind, *_ in native.calls)


@pytest.mark.parametrize("failure", ["select", "insert", "cancel"])
async def test_postgres_refusal_rolls_back_registration(failure):
    """Native permission failures and interrupted guard installation preserve candidate state."""
    native = NativeProtocol()
    if failure == "select":
        native.can_read_ledger = False
    elif failure == "insert":
        native.can_insert = False
    else:
        native.fail_trigger = True
    with pytest.raises((PermissionError, asyncio.CancelledError)):
        await inputs.register_native_input_candidates(native, ledger_schema="sample", candidates=(native.candidate(),))
    assert native.rows == {} and native.guards == {}


@pytest.mark.parametrize("change", [None, "prior_revision", "increment"])
async def test_named_publication_preserves_revision_cas(monkeypatch, change):
    from process import provider_directory_projection_stage as projection

    native = NativeProtocol()
    prepared = await inputs.register_native_input_candidates(
        native, ledger_schema="sample", candidates=(native.candidate(),)
    )
    native.guards[native.target_root] = native.guards.pop(("stage", "incoming"))
    monkeypatch.setattr(projection, "_copy_driver", AsyncMock(return_value=native))
    session = SimpleNamespace(in_transaction=lambda: True)
    if change == "prior_revision":
        native.rows[native.target_root]["revision"] = 5
    elif change == "increment":
        native.fail_update = True
    before = deepcopy(native.rows)
    if change:
        with pytest.raises(RuntimeError, match="publication_revision_changed"):
            await registration.finish_local_native_input_publication(session, prepared=prepared)
        assert native.rows == before
    else:
        await registration.finish_local_native_input_publication(session, prepared=prepared)
        assert native.rows[native.target_root]["revision"] == 1
