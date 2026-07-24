from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from db import migration_ptg2_v4_attempt_audit as attempt_audit
from db import migration_ptg2_v4_attempt_catalog as attempt_catalog
from db import migration_ptg2_v4_attempt_checks as attempt_checks
from db import migration_ptg2_v4_attempt_fence as attempt_fence
from process.ptg_parts import table_setup
from tests.ptg_v4_publish_control_support import Operations


def _patch_table_setup_dependencies(monkeypatch) -> dict[str, AsyncMock]:
    helpers_by_name = {
        "require_migration_owned_tables": AsyncMock(),
        "_require_v4_attempt_migration_tables": AsyncMock(),
        "_require_allowed_amount_migration_tables": AsyncMock(),
        "_ensure_ptg2_serving_rate_columns": AsyncMock(),
        "_ensure_ptg2_serving_rate_compact_columns": AsyncMock(),
        "_ensure_ptg2_price_set_columns": AsyncMock(),
        "_ensure_ptg2_provider_set_columns": AsyncMock(),
        "_drop_ptg2_columns": AsyncMock(),
        "_ensure_ptg2_price_atom_columns": AsyncMock(),
        "_ensure_indexes": AsyncMock(),
    }
    for name, helper in helpers_by_name.items():
        monkeypatch.setattr(table_setup, name, helper)
    monkeypatch.setattr(table_setup, "resolve_ptg2_schema", lambda: "mrf")
    monkeypatch.setattr(table_setup.db, "status", AsyncMock())
    monkeypatch.setattr(table_setup.db, "create_table", AsyncMock())
    return helpers_by_name


@pytest.mark.asyncio
async def test_ensure_tables_dispatches_every_model_specific_upgrade(
    monkeypatch,
) -> None:
    """Run every model-specific compatibility operation before index creation."""

    helpers_by_name = _patch_table_setup_dependencies(monkeypatch)
    model_classes = (
        table_setup.PTG2ServingRate,
        table_setup.PTG2ServingRateCompact,
        table_setup.PTG2PriceSet,
        table_setup.PTG2ProviderSet,
        table_setup.PTG2ProviderSetMember,
        table_setup.PTG2Procedure,
        table_setup.PTG2PriceAtom,
        table_setup.PTG2PriceSetEntry,
        table_setup.PTG2ProviderGroupMember,
        table_setup.PTG2ProviderSetComponent,
        table_setup.PTG2ProviderSetEntry,
        table_setup.PTG2ProviderEntryComponent,
        table_setup.PTG2SourceTrace,
        table_setup.PTG2SourceTraceSet,
    )
    monkeypatch.setattr(table_setup, "PTG2_MODEL_CLASSES", model_classes)
    await table_setup.ensure_ptg2_tables()
    assert table_setup.db.create_table.await_count == len(model_classes)
    assert helpers_by_name["_ensure_indexes"].await_count == len(model_classes)
    helpers_by_name["_ensure_ptg2_serving_rate_columns"].assert_awaited_once_with("mrf")
    helpers_by_name["_ensure_ptg2_serving_rate_compact_columns"].assert_awaited_once_with("mrf")
    helpers_by_name["_ensure_ptg2_price_set_columns"].assert_awaited_once_with("mrf")
    helpers_by_name["_ensure_ptg2_provider_set_columns"].assert_awaited_once_with("mrf")
    helpers_by_name["_ensure_ptg2_price_atom_columns"].assert_awaited_once_with("mrf")
    dropped_tables = {
        call.args[1]
        for call in helpers_by_name["_drop_ptg2_columns"].await_args_list
    }
    assert len(dropped_tables) == 9


@pytest.mark.asyncio
async def test_ensure_tables_wraps_schema_and_model_failures(monkeypatch) -> None:
    """Name the exact failed DDL seam while preserving the original exception."""

    _patch_table_setup_dependencies(monkeypatch)
    table_setup.db.status.side_effect = PermissionError("schema denied")
    with pytest.raises(RuntimeError, match="Failed to ensure PTG2 schema mrf"):
        await table_setup.ensure_ptg2_tables()
    table_setup.db.status.side_effect = None
    monkeypatch.setattr(
        table_setup,
        "PTG2_MODEL_CLASSES",
        (table_setup.PTG2Snapshot,),
    )
    table_setup.db.create_table.side_effect = PermissionError("table denied")
    with pytest.raises(RuntimeError, match=r"mrf\.ptg2_snapshot failed"):
        await table_setup.ensure_ptg2_tables()


@pytest.mark.asyncio
async def test_migration_table_requirements_handle_mapping_rows(monkeypatch) -> None:
    """Accept mapping-style rows and list every missing migration-owned table."""

    class Record:
        _mapping = {"table_name": table_setup.ATTEMPT_FENCE_TABLE}

    monkeypatch.setattr(table_setup.db, "all", AsyncMock(return_value=[Record()]))
    with pytest.raises(RuntimeError) as raised:
        await table_setup._require_v4_attempt_migration_tables("mrf")
    message = str(raised.value)
    assert table_setup.ATTEMPT_STAGE_TABLE in message
    assert table_setup.ATTEMPT_IMPORT_JOB_TABLE in message
    table_setup.db.all.return_value = [
        {"table_name": name}
        for name in table_setup.PTG2_V4_ATTEMPT_MIGRATION_TABLE_NAMES
    ]
    await table_setup._require_v4_attempt_migration_tables("mrf")


def _valid_trigger_mapping(*, legacy: bool = False) -> dict[str, object]:
    return {
        "tgname": attempt_audit._AUDIT_TRIGGER,
        "tgtype": 27,
        "tgenabled": "O",
        "proname": attempt_audit._AUDIT_FUNCTION,
        "function_schema": "mrf",
        "lanname": "plpgsql",
        "prosrc": (
            attempt_audit._LEGACY_AUDIT_BODY
            if legacy
            else attempt_audit._FINAL_AUDIT_BODY
        ),
    }


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    (
        ("tgname", "other"),
        ("tgtype", 1),
        ("tgenabled", "D"),
        ("proname", "other"),
        ("function_schema", "other"),
        ("lanname", "sql"),
        ("prosrc", "BEGIN RETURN NULL; END"),
    ),
)
def test_audit_trigger_contract_rejects_each_catalog_drift(
    field_name,
    invalid_value,
) -> None:
    """Fail each independently drifted trigger property."""

    trigger = _valid_trigger_mapping()
    trigger[field_name] = invalid_value
    assert not attempt_audit._is_expected_trigger(
        trigger,
        "mrf",
        attempt_audit._FINAL_AUDIT_BODY,
    )


def test_audit_trigger_validates_both_shapes_and_installs_exact_ddl(
    monkeypatch,
) -> None:
    """Accept exact legacy/final catalogs and install the nonce-bound guard."""

    for legacy in (False, True):
        trigger = _valid_trigger_mapping(legacy=legacy)
        monkeypatch.setattr(
            attempt_audit,
            "_trigger_catalog",
            lambda _connection, _schema, row=trigger: [row],
        )
        attempt_audit.validate_attempt_audit_trigger(
            object(),
            "mrf",
            is_legacy=legacy,
        )
    monkeypatch.setattr(
        attempt_audit,
        "_trigger_catalog",
        lambda _connection, _schema: [],
    )
    with pytest.raises(RuntimeError, match="audit_trigger_unknown"):
        attempt_audit.validate_attempt_audit_trigger(
            object(),
            "mrf",
            is_legacy=False,
        )
    operations = Operations()
    attempt_audit.install_attempt_audit_guard(operations, 'tenant"x')
    assert len(operations.statements) == 3
    assert 'tenant""x' in operations.statements[0]
    assert "BEFORE UPDATE OR DELETE" in operations.statements[-1]


@pytest.mark.parametrize(
    ("mutation", "expected_error"),
    (
        ("missing", True),
        ("type", True),
        ("unvalidated", True),
        ("expression", True),
        ("valid", False),
    ),
)
def test_check_contract_rejects_noncanonical_constraints(
    monkeypatch,
    mutation,
    expected_error,
) -> None:
    """Require check type, validation, and canonical expressions together."""

    expected_checks_by_name = {"state_check": "CHECK (state = 'active')"}
    monkeypatch.setattr(
        attempt_checks,
        "_canonical_checks",
        lambda _connection, _expressions: expected_checks_by_name,
    )
    constraint_catalog_by_name = {
        "state_check": ("c", expected_checks_by_name["state_check"], True)
    }
    if mutation == "missing":
        constraint_catalog_by_name = {}
    elif mutation == "type":
        constraint_catalog_by_name["state_check"] = (
            "u",
            expected_checks_by_name["state_check"],
            True,
        )
    elif mutation == "unvalidated":
        constraint_catalog_by_name["state_check"] = (
            "c",
            expected_checks_by_name["state_check"],
            False,
        )
    elif mutation == "expression":
        constraint_catalog_by_name["state_check"] = (
            "c",
            "CHECK (false)",
            True,
        )
    if expected_error:
        with pytest.raises(RuntimeError, match="check_constraint_mismatch"):
            attempt_checks.validate_exact_fence_checks(
                object(),
                constraint_catalog_by_name,
                is_legacy=False,
            )
    else:
        attempt_checks.validate_exact_fence_checks(
            object(),
            constraint_catalog_by_name,
            is_legacy=True,
        )


def test_catalog_classifies_allowlisted_shapes_and_rejects_unknown() -> None:
    """Distinguish only the exact legacy and final catalog name sets."""

    legacy_constraints = (
        attempt_catalog.FENCE_FINAL_CONSTRAINTS
        - {
            attempt_catalog.FENCE_AUDIT_CONSTRAINT,
            attempt_catalog.FENCE_NONCE_CONSTRAINT,
        }
        | {attempt_catalog.FENCE_LEGACY_AUDIT_CONSTRAINT}
    )
    assert attempt_catalog._classified_shape(
        set(attempt_catalog.FENCE_LEGACY_COLUMNS),
        {name: ("c", "", True) for name in legacy_constraints},
        "mrf",
    ) == "legacy"
    assert attempt_catalog._classified_shape(
        set(attempt_catalog.FENCE_FINAL_COLUMNS),
        {
            name: ("c", "", True)
            for name in attempt_catalog.FENCE_FINAL_CONSTRAINTS
        },
        "mrf",
    ) == "final"
    with pytest.raises(RuntimeError, match="shape_unknown"):
        attempt_catalog._classified_shape(set(), {}, "mrf")


@pytest.mark.parametrize(
    ("catalog", "include_nonce", "message"),
    (
        (
            {"state": {"default": "'active'::varchar"}, "created_at": {"default": "now()"}},
            False,
            None,
        ),
        ({"state": {"default": "'wrong'::varchar"}}, False, "default_mismatch"),
        ({"snapshot_id": {"default": "'x'::text"}}, False, "unexpected"),
        ({"fence_nonce": {"default": None}}, True, "default_mismatch"),
    ),
)
def test_catalog_defaults_are_exact(catalog, include_nonce, message) -> None:
    """Allow only the documented state, timestamp, and nonce defaults."""

    if message is None:
        attempt_catalog._assert_exact_defaults(
            catalog,
            include_nonce=include_nonce,
        )
    else:
        with pytest.raises(RuntimeError, match=message):
            attempt_catalog._assert_exact_defaults(
                catalog,
                include_nonce=include_nonce,
            )


@pytest.mark.parametrize(
    ("shape", "require_existing", "expected"),
    (
        ("offline", False, "offline"),
        ("absent", False, "absent"),
        ("final", False, "present"),
        ("legacy", False, "present"),
    ),
)
def test_catalog_adoption_covers_offline_absent_and_known_shapes(
    monkeypatch,
    shape,
    require_existing,
    expected,
) -> None:
    """Return stable adoption states and upgrade only the legacy allowlist."""

    connection = object()
    monkeypatch.setattr(
        attempt_catalog,
        "_connection",
        lambda _operations: None if shape == "offline" else connection,
    )
    monkeypatch.setattr(
        attempt_catalog,
        "_has_tables_after_catalog_lock",
        lambda _connection, _schema: shape != "absent",
    )
    monkeypatch.setattr(attempt_catalog, "_shape", lambda *_args: shape)
    monkeypatch.setattr(attempt_catalog, "_shape_without_trigger", lambda *_args: "final")
    upgrade = Mock()
    validate = Mock()
    monkeypatch.setattr(attempt_catalog, "_upgrade_legacy_shape", upgrade)
    monkeypatch.setattr(attempt_catalog, "validate_attempt_audit_trigger", validate)
    observed = attempt_catalog.adopt_or_validate_attempt_tables(
        object(),
        "mrf",
        require_existing=require_existing,
    )
    assert observed == expected
    assert upgrade.call_count == (1 if shape == "legacy" else 0)


def test_catalog_adoption_and_validation_fail_closed(monkeypatch) -> None:
    """Reject missing required tables, unknown final shape, and offline drift."""

    connection = object()
    monkeypatch.setattr(attempt_catalog, "_connection", lambda _operations: connection)
    monkeypatch.setattr(
        attempt_catalog,
        "_has_tables_after_catalog_lock",
        lambda _connection, _schema: False,
    )
    with pytest.raises(RuntimeError, match="tables_missing"):
        attempt_catalog.adopt_or_validate_attempt_tables(
            object(),
            "mrf",
            require_existing=True,
        )
    with pytest.raises(RuntimeError, match="tables_missing"):
        attempt_catalog.validate_current_attempt_authority(object(), "mrf")
    monkeypatch.setattr(
        attempt_catalog,
        "_has_tables_after_catalog_lock",
        lambda _connection, _schema: True,
    )
    monkeypatch.setattr(attempt_catalog, "_shape", lambda *_args: "final")
    monkeypatch.setattr(
        attempt_catalog,
        "_shape_without_trigger",
        lambda *_args: "unknown",
    )
    with pytest.raises(RuntimeError, match="shape_unknown"):
        attempt_catalog.adopt_or_validate_attempt_tables(object(), "mrf")
    with pytest.raises(RuntimeError, match="shape_unknown"):
        attempt_catalog.validate_current_attempt_authority(object(), "mrf")
    monkeypatch.setattr(attempt_catalog, "_connection", lambda _operations: None)
    attempt_catalog.validate_current_attempt_authority(object(), "mrf")


def test_migration_fence_quotes_guarded_ddl_and_offline_locks(monkeypatch) -> None:
    """Quote guarded DDL and use the operations path when no online bind exists."""

    operations = Operations(bind=None)
    attempt_fence.execute_when_table_exists(
        operations,
        'tenant"x',
        "stage",
        "DROP TABLE stage",
    )
    assert "to_regclass" in operations.statements[0]
    assert "tenant\"\"x" in operations.statements[0]
    attempt_fence.install_lifecycle_lock_layer(
        operations,
        "mrf",
        ("first", "second"),
    )
    assert len(operations.statements) == 6
    monkeypatch.setattr(attempt_fence, "validate_current_attempt_authority", Mock())
    attempt_fence.assert_attempt_downgrade_safe(operations, "mrf")
    assert "pg_advisory_xact_lock" in operations.statements[-4]
    assert "requires_empty_authority" in operations.statements[-1]
