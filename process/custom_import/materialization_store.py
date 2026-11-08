# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Protected native pages in the exported helpers' caller-owned transaction.

SQL derives producer lineage. An existing runner window is an extra expectation,
never the authority source or a prerequisite for standalone persistence.
"""

from __future__ import annotations

from sqlalchemy import text

from db.models.custom_import import CustomImportRootScalar
from process.custom_import.runner_types import CandidateRunnerError

_PAGE_ROWS = 256
_RUNNER_REQUIRED = object()
_SCALAR_COLUMNS = (
    ("root_record_id", "bigint[]"),
    ("field_collection_slot", "smallint[]"),
    ("field_slot", "smallint[]"),
    ("projection_slot", "smallint[]"),
    ("field_type", "text[]"),
    ("value_state", "text[]"),
    ("string_value", "text[]"),
    ("integer_value", "bigint[]"),
    ("decimal_value", "numeric[]"),
    ("boolean_value", "boolean[]"),
    ("date_value", "date[]"),
    ("timestamp_value", "timestamptz[]"),
)
_WINNER_COLUMNS = (
    ("profile_slot", "smallint[]"),
    ("entity_binding_id", "bigint[]"),
    ("family_revision_id", "bigint[]"),
    ("context_collection_slot", "smallint[]"),
    ("context_key_sha256", "bytea[]"),
    ("context_child_revision_id", "bigint[]"),
)


async def _runner_window(session):
    from process.custom_import.execution import _root_transaction_context
    from process.custom_import.materialization import WinnerMaterializationError
    from process.custom_import.runner_registry import _MATERIALIZATION_WINDOW_KEY, _MaterializationLeaseWindow

    in_transaction = getattr(session, "in_transaction", None)
    if not callable(in_transaction):
        raise TypeError("materialization persistence requires an AsyncSession-style transaction")
    if not in_transaction():
        raise WinnerMaterializationError("materialization persistence requires an active caller transaction")
    info = getattr(session, "info", {})
    if _MATERIALIZATION_WINDOW_KEY not in info:
        return None
    window = info[_MATERIALIZATION_WINDOW_KEY]
    if not isinstance(window, _MaterializationLeaseWindow) or window.authority is None or window.transaction is None:
        raise CandidateRunnerError("candidate materialization authority is not bound to this transaction")
    transaction, _ = await _root_transaction_context(session)
    if transaction is not window.transaction:
        raise CandidateRunnerError("candidate materialization authority is not bound to this transaction")
    return window


def _expected(window):
    if window is None:
        return (("bigint[]", None), ("bytea", None), ("timestamptz", None))
    return (("bigint[]", window.authority[:6]), ("bytea", window.authority[6]), ("timestamptz", window.expires_at))


async def _authority(session):
    """Keep the internal legacy writers' stricter runner-only contract."""
    window = await _runner_window(session)
    if window is None:
        raise CandidateRunnerError("candidate materialization authority is not bound to this transaction")
    return window


def _authority_arguments(window):
    return tuple(zip(("bigint",) * 6 + ("bytea",), window.authority, strict=True)) + (
        ("timestamptz", window.expires_at),
    )


async def _execute(session, name, arguments, *, budget=False):
    connection = await session.connection()
    model_schema = CustomImportRootScalar.__table__.schema
    schema_map = connection.sync_connection.get_execution_options().get("schema_translate_map") or {}
    schema = schema_map.get(model_schema, model_schema)
    if not schema:
        raise CandidateRunnerError("materialization functions require an explicit model schema")
    quoted = connection.dialect.identifier_preparer.quote_schema(schema)
    parameters_by_name = {f"p{index}": argument for index, (_, argument) in enumerate(arguments)}
    casts = ",".join(f"CAST(:p{index} AS {kind})" for index, (kind, _) in enumerate(arguments))
    call = f"{quoted}.{name}({casts})"
    if budget:
        call = f"set_config('statement_timeout', CAST({call} AS text), true)"
    with session.no_autoflush:
        return (await session.execute(text(f"SELECT {call}"), parameters_by_name)).scalar_one()


async def _page(session, name, arguments, scope, window):
    from process.custom_import.runner_registry import prepare_materialization_statement

    if await _runner_window(session) is not window:
        raise CandidateRunnerError("candidate materialization authority changed during persistence")
    if window is not None:
        await prepare_materialization_statement(session)
    expected = _expected(window)
    await _execute(session, "custom_import_materialization_budget", scope + expected, budget=True)
    return await _execute(session, name, arguments + expected)


async def _call(session, name, arguments):
    from process.custom_import.runner_registry import prepare_materialization_statement

    await _authority(session)
    await prepare_materialization_statement(session)
    return await _execute(session, name, arguments)


async def _flush_pending(session, window=_RUNNER_REQUIRED):
    from process.custom_import.materialization import _flush
    from process.custom_import.runner_registry import prepare_materialization_statement

    if window is _RUNNER_REQUIRED:
        window = await _authority(session)
    if window is not None:
        await prepare_materialization_statement(session)
    await _flush(session, "materialization")


async def verify_materialization_authority(session):
    """Recheck the internal legacy runner before returning to its transaction owner."""
    window = await _authority(session)
    await _call(session, "check_custom_import_materialization_authority", _authority_arguments(window))


async def persist_scalar_models(session, models):
    """Append validated scalar models without committing the caller's transaction.

    Flush pending work once and send bounded native pages. A present runner
    must match the current root transaction; SQL derives producer authority.
    Any page/count failure propagates for caller-owned rollback.
    """
    window = await _runner_window(session)
    if (
        models
        and window is not None
        and any(
            (scalar.dataset_id, scalar.schema_revision_id) != (window.authority[0], window.authority[2])
            for scalar in models
        )
    ):
        raise CandidateRunnerError("scalar projection authority identity differs")
    await _flush_pending(session, window)
    for start in range(0, len(models), _PAGE_ROWS):
        page = models[start : start + _PAGE_ROWS]
        root_ids = tuple(scalar.root_revision_id for scalar in page if isinstance(scalar, CustomImportRootScalar))
        child_ids = tuple(scalar.child_revision_id for scalar in page if not isinstance(scalar, CustomImportRootScalar))
        scope = (("bigint", page[0].dataset_id), ("bigint[]", root_ids), ("bigint[]", child_ids), ("bigint", None))
        revision_ids = tuple(
            scalar.root_revision_id if isinstance(scalar, CustomImportRootScalar) else scalar.child_revision_id
            for scalar in page
        )
        arguments = (("bigint", page[0].dataset_id), ("bigint", page[0].schema_revision_id), ("bigint[]", revision_ids))
        arguments += tuple((kind, tuple(getattr(scalar, name) for scalar in page)) for name, kind in _SCALAR_COLUMNS)
        count = await _page(session, "persist_custom_import_scalar_set", arguments, scope, window)
        if type(count) is not int or count != len(page):
            raise CandidateRunnerError("scalar projection persisted count differs")
    return len(models)


async def persist_winner_models(session, materialization, models):
    """Append validated winners in bounded pages without committing.

    Preserve canonical context alongside native values and an optional real
    runner expectation. Flush errors, stale authority and page/count failures
    propagate to the transaction owner.
    """
    window = await _runner_window(session)
    generation = materialization.generation
    if (
        models
        and window is not None
        and (generation.dataset_id, generation.definition_revision_id, generation.schema_revision_id)
        != window.authority[:3]
    ):
        raise CandidateRunnerError("winner materialization authority identity differs")
    await _flush_pending(session, window)
    for start in range(0, len(models), _PAGE_ROWS):
        page = models[start : start + _PAGE_ROWS]
        scope = (
            ("bigint", generation.dataset_id),
            ("bigint[]", ()),
            ("bigint[]", ()),
            ("bigint", generation.generation_id),
        )
        arguments = (
            ("bigint", generation.generation_id),
            ("bigint", generation.dataset_id),
            ("bigint", generation.definition_revision_id),
            ("bigint", generation.schema_revision_id),
        )
        arguments += tuple((kind, tuple(getattr(winner, name) for winner in page)) for name, kind in _WINNER_COLUMNS)
        arguments += (
            (
                "text[]",
                tuple(winner.canonical_context_key for winner in materialization.winners[start : start + _PAGE_ROWS]),
            ),
        )
        count = await _page(session, "persist_custom_import_winner_set", arguments, scope, window)
        if type(count) is not int or count != len(page):
            raise CandidateRunnerError("winner materialization persisted count differs")
    return len(models)
