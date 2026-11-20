# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Drop temporary Source stages without replacing an original failure."""

from process.network_address_projection import _identifier
from process.uhc_flex_practitioner_async_safety import drain_operation


async def _drop_stage(connection, stage):
    from process.registry_typed_source_stage import drop_typed_source_stage, is_typed_source_stage

    if is_typed_source_stage(stage):
        await drop_typed_source_stage(connection, stage)
        return
    matches = getattr(stage, "office_matches", None)
    for name in (stage.table_name, stage.contribution_table_name, None if matches is None else matches.table_name):
        if name is not None:
            await connection.execute(f"DROP TABLE pg_temp.{_identifier(name)}")


async def _cleanup_independent_source_stage(connection, stage, primary):
    """Drain phase-owned cleanup without replacing its original failure."""
    try:
        await drain_operation(_drop_stage(connection, stage), preserve_cancellation=primary is None)
    except BaseException as cleanup_error:
        if primary is None:
            raise
        primary.add_note("Independent Source stage cleanup failed; caller rollback remains required.")
        primary._registry_source_cleanup_error = cleanup_error
