# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Run optional retained FHIR capture only after the original Source owner commits."""

import asyncio
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from process import provider_directory_generic_source_policy as source
from process.network_cms_registry_address_capture import native_driver
from process.provider_directory_output_snapshot_catalog import require_output
from process.registry_company_approval_fence import _source_engine
from process.registry_generic_accepted_capture import read_current_generic_fhir_head
from process.registry_generic_capture_continuation import (
    RegistryGenericCaptureTarget,
    capture_committed_generic_fhir_source,
)


def _require_scope(directory, candidate, selected, continuation):
    from process.provider_directory_generic_source_artifacts import _SOURCE_ONLY

    require_output(
        _SOURCE_ONLY.get() is selected
        and selected[0] is directory
        and selected[1] is source._FACTORY.get()
        and selected[1].policy is selected[6]
        and selected[2] is asyncio.current_task()
        and not selected[2].done()
        and selected[3] == source.source_producer_identity()
        and selected[5]["active"] is True
        and candidate.source_ids == (selected[4],)
        and getattr(selected[1], "generic_fhir_capture_continuation", None) is continuation
        and directory._schema() == selected[6].schema
        and source._PUBLISHER.get() is None
        and source._OWNER.get() is None
        and directory.db._transaction_binding() is None,
        "rest_source_capture_commit_required",
    )


@dataclass(frozen=True)
class GenericSourceCaptureContinuation:
    """Configured Source reader and administrator preparation sessions for a retained copy."""

    reader_factory: object
    writer_factory: object
    target: RegistryGenericCaptureTarget

    def __post_init__(self):
        require_output(type(self.target) is RegistryGenericCaptureTarget, "rest_source_capture_target_invalid")
        _source_engine(self.reader_factory)
        _source_engine(self.writer_factory)

    async def capture(self, directory, candidate, selected):
        """Own fresh RR transactions; a failed capture leaves the committed Source replayable."""
        _require_scope(directory, candidate, selected, self)
        binding = getattr(selected[1], "storage_binding", None)
        schema = binding.catalog_schema if binding is not None else selected[6].schema
        async with self.reader_factory() as reader_session, self.writer_factory() as writer_session:
            require_output(
                type(reader_session) is AsyncSession
                and type(writer_session) is AsyncSession
                and not reader_session.in_transaction()
                and not writer_session.in_transaction(),
                "rest_source_capture_sessions_invalid",
            )
            async with reader_session.begin(), writer_session.begin():
                await reader_session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
                await writer_session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ WRITE"))
                reader, writer = await native_driver(reader_session), await native_driver(writer_session)
                _require_scope(directory, candidate, selected, self)
                head = await _capture_source_head(reader, schema, candidate, selected)
                _require_scope(directory, candidate, selected, self)
                capture = await capture_committed_generic_fhir_source(
                    reader,
                    writer,
                    schema_name=schema,
                    source_id=selected[4],
                    endpoint_id=candidate.endpoint_id,
                    target=self.target,
                    expected_head=head,
                    source_policy=selected[6],
                )
                _require_scope(directory, candidate, selected, self)
        _require_scope(directory, candidate, selected, self)
        return capture


async def _capture_source_head(reader, schema, candidate, selected):
    head = await read_current_generic_fhir_head(
        reader,
        schema_name=schema,
        source_id=selected[4],
        endpoint_id=candidate.endpoint_id,
    )
    require_output(
        (head.schema_name, head.source_id, head.endpoint_id, head.dataset_id, head.root_run_id)
        == (
            selected[6].schema,
            selected[4],
            candidate.endpoint_id,
            candidate.dataset_id,
            candidate.acquisition_root_run_id,
        ),
        "rest_source_capture_head_changed",
    )
    return head
