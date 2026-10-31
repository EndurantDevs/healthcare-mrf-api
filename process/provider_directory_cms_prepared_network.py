# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Stage CMS network facts in the separate prepared workspace."""

import asyncio

from process import provider_directory_insurance_network_batch as batch
from process.provider_directory_cms_prepared_workspace import CMSPreparedWorkspace
from process.provider_directory_generic_source_policy import _savepoint
from process.provider_directory_rooted_graph_acquisition_worker import drain_operation


async def record_prepared_network_batch(workspace, *, resource_type, resources):
    """Use the existing native encoder and set SQL without a SOURCE binding.

    Workspace checks authenticate private targets and the original session.
    This operation neither writes canonical rows nor issues CMS authority.
    """
    if type(workspace) is not CMSPreparedWorkspace:
        raise ValueError("cms_prepared_network_workspace_required")
    await workspace.verify_current()
    if workspace.binding["indexed"]:
        raise ValueError("cms_prepared_network_already_indexed")
    identity = workspace.identity
    release_id = identity["metadata"]["source_release"]["vector_sha256"]
    input_bytes = batch._envelope("cms-npd", release_id, resource_type, resources)
    encoded = await drain_operation(asyncio.to_thread(batch._encode, input_bytes), preserve_cancellation=True)
    await workspace.verify_current()
    if workspace.binding["indexed"]:
        raise ValueError("cms_prepared_network_already_indexed")
    async with _savepoint(workspace.connection):
        connection = await workspace.session.connection()
        names = batch.network_batch_relations(
            connection, workspace.policy.schema, workspace.catalog_schema, workspace.receipts
        )
        batch_receipt = await batch.write_network_batch_native(
            connection,
            workspace.session,
            workspace.connection,
            names,
            encoded,
            {
                "source_id": "cms-npd",
                "release_id": release_id,
                "resource_type": resource_type,
                "network_resource_id": None,
            },
        )
        await workspace.verify_current()
    return batch_receipt
