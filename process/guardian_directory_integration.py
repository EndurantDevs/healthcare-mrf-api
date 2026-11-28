# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Connect a scoped Guardian dental capture to the ordinary source dataset publisher."""

from functools import partial

from process import guardian_directory_capture as capture
from process import provider_directory_locator_integration as shared

SOURCE_ID = capture.SOURCE_ID
ENDPOINT_SCOPE = capture.ENDPOINT_SCOPE
ENTRY_ID = capture.ENTRY_ID
TRANSPORT = capture.TRANSPORT
RESOURCE_TYPES = capture.RESOURCE_TYPES
ADAPTER_ID = capture.ADAPTER_ID
load_search_plan = capture.load_search_plan

is_explicit_selection = partial(shared.is_explicit_selection, capture)
source_row = partial(shared.source_row, capture)
is_source = partial(shared.is_source, capture)
require_task = partial(shared.require_task, capture)


def endpoint_contract():
    """Identify the employer dental locator independently of the current network list."""
    return {"adapter": ADAPTER_ID, "transport": TRANSPORT, "resources": list(RESOURCE_TYPES)}


async def import_group(directory, source_record, **options):
    """Load the mounted scope for this selected directory."""
    return await shared.import_group(capture, directory, source_record, load_plan=load_search_plan, **options)
