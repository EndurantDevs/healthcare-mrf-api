# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded private operations for trusted retained-segmented-copy producers."""

import time

from api import custom_import_owner_worker as owner
from api import custom_import_retained_copy_worker as worker
from api.control_execution_evidence import _error, _is_authorized, _reply
from process.custom_import import family_shard_handoff as custody
from process.custom_import import retained_capture_copy as service
from process.custom_import.family_shard_owner_cli import profile


async def serve(request, factory):
    """Authenticate before parsing; never reflect input, bearer or exception text."""
    if not _is_authorized(getattr(request, "headers", None)):
        return _error("forbidden", 403)
    try:
        if getattr(request, "query_string", ""):
            raise ValueError()
        document = custody.document(request.body)
        if type(document) is not dict:
            raise ValueError()
        configuration, _key, _limits = profile()
        operation = document.get("operation")
        await owner._require_owner(factory, configuration)
        if operation == "prepare":
            return _reply(await service.prepare(factory, document, configuration))
        fields = (
            {"operation", "signed", "bearer"}
            if operation == "launch"
            else ({"operation", "signed"} if operation == "observe" else {"operation", "signed", "receipt"})
        )
        if document.keys() != fields or operation not in {"launch", "observe", "stop", "cleanup"}:
            raise ValueError()
        bearer = custody.checked_bearer(document["bearer"].encode("ascii")) if operation == "launch" else None
        service.authorize(document["signed"], bearer, configuration, now=time.time() if operation == "launch" else None)
        if operation == "launch":
            operation_receipt = await worker.launch(factory, document["signed"], bearer, configuration)
        elif operation == "observe":
            operation_receipt = await worker.observe(factory, document["signed"], configuration)
        else:
            operation_receipt = await worker.stop(
                factory, document["signed"], document["receipt"], configuration, cleanup=operation == "cleanup"
            )
        return _reply(operation_receipt)
    except Exception:
        return _error("retained_capture_copy_unavailable", 503)
