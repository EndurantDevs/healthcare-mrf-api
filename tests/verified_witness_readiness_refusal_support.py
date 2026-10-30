# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native refusal observations and an optional exact frozen IC recovery seam."""

import hashlib
import importlib
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

from process.registry_membership_refresh_authority import COMMIT_AUTHORITY_PATH
from process.registry_membership_refresh_contract import membership_refresh_command_sha256
from tests.test_registry_generic_source_successor_postgres import _heap_oids, _read
from tests.test_registry_membership_refresh_execution_postgres import _counts, _storage_rows


def frozen_ic_recovery():
    """Require original and copied peer bytes to match the explicit snapshot descriptor."""
    selected_root = os.getenv("NETWORK_IC_TEST_ROOT")
    selected_freeze = os.getenv("NETWORK_IC_TEST_FREEZE")
    if selected_root is None and selected_freeze is None:
        return None
    assert selected_root and selected_freeze, "Both frozen IC inputs are required"
    freeze_by_field = json.loads(Path(selected_freeze).read_text())
    root = Path(selected_root).resolve(strict=True)
    copied = Path(freeze_by_field["ic_snapshot_root"]).resolve(strict=True)
    assert root == Path(freeze_by_field["ic_root"])
    package = freeze_by_field["module_package"]
    if type(package) is not str or not all(part.isidentifier() for part in package.split(".")):
        raise ValueError("Frozen peer package must be a Python package name")
    for name, expected in freeze_by_field["ic_sha256"].items():
        assert hashlib.sha256((root / name).read_bytes()).hexdigest() == expected, name
        assert hashlib.sha256((copied / name).read_bytes()).hexdigest() == expected, name
    for name, module in tuple(sys.modules.items()):
        if name == package or name.startswith(package + "."):
            assert Path(module.__file__).resolve().is_relative_to(copied), name
    if str(copied) not in sys.path:
        sys.path.append(str(copied))
    inputs = importlib.import_module(package + ".registry_membership_inputs")
    dispatch = importlib.import_module(package + ".registry_membership_refresh_dispatch")
    assert all(Path(module.__file__).resolve().is_relative_to(copied) for module in (inputs, dispatch))
    return SimpleNamespace(inputs=inputs, dispatch=dispatch)


async def request_record(fixture, request_id):
    """Read full native request and MVCC identity without issuing a queue operation."""
    return await fixture.connection.fetchrow(
        f'SELECT request.*,xmin::text AS row_xmin,ctid::text AS row_ctid FROM "{fixture.control_schema}".'
        "registry_publication_request request WHERE request_id=$1",
        UUID(request_id),
    )


async def serving_state(fixture, manifest):
    """Observe old physical storage, serving head and publication/custody counts."""
    return {
        "counts": await _counts(fixture),
        "head": tuple(
            await fixture.connection.fetchrow(
                f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control WHERE id=1'
            )
        ),
        "storage": await _storage_rows(fixture, manifest.schema_name),
        "heap_oids": await _heap_oids(fixture, manifest),
        "cas": tuple(fixture.cas_checks),
        "commit_calls": fixture.calls.count(COMMIT_AUTHORITY_PATH),
    }


async def assert_serving_unchanged(fixture, original, old_reader, before_by_field):
    """Refusal may retain a draft candidate; it cannot publish, seal or alter old serving rows."""
    after_by_field = await serving_state(fixture, original[0])
    assert after_by_field["counts"][0] == before_by_field["counts"][0]
    assert after_by_field["counts"][2:] == before_by_field["counts"][2:]
    for name in ("head", "storage", "heap_oids", "cas", "commit_calls"):
        assert after_by_field[name] == before_by_field[name], name
    async with fixture.api.transaction(isolation="repeatable_read", readonly=True):
        assert await _read(fixture, fixture.api) == original
    assert await _read(fixture, old_reader) == original
    return after_by_field


async def assert_ic_request_recovery(ic, recovered, command, request_id):
    """Call the actual IC request-link function; no HTTP or durable IC admission is claimed."""
    if ic is None:
        return "unqualified: no exact frozen peer snapshot supplied"
    snapshot = ic.inputs.validated_membership_refresh_inputs(recovered)
    assert snapshot.refresh_request["request_id"] == request_id
    assert snapshot.refresh_request["state"] == "rejected"
    assert snapshot.refresh_request["command_sha256"] == membership_refresh_command_sha256(command)
    enqueue_boundary = AsyncMock(side_effect=AssertionError("Recovered rejection must not enqueue again"))
    status_boundary = AsyncMock(side_effect=AssertionError("Recovered rejection needs no second status fetch"))
    boundary = SimpleNamespace(
        queue_registry_membership_refresh=enqueue_boundary,
        registry_membership_refresh_request=status_boundary,
    )
    link = await ic.dispatch._request_link(boundary, object(), snapshot)
    assert link == snapshot.refresh_request
    assert link["inputs_digest"] == snapshot.inputs.digest()
    enqueue_boundary.assert_not_awaited()
    status_boundary.assert_not_awaited()
    return "actual frozen IC _request_link: exact rejected digest recovered with zero client calls"
