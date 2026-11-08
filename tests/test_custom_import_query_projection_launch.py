# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Projection stages require a third purpose-bound permit before claiming work."""

import hashlib
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.custom_import import admission_launch as launch
from process.custom_import import query_projection_authorization as wire
from process.custom_import import query_projection_worker as worker
from tests import test_custom_import_admission_launch as legacy
from tests.test_custom_import_admission_launch import mounted
from tests.test_custom_import_derived_query import definition


def _v2(case, **permit_changes):
    context = (
        legacy._permit_context(case.loaded)
        | dict(contract=wire.PROJECTION_PERMIT_CONTRACT, path=wire.PROJECTION_PATH)
        | permit_changes
    )
    signed = legacy.authorization.sign_permit(
        legacy._canonical(context),
        expected_origin=legacy.ORIGIN,
        trusted_now=legacy.NOW,
        launch_expires_at=legacy.EXPIRES,
        keyring=legacy.authorization.AdmissionKeyring("test-key", (("test-key", bytes(range(32))),)),
        is_projection=True,
    )
    raw = legacy._canonical(asdict(signed))
    legacy._write_private(case.directory / ".projection-permit.json", raw)
    case.document = case.document | dict(
        contract="custom-import/admission-launch/v2", projection_permit_sha256=hashlib.sha256(raw).hexdigest()
    )
    legacy._rewrite_launch(case)
    return signed


async def test_v2_preserves_three_purposes_and_one_admitted_ca(mounted):
    expected = _v2(mounted)
    loaded = replace(mounted.loaded, definition=definition())
    async with legacy._load(mounted, loaded=loaded) as transports:
        assert type(transports.projection) is worker.QueryProjectionBatchTransport
        assert transports.projection.signed_permit == expected
        assert transports.projection.tls_context is transports.source.tls_context is transports.admission.tls_context
        assert transports.projection.launch_expires_at == legacy.EXPIRES


def test_staged_v1_cannot_claim_definition_requiring_projection(mounted):
    with pytest.raises(ValueError, match=legacy._UNAVAILABLE):
        legacy._load(mounted, loaded=replace(mounted.loaded, definition=definition()))


def test_absent_stage_preserves_explicit_direct_owner_mode(mounted):
    for path in mounted.directory.iterdir():
        path.unlink()
    assert legacy._load(mounted, loaded=replace(mounted.loaded, definition=definition())) is None


@pytest.mark.parametrize("fault", ["missing", "digest", "v1", "extra", "source", "foreign-scope", "earlier-expiry"])
def test_projection_stage_cannot_be_missing_relabelled_or_foreign(mounted, fault):
    if fault == "foreign-scope":
        _v2(mounted, dataset_id=mounted.loaded.dataset_id + 1)
    elif fault == "earlier-expiry":
        _v2(mounted, expires_at="2030-01-01T00:14:00Z")
    else:
        _v2(mounted)
    path = mounted.directory / ".projection-permit.json"
    _corrupt_stage(mounted, path, fault)
    with pytest.raises(ValueError, match=legacy._UNAVAILABLE):
        legacy._load(mounted)


def _corrupt_stage(mounted, path, fault):
    if fault == "missing":
        path.unlink()
    if fault == "digest":
        legacy._rewrite_launch(mounted, projection_permit_sha256="0" * 64)
    if fault == "v1":
        legacy._rewrite_launch(mounted, contract="custom-import/admission-launch/v1")
    if fault == "extra":
        legacy._rewrite_launch(mounted, extra="closed")
    if fault == "source":
        legacy._write_private(path, mounted.source_bytes)
        legacy._rewrite_launch(mounted, projection_permit_sha256=hashlib.sha256(mounted.source_bytes).hexdigest())


@pytest.mark.parametrize("kind", ["symlink", "mode", "fifo", "oversize"])
def test_projection_file_has_identical_native_custody_rules(mounted, kind):
    _v2(mounted)
    path = mounted.directory / ".projection-permit.json"
    if kind == "mode":
        path.chmod(0o440)
    elif kind == "oversize":
        legacy._write_private(path, b"x" * 4097)
    else:
        path.unlink()
        if kind == "symlink":
            path.symlink_to(mounted.directory / ".source-permit.json")
        else:
            legacy.os.mkfifo(path, mode=0o400)
    with pytest.raises(ValueError, match=legacy._UNAVAILABLE):
        legacy._load(mounted)


async def test_projection_context_failure_closes_both_open_clients():
    admission = SimpleNamespace(__aenter__=AsyncMock(), __aexit__=AsyncMock())
    source = SimpleNamespace(__aenter__=AsyncMock(), __aexit__=AsyncMock())

    # Special methods are resolved on the type, matching real context managers.
    class Context:
        def __init__(self, enter, leave):
            self.enter, self.leave = enter, leave

        async def __aenter__(self):
            return await self.enter()

        async def __aexit__(self, *args):
            return await self.leave(*args)

    projection = Context(AsyncMock(side_effect=RuntimeError("private")), AsyncMock())
    pair = launch.WriterBatchTransports(
        Context(admission.__aenter__, admission.__aexit__), Context(source.__aenter__, source.__aexit__), projection
    )
    with pytest.raises(RuntimeError, match="private"):
        await pair.__aenter__()
    admission.__aexit__.assert_awaited_once()
    source.__aexit__.assert_awaited_once()
    projection.leave.assert_not_awaited()
