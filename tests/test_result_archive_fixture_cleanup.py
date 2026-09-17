# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Keep native fixture cleanup from masking the original publication failure."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from tests import _result_archive_adoption_native_support as support


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["success", "publication_failure", "dirty_success"])
async def test_finalizer_fixture_preserves_failure_and_checks_success(tmp_path, monkeypatch, outcome):
    """A partial work directory must not replace a publication exception."""

    artifacts = SimpleNamespace(cleanup=Mock())
    monkeypatch.setattr(support, "generate_artifacts", lambda *_args: artifacts)
    publication_error = RuntimeError("synthetic publication failure")
    observed_directories = []

    async def publish(request):
        observed_directories.append(request.work_directory)
        if outcome != "success":
            (request.work_directory / "partial-output").mkdir()
        if outcome == "publication_failure":
            raise publication_error
        return SimpleNamespace(publication=SimpleNamespace(manifest=lambda: {"complete": True})), 0, ()

    monkeypatch.setattr(support.finalizer_lifecycle, "_publish_finalizer", publish)
    invocation = support._publish_finalizer_artifacts(
        object(),
        schema_name="synthetic",
        source_snapshot_key=1,
        finalizer_build_token="synthetic-build",
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
    )
    if outcome == "publication_failure":
        with pytest.raises(RuntimeError) as failure:
            await invocation
        assert failure.value is publication_error
    elif outcome == "dirty_success":
        with pytest.raises(AssertionError):
            await invocation
    else:
        assert await invocation == {"complete": True}
        assert not observed_directories[0].exists()
    artifacts.cleanup.assert_called_once_with()
