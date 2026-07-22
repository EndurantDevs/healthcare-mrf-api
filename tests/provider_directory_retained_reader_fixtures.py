# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Isolated filesystem fixture for retained-reader proofs."""

from __future__ import annotations

import shutil
import uuid
from contextlib import suppress
from pathlib import Path

import pytest

from process.provider_directory_retained_blob_store import ARTIFACT_ROOT_ENV


@pytest.fixture
def retained_artifact_test_root(monkeypatch) -> Path:
    """Create an isolated non-system-temporary artifact root."""

    base_path = Path.home() / ".healthporta-retained-reader-tests"
    root_path = base_path / uuid.uuid4().hex
    root_path.mkdir(parents=True)
    monkeypatch.setenv(ARTIFACT_ROOT_ENV, str(root_path))
    try:
        yield root_path
    finally:
        shutil.rmtree(root_path)
        with suppress(OSError):
            base_path.rmdir()
