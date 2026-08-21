# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fallback proofs for linked temporary projection files."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from process.ptg_parts import ptg2_tax_identity_source_copy as source_copy
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    TaxIdentitySourceProjectionError,
)

_ERROR = "ptg2_tax_identity_source_projection_invalid"


def test_linked_scratch_copy_falls_back_to_local_anonymous_file(tmp_path):
    scratch_parent = tmp_path / "scratch"
    scratch_parent.mkdir()
    linked_file = source_copy.tempfile.NamedTemporaryFile(
        mode="w+b",
        buffering=0,
        dir=scratch_parent,
    )
    linked_path = linked_file.name
    real_temporary_file = source_copy.tempfile.TemporaryFile

    def temporary_file(*args, **kwargs):
        if Path(kwargs["dir"]) == scratch_parent:
            return linked_file
        return real_temporary_file(*args, **kwargs)

    with patch.object(source_copy.tempfile, "TemporaryFile", temporary_file):
        with source_copy._open_anonymous_projection_copy(scratch_parent) as copy_file:
            assert os.fstat(copy_file.fileno()).st_nlink == 0

    assert linked_file.closed
    assert not os.path.exists(linked_path)


def test_linked_local_fallback_remains_rejected(tmp_path):
    scratch_parent = tmp_path / "scratch"
    scratch_parent.mkdir()
    linked_files = []

    def temporary_file(*args, **kwargs):
        linked_file = source_copy.tempfile.NamedTemporaryFile(
            mode="w+b",
            buffering=0,
            dir=kwargs["dir"],
        )
        linked_files.append(linked_file)
        return linked_file

    with patch.object(source_copy.tempfile, "TemporaryFile", temporary_file):
        with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
            source_copy._open_anonymous_projection_copy(scratch_parent)

    assert len(linked_files) == 2
    assert all(linked_file.closed for linked_file in linked_files)


def test_nonempty_anonymous_copy_remains_rejected(tmp_path):
    scratch_parent = tmp_path / "scratch"
    scratch_parent.mkdir()
    nonempty_file = source_copy.tempfile.TemporaryFile(
        mode="w+b",
        buffering=0,
        dir=scratch_parent,
    )
    nonempty_file.write(b"unexpected")

    with (
        patch.object(
            source_copy.tempfile,
            "TemporaryFile",
            return_value=nonempty_file,
        ),
        pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR),
    ):
        source_copy._open_anonymous_projection_copy(scratch_parent)

    assert nonempty_file.closed


def test_local_directory_cleanup_failure_closes_fallback_copy(tmp_path):
    scratch_parent = tmp_path / "scratch"
    scratch_parent.mkdir()
    linked_file = source_copy.tempfile.NamedTemporaryFile(
        mode="w+b",
        buffering=0,
        dir=scratch_parent,
    )
    linked_path = linked_file.name
    fallback_files = []
    real_temporary_file = source_copy.tempfile.TemporaryFile
    real_temporary_directory = source_copy.tempfile.TemporaryDirectory

    def temporary_file(*args, **kwargs):
        if Path(kwargs["dir"]) == scratch_parent:
            return linked_file
        fallback_file = real_temporary_file(*args, **kwargs)
        fallback_files.append(fallback_file)
        return fallback_file

    class CleanupFailure:
        def __init__(self, *args, **kwargs):
            self._temporary_directory = real_temporary_directory(*args, **kwargs)

        def __enter__(self):
            return self._temporary_directory.__enter__()

        def __exit__(self, exc_type, exc_value, traceback):
            self._temporary_directory.__exit__(exc_type, exc_value, traceback)
            raise OSError("synthetic cleanup failure")

    with (
        patch.object(source_copy.tempfile, "TemporaryFile", temporary_file),
        patch.object(source_copy.tempfile, "TemporaryDirectory", CleanupFailure),
        pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR),
    ):
        source_copy._open_anonymous_projection_copy(scratch_parent)

    assert linked_file.closed
    assert not os.path.exists(linked_path)
    assert len(fallback_files) == 1
    assert fallback_files[0].closed
