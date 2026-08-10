# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Descriptor-validated private staging directories for retained artifacts."""

from __future__ import annotations

import os
from pathlib import Path
import re
import stat

from process import provider_directory_retained_blob_install_support as install_io
from process.provider_directory_retained_artifact_base import RetainedArtifactError
from process.provider_directory_retained_blob_store import (
    _configured_root_components,
)
from process.provider_directory_retained_blob_store import _directory_identity


_STAGING_DIRECTORY_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}\Z")


def _require_private_owned_directory(directory_descriptor: int) -> None:
    try:
        directory_state = os.fstat(directory_descriptor)
        _directory_identity(directory_state)
        if directory_state.st_uid != os.geteuid():
            raise RetainedArtifactError("retained_artifact_path_unsafe")
        if stat.S_IMODE(directory_state.st_mode) != 0o700:
            os.fchmod(directory_descriptor, 0o700)
            install_io._sync_directory(directory_descriptor)
            directory_state = os.fstat(directory_descriptor)
        if (
            directory_state.st_uid != os.geteuid()
            or stat.S_IMODE(directory_state.st_mode) != 0o700
        ):
            raise RetainedArtifactError("retained_artifact_path_unsafe")
    except RetainedArtifactError:
        raise
    except OSError as error:
        raise RetainedArtifactError("retained_artifact_path_unsafe") from error


def _close_after_failure(
    primary_error: BaseException,
    descriptors: list[int],
) -> None:
    install_io._close_after_error(
        primary_error,
        lambda: install_io._close_descriptor_sequence(
            descriptors,
            "retained_blob_directory_close_failed",
        ),
    )


def prepare_retained_artifact_staging_directory(name: str) -> Path:
    """Create one private staging directory through pinned descriptors."""

    if type(name) is not str or not _STAGING_DIRECTORY_PATTERN.fullmatch(name):
        raise RetainedArtifactError("retained_artifact_path_unsafe")
    install_io._require_descriptor_install_platform()
    root_components = _configured_root_components()
    descriptors, _identities = install_io._open_directory_chain(root_components)
    try:
        for component in ("tmp", name):
            descriptor = install_io._open_or_create_directory(
                descriptors[-1],
                component,
            )
            descriptors.append(descriptor)
            _require_private_owned_directory(descriptor)
        result = Path("/").joinpath(*root_components, "tmp", name)
        install_io._close_descriptor_sequence(
            descriptors,
            "retained_blob_directory_close_failed",
        )
        return result
    except BaseException as primary_error:
        _close_after_failure(primary_error, descriptors)
        raise


__all__ = ("prepare_retained_artifact_staging_directory",)
