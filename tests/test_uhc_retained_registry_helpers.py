from __future__ import annotations

import pytest

from process.uhc_retained_registry_store import file_uri, table_name
from process.uhc_retained_types import UHCRetainedAdmissionError


def test_registry_identifiers_and_file_uris_fail_closed(tmp_path, monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "unsafe-schema")
    with pytest.raises(UHCRetainedAdmissionError, match="registry schema"):
        table_name("provider_directory_uhc_raw_artifact")
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    with pytest.raises(UHCRetainedAdmissionError, match="registry table"):
        table_name("Unsafe-Table")
    with pytest.raises(UHCRetainedAdmissionError, match="artifact is missing"):
        file_uri(tmp_path / "missing.json")

    directory = tmp_path / "directory"
    directory.mkdir()
    with pytest.raises(UHCRetainedAdmissionError, match="unsafe"):
        file_uri(directory)

    target_path = tmp_path / "target.json"
    target_path.write_text("[]", encoding="ascii")
    symlink = tmp_path / "link.json"
    symlink.symlink_to(target_path)
    with pytest.raises(UHCRetainedAdmissionError, match="unsafe"):
        file_uri(symlink)

    assert file_uri(target_path) == target_path.absolute().as_uri()

    target_path.chmod(0o666)
    with pytest.raises(UHCRetainedAdmissionError, match="unsafe"):
        file_uri(target_path)
    target_path.chmod(0o644)
    hard_link = tmp_path / "hard-link.json"
    hard_link.hardlink_to(target_path)
    with pytest.raises(UHCRetainedAdmissionError, match="unsafe"):
        file_uri(target_path)
