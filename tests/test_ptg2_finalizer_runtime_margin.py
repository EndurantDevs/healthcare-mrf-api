# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import copy
from pathlib import Path
from types import SimpleNamespace

import pytest

from process.ptg_parts import ptg2_shared_finalize as finalizer
from tests.ptg2_shared_finalize_test_support import (
    _identity,
    _one_source_contracted_inputs,
    _provider_metadata_entries,
)


def _contracts_and_dictionary(tmp_path: Path):
    identity, serving_entries, dictionary_entries = (
        _one_source_contracted_inputs(
            tmp_path,
            dictionary_file_count=1,
        )
    )
    _prepared, _source_count, source_contracts = (
        finalizer._prepare_serving_entries(
            copy.deepcopy(serving_entries),
            expected_source_identities=[identity],
        )
    )
    return identity, serving_entries, dictionary_entries, source_contracts


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        (
            lambda entry, contracts: entry.pop("identity_sha256"),
            "incomplete physical identity",
        ),
        (
            lambda entry, contracts: entry.update(identity_sha256="b" * 64),
            "not part of the complete physical input set",
        ),
        (
            lambda entry, contracts: entry.update(
                source_run_contract_sha256="00" * 32
            ),
            "bound to another source contract",
        ),
    ),
)
def test_dictionary_entry_binding_requires_exact_source_contract(
    tmp_path: Path,
    mutation: object,
    message: str,
) -> None:
    _identity_map, _serving, dictionary, source_contracts = (
        _contracts_and_dictionary(tmp_path)
    )
    entry = copy.deepcopy(dictionary[0])
    mutation(entry, source_contracts)
    contracts_by_identity = finalizer._source_contracts_by_identity(
        source_contracts
    )

    with pytest.raises(RuntimeError, match=message):
        finalizer._dictionary_entry_source_binding(
            entry, contracts_by_identity
        )


def test_dictionary_contract_registration_is_single_and_digest_bound(
    tmp_path: Path,
) -> None:
    _identity_map, _serving, dictionary, _source_contracts = (
        _contracts_and_dictionary(tmp_path)
    )
    entry = dictionary[0]
    digest = entry["code_dictionary_contract_sha256"]
    contract_by_source: dict[int, dict[str, object]] = {}

    finalizer._register_dictionary_source_contract(
        {"code_dictionary_source_contract": None},
        source_key=0,
        dictionary_contract_sha256=digest,
        dictionary_contract_by_source=contract_by_source,
    )
    assert contract_by_source == {}

    with pytest.raises(RuntimeError, match="is incompatible"):
        finalizer._register_dictionary_source_contract(
            {"code_dictionary_source_contract": []},
            source_key=0,
            dictionary_contract_sha256=digest,
            dictionary_contract_by_source=contract_by_source,
        )
    with pytest.raises(RuntimeError, match="digest is invalid"):
        finalizer._register_dictionary_source_contract(
            entry,
            source_key=0,
            dictionary_contract_sha256="00" * 32,
            dictionary_contract_by_source=contract_by_source,
        )

    finalizer._register_dictionary_source_contract(
        entry,
        source_key=0,
        dictionary_contract_sha256=digest,
        dictionary_contract_by_source=contract_by_source,
    )
    with pytest.raises(RuntimeError, match="repeat physical identity"):
        finalizer._register_dictionary_source_contract(
            entry,
            source_key=0,
            dictionary_contract_sha256=digest,
            dictionary_contract_by_source=contract_by_source,
        )


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        (lambda contracts: contracts.clear(), "missing a complete"),
        (
            lambda contracts: contracts[0].update(unexpected=True),
            "fields are incompatible",
        ),
        (
            lambda contracts: contracts[0].update(version=2),
            "version is incompatible",
        ),
        (
            lambda contracts: contracts[0].update(
                source_identity=_identity("b")
            ),
            "another physical source",
        ),
        (
            lambda contracts: contracts[0].update(
                source_run_contract_sha256="00" * 32
            ),
            "another source-run contract",
        ),
    ),
)
def test_dictionary_source_contract_revalidates_identity_and_version(
    tmp_path: Path,
    mutation: object,
    message: str,
) -> None:
    _identity_map, _serving, dictionary, source_contracts = (
        _contracts_and_dictionary(tmp_path)
    )
    contract_by_source = {
        0: copy.deepcopy(dictionary[0]["code_dictionary_source_contract"])
    }
    mutation(contract_by_source)

    with pytest.raises(RuntimeError, match=message):
        finalizer._validated_dictionary_source_contract(
            source_contracts[0], contract_by_source
        )


@pytest.mark.parametrize(
    ("files", "message"),
    (
        (None, "missing file digests"),
        ([None], "file fields are incompatible"),
        (
            [
                {
                    "row_count": True,
                    "bytes": 64,
                    "sha256": "11" * 32,
                }
            ],
            "file counts are incompatible",
        ),
        (
            [
                {
                    "row_count": 1,
                    "bytes": -1,
                    "sha256": "11" * 32,
                }
            ],
            "file counts are incompatible",
        ),
    ),
)
def test_dictionary_file_descriptors_are_complete_and_nonnegative(
    files: object,
    message: str,
) -> None:
    with pytest.raises(RuntimeError, match=message):
        finalizer._validated_dictionary_file_descriptors({"files": files})


def test_dictionary_file_descriptors_require_canonical_order() -> None:
    files = [
        {"row_count": 1, "bytes": 2, "sha256": "22" * 32},
        {"row_count": 1, "bytes": 2, "sha256": "11" * 32},
    ]
    with pytest.raises(RuntimeError, match="file order is incompatible"):
        finalizer._validated_dictionary_file_descriptors({"files": files})


@pytest.mark.parametrize(
    ("contract", "observed", "message"),
    (
        (
            {"file_count": True, "row_count": 1, "byte_count": 1},
            [],
            "aggregates are incompatible",
        ),
        (
            {
                "file_count": 2,
                "row_count": 1,
                "byte_count": 1,
                "files": [],
            },
            [],
            "aggregates do not match",
        ),
        (
            {
                "file_count": 1,
                "row_count": 1,
                "byte_count": 2,
                "files": [
                    {
                        "row_count": 1,
                        "bytes": 2,
                        "sha256": "11" * 32,
                    }
                ],
            },
            [{"row_count": 1, "bytes": 2, "sha256": "22" * 32}],
            "file digests do not match",
        ),
    ),
)
def test_dictionary_aggregate_contract_matches_exact_files(
    contract: dict[str, object],
    observed: list[dict[str, object]],
    message: str,
) -> None:
    with pytest.raises(RuntimeError, match=message):
        finalizer._validate_dictionary_contract_files(contract, observed)


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        (
            lambda entries: entries[0].pop("identity_sha256"),
            "incomplete physical identity",
        ),
        (
            lambda entries: entries[0].update(identity_sha256="b" * 64),
            "outside the complete physical input set",
        ),
        (
            lambda entries: entries[0].update(
                source_run_contract_sha256="00" * 32
            ),
            "bound to another source contract",
        ),
    ),
)
def test_provider_set_metadata_is_bound_to_source_contract(
    tmp_path: Path,
    mutation: object,
    message: str,
) -> None:
    identity, serving, _dictionary, source_contracts = _contracts_and_dictionary(
        tmp_path
    )
    entries = _provider_metadata_entries(
        tmp_path, (identity, serving[0])
    )
    mutation(entries)
    with pytest.raises(RuntimeError, match=message):
        finalizer._prepare_provider_set_metadata_entries(
            entries, source_run_contracts=source_contracts
        )


class _ProcChild:
    def __init__(self, text: str) -> None:
        self.text = text

    def read_text(self, *, encoding: str) -> str:
        assert encoding == "ascii"
        return self.text


class _ProcRoot:
    def __init__(self, stat_text: str, io_text: str) -> None:
        self.children = {
            "stat": _ProcChild(stat_text),
            "io": _ProcChild(io_text),
        }

    def is_dir(self) -> bool:
        return True

    def __truediv__(self, name: str) -> _ProcChild:
        return self.children[name]


def test_linux_process_usage_reads_cpu_and_io_without_shelling_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stat_fields = ["0"] * 11 + ["2", "3"]
    root = _ProcRoot(
        f"7 (worker) R {' '.join(stat_fields)}",
        "read_bytes: 5\nwrite_bytes: 7\n",
    )
    monkeypatch.setattr(finalizer.sys, "platform", "linux")
    monkeypatch.setattr(finalizer, "Path", lambda _value: root)
    monkeypatch.setattr(finalizer.os, "sysconf", lambda _name: 100)

    assert finalizer._finalizer_process_usage(7) == (20, 12)


def test_linux_process_usage_fails_closed_on_proc_shape_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _ProcRoot("invalid", "invalid")
    monkeypatch.setattr(finalizer.sys, "platform", "linux")
    monkeypatch.setattr(finalizer, "Path", lambda _value: root)

    assert finalizer._finalizer_process_usage(7) == (0, 0)


class _CommunicatingProcess:
    pid = 7

    async def communicate(self):
        return b"stdout", b"stderr"


@pytest.mark.asyncio
async def test_finalizer_communication_runs_and_stops_progress_monitor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monitor_stopped = asyncio.Event()

    async def monitor(**kwargs):
        await kwargs["stop"].wait()
        monitor_stopped.set()

    monkeypatch.setattr(finalizer, "_monitor_finalizer_output", monitor)
    with finalizer.observe_v3_finalizer_progress(
        lambda _name, _value: None
    ):
        observed = await finalizer._communicate_with_finalizer_progress(
            _CommunicatingProcess(),
            output_directory=tmp_path,
        )

    assert observed == (b"stdout", b"stderr")
    assert monitor_stopped.is_set()


@pytest.mark.asyncio
async def test_direct_finalizer_preflights_binary_output_and_price_map(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configuration = SimpleNamespace()
    monkeypatch.setattr(
        finalizer,
        "_load_v3_finalizer_resource_configuration",
        lambda: configuration,
    )
    finalizer_options_by_name = {
        "work_directory": tmp_path,
        "serving_run_entries": (),
        "code_dictionary_entries": (),
        "provider_set_metadata_entries": (),
        "expected_source_identities": (),
        "price_key_map_input": tmp_path / "price-map",
        "price_key_map_row_count": 1,
    }

    monkeypatch.setattr(finalizer, "_ptg2_rust_scanner_binary", lambda: None)
    with pytest.raises(RuntimeError, match="requires the PTG2 Rust scanner"):
        await finalizer.run_v3_direct_finalizer(**finalizer_options_by_name)

    binary = tmp_path / "scanner"
    binary.write_bytes(b"x")
    monkeypatch.setattr(
        finalizer, "_ptg2_rust_scanner_binary", lambda: binary
    )
    (tmp_path / "finalized").mkdir()
    with pytest.raises(RuntimeError, match="output already exists"):
        await finalizer.run_v3_direct_finalizer(**finalizer_options_by_name)
    (tmp_path / "finalized").rmdir()

    with pytest.raises(RuntimeError, match="non-empty price-key map"):
        await finalizer.run_v3_direct_finalizer(**finalizer_options_by_name)

    price_map = tmp_path / "price-map"
    price_map.write_bytes(b"x")
    with pytest.raises(RuntimeError, match="positive price-key map row count"):
        await finalizer.run_v3_direct_finalizer(
            **{**finalizer_options_by_name, "price_key_map_row_count": 0}
        )
