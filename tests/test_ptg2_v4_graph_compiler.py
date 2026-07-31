import asyncio
from copy import deepcopy
import io
import json
from pathlib import Path

import pytest

from process.ptg_parts import ptg2_v4_graph_compiler as compiler
from process.ptg_parts import ptg2_shared_snapshot_publish
from process.ptg_parts import ptg2_v4_snapshot_maps
from process.ptg_parts.ptg2_v4_graph_compiler import (
    V4GraphResourceAdmissionError,
    compile_provider_graph_v4_rust,
)
from tests.ptg2_v4_graph_compiler_test_support import (
    compiler_inputs as _compiler_inputs,
    compiler_fixture as _fixture,
    scanner_binary as _binary,
)


def _progress_event(
    seq: int,
    phase: str,
    done: int,
    total: int,
    *,
    unit: str = "stage",
    elapsed_ms: int = 0,
    terminal: bool = False,
) -> bytes:
    return (
        compiler.PTG2_V4_PROGRESS_PREFIX
        + json.dumps(
            {
                "version": compiler.PTG2_V4_PROGRESS_VERSION,
                "seq": seq,
                "phase": phase,
                "done": done,
                "total": total,
                "unit": unit,
                "elapsed_ms": elapsed_ms,
                "terminal": terminal,
            },
            separators=(",", ":"),
        ).encode("ascii")
        + b"\n"
    )


class _HeartbeatProcess:
    """Hold a fake compiler open until its wrapper publishes one heartbeat."""

    def __init__(self, *, child_progress: bool, factor_edge_count: int) -> None:
        self.pid = 99_999_998
        self.returncode = None
        self.stderr = asyncio.StreamReader()
        if child_progress:
            self.stderr.feed_data(
                _progress_event(
                    1,
                    "derive_patterns",
                    4,
                    factor_edge_count,
                    unit="groups",
                )
            )
        self.finished = asyncio.Event()
        self.observed_events: list[dict[str, object]] = []

    async def wait(self):
        await self.finished.wait()
        return self.returncode

    async def create_subprocess(self, *_args, **_kwargs):
        return self

    async def capture_progress(self, **progress_by_field):
        self.observed_events.append(progress_by_field)
        if str(progress_by_field.get("message", "")).endswith("; active"):
            self.returncode = 1
            self.stderr.feed_eof()
            self.finished.set()

    def active_heartbeat(self) -> dict[str, object]:
        return next(
            event
            for event in self.observed_events
            if str(event.get("message", "")).endswith("; active")
        )


class _PendingNpiScopeProcess:
    """Keep one fake NPI-scope subprocess pending until cancellation."""

    def __init__(self) -> None:
        self.pid = 99_999_997
        self.returncode = None
        self.finished = asyncio.Event()

    async def wait(self):
        await self.finished.wait()
        return self.returncode

    async def create_subprocess(self, *_args, **_kwargs):
        return self


class _PendingCompilerProcess:
    """Keep one fake compiler pending and record wrapper termination."""

    def __init__(self) -> None:
        self.pid = 99_999_999
        self.returncode = None
        self.stderr = asyncio.StreamReader()
        self.stderr.feed_data(_progress_event(1, "resource_admission", 0, 1))
        self.created = asyncio.Event()
        self.finished = asyncio.Event()
        self.terminated = asyncio.Event()

    async def wait(self):
        await self.finished.wait()
        return self.returncode

    async def create_subprocess(self, *_args, **_kwargs):
        self.created.set()
        return self

    async def terminate(self, process):
        assert process is self
        self.returncode = -15
        self.stderr.feed_eof()
        self.finished.set()
        self.terminated.set()


def _assert_summary_rejected(
    summary_by_field: dict,
    validation_arguments_by_name: dict,
    expected_message: str,
) -> None:
    """Assert that one mutated compiler summary fails authentication."""

    with pytest.raises(RuntimeError, match=expected_message):
        compiler._validate_compiler_summary(
            summary_by_field,
            **_packed_summary_validation(validation_arguments_by_name),
        )


def _packed_summary_validation(arguments_by_name: dict) -> dict:
    """Pack legacy fixture fields into one summary expectation."""

    copied_arguments_by_name = dict(arguments_by_name)
    expectation = compiler._CompilerSummaryExpectation(
        input_bytes=copied_arguments_by_name.pop("expected_input_bytes"),
        factor_edges=copied_arguments_by_name.pop("expected_factor_edges"),
        factor_owners=copied_arguments_by_name.pop("expected_factor_owners"),
        options=copied_arguments_by_name.pop("expected_options"),
        tax_identity=copied_arguments_by_name.pop(
            "expected_tax_identity",
            None,
        ),
        taxonomy_rule_count=copied_arguments_by_name.pop(
            "expected_taxonomy_rule_count",
            None,
        ),
    )
    return {**copied_arguments_by_name, "expectation": expectation}


def _tampered_summary_cases(
    summary_by_field: dict,
    expected_options: dict,
) -> tuple[tuple[dict, str], ...]:
    """Build representative sealed-option and diagnostic drift cases."""

    changed_limit = deepcopy(summary_by_field)
    changed_limit["max_set_components_per_fallback_set"] += 1
    changed_provider_page = deepcopy(summary_by_field)
    changed_provider_page["provider_expansion_rate_page_rows"] += 1
    inconsistent = deepcopy(summary_by_field)
    inconsistent["observe"]["pattern_overflow_set_count"] = 1
    unsafe_second_hop = deepcopy(summary_by_field)
    unsafe_second_hop["observe"]["maximum_online_group_npi_batch_work"] = (
        expected_options["max_online_group_npi_batches_per_set"] + 1
    )
    changed_decision = deepcopy(summary_by_field)
    changed_decision["pattern_layout_serving_degree_eligible"] = not bool(
        changed_decision["pattern_layout_serving_degree_eligible"]
    )
    return (
        (changed_limit, "fallback-degree limit changed"),
        (changed_provider_page, "option .* changed"),
        (inconsistent, "diagnostics are inconsistent"),
        (unsafe_second_hop, "diagnostics are inconsistent"),
        (changed_decision, "serving-degree decision changed"),
    )


@pytest.mark.asyncio
async def test_wrapper_discards_mismatched_checkpoint_and_rebuilds(
    tmp_path: Path,
) -> None:
    artifacts, provider_map = _fixture(tmp_path)
    npi_scope, inferred_taxonomy = await _compiler_inputs(tmp_path, artifacts)
    output = tmp_path / "compiled"
    first = await compile_provider_graph_v4_rust(
        graph_artifact_entries=artifacts,
        provider_set_key_map_path=provider_map,
        npi_scope=npi_scope,
        inferred_taxonomy=inferred_taxonomy,
        output_directory=output,
        binary_path=_binary(),
    )
    map_summary = ptg2_v4_snapshot_maps.summarize_v4_snapshot_map_packs(
        ptg2_v4_snapshot_maps.iter_v4_snapshot_map_packs(
            ptg2_shared_snapshot_publish._iter_v4_block_references(
                first.reference_manifest_path
            )
        )
    )
    selected_prefix = first.selected_layout
    assert (
        first.summary[f"{selected_prefix}_map_payload_encoded_bytes"]
        == map_summary.stored_map_byte_count
    )
    assert (
        first.summary[f"{selected_prefix}_map_coordinate_count"]
        == map_summary.coordinate_count
    )
    assert (
        first.summary[f"{selected_prefix}_map_pack_count"] == map_summary.map_pack_count
    )
    assert (
        first.summary[f"{selected_prefix}_map_object_kind_count"]
        == map_summary.object_kind_count
    )
    (output / "v4-complete.json").write_text("{}\n")

    rebuilt = await compile_provider_graph_v4_rust(
        graph_artifact_entries=artifacts,
        provider_set_key_map_path=provider_map,
        npi_scope=npi_scope,
        inferred_taxonomy=inferred_taxonomy,
        output_directory=output,
        binary_path=_binary(),
    )
    assert rebuilt.checkpoint_reused is False


@pytest.mark.asyncio
async def test_wrapper_rebuilds_v1_checkpoint_without_migration(
    tmp_path: Path,
) -> None:
    artifacts, provider_map = _fixture(tmp_path)
    npi_scope, inferred_taxonomy = await _compiler_inputs(
        tmp_path,
        artifacts,
    )
    output = tmp_path / "compiled-v1"
    await compile_provider_graph_v4_rust(
        graph_artifact_entries=artifacts,
        provider_set_key_map_path=provider_map,
        npi_scope=npi_scope,
        inferred_taxonomy=inferred_taxonomy,
        output_directory=output,
        binary_path=_binary(),
    )
    checkpoint_path = output / compiler.PTG2_V4_GRAPH_CHECKPOINT_NAME
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    checkpoint["format"] = "ptg2_provider_graph_v4_checkpoint_v1"
    checkpoint_path.write_text(
        json.dumps(checkpoint, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    rebuilt = await compile_provider_graph_v4_rust(
        graph_artifact_entries=artifacts,
        provider_set_key_map_path=provider_map,
        npi_scope=npi_scope,
        inferred_taxonomy=inferred_taxonomy,
        output_directory=output,
        binary_path=_binary(),
    )

    assert rebuilt.checkpoint_reused is False
    assert (
        json.loads(checkpoint_path.read_text(encoding="utf-8"))["format"]
        == compiler.PTG2_V4_GRAPH_CHECKPOINT_FORMAT
    )


@pytest.mark.asyncio
async def test_python_scratch_rejects_dangling_and_unowned_paths(
    tmp_path: Path,
) -> None:
    artifacts, provider_map = _fixture(tmp_path)
    dangling_scope = tmp_path / "dangling-scope.copy"
    dangling_scope.symlink_to(tmp_path / "missing-scope.copy")

    with pytest.raises(RuntimeError, match="symbolic link"):
        await compiler.prepare_provider_graph_v4_npi_scope_rust(
            graph_artifact_entries=artifacts,
            output_path=dangling_scope,
            binary_path=_binary(),
        )
    assert dangling_scope.is_symlink()

    npi_scope, inferred_taxonomy = await _compiler_inputs(
        tmp_path,
        artifacts,
    )
    unowned_output = tmp_path / "unowned-output"
    unowned_output.mkdir()
    sentinel = unowned_output / "preserve.txt"
    sentinel.write_text("preserve", encoding="ascii")
    with pytest.raises(RuntimeError, match="scratch owner"):
        await compile_provider_graph_v4_rust(
            graph_artifact_entries=artifacts,
            provider_set_key_map_path=provider_map,
            npi_scope=npi_scope,
            inferred_taxonomy=inferred_taxonomy,
            output_directory=unowned_output,
            binary_path=_binary(),
        )
    assert sentinel.read_text(encoding="ascii") == "preserve"


@pytest.mark.asyncio
async def test_wrapper_rejects_component_fallback_summary_tampering(
    tmp_path: Path,
) -> None:
    """Reject sealed limits, second-hop work, and layout decision drift."""

    artifacts, provider_map = _fixture(tmp_path)
    npi_scope, inferred_taxonomy = await _compiler_inputs(tmp_path, artifacts)
    output = tmp_path / "compiled"
    compilation = await compile_provider_graph_v4_rust(
        graph_artifact_entries=artifacts,
        provider_set_key_map_path=provider_map,
        npi_scope=npi_scope,
        inferred_taxonomy=inferred_taxonomy,
        output_directory=output,
        binary_path=_binary(),
    )
    summary_by_field = deepcopy(compilation.summary)
    expected_options = compiler._effective_compiler_options(None)
    validation_arguments_by_name = {
        "output_directory": output,
        "expected_input_bytes": int(summary_by_field["input_byte_count"]),
        "expected_factor_edges": int(
            summary_by_field["resource_admission"]["factor_edge_count"]
        ),
        "expected_factor_owners": int(
            summary_by_field["resource_admission"]["factor_owner_count"]
        ),
        "expected_options": expected_options,
        "allow_checkpoint": True,
    }

    for tampered_summary, expected_message in _tampered_summary_cases(
        summary_by_field,
        expected_options,
    ):
        _assert_summary_rejected(
            tampered_summary,
            validation_arguments_by_name,
            expected_message,
        )


@pytest.mark.asyncio
async def test_wrapper_surfaces_typed_resource_admission_failure(
    tmp_path: Path,
) -> None:
    artifacts, provider_map = _fixture(tmp_path)
    npi_scope, inferred_taxonomy = await _compiler_inputs(tmp_path, artifacts)
    output = tmp_path / "compiled"

    with pytest.raises(V4GraphResourceAdmissionError, match="factor edge count"):
        await compile_provider_graph_v4_rust(
            graph_artifact_entries=artifacts,
            provider_set_key_map_path=provider_map,
            npi_scope=npi_scope,
            inferred_taxonomy=inferred_taxonomy,
            output_directory=output,
            binary_path=_binary(),
            options={"max_factor_edges": 1},
        )
    assert not output.exists()

    with pytest.raises(RuntimeError, match="unknown option"):
        await compile_provider_graph_v4_rust(
            graph_artifact_entries=artifacts,
            provider_set_key_map_path=provider_map,
            npi_scope=npi_scope,
            inferred_taxonomy=inferred_taxonomy,
            output_directory=output,
            binary_path=_binary(),
            options={"max_npi_prefix_override_owner": 250_001},
        )


@pytest.mark.asyncio
async def test_scope_prepass_rebuilds_deleted_scanner_scope(
    tmp_path: Path,
) -> None:
    artifacts, _provider_map = _fixture(tmp_path)
    original_scope = next(
        Path(str(artifact["path"]))
        for artifact in artifacts
        if artifact["name"] == "provider_npi_scope"
    )
    first = await compiler.prepare_provider_graph_v4_npi_scope_rust(
        graph_artifact_entries=artifacts,
        output_path=tmp_path / "first-scope.copy",
        binary_path=_binary(),
    )
    expected_digest = first.manifest["output_sha256"]
    first.cleanup()
    original_scope.unlink()

    rebuilt = await compiler.prepare_provider_graph_v4_npi_scope_rust(
        graph_artifact_entries=artifacts,
        output_path=tmp_path / "rebuilt-scope.copy",
        binary_path=_binary(),
    )

    assert rebuilt.manifest["output_sha256"] == expected_digest
    assert rebuilt.copy_path.is_file()
    assert rebuilt.source_scope_directory.is_dir()
    rebuilt.cleanup()
    assert not rebuilt.copy_path.exists()
    assert not rebuilt.source_scope_directory.exists()


@pytest.mark.asyncio
async def test_scope_prepass_refuses_reciprocal_symlink(
    tmp_path: Path,
) -> None:
    artifacts, _provider_map = _fixture(tmp_path)
    reciprocal_entry = next(
        artifact for artifact in artifacts if artifact["name"] == "provider_npi_group"
    )
    original_path = Path(str(reciprocal_entry["path"]))
    linked_path = tmp_path / "linked-reciprocal.bin"
    linked_path.symlink_to(original_path)
    linked_artifacts = tuple(
        (
            {**artifact, "path": str(linked_path)}
            if artifact is reciprocal_entry
            else artifact
        )
        for artifact in artifacts
    )

    with pytest.raises(RuntimeError, match="not a regular file"):
        await compiler.prepare_provider_graph_v4_npi_scope_rust(
            graph_artifact_entries=linked_artifacts,
            output_path=tmp_path / "linked-scope.copy",
            binary_path=_binary(),
        )


@pytest.mark.asyncio
async def test_scope_prepass_refuses_reciprocal_digest_drift(
    tmp_path: Path,
) -> None:
    artifacts, _provider_map = _fixture(tmp_path)
    reciprocal_path = next(
        Path(str(artifact["path"]))
        for artifact in artifacts
        if artifact["name"] == "provider_npi_group"
    )
    changed_bytes = bytearray(reciprocal_path.read_bytes())
    changed_bytes[-1] ^= 1
    reciprocal_path.write_bytes(changed_bytes)

    with pytest.raises(RuntimeError, match="changed before extraction"):
        await compiler.prepare_provider_graph_v4_npi_scope_rust(
            graph_artifact_entries=artifacts,
            output_path=tmp_path / "changed-scope.copy",
            binary_path=_binary(),
        )
