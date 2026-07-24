from pathlib import Path

import pytest

from process.ptg_parts.ptg2_v4_graph_compiler import compile_provider_graph_v4_rust
from tests.test_ptg2_v4_graph_compiler import _binary, _fixture


def _summary_bridge_binary(
    tmp_path: Path,
    *,
    padding_bytes: int,
    disagree: bool = False,
    remove_summary: bool = False,
) -> Path:
    executable = tmp_path / "summary-bridge-compiler"
    executable.write_text(
        "\n".join(
            (
                "#!/usr/bin/env python3",
                "import json",
                "import os",
                "import subprocess",
                "import sys",
                f"BINARY = {str(_binary())!r}",
                f"PADDING_BYTES = {padding_bytes}",
                f"DISAGREE = {disagree!r}",
                f"REMOVE_SUMMARY = {remove_summary!r}",
                "completed = subprocess.run(",
                "    [BINARY, sys.argv[1]],",
                "    stdout=subprocess.PIPE,",
                "    stderr=subprocess.PIPE,",
                "    check=False,",
                ")",
                "sys.stderr.buffer.write(completed.stderr)",
                "if completed.returncode:",
                "    raise SystemExit(completed.returncode)",
                "summary = json.loads(completed.stdout)",
                "summary['transport_padding'] = 'x' * PADDING_BYTES",
                "with open(summary['summary_path'], 'w', encoding='utf-8') as output:",
                "    json.dump(summary, output, indent=2)",
                "if DISAGREE:",
                "    summary['transport_padding'] += 'y'",
                "if REMOVE_SUMMARY:",
                "    os.unlink(summary['summary_path'])",
                "json.dump(summary, sys.stdout, separators=(',', ':'))",
                "sys.stdout.write('\\n')",
                "",
            )
        ),
        encoding="utf-8",
    )
    executable.chmod(0o755)
    return executable


@pytest.mark.asyncio
async def test_wrapper_accepts_equivalent_summary_larger_than_legacy_envelope(
    tmp_path: Path,
) -> None:
    artifacts, provider_map = _fixture(tmp_path)
    padding_bytes = 2 * 1024 * 1024 + 1
    output = tmp_path / "compiled"
    binary = _summary_bridge_binary(tmp_path, padding_bytes=padding_bytes)

    compilation = await compile_provider_graph_v4_rust(
        graph_artifact_entries=artifacts,
        provider_set_key_map_path=provider_map,
        output_directory=output,
        binary_path=binary,
    )
    assert len(compilation.summary["transport_padding"]) == padding_bytes
    reused = await compile_provider_graph_v4_rust(
        graph_artifact_entries=artifacts,
        provider_set_key_map_path=provider_map,
        output_directory=output,
        binary_path=binary,
    )
    assert reused.checkpoint_reused is True
    assert len(reused.summary["transport_padding"]) == padding_bytes


@pytest.mark.asyncio
async def test_wrapper_rejects_summary_digest_disagreement(tmp_path: Path) -> None:
    artifacts, provider_map = _fixture(tmp_path)
    output = tmp_path / "compiled"

    with pytest.raises(RuntimeError, match="stdout and summary file disagree"):
        await compile_provider_graph_v4_rust(
            graph_artifact_entries=artifacts,
            provider_set_key_map_path=provider_map,
            output_directory=output,
            binary_path=_summary_bridge_binary(
                tmp_path,
                padding_bytes=16,
                disagree=True,
            ),
        )

    assert not output.exists()


@pytest.mark.asyncio
async def test_wrapper_rejects_missing_summary_file(tmp_path: Path) -> None:
    artifacts, provider_map = _fixture(tmp_path)
    output = tmp_path / "compiled"

    with pytest.raises(RuntimeError, match="did not publish its summary"):
        await compile_provider_graph_v4_rust(
            graph_artifact_entries=artifacts,
            provider_set_key_map_path=provider_map,
            output_directory=output,
            binary_path=_summary_bridge_binary(
                tmp_path,
                padding_bytes=16,
                remove_summary=True,
            ),
        )

    assert not output.exists()
