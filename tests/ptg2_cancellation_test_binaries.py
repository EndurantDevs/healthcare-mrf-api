from __future__ import annotations

import textwrap
from pathlib import Path


def _sigterm_ignoring_child_source(pid_environment_name: str) -> str:
    return textwrap.dedent(
        f"""
        import os
        import signal
        import time
        from pathlib import Path

        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        Path(os.environ[{pid_environment_name!r}]).write_text(str(os.getpid()))
        while True:
            time.sleep(1)
        """
    )


_BLOCKING_SCANNER_SOURCE = f"""\
#!/usr/bin/env python3
import os
import subprocess
import sys
import time
from pathlib import Path

subprocess.Popen(
    [sys.executable, "-c", {_sigterm_ignoring_child_source("PTG_SCANNER_CHILD_PID")!r}]
)
Path(os.environ["PTG_SCANNER_PARENT_PID"]).write_text(str(os.getpid()))
while True:
    time.sleep(1)
"""


_RETRYING_SCANNER_SOURCE = f"""\
#!/usr/bin/env python3
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

attempts_path = Path(os.environ["PTG_SCANNER_ATTEMPTS"])
attempt = int(attempts_path.read_text()) + 1 if attempts_path.exists() else 1
attempts_path.write_text(str(attempt))
output_path = Path(os.environ["PTG_SCANNER_OUTPUT"])

if attempt == 1:
    def hold_sigterm(_signal_number, _frame):
        Path(os.environ["PTG_SCANNER_TERM_SEEN"]).write_text("term")

    signal.signal(signal.SIGTERM, hold_sigterm)
    subprocess.Popen(
        [sys.executable, "-c", {_sigterm_ignoring_child_source("PTG_SCANNER_CHILD_PID")!r}]
    )
    Path(os.environ["PTG_SCANNER_PARENT_PID"]).write_text(str(os.getpid()))
    while True:
        output_path.write_text("first-active")
        time.sleep(0.01)

def emit(kind, payload):
    encoded = json.dumps(payload, separators=(",", ":")).encode("ascii")
    sys.stdout.buffer.write(
        kind.encode("ascii")
        + b"\\t"
        + str(len(encoded)).encode("ascii")
        + b"\\n"
        + encoded
        + b"\\n"
    )
    sys.stdout.buffer.flush()

output_path.write_text("retry-complete")
emit(
    "scanner_config",
    {{
        "snapshot_arch": "postgres_binary_v3",
        "storage_generation": "shared_blocks_v3",
        "serving_row_semantics": "source_multiset_v1",
        "serving_run_format": "ptg2_v3_serving_run",
        "serving_run_version": 1,
    }},
)
emit("scanner_summary", {{"attempt": attempt}})
"""


_RETRYING_CONVERTER_SOURCE = f"""\
#!/usr/bin/env python3
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

attempts_path = Path(os.environ["PTG_CONVERTER_ATTEMPTS"])
attempt = int(attempts_path.read_text()) + 1 if attempts_path.exists() else 1
attempts_path.write_text(str(attempt))
if attempt == 1:
    def hold_sigterm(_signal_number, _frame):
        Path(os.environ["PTG_CONVERTER_TERM_SEEN"]).write_text("term")

    signal.signal(signal.SIGTERM, hold_sigterm)
    subprocess.Popen(
        [sys.executable, "-c", {_sigterm_ignoring_child_source("PTG_CONVERTER_CHILD_PID")!r}]
    )
    Path(os.environ["PTG_CONVERTER_PARENT_PID"]).write_text(str(os.getpid()))
    while True:
        time.sleep(1)
sys.stdout.buffer.write(b"converted")
sys.stdout.buffer.flush()
"""


_RETRYING_FINALIZER_SOURCE = f"""\
#!/usr/bin/env python3
import json
import os
import subprocess
import sys
import time
from pathlib import Path

arguments = sys.argv[1:]
output = Path(arguments[arguments.index("--finalize-v3-runs") + 1])
attempts_path = Path(os.environ["PTG_FINALIZER_ATTEMPTS"])
attempt = int(attempts_path.read_text()) + 1 if attempts_path.exists() else 1
attempts_path.write_text(str(attempt))
output.mkdir()
(output / "committed.marker").write_text(str(attempt))
if attempt == 1:
    staging = output.parent / (
        f".{{output.name}}.ptg2-finalizer-{{os.getpid()}}-0000.tmp"
    )
    staging.mkdir()
    subprocess.Popen(
        [sys.executable, "-c", {_sigterm_ignoring_child_source("PTG_FINALIZER_CHILD_PID")!r}]
    )
    Path(os.environ["PTG_FINALIZER_PARENT_PID"]).write_text(str(os.getpid()))
    while True:
        time.sleep(1)

def argument_value(name):
    return int(arguments[arguments.index(name) + 1])

resources = {{
    "contract": "ptg2_v3_finalizer_resources_v1",
    "workers": argument_value("--workers"),
    "identity_map_max_bytes": argument_value("--identity-map-max-bytes"),
    "total_sort_memory_bytes": argument_value("--total-sort-memory-bytes"),
    "sort_memory_scope": "process_total_across_workers_v1",
}}
summary = {{
    "format": "ptg2_v3_direct_finalizer_v3",
    "storage_generation": "shared_blocks_v3",
    "cold_lookup_contract": "ptg_v3_cold_v2",
    "shared_block_layout": "dense_shared_blocks_v3",
    "source_count": 1,
    "output_directory": str(output.resolve()),
    "resource_configuration": resources,
    "price_key_map": {{
        "copy_format": "postgresql_binary_copy",
        "row_count": 1,
        "dense_price_ordering": "minimum_negotiated_rate_then_global_id_128_v1",
        "keys_unique_dense_contiguous": True,
        "source_ids_exact_match": True,
    }},
    "dense_keys": {{
        "price": {{
            "count": 1,
            "ordering": "minimum_negotiated_rate_then_global_id_128_v1",
        }}
    }},
    "blocks": {{
        "serving": {{
            "copy_bytes": 1,
            "copy_sha256": "a" * 64,
            "artifact_record_counts": {{
                "by_code_provider_shard_v1": 1,
                "by_code_price_page_v4": 1,
                "provider_set_count_dictionary": 1,
                "provider_set_codes_v3": 1,
                "provider_set_page_v3_s2": 1,
            }}
        }},
        "price_dictionary": {{
            "copy_bytes": 1,
            "copy_sha256": "b" * 64,
            "artifact_record_counts": {{"by_code_price_dictionary": 1}}
        }},
    }},
}}
encoded = json.dumps(summary, separators=(",", ":")).encode("ascii")
sys.stdout.buffer.write(
    b"v3_finalizer_summary\\t"
    + str(len(encoded)).encode("ascii")
    + b"\\n"
    + encoded
)
sys.stdout.buffer.flush()
"""


def _write_executable(path: Path, source: str) -> Path:
    path.write_text(textwrap.dedent(source), encoding="utf-8")
    path.chmod(0o755)
    return path


def write_blocking_scanner_binary(tmp_path: Path) -> Path:
    return _write_executable(tmp_path / "blocking-scanner", _BLOCKING_SCANNER_SOURCE)


def write_retrying_scanner_binary(tmp_path: Path) -> Path:
    return _write_executable(tmp_path / "retrying-scanner", _RETRYING_SCANNER_SOURCE)


def write_retrying_converter_binary(tmp_path: Path) -> Path:
    return _write_executable(
        tmp_path / "retrying-converter", _RETRYING_CONVERTER_SOURCE
    )


def write_retrying_finalizer_binary(tmp_path: Path) -> Path:
    return _write_executable(tmp_path / "test-finalizer", _RETRYING_FINALIZER_SOURCE)
