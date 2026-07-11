# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Report and clean unreferenced PTG2 sidecar artifacts.

The database manifest is the source of truth for retained sidecar files. This
module only targets files under the configured PTG2 artifact sidecar directory;
raw payer downloads are intentionally out of scope.
"""

from __future__ import annotations

import argparse
import asyncio
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_artifact_blobs import ptg2_artifact_id_from_db_uri


def _row_value(row: Any, key: str, default: Any = None) -> Any:
    return row.get(key, default) if isinstance(row, dict) else getattr(row, key, default)


def _format_bytes(size: int) -> str:
    units = ("B", "KB", "MB", "GB", "TB")
    value = float(size)
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            return f"{value:.1f}{unit}" if unit != "B" else f"{int(value)}B"
        value /= 1024.0
    return f"{size}B"


@dataclass(frozen=True)
class PTG2ArtifactCleanupPlan:
    root: Path
    referenced_files: tuple[Path, ...]
    unreferenced_files: tuple[Path, ...]
    missing_referenced_files: tuple[Path, ...]

    @property
    def referenced_bytes(self) -> int:
        return sum(path.stat().st_size for path in self.referenced_files if path.exists())

    @property
    def unreferenced_bytes(self) -> int:
        return sum(path.stat().st_size for path in self.unreferenced_files if path.exists())

    @property
    def has_actions(self) -> bool:
        return bool(self.unreferenced_files)


def _resolve_existing_root(root: str | Path | None = None) -> Path:
    if root:
        return Path(root).expanduser().resolve()
    artifact_root = os.getenv("HLTHPRT_PTG2_ARTIFACT_DIR")
    if artifact_root:
        base = Path(artifact_root).expanduser()
    else:
        base = Path(os.getenv("TMPDIR") or "/tmp") / "healthporta_ptg2"
    sidecar_root = base / "serving"
    return sidecar_root.resolve()


def _resolve_referenced_path(value: str | Path, sidecar_root: Path) -> Path:
    path = Path(value).expanduser()
    if path.exists():
        return path.resolve()
    if not path.is_absolute():
        candidate = sidecar_root / path
        return candidate.resolve() if candidate.exists() else path.resolve()

    marker = "healthporta-ptg2-artifacts"
    try:
        marker_index = path.parts.index(marker)
    except ValueError:
        return path.resolve()
    suffix_parts = path.parts[marker_index + 1 :]
    if not suffix_parts:
        return path.resolve()
    artifact_root = sidecar_root.parent
    candidate = artifact_root.joinpath(*suffix_parts)
    if candidate.exists():
        return candidate.resolve()

    if len(suffix_parts) >= 3 and suffix_parts[0] == "serving":
        filename = suffix_parts[-1]
        matches = [item for item in sidecar_root.glob(f"*/{filename}") if item.exists()]
        if len(matches) == 1:
            return matches[0].resolve()
        prefix = filename.rsplit("_", 1)[0]
        if prefix and prefix != filename:
            prefix_matches = [
                item for item in sidecar_root.glob(f"*/{prefix}_*.ptg2sc") if item.exists()
            ]
            if len(prefix_matches) == 1:
                return prefix_matches[0].resolve()
    return path.resolve()


def _normalize_referenced_paths(paths: Iterable[str | Path], *, sidecar_root: Path) -> set[Path]:
    normalized: set[Path] = set()
    for value in paths:
        if not value:
            continue
        normalized.add(_resolve_referenced_path(value, sidecar_root))
    return normalized


def build_ptg2_artifact_cleanup_plan(
    *,
    root: str | Path | None = None,
    referenced_paths: Iterable[str | Path] = (),
) -> PTG2ArtifactCleanupPlan:
    sidecar_root = _resolve_existing_root(root)
    referenced = _normalize_referenced_paths(referenced_paths, sidecar_root=sidecar_root)
    if not sidecar_root.exists():
        return PTG2ArtifactCleanupPlan(
            root=sidecar_root,
            referenced_files=tuple(),
            unreferenced_files=tuple(),
            missing_referenced_files=tuple(sorted(referenced)),
        )
    all_files = tuple(sorted(path.resolve() for path in sidecar_root.rglob("*") if path.is_file()))
    root_files = set(all_files)
    referenced_in_root = tuple(sorted(path for path in referenced if path in root_files))
    missing_referenced = tuple(sorted(path for path in referenced if path not in root_files))
    unreferenced = tuple(sorted(path for path in all_files if path not in referenced))
    return PTG2ArtifactCleanupPlan(
        root=sidecar_root,
        referenced_files=referenced_in_root,
        unreferenced_files=unreferenced,
        missing_referenced_files=missing_referenced,
    )


async def fetch_referenced_ptg2_sidecar_paths(*, schema_name: str | None = None) -> tuple[str, ...]:
    schema_name = schema_name or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    rows = await db.all(
        f"""
        SELECT sidecar->>'path' AS sidecar_path,
               sidecar->>'storage' AS storage,
               sidecar->>'storage_uri' AS storage_uri
          FROM {_quote_ident(schema_name)}.ptg2_snapshot snapshot
          CROSS JOIN LATERAL jsonb_array_elements(
              COALESCE(
                  snapshot.manifest::jsonb->'serving_index'->'artifacts'->'sidecars',
                  '[]'::jsonb
              )
          ) AS sidecar
         WHERE sidecar ? 'path'
         ORDER BY sidecar->>'path'
        """
    )
    return tuple(
        str(_row_value(row, "sidecar_path"))
        for row in rows
        if _row_value(row, "sidecar_path")
        and str(_row_value(row, "storage") or "") != "postgresql_chunks_v1"
        and not ptg2_artifact_id_from_db_uri(str(_row_value(row, "storage_uri") or ""))
    )


def execute_ptg2_artifact_cleanup_plan(plan: PTG2ArtifactCleanupPlan) -> None:
    for path in plan.unreferenced_files:
        if path.exists() and path.is_file():
            path.unlink()
    if not plan.root.exists():
        return
    for directory in sorted((path for path in plan.root.rglob("*") if path.is_dir()), reverse=True):
        try:
            directory.rmdir()
        except OSError:
            continue


def _top_dir(path: Path, root: Path) -> Path:
    try:
        relative = path.relative_to(root)
    except ValueError:
        return path.parent
    return root / relative.parts[0] if relative.parts else root


def _print_plan(plan: PTG2ArtifactCleanupPlan, *, max_dirs: int = 20) -> None:
    print(f"sidecar_root={plan.root}")
    print(f"referenced_files={len(plan.referenced_files)}")
    print(f"referenced_bytes={_format_bytes(plan.referenced_bytes)}")
    print(f"unreferenced_files={len(plan.unreferenced_files)}")
    print(f"unreferenced_bytes={_format_bytes(plan.unreferenced_bytes)}")
    print(f"missing_referenced_files={len(plan.missing_referenced_files)}")
    by_dir: dict[Path, tuple[int, int]] = {}
    for path in plan.unreferenced_files:
        directory = _top_dir(path, plan.root)
        count, size = by_dir.get(directory, (0, 0))
        by_dir[directory] = (count + 1, size + path.stat().st_size)
    for directory, (count, size) in sorted(by_dir.items(), key=lambda item: item[1][1], reverse=True)[:max_dirs]:
        print(f"  unreferenced_dir={directory} files={count} bytes={_format_bytes(size)}")
    for path in plan.missing_referenced_files[:max_dirs]:
        print(f"  missing_referenced={path}")


async def _amain() -> None:
    parser = argparse.ArgumentParser(description="Report or clean unreferenced PTG2 sidecar artifacts.")
    parser.add_argument("--schema", default=os.getenv("HLTHPRT_DB_SCHEMA") or "mrf")
    parser.add_argument("--root", default=None, help="Sidecar root to scan. Defaults to HLTHPRT_PTG2_ARTIFACT_DIR/manifest.")
    parser.add_argument("--execute", action="store_true", help="Delete unreferenced files. Default is dry-run.")
    parser.add_argument("--max-dirs", type=int, default=20, help="Maximum unreferenced directories to print.")
    args = parser.parse_args()
    referenced_paths = await fetch_referenced_ptg2_sidecar_paths(schema_name=args.schema)
    plan = build_ptg2_artifact_cleanup_plan(root=args.root, referenced_paths=referenced_paths)
    _print_plan(plan, max_dirs=args.max_dirs)
    if args.execute:
        execute_ptg2_artifact_cleanup_plan(plan)
        print("cleanup_executed=true")
    else:
        print("cleanup_executed=false")


if __name__ == "__main__":
    asyncio.run(_amain())
