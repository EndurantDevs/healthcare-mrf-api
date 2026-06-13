# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import zipfile
from pathlib import Path


def _ensure_safe_member(destination: Path, filename: str) -> None:
    target = (destination / filename).resolve()
    try:
        target.relative_to(destination)
    except ValueError as exc:
        raise ValueError(f"Unsafe zip member path: {filename}") from exc


def _extract_zip(zip_path: str, destination: str) -> None:
    destination_path = Path(destination).resolve()
    destination_path.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        for member in zip_ref.infolist():
            _ensure_safe_member(destination_path, member.filename)
        zip_ref.extractall(destination_path)


async def unzip(zip_path: str, destination: str, **_kwargs) -> None:
    """Extract a zip archive without blocking the event loop."""
    await asyncio.to_thread(_extract_zip, zip_path, destination)
