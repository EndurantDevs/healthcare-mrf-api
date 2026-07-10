"""Shared fixtures for Provider Directory endpoint acquisition harness tests."""


def manifest_with_attached_entry(harness_module, entry_id: str, run_id: str):
    """Return a manifest copy with one create entry converted to attach mode."""

    manifest = harness_module.load_manifest()
    entry = next(item for item in manifest["entries"] if item["entry_id"] == entry_id)
    entry.update(launch_mode="attach", attached_run_id=run_id)
    return manifest, entry
