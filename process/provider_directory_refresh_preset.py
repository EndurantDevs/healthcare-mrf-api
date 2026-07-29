"""Canonical Provider Directory refresh-preset expansion."""

from __future__ import annotations

from typing import Any, Mapping


PROVIDER_DIRECTORY_REFRESH_PRESET_MONTHLY_FULL = "monthly-full"
PROVIDER_DIRECTORY_REFRESH_PRESETS = (
    PROVIDER_DIRECTORY_REFRESH_PRESET_MONTHLY_FULL,
)
PROVIDER_DIRECTORY_MONTHLY_FULL_DEFAULTS: Mapping[str, object] = {
    "import_resources": True,
    "full_refresh": True,
    "stale_cleanup": True,
    "publish_artifacts": True,
    "publish_corroboration": True,
    "open_only": False,
    "include_auth_required": True,
    "bulk_export": True,
    "include_supplemental_catalogs": True,
}


def apply_provider_directory_refresh_preset(
    task_by_field: dict[str, Any],
) -> dict[str, Any]:
    """Expand one recognized preset into its effective task parameters."""

    preset_value = (
        task_by_field.get("refresh_preset")
        or task_by_field.get("preset")
    )
    if preset_value is None:
        return task_by_field
    preset = str(preset_value).strip()
    if not preset:
        return task_by_field
    normalized_preset = preset.lower().replace("_", "-")
    if normalized_preset not in PROVIDER_DIRECTORY_REFRESH_PRESETS:
        raise ValueError(
            "Unsupported Provider Directory refresh_preset "
            f"{preset!r}; expected one of "
            f"{', '.join(PROVIDER_DIRECTORY_REFRESH_PRESETS)}"
        )
    normalized_task_by_field = dict(task_by_field)
    normalized_task_by_field["refresh_preset"] = normalized_preset
    for field_name, default_value in (
        PROVIDER_DIRECTORY_MONTHLY_FULL_DEFAULTS.items()
    ):
        if normalized_task_by_field.get(field_name) in (None, ""):
            normalized_task_by_field[field_name] = default_value
    return normalized_task_by_field


__all__ = [
    "PROVIDER_DIRECTORY_MONTHLY_FULL_DEFAULTS",
    "PROVIDER_DIRECTORY_REFRESH_PRESET_MONTHLY_FULL",
    "PROVIDER_DIRECTORY_REFRESH_PRESETS",
    "apply_provider_directory_refresh_preset",
]
