"""Guarded adoption of existing Provider Directory acquisition runs."""

from __future__ import annotations

import datetime as dt
from typing import Any, Callable

try:
    from scripts.research.provider_directory_endpoint_acquisition_restart import (
        ACTIVE_STATUSES,
        HarnessConflict,
    )
except ModuleNotFoundError:
    from provider_directory_endpoint_acquisition_restart import ACTIVE_STATUSES, HarnessConflict


RunParameterValidator = Callable[[dict[str, Any], dict[str, Any]], list[str]]
RunRememberer = Callable[[dict[str, Any], dict[str, Any]], None]


def parse_adopt_runs(raw_values: list[str], entry_pattern: Any) -> tuple[tuple[str, str], ...]:
    """Parse unique ENTRY=RUN_ID adoption declarations."""
    adopted_by_entry: dict[str, str] = {}
    for raw_value in raw_values:
        entry_id, separator, run_id = raw_value.partition("=")
        if not separator or not entry_pattern.fullmatch(entry_id) or not run_id.startswith("run_"):
            raise SystemExit("--adopt-run must use ENTRY=RUN_ID")
        if entry_id in adopted_by_entry:
            raise SystemExit(f"duplicate --adopt-run entry: {entry_id}")
        adopted_by_entry[entry_id] = run_id
    return tuple(sorted(adopted_by_entry.items()))


class AdoptionManager:
    """Validate and attach explicit existing run lineage tips."""

    def __init__(
        self,
        manifest: dict[str, Any],
        state: dict[str, Any],
        client: Any,
        validate_run_parameters: RunParameterValidator,
        remember_run: RunRememberer,
    ):
        self.manifest = manifest
        self.state = state
        self.client = client
        self.validate_run_parameters = validate_run_parameters
        self.remember_run = remember_run

    def apply(
        self,
        adopt_run_ids: tuple[tuple[str, str], ...],
        restart_entry_ids: frozenset[str],
        is_apply: bool,
    ) -> None:
        """Adopt every requested run after fail-closed lineage checks."""
        if not adopt_run_ids:
            return
        if not is_apply:
            raise HarnessConflict("--adopt-run requires --apply")
        entries_by_id = {entry["entry_id"]: entry for entry in self.manifest["entries"]}
        for entry_id, run_id in adopt_run_ids:
            if entry_id in restart_entry_ids:
                raise HarnessConflict(f"{entry_id}: cannot adopt and restart the same entry")
            self._adopt_run(entries_by_id[entry_id], self.state["entries"][entry_id], run_id)

    def _adopt_run(
        self,
        entry: dict[str, Any],
        entry_state: dict[str, Any],
        run_id: str,
    ) -> None:
        """Attach one exact lineage tip after all adoption gates pass."""
        known_run_id = str(entry_state.get("current_run_id") or "")
        if known_run_id:
            if known_run_id == run_id:
                return
            raise HarnessConflict(f"{entry['entry_id']}: state already follows {known_run_id}")
        run_record, root_run_id, run_list = self._validated_lineage(entry, run_id)
        self._reject_non_tip_or_active_conflict(entry, run_id, root_run_id, run_list)
        self._record_adoption(entry_state, run_record, run_id, root_run_id)

    def _validated_lineage(
        self,
        entry: dict[str, Any],
        run_id: str,
    ) -> tuple[dict[str, Any], str, list[dict[str, Any]]]:
        run_record = self.client.get_run(run_id)
        parameter_errors = self.validate_run_parameters(entry, run_record)
        if parameter_errors:
            raise HarnessConflict(
                f"{entry['entry_id']}: adopted run does not match: " + "; ".join(parameter_errors)
            )
        parameters = run_record.get("params") if isinstance(run_record.get("params"), dict) else {}
        retry_of_run_id = str(parameters.get("retry_of_run_id") or "")
        root_run_id = str(parameters.get("provider_directory_pagination_root_run_id") or "")
        if retry_of_run_id and not root_run_id:
            raise HarnessConflict(f"{entry['entry_id']}: retry run lacks provider-directory root lineage")
        root_run_id = root_run_id or run_id
        root_run = run_record if root_run_id == run_id else self.client.get_run(root_run_id)
        root_errors = self.validate_run_parameters(entry, root_run)
        if root_errors:
            raise HarnessConflict(
                f"{entry['entry_id']}: adopted root does not match: " + "; ".join(root_errors)
            )
        return run_record, root_run_id, self.client.list_runs()

    def _reject_non_tip_or_active_conflict(
        self,
        entry: dict[str, Any],
        run_id: str,
        root_run_id: str,
        run_list: list[dict[str, Any]],
    ) -> None:
        child_runs = [
            candidate
            for candidate in run_list
            if (candidate.get("params") or {}).get("retry_of_run_id") == run_id
        ]
        if child_runs:
            child_ids = ",".join(str(candidate.get("run_id")) for candidate in child_runs)
            raise HarnessConflict(f"{entry['entry_id']}: adopted run is not the lineage tip: {child_ids}")
        unrelated_active_ids = [
            str(candidate.get("run_id"))
            for candidate in run_list
            if candidate.get("status") in ACTIVE_STATUSES
            and not self._is_run_in_root(candidate, root_run_id)
        ]
        if unrelated_active_ids:
            raise HarnessConflict(
                f"{entry['entry_id']}: active Provider Directory conflict: "
                + ",".join(unrelated_active_ids)
            )

    def _record_adoption(
        self,
        entry_state: dict[str, Any],
        run_record: dict[str, Any],
        run_id: str,
        root_run_id: str,
    ) -> None:
        run_ids = entry_state.setdefault("run_ids", [])
        if root_run_id not in run_ids:
            run_ids.append(root_run_id)
        entry_state.update(
            {
                "action": "adopt",
                "adopted_at": dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
                "adopted_run_id": run_id,
                "root_run_id": root_run_id,
            }
        )
        self.remember_run(entry_state, run_record)

    @staticmethod
    def _is_run_in_root(run_record: dict[str, Any], root_run_id: str) -> bool:
        run_id = str(run_record.get("run_id") or "")
        parameters = run_record.get("params") if isinstance(run_record.get("params"), dict) else {}
        return run_id == root_run_id or parameters.get("provider_directory_pagination_root_run_id") == root_run_id
