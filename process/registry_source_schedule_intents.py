# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure, bounded refresh-check intentions; never register or execute jobs."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone

MAX_SOURCE_SCOPES = 100
_SUPPORTED_SCOPES = frozenset({("cms", "commercial-mlr"), ("cms", "plan-finder"), ("naic", "company-directory")})
_POLICIES = frozenset({"latest-verified", "fixed-year"})
_KEY_DOMAIN = b"registry_source_refresh_window:v1:"


class RegistrySourceScheduleError(ValueError):
    """Reject the whole batch before returning any scheduler intentions."""

    def __init__(self):
        super().__init__("Registry source refresh scopes or clock are invalid")


def _canonical_text(value, limit):
    return type(value) is str and value == value.strip() and 0 < len(value) <= limit and value.isprintable()


def _aware_utc(value):
    try:
        if type(value) is not datetime or value.utcoffset() is None:
            raise RegistrySourceScheduleError()
        return value.astimezone(timezone.utc)
    except Exception:
        raise RegistrySourceScheduleError() from None


@dataclass(frozen=True)
class RegistrySourceRefreshScope:
    source_system: str
    source_id: str
    edition_policy: str
    reporting_year: int | None = None
    available: bool = True
    last_checked_at: datetime | None = None

    def __post_init__(self):
        if not _canonical_text(self.source_system, 64) or not _canonical_text(self.source_id, 128):
            raise RegistrySourceScheduleError()
        if type(self.edition_policy) is not str or self.edition_policy not in _POLICIES:
            raise RegistrySourceScheduleError()
        if self.reporting_year is not None and (
            type(self.reporting_year) is not int or not 1900 <= self.reporting_year <= 9999
        ):
            raise RegistrySourceScheduleError()
        if self.edition_policy == "fixed-year" and self.reporting_year is None:
            raise RegistrySourceScheduleError()
        if type(self.available) is not bool:
            raise RegistrySourceScheduleError()
        if self.last_checked_at is not None:
            object.__setattr__(self, "last_checked_at", _aware_utc(self.last_checked_at))


@dataclass(frozen=True)
class RegistrySourceScheduleIntent:
    source_system: str
    source_id: str
    edition_policy: str
    reporting_year: int | None
    due_at: datetime
    dedup_key: str
    importer: str = "registry-source-check"


@dataclass(frozen=True)
class RegistrySourceScheduleSkip:
    source_system: str
    source_id: str
    edition_policy: str
    reporting_year: int | None
    reason: str
    due_at: datetime | None


@dataclass(frozen=True)
class RegistrySourceScheduleBatch:
    intents: tuple[RegistrySourceScheduleIntent, ...]
    skipped: tuple[RegistrySourceScheduleSkip, ...]


def _identity(scope):
    return scope.source_system, scope.source_id, scope.edition_policy, scope.reporting_year


def _validated_scopes(scopes):
    if type(scopes) not in {list, tuple} or len(scopes) > MAX_SOURCE_SCOPES:
        raise RegistrySourceScheduleError()
    scopes_by_identity = {}
    for scope in tuple(scopes):
        if type(scope) is not RegistrySourceRefreshScope:
            raise RegistrySourceScheduleError()
        scope = replace(scope)
        identity = _identity(scope)
        previous = scopes_by_identity.get(identity)
        if previous is not None and previous != scope:
            raise RegistrySourceScheduleError()
        scopes_by_identity[identity] = scope
    return sorted(
        scopes_by_identity.values(),
        key=lambda scope: (*_identity(scope)[:3], scope.reporting_year if scope.reporting_year is not None else -1),
    )


def _due_window(scope, now_utc):
    due_at = now_utc.replace(hour=3, minute=0, second=0, microsecond=0)
    try:
        if (scope.source_system, scope.source_id) == ("cms", "plan-finder"):
            due_at -= timedelta(days=now_utc.weekday())
            return due_at if due_at <= now_utc else due_at - timedelta(days=7)
        due_at = due_at.replace(day=1)
        return due_at if due_at <= now_utc else (due_at - timedelta(days=1)).replace(day=1)
    except OverflowError:
        return None


def _dedup_key(scope, due_at):
    coordinates = (*_identity(scope), due_at.isoformat())
    encoded = json.dumps(coordinates, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(_KEY_DOMAIN + encoded).hexdigest()


def _skip_reason(scope, due_at):
    if (scope.source_system, scope.source_id) not in _SUPPORTED_SCOPES:
        return "unsupported"
    if not scope.available:
        return "unavailable"
    if due_at is None:
        return "not_due"
    if scope.last_checked_at is not None and scope.last_checked_at >= due_at:
        return "already_checked"
    return None


def build_registry_source_schedule_intents(
    scopes: list[RegistrySourceRefreshScope] | tuple[RegistrySourceRefreshScope, ...],
    now_utc: datetime,
) -> RegistrySourceScheduleBatch:
    """Return at most one latest elapsed window per explicit source/policy/year.

    Plan Finder windows are Mondays at 03:00 UTC; MLR and NAIC windows are the
    first of each month at 03:00 UTC. Aware clocks normalize to UTC. Identical
    inputs deduplicate; conflicting observations of one scope reject the batch.
    Publication dates and edition contents are never inferred or inspected.
    """
    normalized_now = _aware_utc(now_utc)
    validated_scopes = _validated_scopes(scopes)
    intents = []
    skips = []
    for scope in validated_scopes:
        due_at = (
            _due_window(scope, normalized_now) if (scope.source_system, scope.source_id) in _SUPPORTED_SCOPES else None
        )
        reason = _skip_reason(scope, due_at)
        if reason is not None:
            skips.append(RegistrySourceScheduleSkip(*_identity(scope), reason, due_at))
        else:
            intents.append(RegistrySourceScheduleIntent(*_identity(scope), due_at, _dedup_key(scope, due_at)))
    return RegistrySourceScheduleBatch(tuple(intents), tuple(skips))
