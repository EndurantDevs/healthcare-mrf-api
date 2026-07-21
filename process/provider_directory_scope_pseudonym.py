# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed decisions for protected Provider Directory scope lookup."""

from __future__ import annotations

import hashlib

from process.provider_directory_scope_alias import (
    LogicalScopeLookupRequest,
    _LogicalScopeLookupAlias,
    _registry_aliases_for_adapter,
    build_scope_lookup_request,
)
from process.provider_directory_scope_protection import (
    LogicalScopePseudonymError,
    _canonical_json,
    _InertScopeSnapshot,
    _MAX_SCOPE_LOOKUP_ROTATIONS,
    _validate_registry_scope_id,
    build_scope_pseudonymizer,
)


LOOKUP_DECISION_CREATE = "create"
LOOKUP_DECISION_MATCHED = "matched"
LOOKUP_DECISION_AMBIGUOUS = "ambiguous"


def _compute_decision_binding(
    *,
    status: str,
    scope_id: str | None,
    matched_alias_count: int,
    distinct_scope_count: int,
) -> str:
    return hashlib.sha256(
        _canonical_json(
            {
                "status": status,
                "scope_id": scope_id,
                "matched_alias_count": matched_alias_count,
                "distinct_scope_count": distinct_scope_count,
            }
        )
    ).hexdigest()


class LogicalScopeRegistryDecision:
    """Sealed fail-closed result of resolving exact live alias capabilities."""

    __slots__ = (
        "_decision_binding",
        "_distinct_scope_count",
        "_matched_alias_count",
        "_scope_id",
        "_status",
    )

    def __new__(cls, *args: object, **kwargs: object) -> LogicalScopeRegistryDecision:
        raise TypeError("scope registry decisions are resolver-created")

    def __setattr__(self, name: str, value: object) -> None:
        raise TypeError("scope registry decision is immutable")

    @property
    def status(self) -> str:
        """Return create, matched, or ambiguous."""

        return self._validated_snapshot()[0]

    @property
    def scope_id(self) -> str | None:
        """Return the matched random identity, otherwise none."""

        return self._validated_snapshot()[1]

    @property
    def matched_alias_count(self) -> int:
        """Return the number of exact live aliases found by the registry."""

        return self._validated_snapshot()[2]

    @property
    def distinct_scope_count(self) -> int:
        """Return the number of distinct random identities found."""

        return self._validated_snapshot()[3]

    def _validated_snapshot(self) -> tuple[str, str | None, int, int]:
        try:
            status_value = self._status
            scope_id_value = self._scope_id
            matched_count_value = self._matched_alias_count
            distinct_count_value = self._distinct_scope_count
            binding_value = self._decision_binding
        except AttributeError:
            raise LogicalScopePseudonymError(
                "scope registry decision state is invalid"
            ) from None
        status, scope_id, matched_count, distinct_count = _validated_decision_fields(
            status=status_value,
            scope_id=scope_id_value,
            matched_alias_count=matched_count_value,
            distinct_scope_count=distinct_count_value,
        )
        expected_binding = _compute_decision_binding(
            status=status,
            scope_id=scope_id,
            matched_alias_count=matched_count,
            distinct_scope_count=distinct_count,
        )
        if type(binding_value) is not str or binding_value != expected_binding:
            raise LogicalScopePseudonymError(
                "scope registry decision binding is invalid"
            )
        return status, scope_id, matched_count, distinct_count

    def validate(self) -> None:
        """Reject malformed counts, shapes, or later mutation."""

        self._validated_snapshot()

    def public_payload(self) -> dict[str, object]:
        """Serialize the fail-closed decision without any alias material."""

        status, scope_id, matched_count, distinct_count = self._validated_snapshot()
        return {
            "status": status,
            "scope_id": scope_id,
            "matched_alias_count": matched_count,
            "distinct_scope_count": distinct_count,
        }

    def __repr__(self) -> str:
        return f"LogicalScopeRegistryDecision({self.public_payload()!r})"

    def __deepcopy__(self, memo: object) -> _InertScopeSnapshot:
        return _InertScopeSnapshot("scope-registry-decision")

    def __copy__(self) -> _InertScopeSnapshot:
        return _InertScopeSnapshot("scope-registry-decision")

    def __reduce__(self) -> tuple[type[_InertScopeSnapshot], tuple[str, ...]]:
        return _InertScopeSnapshot, ("scope-registry-decision",)


def _is_valid_decision_shape(
    *,
    status: object,
    scope_id: object,
    matched_alias_count: int,
    distinct_scope_count: int,
) -> bool:
    if status == LOOKUP_DECISION_CREATE:
        return (
            scope_id is None
            and matched_alias_count == 0
            and distinct_scope_count == 0
        )
    if status == LOOKUP_DECISION_MATCHED:
        return (
            type(scope_id) is str
            and matched_alias_count >= 1
            and distinct_scope_count == 1
        )
    if status == LOOKUP_DECISION_AMBIGUOUS:
        return (
            scope_id is None
            and matched_alias_count >= 2
            and distinct_scope_count >= 2
        )
    return False


def _validated_decision_fields(
    *,
    status: object,
    scope_id: object,
    matched_alias_count: object,
    distinct_scope_count: object,
) -> tuple[str, str | None, int, int]:
    if (
        type(matched_alias_count) is not int
        or not 0 <= matched_alias_count <= _MAX_SCOPE_LOOKUP_ROTATIONS
        or type(distinct_scope_count) is not int
        or not 0 <= distinct_scope_count <= _MAX_SCOPE_LOOKUP_ROTATIONS
        or distinct_scope_count > matched_alias_count
    ):
        raise LogicalScopePseudonymError("scope registry counts are invalid")
    if type(status) is not str or status not in {
        LOOKUP_DECISION_CREATE,
        LOOKUP_DECISION_MATCHED,
        LOOKUP_DECISION_AMBIGUOUS,
    }:
        raise LogicalScopePseudonymError("scope registry decision is invalid")
    if scope_id is not None and type(scope_id) is not str:
        raise LogicalScopePseudonymError("scope registry decision is invalid")
    if not _is_valid_decision_shape(
        status=status,
        scope_id=scope_id,
        matched_alias_count=matched_alias_count,
        distinct_scope_count=distinct_scope_count,
    ):
        raise LogicalScopePseudonymError("scope registry decision is invalid")
    if scope_id is not None:
        _validate_registry_scope_id(scope_id)
    return status, scope_id, matched_alias_count, distinct_scope_count


def _build_registry_decision(
    *,
    status: str,
    scope_id: str | None,
    matched_alias_count: int,
    distinct_scope_count: int,
) -> LogicalScopeRegistryDecision:
    status, scope_id, matched_alias_count, distinct_scope_count = (
        _validated_decision_fields(
            status=status,
            scope_id=scope_id,
            matched_alias_count=matched_alias_count,
            distinct_scope_count=distinct_scope_count,
        )
    )
    decision = object.__new__(LogicalScopeRegistryDecision)
    decision_fields_by_name = {
        "_status": status,
        "_scope_id": scope_id,
        "_matched_alias_count": matched_alias_count,
        "_distinct_scope_count": distinct_scope_count,
        "_decision_binding": _compute_decision_binding(
            status=status,
            scope_id=scope_id,
            matched_alias_count=matched_alias_count,
            distinct_scope_count=distinct_scope_count,
        ),
    }
    for field_name, field_value in decision_fields_by_name.items():
        object.__setattr__(decision, field_name, field_value)
    decision.validate()
    return decision


def _snapshot_registry_results(
    scope_ids_by_alias: dict[object, str],
) -> tuple[tuple[object, str], ...]:
    result_snapshot = dict.copy(scope_ids_by_alias)
    return tuple(dict.items(result_snapshot))


def _resolve_scope_lookup_aliases_for_adapter(
    request: LogicalScopeLookupRequest,
    scope_ids_by_alias: object,
) -> LogicalScopeRegistryDecision:
    """Resolve only exact live request aliases to zero, one, or conflict."""

    aliases = _registry_aliases_for_adapter(request)
    if (
        type(scope_ids_by_alias) is not dict
        or len(scope_ids_by_alias) > len(aliases)
    ):
        raise LogicalScopePseudonymError("scope registry lookup input is invalid")
    try:
        result_items = _snapshot_registry_results(scope_ids_by_alias)
    except (MemoryError, RuntimeError):
        raise LogicalScopePseudonymError(
            "scope registry lookup input is invalid"
        ) from None
    for returned_alias, scope_id in result_items:
        if type(returned_alias) is not _LogicalScopeLookupAlias:
            raise LogicalScopePseudonymError("scope registry returned an invalid alias")
        if not any(returned_alias is alias for alias in aliases):
            raise LogicalScopePseudonymError("scope registry returned an unknown alias")
        returned_alias._validated_snapshot(expected_owner_request=request)
        _validate_registry_scope_id(scope_id)
    matched_scope_ids = tuple(scope_id for _, scope_id in result_items)
    distinct_scope_ids = tuple(sorted(set(matched_scope_ids)))
    if not matched_scope_ids:
        return _build_registry_decision(
            status=LOOKUP_DECISION_CREATE,
            scope_id=None,
            matched_alias_count=0,
            distinct_scope_count=0,
        )
    if len(distinct_scope_ids) == 1:
        return _build_registry_decision(
            status=LOOKUP_DECISION_MATCHED,
            scope_id=distinct_scope_ids[0],
            matched_alias_count=len(matched_scope_ids),
            distinct_scope_count=1,
        )
    return _build_registry_decision(
        status=LOOKUP_DECISION_AMBIGUOUS,
        scope_id=None,
        matched_alias_count=len(matched_scope_ids),
        distinct_scope_count=len(distinct_scope_ids),
    )


__all__ = [
    "LOOKUP_DECISION_AMBIGUOUS",
    "LOOKUP_DECISION_CREATE",
    "LOOKUP_DECISION_MATCHED",
    "LogicalScopeLookupRequest",
    "LogicalScopePseudonymError",
    "LogicalScopeRegistryDecision",
    "build_scope_lookup_request",
    "build_scope_pseudonymizer",
]
