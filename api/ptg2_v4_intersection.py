# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native-first sorted integer intersections for packed PTG V4 graph keys."""

from __future__ import annotations

from typing import Iterable


try:
    from ptg2_address_canon import intersect_sorted_u32 as _native_intersection
except (ImportError, AttributeError):
    _native_intersection = None


def _strict_sorted_u32(values: Iterable[int]) -> tuple[int, ...]:
    normalized_values = tuple(int(value) for value in values)
    if any(value < 0 or value > 0xFFFFFFFF for value in normalized_values):
        raise ValueError("PTG V4 intersection value is outside uint32 range")
    if any(
        left >= right
        for left, right in zip(normalized_values, normalized_values[1:])
    ):
        raise ValueError("PTG V4 intersection input must be strictly increasing")
    return normalized_values


def intersect_sorted_u32(
    left: Iterable[int],
    right: Iterable[int],
) -> tuple[int, ...]:
    """Return an exact ordered intersection with a proven Python fallback."""

    normalized_left = _strict_sorted_u32(left)
    normalized_right = _strict_sorted_u32(right)
    if _native_intersection is not None:
        return tuple(
            int(value)
            for value in _native_intersection(normalized_left, normalized_right)
        )
    common_values: list[int] = []
    left_index = 0
    right_index = 0
    while left_index < len(normalized_left) and right_index < len(normalized_right):
        left_value = normalized_left[left_index]
        right_value = normalized_right[right_index]
        if left_value < right_value:
            left_index += 1
        elif left_value > right_value:
            right_index += 1
        else:
            common_values.append(left_value)
            left_index += 1
            right_index += 1
    return tuple(common_values)
