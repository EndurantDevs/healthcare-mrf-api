"""Shared data shapes for readability budget checks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

DEFAULT_ISSUE_CATEGORIES = (
    "ambiguous_function_names",
    "ambiguous_variable_names",
    "boolean_name_mismatch",
    "builtin_shadowing",
    "class_name_shape",
    "collection_name_mismatch",
    "comment_noise",
    "confusable_function_names",
    "deep_nesting",
    "function_name_shape",
    "global_state_usage",
    "inline_suppressions",
    "huge_file_growth",
    "long_files",
    "long_functions",
    "module_attribute_injection",
    "missing_contract_docstrings",
    "pass_placeholders",
    "single_letter_names",
    "split_module_name",
    "syntax_errors",
    "too_many_locals",
    "too_many_parameters",
)


@dataclass(frozen=True)
class Issue:
    category: str
    identifier: str
    path: str
    detail: dict[str, Any]

    def to_json(self) -> dict[str, Any]:
        """Return the stable JSON shape used by baselines and CLI output."""
        payload = {"id": self.identifier, "path": self.path}
        payload.update(self.detail)
        return payload
