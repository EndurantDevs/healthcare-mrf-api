# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Structural boundary checks for the pure billing-search access contract."""

from __future__ import annotations

import ast
from pathlib import Path

MODULE_PATH = Path(__file__).parents[1] / "api" / "billing_search_access_contract.py"
ALLOWED_IMPORT_ROOTS = frozenset(
    {
        "__future__",
        "dataclasses",
        "datetime",
        "hashlib",
        "hmac",
        "json",
        "re",
        "typing",
    }
)
FORBIDDEN_CALL_NAMES = frozenset(
    {
        "APIRouter",
        "Depends",
        "Session",
        "__import__",
        "add_api_route",
        "allow",
        "close",
        "commit",
        "connect",
        "consume",
        "create_connection",
        "decr",
        "delete",
        "enqueue_job",
        "eval",
        "exec",
        "execute",
        "executemany",
        "expire",
        "flush",
        "incr",
        "open",
        "publish",
        "put",
        "read",
        "request",
        "rollback",
        "save",
        "scalar",
        "send",
        "setex",
        "setnx",
        "socket",
        "urlopen",
        "write",
    }
)
FORBIDDEN_DIRECT_CALL_NAMES = frozenset(
    {"__import__", "compile", "eval", "exec", "open"}
)
PUBLIC_FUNCTION_SIGNATURES = {
    "billing_search_access_journal_record": (
        False,
        (),
        ("seed",),
        None,
        (),
        None,
        0,
        (),
        (),
    ),
    "billing_search_metering_key": (
        False,
        (),
        ("context",),
        None,
        ("trusted_now",),
        None,
        0,
        (False,),
        (),
    ),
    "build_billing_search_access_journal_seed": (
        False,
        (),
        ("context",),
        None,
        (
            "generation_bundle_sha256",
            "request_shape_sha256",
            "selector_kind",
            "decision",
            "trusted_observed_at",
            "duration_us",
            "detailed_provenance",
        ),
        None,
        0,
        (False,) * 7,
        (),
    ),
    "build_billing_search_authorization_context": (
        False,
        (),
        ("verified_claims",),
        None,
        ("trusted_now",),
        None,
        0,
        (False,),
        (),
    ),
    "require_billing_search_access": (
        False,
        (),
        ("context",),
        None,
        (
            "requested_plan_entitlement_sha256",
            "detailed_provenance",
            "trusted_now",
        ),
        None,
        0,
        (False,) * 3,
        (),
    ),
    "validate_billing_search_access_journal_seed": (
        False,
        (),
        ("seed",),
        None,
        (),
        None,
        0,
        (),
        (),
    ),
    "validate_billing_search_authorization_context": (
        False,
        (),
        ("context",),
        None,
        ("trusted_now",),
        None,
        0,
        (False,),
        (),
    ),
}
PUBLIC_CLASS_FIELDS = {
    "BillingSearchAuthorizationContext": (
        "principal_scope_sha256",
        "tenant_scope_sha256",
        "plan_entitlement_sha256",
        "audit_scope_sha256",
        "quota_scope_sha256",
        "capabilities",
        "issued_at",
        "expires_at",
        "context_sha256",
        "contract",
        "authentication_capability",
        "self_authorizing",
    ),
    "BillingSearchAccessJournalSeed": (
        "audit_scope_sha256",
        "authorization_context_sha256",
        "plan_entitlement_sha256",
        "generation_bundle_sha256",
        "request_shape_sha256",
        "selector_kind",
        "decision",
        "observed_at",
        "duration_us",
        "detailed_provenance",
        "event_sha256",
        "contract",
    ),
}
PUBLIC_CLASS_METHODS = {
    "BillingSearchAuthorizationContext": ("__post_init__",),
    "BillingSearchAccessJournalSeed": ("__post_init__",),
}
MODULE_ASSIGNMENT_NAMES = frozenset(
    {
        "BILLING_SEARCH_ACCESS_CONTRACT",
        "BILLING_SEARCH_CAPABILITY",
        "BILLING_SEARCH_PROVENANCE_CAPABILITY",
        "BILLING_SEARCH_CACHE_CONTROL",
        "_CONTEXT_DIGEST_DOMAIN",
        "_JOURNAL_DIGEST_DOMAIN",
        "_METER_DIGEST_DOMAIN",
        "_INVALID",
        "_DENIED",
        "_REDACTED",
        "_MAX_VALIDITY_SECONDS",
        "_MAX_JOURNAL_DURATION_US",
        "_SHA256_RE",
        "_UTC_RE",
        "_CAPABILITY_SETS",
        "_CONTEXT_FIELDS",
        "_SELECTOR_KINDS",
        "_ACCESS_DECISIONS",
    }
)


def _import_roots(tree: ast.AST) -> set[str]:
    roots = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots.update(alias.name.partition(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            roots.add(node.module.partition(".")[0])
    return roots


def _called_names(tree: ast.AST) -> set[str]:
    names = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Name):
            names.add(node.func.id)
        elif isinstance(node.func, ast.Attribute):
            names.add(node.func.attr)
    return names


def _direct_called_names(tree: ast.AST) -> set[str]:
    return {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }


def _public_function_signatures(
    tree: ast.Module,
) -> dict[str, tuple[object, ...]]:
    signatures_by_function = {}
    for node in tree.body:
        if not isinstance(
            node, (ast.FunctionDef, ast.AsyncFunctionDef)
        ) or node.name.startswith("_"):
            continue
        positional_only_names = tuple(
            argument.arg for argument in node.args.posonlyargs
        )
        positional_names = tuple(argument.arg for argument in node.args.args)
        keyword_only_names = tuple(argument.arg for argument in node.args.kwonlyargs)
        signatures_by_function[node.name] = (
            isinstance(node, ast.AsyncFunctionDef),
            positional_only_names,
            positional_names,
            None if node.args.vararg is None else node.args.vararg.arg,
            keyword_only_names,
            None if node.args.kwarg is None else node.args.kwarg.arg,
            len(node.args.defaults),
            tuple(default is not None for default in node.args.kw_defaults),
            tuple(ast.unparse(decorator) for decorator in node.decorator_list),
        )
    return signatures_by_function


def _public_class_fields(tree: ast.Module) -> dict[str, tuple[str, ...]]:
    fields_by_class = {}
    for node in tree.body:
        if not isinstance(node, ast.ClassDef) or node.name not in PUBLIC_CLASS_FIELDS:
            continue
        field_names = []
        for item in node.body:
            if isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name):
                field_names.append(item.target.id)
            elif isinstance(item, ast.Assign):
                field_names.extend(
                    target.id for target in item.targets if isinstance(target, ast.Name)
                )
        fields_by_class[node.name] = tuple(field_names)
    return fields_by_class


def _public_class_methods(tree: ast.Module) -> dict[str, tuple[str, ...]]:
    methods_by_class = {}
    for node in tree.body:
        if not isinstance(node, ast.ClassDef) or node.name not in PUBLIC_CLASS_METHODS:
            continue
        methods_by_class[node.name] = tuple(
            item.name
            for item in node.body
            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
        )
    return methods_by_class


def _module_assignments(tree: ast.Module) -> dict[str, ast.expr]:
    values_by_name = {}
    for node in tree.body:
        if isinstance(node, ast.Assign):
            targets = node.targets
            value = node.value
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            targets = (node.target,)
            value = node.value
        else:
            continue
        for target in targets:
            if not isinstance(target, ast.Name) or target.id in values_by_name:
                return {}
            values_by_name[target.id] = value
    return values_by_name


def _is_fixed_initializer(value: ast.expr) -> bool:
    if isinstance(value, (ast.Constant, ast.Name)):
        return True
    if isinstance(value, ast.Tuple):
        return all(_is_fixed_initializer(item) for item in value.elts)
    if not isinstance(value, ast.Call):
        return False
    if (
        isinstance(value.func, ast.Name)
        and value.func.id == "frozenset"
        and len(value.args) == 1
        and isinstance(value.args[0], ast.Set)
        and not value.keywords
    ):
        return all(isinstance(item, ast.Constant) for item in value.args[0].elts)
    return (
        isinstance(value.func, ast.Attribute)
        and isinstance(value.func.value, ast.Name)
        and value.func.value.id == "re"
        and value.func.attr == "compile"
        and len(value.args) == 1
        and isinstance(value.args[0], ast.Constant)
        and len(value.keywords) == 1
        and value.keywords[0].arg == "flags"
        and isinstance(value.keywords[0].value, ast.Attribute)
        and isinstance(value.keywords[0].value.value, ast.Name)
        and value.keywords[0].value.value.id == "re"
        and value.keywords[0].value.attr == "ASCII"
    )


def _has_only_fixed_module_state(tree: ast.Module) -> bool:
    values_by_name = _module_assignments(tree)
    return set(values_by_name) == MODULE_ASSIGNMENT_NAMES and all(
        _is_fixed_initializer(value) for value in values_by_name.values()
    )


def test_contract_has_no_route_storage_network_or_quota_wiring():
    tree = ast.parse(MODULE_PATH.read_text(encoding="utf-8"))

    assert _import_roots(tree) == ALLOWED_IMPORT_ROOTS
    assert _called_names(tree).isdisjoint(FORBIDDEN_CALL_NAMES)
    assert _direct_called_names(tree).isdisjoint(FORBIDDEN_DIRECT_CALL_NAMES)
    assert _public_function_signatures(tree) == PUBLIC_FUNCTION_SIGNATURES
    assert _public_class_fields(tree) == PUBLIC_CLASS_FIELDS
    assert _public_class_methods(tree) == PUBLIC_CLASS_METHODS
    assert _has_only_fixed_module_state(tree)
    assert not any(
        isinstance(node, (ast.Global, ast.Nonlocal)) for node in ast.walk(tree)
    )


def test_boundary_rejects_an_injected_module_cache():
    source = MODULE_PATH.read_text(encoding="utf-8")
    injected_tree = ast.parse(
        source
        + "\n_CACHE = {}\ndef _cache_write(key, value):\n"
        + "    _CACHE.setdefault(key, value)\n"
    )

    assert not _has_only_fixed_module_state(injected_tree)


def test_boundary_rejects_expanded_async_or_stateful_public_surfaces():
    source = MODULE_PATH.read_text(encoding="utf-8")
    expanded_source = source.replace(
        "def billing_search_access_journal_record(seed: object)",
        "def billing_search_access_journal_record(seed: object, *args, **kwargs)",
    )
    async_source = source + "\nasync def injected_public():\n    return None\n"
    stateful_source = source.replace(
        "class BillingSearchAuthorizationContext(_RedactedImmutable):\n",
        "class BillingSearchAuthorizationContext(_RedactedImmutable):\n"
        "    cache = {}\n",
    )

    assert expanded_source != source
    assert _public_function_signatures(ast.parse(expanded_source)) != (
        PUBLIC_FUNCTION_SIGNATURES
    )
    assert _public_function_signatures(ast.parse(async_source)) != (
        PUBLIC_FUNCTION_SIGNATURES
    )
    assert _public_class_fields(ast.parse(stateful_source)) != PUBLIC_CLASS_FIELDS
