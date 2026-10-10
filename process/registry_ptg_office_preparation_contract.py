# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed selectors for explicit reviewed offices; selectors grant no authority."""

import json

from process.registry_ptg_scope_engine import _object, _sha256, _uuid

MAX_SELECTIONS = 100
_HANDLES = {"capture_id", "scope_id", "client_id", "scope_approval_sha256", "idempotency_key"}


def _line(value, maximum):
    if (
        type(value) is not str
        or not 1 <= len(value.encode()) <= maximum
        or value.strip() != value
        or not value.isprintable()
    ):
        raise ValueError("registry_ptg_office_selection_invalid")


def _positive(value, maximum=2**63 - 1, *, zero=False):
    if type(value) is not int or not int(not zero) <= value <= maximum:
        raise ValueError("registry_ptg_office_selection_invalid")


def validated_prepare_selector(document, *, read=False):
    """Freeze exact handles and reviewed binding/occurrence choices before sessions."""
    fields = _HANDLES if read else _HANDLES | {"retained_generation_id", "reason", "selections"}
    _object(document, fields)
    for name in ("capture_id", "scope_id"):
        _uuid(document[name])
    _sha256(document["scope_approval_sha256"])
    _line(document["client_id"], 64)
    _line(document["idempotency_key"], 128)
    if document["client_id"] in {"system", "__platform__"}:
        raise ValueError("registry_ptg_office_selection_invalid")
    if not read:
        _positive(document["retained_generation_id"])
        _line(document["reason"], 1000)
        _selections(document["selections"])
    encoded = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    if len(encoded.encode()) > 65536:
        raise ValueError("registry_ptg_office_selection_invalid")
    return json.loads(encoded)


def _selections(selections):
    if type(selections) is not list or not 1 <= len(selections) <= MAX_SELECTIONS:
        raise ValueError("registry_ptg_office_selection_invalid")
    previous = None
    for selection in selections:
        _object(selection, {"binding_id", "binding_revision", "dense_source_key", "source_record_ordinal"})
        _uuid(selection["binding_id"])
        _positive(selection["binding_revision"])
        _positive(selection["dense_source_key"], 2147483647, zero=True)
        _positive(selection["source_record_ordinal"], zero=True)
        if previous is not None and selection["binding_id"] <= previous:
            raise ValueError("registry_ptg_office_selection_invalid")
        previous = selection["binding_id"]


def validated_preparation_envelope(document, *, read=False):
    """The signed transport supplies the original actor and session hash."""
    from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget

    _object(document, {"actor", "session_token_sha256", "selector", "read_limits"})
    actor = _object(document["actor"], {"kind", "user_id", "client_id"})
    if actor["kind"] != "platform_admin" or actor["client_id"] != "system":
        raise PermissionError("registry_ptg_scope_actor_invalid")
    _uuid(actor["user_id"])
    _sha256(document["session_token_sha256"])
    limits = _object(document["read_limits"], {"maximum_bytes", "maximum_pages", "maximum_coordinates"})
    RegistryPTGGraphReadBudget(**limits)
    return {
        "actor": dict(actor),
        "session_token_sha256": document["session_token_sha256"],
        "selector": validated_prepare_selector(document["selector"], read=read),
        "read_limits": dict(limits),
    }


def validated_office_choices_envelope(document):
    """Pin a bounded page without accepting browser-supplied source authority."""
    from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget

    _object(document, {"actor", "session_token_sha256", "selector", "read_limits"})
    actor = _object(document["actor"], {"kind", "user_id", "client_id"})
    if actor["kind"] != "platform_admin" or actor["client_id"] != "system":
        raise PermissionError("registry_ptg_scope_actor_invalid")
    _uuid(actor["user_id"])
    _sha256(document["session_token_sha256"])
    limits = _object(document["read_limits"], {"maximum_bytes", "maximum_pages", "maximum_coordinates"})
    RegistryPTGGraphReadBudget(**limits)
    selector = _object(
        document["selector"],
        {"scope_id", "client_id", "scope_approval_sha256", "retained_generation_id", "after", "limit"},
    )
    _uuid(selector["scope_id"])
    _sha256(selector["scope_approval_sha256"])
    _line(selector["client_id"], 64)
    if (
        selector["client_id"] in {"system", "__platform__"}
        or type(selector["limit"]) is not int
        or selector["limit"] != 20
    ):
        raise ValueError("registry_ptg_office_selection_invalid")
    if selector["retained_generation_id"] is None:
        if selector["after"] is not None:
            raise ValueError("registry_ptg_office_selection_invalid")
    else:
        _positive(selector["retained_generation_id"])
    if selector["after"] is not None:
        after = selector["after"]
        if type(after) is not str or not 1 <= len(after) <= 1024 or not after.isascii() or not after.isprintable():
            raise ValueError("registry_ptg_office_selection_invalid")
    return json.loads(json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False))
