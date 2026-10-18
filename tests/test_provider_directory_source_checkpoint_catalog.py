# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Control custody rejects partial guards, body drift and changed native layout."""

import hashlib
from copy import deepcopy
from types import SimpleNamespace

import pytest

from process import provider_directory_source_checkpoint_catalog as catalog
from process.provider_directory_generic_candidate import GenericCandidateError


def _guards(root):
    return [
        dict(
            tgname=name,
            tgtype=event,
            tgdeferrable=deferred,
            tginitdeferred=initial,
            tgoldtable=old,
            tgnewtable=new,
            proname=function,
            tgenabled="A",
            unconditional=True,
            update_columns=[],
            tgnargs=0,
            tgargs=b"",
            nspname="sample",
            pronargs=0,
            lanname="plpgsql",
            returns_trigger=True,
            prosecdef=True,
            provolatile="v",
            proconfig=["search_path=pg_catalog"],
            function_owner="builder",
            prosrc="BEGIN RETURN NULL; END;",
        )
        for name, (event, deferred, initial, old, new, function) in catalog._GUARDS[root].items()
    ]


@pytest.mark.parametrize("root", catalog.CHECKPOINT_LAYOUTS)
def test_complete_old_and_finished_guard_sets_are_distinct(root, monkeypatch):
    policy = SimpleNamespace(schema="sample", owner_role="owner", control_runtime_roles=("builder",))
    monkeypatch.setattr(
        catalog, "_BODIES", {name: hashlib.sha256(b"BEGIN RETURN NULL; END;").hexdigest() for name in catalog._BODIES}
    )
    guards = _guards(root)
    catalog._require_guards(guards, root, policy, legacy=True)
    finished_guards = [guard for guard in guards if guard["tgname"] not in catalog.RETIRED_BULK_HOOKS]
    catalog._require_guards(finished_guards, root, policy, legacy=False)
    if root == catalog.BULK_ROOT:
        with pytest.raises(GenericCandidateError, match="checkpoint_hooks_changed"):
            catalog._require_guards(guards, root, policy, legacy=False)
        with pytest.raises(GenericCandidateError, match="checkpoint_hooks_changed"):
            catalog._require_guards(guards[1:], root, policy, legacy=True)


@pytest.mark.parametrize("change", ["body", "owner", "security", "transition", "deferred", "arguments", "event"])
def test_authentic_guard_invocation_cannot_be_substituted(change, monkeypatch):
    guards = _guards(catalog.BULK_ROOT)
    monkeypatch.setattr(
        catalog, "_BODIES", {name: hashlib.sha256(b"BEGIN RETURN NULL; END;").hexdigest() for name in catalog._BODIES}
    )
    fields_by_change = {
        "body": ("prosrc", "BEGIN RETURN NEW; END;"),
        "owner": ("function_owner", "unknown"),
        "security": ("prosecdef", False),
        "transition": ("tgoldtable", "substituted"),
        "deferred": ("tgdeferrable", True),
        "arguments": ("tgargs", b"arg\x00"),
        "event": ("tgtype", 31),
    }
    key, value = fields_by_change[change]
    guards[0][key] = value
    with pytest.raises(GenericCandidateError, match="checkpoint_guard_changed"):
        catalog._require_guards(
            guards,
            catalog.BULK_ROOT,
            SimpleNamespace(schema="sample", owner_role="owner", control_runtime_roles=("builder",)),
            legacy=True,
        )


def _relation(root):
    layout = deepcopy(catalog.CHECKPOINT_LAYOUTS[root])
    return dict(
        relname=root,
        relkind="r",
        relpersistence="p",
        relispartition=False,
        parents=[],
        relrowsecurity=False,
        relforcerowsecurity=False,
        native_checks=True,
        columns=layout["columns"],
        constraints=[dict(type=kind, definition=definition, valid=True) for kind, definition in layout["constraints"]],
        indexes=[
            dict(
                primary=primary,
                unique=unique,
                method=method,
                predicate=predicate,
                expression=expression,
                keys=keys,
                valid=True,
            )
            for primary, unique, method, predicate, expression, keys in layout["indexes"]
        ],
    )


@pytest.mark.parametrize("root", catalog.CHECKPOINT_LAYOUTS)
@pytest.mark.parametrize("change", [None, "column", "constraint", "index", "partition"])
def test_only_the_full_fixed_native_layout_is_accepted(root, change):
    relation = _relation(root)
    if change == "column":
        relation["columns"][0][2] = False
    elif change == "constraint":
        relation["constraints"].pop()
    elif change == "index":
        relation["indexes"][0]["valid"] = False
    elif change == "partition":
        relation["parents"] = [123]
    if change is None:
        catalog._require_layout(relation, "mrf")
    else:
        with pytest.raises(GenericCandidateError, match="checkpoint_layout_changed"):
            catalog._require_layout(relation, "mrf")
