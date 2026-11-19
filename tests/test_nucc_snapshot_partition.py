"""Named native partition/model contracts and atomic exchange regressions."""

from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import nucc_snapshot_partition as native


def model_index(name, columns, classes, *, primary=False, method="btree", parents=(), schema="mrf"):
    return dict(
        schema=schema,
        name=f"index_{name}",
        primary=primary,
        unique=primary,
        ready=True,
        plain=True,
        code_safe=True,
        method=method,
        columns=columns,
        classes=classes,
        parents=[list(parent) for parent in parents],
    )


def parent_relation():
    return dict(
        kind="p",
        schema="mrf",
        name="nucc_taxonomy",
        persistence="p",
        is_partition=False,
        bound=None,
        options=None,
        safe=True,
        columns=[[*column, -1, "", "", False, True] for column in native._COLUMNS],
        indexes=[model_index("parent_pk", ["code"], ["text_ops"], primary=True)],
    )


def parent_catalog():
    relation = parent_relation()
    schema = "reference_family_archive_" + "a" * 32
    child = deepcopy(relation)
    child.update(
        schema=schema,
        kind="r",
        is_partition=True,
        bound="FOR VALUES FROM (MINVALUE) TO (MAXVALUE)",
        indexes=[
            model_index(
                "child_pk", ["code"], ["text_ops"], primary=True, schema=schema, parents=(("mrf", "index_parent_pk"),)
            ),
            model_index("int_code", ["int_code"], ["int4_ops"], schema=schema),
            model_index("display_name", ["display_name"], ["text_ops"], schema=schema),
            model_index(
                "classification",
                ["classification", "section"],
                ["varchar_ops", "varchar_ops"],
                method="gin",
                schema=schema,
            ),
        ],
    )
    return dict(
        parent=relation,
        partition=dict(
            strategy="r",
            attributes="2",
            count=1,
            key="RANGE (code)",
            expressions=None,
            default=False,
            classes_safe=True,
        ),
        children=[dict(relation=child, pending=False, parents=1, children=0)],
    )


def legacy_parent_catalog():
    """The original two-index layout remains portable without a custody marker."""
    value = parent_catalog()
    child = value["children"][0]["relation"]
    duplicate = model_index("legacy_code", ["code"], ["text_ops"], schema=child["schema"])
    duplicate["unique"] = True
    child["indexes"] = [child["indexes"][0], duplicate]
    return value


def original_baseline_rollback_catalogs():
    baseline, current = legacy_parent_catalog(), parent_catalog()
    child = current["children"][0]["relation"]
    child["schema"] = "reference_family_archive_" + "b" * 32
    for index in child["indexes"]:
        index["schema"] = child["schema"]
    old = deepcopy(baseline["children"][0]["relation"])
    old.update(is_partition=False, bound=None)
    old["indexes"][0]["parents"] = []
    return baseline, current, old


@pytest.mark.parametrize("legacy", (False, True))
def test_catalog_returns_named_pointer_and_member_with_permanent_model_semantics(legacy):
    captured = legacy_parent_catalog() if legacy else parent_catalog()
    assert native.validate_parent_catalog(captured) == dict(
        contract=native.CONTRACT,
        parent=("mrf", "nucc_taxonomy"),
        parent_index=("mrf", "index_parent_pk"),
        child=("reference_family_archive_" + "a" * 32, "nucc_taxonomy"),
    )
    for forbidden in ("relowner", "relfilenode", "aclexplode", "pg_has_role", "current_database", "obj_description"):
        assert forbidden not in native.CATALOG_SQL


@pytest.mark.parametrize(
    "path,replacement",
    [
        (("parent", "kind"), "r"),
        (("parent", "schema"), "other"),
        (("parent", "persistence"), "u"),
        (("parent", "safe"), False),
        (("parent", "options"), ["autovacuum_enabled=false"]),
        (("parent", "name"), "other"),
        (("partition", "strategy"), "l"),
        (("partition", "attributes"), "1"),
        (("partition", "default"), True),
        (("partition", "classes_safe"), False),
        (("children", 0, "pending"), True),
        (("children", 0, "parents"), 2),
        (("children", 0, "children"), 1),
        (("children", 0, "relation", "schema"), "mrf"),
        (("children", 0, "relation", "bound"), "FOR VALUES FROM ('0') TO (MAXVALUE)"),
        (("children", 0, "relation", "indexes", 0, "parents"), [["other", "index_parent_pk"]]),
        (("children", 0, "relation", "indexes", 2, "ready"), False),
        (("children", 0, "relation", "indexes", 3, "code_safe"), False),
        (("children", 0, "relation", "indexes", 3, "columns"), ["classification"]),
        (("children", 0, "relation", "indexes", 3, "classes"), ["text_ops", "text_ops"]),
        (("children", 0, "relation", "columns", 0, 6), True),
        (("children", 0, "relation", "columns", 0, 7), False),
    ],
)
def test_catalog_refuses_changed_name_partition_column_or_index_shape(path, replacement):
    captured = parent_catalog()
    field = captured
    for key in path[:-1]:
        field = field[key]
    field[path[-1]] = replacement
    with pytest.raises(native.NuccPartitionError):
        native.validate_parent_catalog(captured)


@pytest.mark.parametrize("fault", ("child", "index", "primary", "duplicate-index"))
def test_catalog_refuses_incomplete_or_ambiguous_sets(fault):
    captured = parent_catalog()
    indexes = captured["children"][0]["relation"]["indexes"]
    if fault == "child":
        captured["children"].append(deepcopy(captured["children"][0]))
    elif fault == "index":
        indexes.pop()
    elif fault == "primary":
        indexes[0]["unique"] = False
    else:
        indexes[-1]["name"] = indexes[0]["name"]
    with pytest.raises(native.NuccPartitionError):
        native.validate_parent_catalog(captured)


@pytest.mark.parametrize("legacy", (False, True))
def test_detached_generation_keeps_exact_named_scope_and_unattached_primary(legacy):
    child = (legacy_parent_catalog() if legacy else parent_catalog())["children"][0]["relation"]
    child.update(is_partition=False, bound=None)
    child["indexes"][0]["parents"] = []
    expected = child["schema"], child["name"]
    assert native.validate_detached_child(child, expected=expected, legacy=legacy) is child
    if legacy:
        with pytest.raises(native.NuccPartitionError, match="model indexes"):
            native.validate_detached_child(child, expected=expected)
    child["indexes"][0]["parents"] = [["mrf", "index_parent_pk"]]
    with pytest.raises(native.NuccPartitionError, match="primary is attached"):
        native.validate_detached_child(child, expected=expected, legacy=legacy)


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", (False, True))
async def test_capture_locks_named_parent_member_and_rechecks_logical_projection(monkeypatch, drift):
    from process import reference_family_archive as reference

    @asynccontextmanager
    async def path(_session):
        yield

    monkeypatch.setattr(reference, "_nucc_catalog_search_path", path)
    first, after = parent_catalog(), parent_catalog()
    if drift:
        child = after["children"][0]["relation"]
        child["schema"] = "reference_family_archive_" + "b" * 32
    session = SimpleNamespace(
        in_transaction=lambda: True, execute=AsyncMock(), scalar=AsyncMock(side_effect=[first, after])
    )
    if drift:
        with pytest.raises(native.NuccPartitionError, match="named generation changed"):
            await native.capture_nucc_parent(session)
    else:
        result = await native.capture_nucc_parent(session)
        assert result["child"] == ("reference_family_archive_" + "a" * 32, "nucc_taxonomy")
    mode = "ACCESS SHARE"
    assert [str(call.args[0]) for call in session.execute.await_args_list] == [
        f"LOCK TABLE ONLY {native.PARENT} IN {mode} MODE NOWAIT",
        'LOCK TABLE ONLY "reference_family_archive_' + "a" * 32 + '"."nucc_taxonomy" IN ACCESS SHARE MODE NOWAIT',
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("fails", (False, True))
async def test_exchange_materializes_savepoint_and_preserves_native_failure(monkeypatch, fails):
    events = []
    previous = ("reference_family_archive_" + "a" * 32, "nucc_taxonomy")
    incoming = ("reference_family_archive_" + "b" * 32, "nucc_taxonomy")

    class Session:
        def in_transaction(self):
            return True

        @asynccontextmanager
        async def begin_nested(self):
            events.append("savepoint")
            try:
                yield
            except BaseException:
                events.append("rollback")
                raise
            else:
                events.append("release")

        async def connection(self):
            events.append("materialize")

    async def exchange(session, before, after):
        assert isinstance(session, Session) and (before, after) == (previous, incoming)
        assert events == ["savepoint", "materialize"]
        events.append("native")
        if fails:
            raise RuntimeError("native attach failed")
        return {"done": True}

    monkeypatch.setattr(native, "_exchange_nucc_child", exchange)
    if fails:
        with pytest.raises(RuntimeError, match="native attach failed"):
            await native.exchange_nucc_child(Session(), expected_child=previous, incoming_child=incoming)
    else:
        assert await native.exchange_nucc_child(Session(), expected_child=previous, incoming_child=incoming) == {
            "done": True
        }
    assert events[-1] == ("rollback" if fails else "release")


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy", (False, True))
async def test_exchange_locks_both_named_members_before_grants_and_atomic_detach_attach(monkeypatch, legacy):
    before = native.validate_parent_catalog(parent_catalog())
    incoming = "reference_family_archive_" + "b" * 32, "nucc_taxonomy"
    events = []

    async def capture(_session, *, expected_child):
        return before if expected_child == before["child"] else {**before, "child": incoming}

    async def candidate(_session, child):
        assert child == incoming
        return (legacy_parent_catalog() if legacy else parent_catalog())["children"][0]["relation"], None

    async def readers(_session, child):
        assert child == incoming
        events.append("readers")

    async def execute(statement):
        events.append(str(statement))

    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    monkeypatch.setenv("HLTHPRT_PRICING_TAXONOMY_PUBLICATION_SCHEMA", "taxonomy_test")
    monkeypatch.setattr(native, "capture_nucc_parent", capture)
    monkeypatch.setattr(native, "_capture_exchange_target", candidate)
    monkeypatch.setattr(native, "_preserve_parent_readers", readers)
    exchange = await native._exchange_nucc_child(SimpleNamespace(execute=execute), before["child"], incoming)
    assert exchange == {"contract": native.CONTRACT, "before": before, "after": {**before, "child": incoming}}
    assert events == [
        f"LOCK TABLE ONLY {native.relation_sql(incoming)} IN ACCESS SHARE MODE NOWAIT",
        f"LOCK TABLE ONLY {native.relation_sql(before['child'])} IN ACCESS SHARE MODE NOWAIT",
        "readers",
        f"ALTER TABLE {native.PARENT} DETACH PARTITION {native.relation_sql(before['child'])}",
        f"ALTER TABLE {native.PARENT} ATTACH PARTITION {native.relation_sql(incoming)} FOR VALUES FROM (MINVALUE) TO (MAXVALUE)",
        'UPDATE "taxonomy_test_control"."procedure_taxonomy_publication" '
        "SET input_revision = input_revision + 1 WHERE singleton",
    ]


@pytest.mark.asyncio
async def test_configured_readers_receive_only_schema_usage_and_select(monkeypatch):
    from process import reference_family_archive as reference

    monkeypatch.setattr(
        reference, "model_family_roles", lambda: ("snapshot_owner", ("snapshot_owner", "publisher", "reader"))
    )
    session = SimpleNamespace(execute=AsyncMock())
    child = "reference_family_archive_" + "b" * 32, "nucc_taxonomy"
    await native._preserve_parent_readers(session, child)
    assert [str(call.args[0]) for call in session.execute.await_args_list] == [
        sql
        for role in ("snapshot_owner", "publisher", "reader")
        for sql in (
            f'GRANT USAGE ON SCHEMA "{child[0]}" TO "{role}"',
            f'GRANT SELECT ON {native.relation_sql(child)} TO "{role}"',
        )
    ]
