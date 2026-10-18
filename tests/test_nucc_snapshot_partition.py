"""Closed native-shaped parent/physical-child catalog and lock recapture regressions."""

from contextlib import asynccontextmanager
from copy import deepcopy

import pytest

from process import nucc_snapshot_partition as native


def model_index(oid, columns, classes, *, primary=False, method="btree", parents=()):
    return dict(
        oid=oid,
        owner=101,
        primary=primary,
        unique=primary,
        ready=True,
        plain=True,
        code_safe=True,
        method=method,
        columns=columns,
        classes=classes,
        parents=list(parents),
    )


def parent_relation():
    owner = 101
    acl_entries = [[owner, owner, privilege, False] for privilege in sorted(native._OWNER_RIGHTS)]
    acl_entries.extend([[102, owner, "SELECT", False], [103, owner, "SELECT", False]])
    return dict(
        oid=201,
        owner=owner,
        kind="p",
        schema="mrf",
        schema_oid=301,
        schema_owner=401,
        name="nucc_taxonomy",
        filenode=0,
        persistence="p",
        is_partition=False,
        bound=None,
        options=None,
        safe=True,
        ordinary_write=False,
        columns=[[*column, -1, "", "", False, None, True] for column in native._COLUMNS],
        acl=acl_entries,
        indexes=[model_index(501, ["code"], ["text_ops"], primary=True)],
    )


def parent_catalog():
    owner = 101
    relation = parent_relation()
    child = deepcopy(relation)
    child.update(
        oid=202,
        schema="reference_family_archive_" + "a" * 32,
        schema_oid=302,
        schema_owner=owner,
        kind="r",
        filenode=602,
        is_partition=True,
        bound="FOR VALUES FROM (MINVALUE) TO (MAXVALUE)",
        indexes=[
            model_index(502, ["code"], ["text_ops"], primary=True, parents=(501,)),
            model_index(503, ["int_code"], ["int4_ops"]),
            model_index(504, ["display_name"], ["text_ops"]),
            model_index(
                505,
                ["classification", "section"],
                ["varchar_ops", "varchar_ops"],
                method="gin",
            ),
        ],
    )
    return dict(
        owner=dict(oid=owner, safe=True),
        parent=relation,
        partition=dict(
            strategy="r",
            attributes="2",
            count=1,
            key="RANGE (code)",
            expressions=None,
            default=0,
            classes_safe=True,
        ),
        children=[dict(relation=child, pending=False, parents=1, children=0)],
    )


def test_catalog_returns_actual_parent_and_physical_generation_separately():
    observed = native.validate_parent_catalog(parent_catalog())
    assert observed == dict(
        contract=native.CONTRACT,
        owner_oid=101,
        parent_oid=201,
        parent_index_oid=501,
        child_oid=202,
        child_schema="reference_family_archive_" + "a" * 32,
        child_schema_oid=302,
        child_table="nucc_taxonomy",
        child_filenode=602,
    )


def test_catalog_uses_actual_login_closure():
    from process.entity_address_alias_guard import ORDINARY_PRINCIPAL_SQL

    predicate = ORDINARY_PRINCIPAL_SQL.replace("CAST(:owner_oid AS oid)", "c.relowner")
    assert native._RELATION_CATALOG_SQL.count(predicate) == 2
    assert "NOT principal.rolsuper" not in native._RELATION_CATALOG_SQL
    assert "login.rolcanlogin" in predicate
    assert "'MEMBER'" in predicate and "'USAGE'" in predicate


@pytest.mark.parametrize(
    "path,replacement",
    [
        (("owner", "safe"), False),
        (("parent", "owner"), 102),
        (("parent", "kind"), "r"),
        (("parent", "ordinary_write"), True),
        (("parent", "safe"), False),
        (("parent", "filenode"), 999),
        (("partition", "strategy"), "l"),
        (("partition", "attributes"), "1"),
        (("partition", "classes_safe"), False),
        (("children", 0, "pending"), True),
        (("children", 0, "parents"), 2),
        (("children", 0, "children"), 1),
        (("children", 0, "relation", "owner"), 102),
        (("children", 0, "relation", "schema_owner"), 102),
        (("children", 0, "relation", "schema"), "mrf"),
        (("children", 0, "relation", "bound"), "FOR VALUES FROM ('0') TO (MAXVALUE)"),
        (("children", 0, "relation", "indexes", 0, "parents"), [999]),
        (("children", 0, "relation", "indexes", 2, "ready"), False),
        (("children", 0, "relation", "indexes", 3, "code_safe"), False),
        (("children", 0, "relation", "indexes", 3, "columns"), ["classification"]),
        (("children", 0, "relation", "columns", 0, 6), True),
        (("children", 0, "relation", "oid"), "202"),
    ],
)
def test_catalog_refuses_changed_native_shape(path, replacement):
    captured = parent_catalog()
    field = captured
    for key in path[:-1]:
        field = field[key]
    field[path[-1]] = replacement
    with pytest.raises(native.NuccPartitionError):
        native.validate_parent_catalog(captured)


@pytest.mark.parametrize(
    "entry",
    [
        [102, 101, "INSERT", False],
        [102, 101, "SELECT", True],
        [102, 102, "SELECT", False],
    ],
)
def test_catalog_refuses_extra_mutation_grant_or_foreign_grantor(entry):
    captured = parent_catalog()
    captured["children"][0]["relation"]["acl"].append(entry)
    with pytest.raises(native.NuccPartitionError):
        native.validate_parent_catalog(captured)


def test_catalog_refuses_extra_child_and_missing_model_index():
    captured = parent_catalog()
    captured["children"].append(deepcopy(captured["children"][0]))
    with pytest.raises(native.NuccPartitionError):
        native.validate_parent_catalog(captured)
    captured = parent_catalog()
    captured["children"][0]["relation"]["indexes"].pop()
    with pytest.raises(native.NuccPartitionError):
        native.validate_parent_catalog(captured)


def test_detached_generation_requires_its_exact_model_and_unattached_primary():
    captured = parent_catalog()["children"][0]["relation"]
    captured.update(is_partition=False, bound=None)
    captured["indexes"][0]["parents"] = []
    assert native.validate_detached_child(captured, owner_oid=101, expected_oid=202) == captured
    captured["indexes"][0]["parents"] = [501]
    with pytest.raises(native.NuccPartitionError):
        native.validate_detached_child(captured, owner_oid=101, expected_oid=202)


@pytest.mark.asyncio
@pytest.mark.parametrize("exclusive", [False, True])
async def test_capture_locks_parent_and_exact_child_then_recaptures(monkeypatch, exclusive):
    from process import reference_family_archive as reference

    @asynccontextmanager
    async def search_path(session):
        assert session.in_transaction()
        yield

    monkeypatch.setattr(reference, "_nucc_catalog_search_path", search_path)
    statements = []

    class Session:
        def in_transaction(self):
            return True

        async def scalar(self, statement):
            assert str(statement) == native.CATALOG_SQL
            return parent_catalog()

        async def execute(self, statement):
            statements.append(str(statement))

    observed = await native.capture_nucc_parent(Session(), expected_child_oid=202, exclusive=exclusive)
    mode = "ACCESS EXCLUSIVE" if exclusive else "ACCESS SHARE"
    assert observed["child_oid"] == 202
    assert statements == [
        f"LOCK TABLE ONLY {native.PARENT} IN {mode} MODE NOWAIT",
        'LOCK TABLE ONLY "reference_family_archive_' + "a" * 32 + '"."nucc_taxonomy" IN ACCESS SHARE MODE NOWAIT',
    ]


@pytest.mark.asyncio
async def test_capture_refuses_changed_full_catalog_after_lock(monkeypatch):
    from process import reference_family_archive as reference

    @asynccontextmanager
    async def search_path(_session):
        yield

    monkeypatch.setattr(reference, "_nucc_catalog_search_path", search_path)

    class Session:
        reads = 0

        def in_transaction(self):
            return True

        async def scalar(self, _statement):
            self.reads += 1
            captured = parent_catalog()
            if self.reads > 1:
                captured["parent"]["acl"].append([104, 101, "SELECT", False])
            return captured

        async def execute(self, _statement):
            return None

    with pytest.raises(native.NuccPartitionError, match="catalogue changed"):
        await native.capture_nucc_parent(Session())


@pytest.mark.asyncio
@pytest.mark.parametrize("fails", [False, True])
async def test_exchange_materializes_owned_savepoint_before_native_ddl(monkeypatch, fails):
    events = []

    class Session:
        def in_transaction(self):
            return True

        @asynccontextmanager
        async def begin_nested(self):
            events.append("savepoint")
            try:
                yield
            finally:
                events.append("exit")

        async def connection(self):
            events.append("materialize")

    async def exchange(session, previous, incoming):
        assert isinstance(session, Session) and (previous, incoming) == (202, 203)
        assert events == ["savepoint", "materialize"]
        events.append("native")
        if fails:
            raise native.NuccPartitionError("genuine DDL failure")
        return {"done": True}

    monkeypatch.setattr(native, "_exchange_nucc_child", exchange)
    if fails:
        with pytest.raises(native.NuccPartitionError, match="genuine DDL failure"):
            await native.exchange_nucc_child(Session(), expected_child_oid=202, incoming_child_oid=203)
    else:
        assert await native.exchange_nucc_child(Session(), expected_child_oid=202, incoming_child_oid=203) == {
            "done": True
        }
    assert events == ["savepoint", "materialize", "native", "exit"]


@pytest.mark.asyncio
async def test_exchange_pins_outgoing_child_before_reader_grants_or_detach(monkeypatch):
    from process import entity_address_snapshot_preparation as preparation
    from process import reference_family_archive as reference

    before = native.validate_parent_catalog(parent_catalog())
    incoming_by_field = {"schema": "reference_family_archive_" + "b" * 32}
    events = []

    class Session:
        async def execute(self, statement):
            events.append(str(statement))

        async def scalar(self, _statement):
            return "42"

    async def parent(_session, *, expected_child_oid, exclusive):
        assert exclusive is True
        return before if expected_child_oid == 202 else {**before, "child_oid": 203}

    async def owner(_session):
        return 101

    async def candidate(_session, child_oid, owner_oid):
        assert (child_oid, owner_oid) == (203, 101)
        return incoming_by_field, None

    async def readers(_session, child):
        assert child is incoming_by_field
        events.append("reader-grants")

    @asynccontextmanager
    async def search_path(_session):
        yield

    monkeypatch.setattr(native, "capture_nucc_parent", parent)
    monkeypatch.setattr(preparation, "_publisher_authority", owner)
    monkeypatch.setattr(native, "_capture_exchange_target", candidate)
    monkeypatch.setattr(native, "_preserve_parent_readers", readers)
    monkeypatch.setattr(reference, "_nucc_catalog_search_path", search_path)
    await native._exchange_nucc_child(Session(), 202, 203)
    outgoing = '"reference_family_archive_' + "a" * 32 + '"."nucc_taxonomy"'
    assert events.index(f"LOCK TABLE ONLY {outgoing} IN ACCESS EXCLUSIVE MODE NOWAIT") < events.index("reader-grants")
    assert events.index("reader-grants") < next(index for index, sql in enumerate(events) if "DETACH PARTITION" in sql)


@pytest.mark.asyncio
async def test_reader_replay_uses_only_exact_native_parent_select(monkeypatch):
    captured = parent_catalog()
    captured["parent"]["acl"].append([0, 101, "SELECT", False])
    statements = []

    class Session:
        async def scalar(self, statement, parameters=None):
            if str(statement) == native.CATALOG_SQL:
                return captured
            assert parameters["oid"] in {102, 103}
            return '"reader_' + str(parameters["oid"]) + '"'

        async def execute(self, statement):
            statements.append(str(statement))

    schema = "reference_family_archive_" + "b" * 32
    await native._preserve_parent_readers(Session(), {"schema": schema})
    assert statements == [
        sql
        for role in ['"reader_102"', '"reader_103"', "PUBLIC"]
        for sql in [
            f'GRANT USAGE ON SCHEMA "{schema}" TO {role}',
            f'GRANT SELECT ON "{schema}"."nucc_taxonomy" TO {role}',
        ]
    ]


def test_ordinary_child_namespace_create_is_a_mutation_path():
    assert "OR c.relkind='r' AND has_schema_privilege(principal.oid,n.oid,'CREATE')" in native._RELATION_CATALOG_SQL


def legacy_parent_catalog():
    from process import nucc_protected_bootstrap as bootstrap
    from process.nucc_protected_control import receipt_digest
    from tests.test_nucc_protected_control import source_generation

    captured = parent_catalog()
    child = captured["children"][0]["relation"]
    duplicate = model_index(503, ["code"], ["text_ops"])
    duplicate["unique"] = True
    child["indexes"] = [child["indexes"][0], duplicate]
    physical_by_field = dict(
        contract=native.CONTRACT,
        owner_oid=101,
        parent_oid=201,
        parent_index_oid=501,
        child_oid=202,
        child_schema=child["schema"],
        child_schema_oid=302,
        child_table="nucc_taxonomy",
        child_filenode=602,
    )
    marker_by_field = dict(
        contract=bootstrap.CONTRACT,
        database_oid=100,
        **{
            key: physical_by_field[key]
            for key in (
                "owner_oid",
                "parent_oid",
                "parent_index_oid",
                "child_oid",
                "child_schema",
                "child_schema_oid",
                "child_filenode",
            )
        },
        child_index_oids=[502, 503],
        old_owner_oid=110,
        old_schema_oid=301,
        old_schema_owner_oid=111,
        old_native_sha256="a" * 64,
        protected_native_sha256=receipt_digest(captured),
        protected_child_sha256=receipt_digest(child),
        semantic_sha256="c" * 64,
        row_count=4,
        source_authority=source_generation(0, None),
        source_revision_tracked=False,
        adopted_at="2026-10-17T05:00:00.000000Z",
    )
    marker_by_field["baseline_sha256"] = receipt_digest(marker_by_field)
    captured["parent"]["baseline"] = {"database_oid": 100, "comment": marker_by_field}
    return captured


def test_preserved_legacy_indexes_require_exact_native_owner_marker():
    captured = legacy_parent_catalog()
    assert native.validate_parent_catalog(captured)["child_oid"] == 202
    captured["parent"]["baseline"]["comment"] = None
    with pytest.raises(native.NuccPartitionError):
        native.validate_parent_catalog(captured)


@pytest.mark.parametrize(
    "path,replacement",
    [
        (("children", 0, "relation", "oid"), 203),
        (("children", 0, "relation", "indexes", 1, "oid"), 999),
        (("parent", "baseline", "comment", "protected_native_sha256"), "f" * 64),
        (("parent", "baseline", "database_oid"), 101),
        (("parent", "baseline", "comment", "source_authority", "local_generation"), 1),
        (("children", 0, "relation", "indexes", 1, "columns"), ["display_name"]),
    ],
)
def test_legacy_baseline_refuses_unbound_native_shape(path, replacement):
    captured = legacy_parent_catalog()
    field = captured
    for key in path[:-1]:
        field = field[key]
    field[path[-1]] = replacement
    with pytest.raises((native.NuccPartitionError, ValueError)):
        native.validate_parent_catalog(captured)


def test_retained_old_two_indexes_are_never_new_incoming_model_indexes():
    captured = legacy_parent_catalog()
    child = deepcopy(captured["children"][0]["relation"])
    child.update(is_partition=False, bound=None)
    child["indexes"][0]["parents"] = []
    with pytest.raises(native.NuccPartitionError, match="model indexes"):
        native.validate_detached_child(child, owner_oid=101, expected_oid=202)
    assert native.validate_retained_baseline_child(child, captured) == child
    child["indexes"][1]["oid"] += 1
    with pytest.raises(native.NuccPartitionError):
        native.validate_retained_baseline_child(child, captured)


def test_baseline_digest_excludes_only_the_native_parent_comment():
    from process.nucc_protected_control import receipt_digest

    captured = legacy_parent_catalog()
    expected = deepcopy(captured)
    expected["parent"].pop("baseline")
    assert native.catalog_digest(captured) == receipt_digest(expected)
    captured["parent"]["acl"].append([104, 101, "SELECT", False])
    assert native.catalog_digest(captured) != receipt_digest(expected)


def original_baseline_rollback_catalogs():
    baseline = legacy_parent_catalog()
    current = parent_catalog()
    current["parent"]["baseline"] = deepcopy(baseline["parent"]["baseline"])
    current_child = current["children"][0]["relation"]
    current_child.update(oid=203, schema="reference_family_archive_" + "b" * 32, schema_oid=303, filenode=603)
    for index in current_child["indexes"]:
        index["oid"] += 1000
    old_child = deepcopy(baseline["children"][0]["relation"])
    old_child.update(is_partition=False, bound=None)
    old_child["indexes"][0]["parents"] = []
    return baseline, current, old_child


@pytest.mark.asyncio
async def test_exchange_restores_only_the_original_marker_bound_two_index_child(monkeypatch):
    from process import entity_address_native_publication as publication
    from process import entity_address_snapshot_preparation as preparation
    from process import reference_family_archive as reference

    baseline, current, old_child = original_baseline_rollback_catalogs()
    before = native.validate_parent_catalog(current)
    after = native.validate_parent_catalog(baseline)
    events = []

    class Session:
        async def scalar(self, statement, parameters=None):
            sql = str(statement)
            if sql == native.CHILD_CATALOG_SQL:
                assert parameters == {"child_oid": 202}
                events.append("original-child-catalog")
                return deepcopy(old_child)
            if sql == native.CATALOG_SQL:
                events.append("native-parent-marker")
                return deepcopy(current)
            if "pg_inherits" in sql:
                return False
            assert sql == "SELECT pg_current_xact_id()::text"
            return "42"

        async def execute(self, statement):
            events.append(str(statement))

    async def capture(_session, *, expected_child_oid, exclusive):
        assert exclusive is True
        return before if expected_child_oid == 203 else after

    async def owner(_session):
        return 101

    async def execution(_session, captured, *, allowed_extensions):
        assert captured == {"stage_relations": [{"relation_oid": 202}]}
        assert allowed_extensions == ("btree_gin",)

    async def readers(_session, incoming):
        assert incoming == old_child
        events.append("readers")

    @asynccontextmanager
    async def search_path(_session):
        yield

    monkeypatch.setattr(native, "capture_nucc_parent", capture)
    monkeypatch.setattr(preparation, "_publisher_authority", owner)
    monkeypatch.setattr(publication, "_require_stage_execution_catalog", execution)
    monkeypatch.setattr(native, "_preserve_parent_readers", readers)
    monkeypatch.setattr(reference, "_nucc_catalog_search_path", search_path)
    restored = await native._exchange_nucc_child(Session(), 203, 202)
    assert restored["before"] == before and restored["after"] == after
    assert events.count("original-child-catalog") >= 2 and events.count("native-parent-marker") >= 2
    assert events.index("native-parent-marker") < events.index("readers")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "target,path,replacement",
    [
        ("child", ("oid",), 204),
        ("child", ("owner",), 104),
        ("child", ("indexes", 1, "oid"), 999),
        ("parent", ("parent", "baseline", "database_oid"), 999),
        ("parent", ("parent", "baseline", "comment"), None),
        ("child", ("acl",), [[101, 101, "SELECT", False], [104, 101, "SELECT", False]]),
    ],
)
async def test_original_rollback_refuses_same_name_or_changed_native_identity(target, path, replacement):
    _baseline, current, old_child = original_baseline_rollback_catalogs()
    field = current if target == "parent" else old_child
    for key in path[:-1]:
        field = field[key]
    field[path[-1]] = replacement

    class Session:
        async def scalar(self, statement, parameters=None):
            if str(statement) == native.CHILD_CATALOG_SQL:
                assert parameters == {"child_oid": 202}
                return old_child
            assert str(statement) == native.CATALOG_SQL
            return current

    with pytest.raises((native.NuccPartitionError, ValueError)):
        await native._capture_exchange_target(Session(), 202, 101)


@pytest.mark.asyncio
async def test_exchange_rechecks_complete_parent_catalog_after_original_child_lock(monkeypatch):
    from process import entity_address_native_publication as publication
    from process import entity_address_snapshot_preparation as preparation
    from process import reference_family_archive as reference

    _baseline, current, old_child = original_baseline_rollback_catalogs()
    statements = []

    class Session:
        reads = 0

        async def scalar(self, statement, parameters=None):
            sql = str(statement)
            if sql == native.CHILD_CATALOG_SQL:
                return old_child
            if sql == native.CATALOG_SQL:
                self.reads += 1
                captured = deepcopy(current)
                if self.reads > 1:
                    captured["parent"]["acl"].append([104, 101, "SELECT", False])
                return captured
            assert "pg_inherits" in sql
            return False

        async def execute(self, statement):
            statements.append(str(statement))

    async def parent(*_args, **_options):
        return native.validate_parent_catalog(current)

    async def owner(_session):
        return 101

    async def execution(_session, captured, *, allowed_extensions):
        assert captured == {"stage_relations": [{"relation_oid": 202}]}
        assert allowed_extensions == ("btree_gin",)

    @asynccontextmanager
    async def search_path(_session):
        yield

    monkeypatch.setattr(native, "capture_nucc_parent", parent)
    monkeypatch.setattr(preparation, "_publisher_authority", owner)
    monkeypatch.setattr(publication, "_require_stage_execution_catalog", execution)
    monkeypatch.setattr(reference, "_nucc_catalog_search_path", search_path)
    with pytest.raises(native.NuccPartitionError, match="incoming child changed"):
        await native._exchange_nucc_child(Session(), 203, 202)
    assert len(statements) == 1 and "ACCESS EXCLUSIVE" in statements[0]
    assert not any("ALTER TABLE" in sql for sql in statements)


@pytest.mark.parametrize("fault", ["oversized_transaction_id", "invalid_registration_shape"])
def test_native_baseline_catalog_reports_typed_refusal(fault):
    from process import nucc_protected_bootstrap as bootstrap
    from process.nucc_protected_control import receipt_digest
    from tests.test_nucc_protected_bootstrap import native_registration

    captured = legacy_parent_catalog()
    marker = captured["parent"]["baseline"]["comment"]
    marker.update(contract=bootstrap.NATIVE_CONTRACT, native_registration=native_registration())
    if fault == "oversized_transaction_id":
        marker["native_registration"]["mutation_xid"] = "1" * 5000
    else:
        marker["native_registration"] = []
    marker["baseline_sha256"] = receipt_digest(
        {key: field for key, field in marker.items() if key != "baseline_sha256"}
    )
    with pytest.raises(native.NuccPartitionError, match="baseline differs"):
        native.validate_parent_catalog(captured)
