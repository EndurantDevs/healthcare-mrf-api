# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Normal-import checks for cross-family protected legacy graph pages."""

from __future__ import annotations

import json
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace

import pytest

from process.custom_import import legacy_family_store as store
from process.custom_import import runner_graph as graph
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family import RootFamily, assemble_root_families
from process.custom_import.runner_codec import (
    child_key_document,
    child_key_hash,
    digest_text,
    family_child_payload_hashes,
    new_family_hash,
    record_payload,
    root_key_document,
    root_key_hash,
    root_payload_hash,
)
from process.custom_import.runner_types import CandidateRunnerError, StoredCandidateChild, StoredCandidateFamily
from tests.test_custom_import_legacy_graph_store import _bind, _grant, _registry, _request, _Session


class _GraphSession(_Session):
    def __init__(self, request):
        super().__init__(request)
        self.root_ids = {}
        self.entity_ids = {}
        self.next_revision = 30_000
        self.next_family = 40_000
        self.next_child = 50_000
        self.fail_kind = None
        self.fail_page = None
        self.kind_counts = {}
        self.bad_identity = False

    async def execute(self, statement, parameters=None):
        sql = str(statement)
        kinds = ("identity", "family_root", "child")
        kind = next((kind for kind in kinds if f"persist_custom_import_legacy_{kind}_set" in sql), None)
        if kind is None:
            return await super().execute(statement, parameters)
        self.calls.append((sql, parameters))
        self.kind_counts[kind] = self.kind_counts.get(kind, 0) + 1
        if self.fail_kind == kind and self.kind_counts[kind] == self.fail_page:
            raise RuntimeError("synthetic family page failure")
        if self.bad_identity:
            return SimpleNamespace(scalar_one=lambda: [True])
        returned_ids = self._page_ids(kind, parameters)
        return SimpleNamespace(scalar_one=lambda: returned_ids)

    def _page_ids(self, kind, parameters):
        if kind == "identity":
            returned_ids = []
            for digest, entity, expected_root, expected_entity in zip(
                parameters["p10"],
                parameters["p11"],
                parameters["p13"],
                parameters["p14"],
                strict=True,
            ):
                root_id = self.root_ids.setdefault(digest, expected_root or 10_000 + len(self.root_ids))
                entity_id = self.entity_ids.setdefault(entity, expected_entity or 20_000 + len(self.entity_ids))
                returned_ids.extend((root_id, entity_id))
        elif kind == "family_root":
            returned_ids = []
            for revision, family in zip(parameters["p8"], parameters["p9"], strict=True):
                self.next_revision += 1
                self.next_family += 1
                returned_ids.extend((revision or self.next_revision, family or self.next_family))
        else:
            returned_ids = []
            for revision in parameters["p8"]:
                self.next_child += 1
                returned_ids.append(revision or self.next_child)
        return returned_ids


def _case_request():
    request = _request()
    document = json.loads(request.definition.canonical)
    document["schema"]["root"]["logical_key"] = ["root_id"]
    document["schema"]["root"]["fields"].append(
        {
            "id": "root_id",
            "slot": 6,
            "type": "string",
            "nullable": False,
        }
    )
    document["schema"]["children"][0]["parent_key"] = [{"child": "rate_npi", "root": "root_id"}]
    return replace(request, definition=CustomImportDefinition.from_json(json.dumps(document)))


def _family(index, children=2):
    key = str(index)
    return RootFamily(
        root_key=(key,),
        root={"npi": "1234567893", "display_name": "Synthetic", "root_id": key},
        children={
            "rates": tuple(
                {"rate_npi": key, "service_code": str(value), "amount": Decimal("2.50")}
                for value in reversed(range(children))
            )
        },
    )


def _packs(request, session, selected):
    root_hashes = [root_payload_hash(request.definition, family) for family in selected]
    child_hashes_by_collection = {}
    for family in selected:
        for collection, digest in family_child_payload_hashes(request.definition, family):
            child_hashes_by_collection.setdefault(collection, []).append(digest)
    packs = graph.pack_models(request, _grant(session), _registry(), 51, root_hashes, child_hashes_by_collection)
    packs[None].pack_id = 71
    packs["rates"].pack_id = 72
    return packs


def _calls(session, kind):
    return [parameters for sql, parameters in session.calls if f"persist_custom_import_legacy_{kind}_set" in sql]


async def _publish(selected):
    request = _case_request()
    admitted = assemble_root_families(
        request.definition,
        [family.root for family in selected],
        {
            "rates": [child for family in selected for child in family.children["rates"]],
        },
    )
    assert not admitted.rejections and not admitted.candidate_errors and len(admitted.families) == len(selected)
    session = _GraphSession(request)
    await _bind(session, request)
    result = await graph.publish_families(
        session, request, _grant(session), _registry(), _packs(request, session, selected), selected
    )
    return request, session, result


@pytest.mark.asyncio
async def test_fresh_family_uses_unchanged_codecs_and_child_order():
    selected = (_family(0),)
    request, session, result = await _publish(selected)
    [family] = result
    [identity] = _calls(session, "identity")
    [root] = _calls(session, "family_root")
    [children] = _calls(session, "child")
    assert identity["p15"] == root_key_document(request.definition, selected[0].root)
    assert identity["p10"] == (root_key_hash(request.definition, selected[0].root),)
    payload = record_payload(request.definition.root_fields, selected[0].root)
    assert root["p19"] == payload and root["p15"] == (digest_text("root-payload", payload),)
    assert root["p16"] == (new_family_hash(request.definition, selected[0]),)
    assert root["p13"] == (0,) and root["p18"] == (None,)
    ordered = sorted(selected[0].children["rates"], key=lambda row: child_key_hash(request.definition, "rates", row))
    assert children["p13"] == (0, 1)
    assert children["p16"] == tuple(child_key_document(request.definition, "rates", row) for row in ordered)
    assert [dict(row.values_by_field) for row in family.children] == ordered
    assert family.root_values_by_field == selected[0].root and family.root_values_by_field is not selected[0].root
    assert children["p10"] == (family.root_record_id,) * 2 and children["p9"] == (family.family_revision_id,) * 2
    assert session.flushes == 1 and "check_custom_import_materialization_authority" in session.calls[-1][0]


@pytest.mark.asyncio
async def test_pages_cross_families_and_preserve_global_source_ordinals():
    selected_families = tuple(_family(index) for index in range(257))
    _, session, result = await _publish(selected_families)
    assert [len(page["p8"]) for page in _calls(session, "identity")] == [64, 64, 64, 64, 1]
    assert [len(page["p8"]) for page in _calls(session, "family_root")] == [64, 64, 64, 64, 1]
    assert [len(page["p8"]) for page in _calls(session, "child")] == [85, 85, 85, 85, 85, 85, 4]
    assert tuple(value for page in _calls(session, "family_root") for value in page["p13"]) == tuple(range(257))
    assert tuple(value for page in _calls(session, "child") for value in page["p13"]) == tuple(range(514))
    assert len(result) == 257 and all(len(family.children) == 2 for family in result)
    assert len({child.child_revision_id for family in result for child in family.children}) == 514
    assert len({family.entity_binding_id for family in result}) == 1
    assert session.flushes == 1


@pytest.mark.asyncio
async def test_one_large_family_spans_child_pages_without_family_restart():
    _, session, [family] = await _publish((_family(0, children=300),))
    assert [len(page["p8"]) for page in _calls(session, "child")] == [85, 85, 85, 45]
    assert len(_calls(session, "family_root")) == 1 and len(family.children) == 300
    assert tuple(value for page in _calls(session, "child") for value in page["p13"]) == tuple(range(300))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("root_count", "child_count", "root_sizes", "child_sizes"),
    [(64, 0, [64], []), (65, 0, [64, 1], []), (1, 85, [1], [85]), (1, 86, [1], [85, 1])],
)
async def test_pages_split_at_native_root_and_child_row_bounds(root_count, child_count, root_sizes, child_sizes):
    selected_families = tuple(_family(index, children=child_count) for index in range(root_count))
    _, session, _ = await _publish(selected_families)
    assert [len(page["p8"]) for page in _calls(session, "identity")] == root_sizes
    assert [len(page["p8"]) for page in _calls(session, "family_root")] == root_sizes
    assert [len(page["p8"]) for page in _calls(session, "child")] == child_sizes


@pytest.mark.asyncio
@pytest.mark.parametrize("dominant_call", ["identity", "family_root"])
@pytest.mark.parametrize("budget_delta", [0, -1])
async def test_root_pages_include_sql_utf8_work_bytes(monkeypatch, dominant_call, budget_delta):
    definition = _case_request().definition
    selected, costs = [], []
    for index in range(2):
        source = _family("\u2603" * 256 + str(index) if dominant_call == "identity" else index, children=0)
        if dominant_call == "family_root":
            source = replace(source, root=source.root | {"display_name": "\u2603" * 256})
        selected.append(source)
        identity_cost = 1024 + 2 * (
            len(root_key_document(definition, source.root).encode()) + len(source.root["npi"].encode())
        )
        root_cost = 640 + len(record_payload(definition.root_fields, source.root).encode())
        assert (identity_cost > root_cost) == (dominant_call == "identity")
        costs.append(max(identity_cost, root_cost))
    assert store._PAGE_BYTES == 8 * 1024 * 1024
    monkeypatch.setattr(store, "_PAGE_BYTES", 1024 + sum(costs) + budget_delta)
    _, session, _ = await _publish(tuple(selected))
    expected = [2] if budget_delta == 0 else [1, 1]
    assert [len(page["p8"]) for page in _calls(session, "identity")] == expected
    assert [len(page["p8"]) for page in _calls(session, "family_root")] == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("budget_delta", [0, -1])
async def test_child_pages_include_sql_utf8_work_bytes(monkeypatch, budget_delta):
    definition = _case_request().definition
    source = _family(0)
    child_rows = tuple(row | {"service_code": "\u2603" * 64 + row["service_code"]} for row in source.children["rates"])
    source = replace(source, children={"rates": child_rows})
    parent_bytes = len(root_key_document(definition, source.root).encode())
    fields = store.fields_by_collection(definition)["rates"]
    work_bytes = 1024 + sum(
        640
        + parent_bytes
        + len(child_key_document(definition, "rates", row).encode())
        + len(record_payload(fields, row).encode())
        for row in child_rows
    )
    assert store._PAGE_BYTES == 8 * 1024 * 1024
    monkeypatch.setattr(store, "_PAGE_BYTES", work_bytes + budget_delta)
    _, session, _ = await _publish((source,))
    assert [len(page["p8"]) for page in _calls(session, "child")] == ([2] if budget_delta == 0 else [1, 1])


@pytest.mark.asyncio
async def test_page_models_retain_native_ids_for_exact_replay():
    request = _case_request()
    session = _GraphSession(request)
    await _bind(session, request)
    selected = (_family(0),)
    packs = _packs(request, session, selected)
    window = await store._authority(session)
    roots = list(store._root_rows(request, _grant(session), packs[None], selected))
    await store._persist_root_page(session, window, roots)
    ids = roots[0].revision.root_revision_id, roots[0].family.family_revision_id
    await store._persist_root_page(session, window, roots)
    last = _calls(session, "family_root")[-1]
    assert (last["p8"], last["p9"]) == ((ids[0],), (ids[1],))
    published = (
        store.PublishedCandidateFamily(
            roots[0].record.root_record_id,
            ids[0],
            ids[1],
            roots[0].binding.entity_binding_id,
            bytes(roots[0].family.family_sha256),
            selected[0].root,
            (),
        ),
    )
    child_rows = list(store._child_rows(request, _registry(), packs, selected, published))
    await store._persist_child_page(session, window, child_rows)
    child_ids = tuple(row.revision.child_revision_id for row in child_rows)
    await store._persist_child_page(session, window, child_rows)
    assert _calls(session, "child")[-1]["p8"] == child_ids


def _retained(request, session, source):
    packs = _packs(request, session, (source,))
    root_write = next(store._root_rows(request, _grant(session), packs[None], (source,)))
    root_write.record.root_record_id = 91
    root_write.binding.entity_binding_id = 92
    root_write.revision.root_revision_id = root_write.family.root_revision_id = 93
    root_write.revision.root_record_id = root_write.family.root_record_id = 91
    root_write.family.entity_binding_id = 92
    root_write.family.family_revision_id = 94
    root_write.family.producing_execution_id = 7
    root_write.family.producing_fence = 1
    root_write.family.producing_token_sha256 = b"b" * 32
    root_write.revision.definition_revision_id = 20
    root_write.revision.pack_id = 900
    family = store.PublishedCandidateFamily(91, 93, 94, 92, bytes(root_write.family.family_sha256), source.root, ())
    child_rows = list(store._child_rows(request, _registry(), packs, (source,), (family,)))
    for index, child in enumerate(child_rows):
        child.revision.child_revision_id = 95 + index
        child.revision.definition_revision_id = 20
        child.revision.pack_id = 901
    return StoredCandidateFamily(
        root_write.record,
        root_write.revision,
        root_write.family,
        root_write.binding,
        source.root,
        tuple(StoredCandidateChild(child.collection, child.revision, child.values) for child in child_rows),
    )


@pytest.mark.asyncio
async def test_retained_sources_keep_prior_lineage_but_new_copies_use_current_tuple():
    request = _case_request()
    session = _GraphSession(request)
    await _bind(session, request)
    retained = _retained(request, session, _family(1))
    selected = (retained, _family(2))
    result = await graph.publish_families(
        session, request, _grant(session), _registry(), _packs(request, session, selected), selected
    )
    [identity] = _calls(session, "identity")
    [root] = _calls(session, "family_root")
    [children] = _calls(session, "child")
    assert identity["p13"] == (91, None) and identity["p14"] == (92, None)
    assert root["p18"] == (94, None) and root["p3"] == request.execution_id and root["p5"] == 2
    assert root["p14"][0] == retained.root_revision.canonical_payload
    assert root["p12"] == (71, 71) and root["p13"] == (0, 1)
    assert children["p20"] == (94, 94, None, None) and children["p21"] == (95, 96, None, None)
    assert children["p12"] == (72,) * 4 and children["p13"] == (0, 1, 2, 3)
    assert result[0].root_values_by_field is retained.root_values_by_field
    assert result[0].children[0].values_by_field is retained.children[0].values_by_field
    assert retained.family.producing_execution_id == 7 and retained.family.producing_fence == 1


@pytest.mark.asyncio
async def test_oversized_key_and_payload_keep_same_set_entrypoints():
    key = "x" * store._PAGE_BYTES
    source = _family(key, children=0)
    request, session, families = await _publish((_family(0, children=0), source, _family(1, children=0)))
    identity_pages = _calls(session, "identity")
    root_pages = _calls(session, "family_root")
    assert [len(page["p8"]) for page in identity_pages] == [1, 1, 1]
    assert [len(page["p8"]) for page in root_pages] == [1, 1, 1]
    identity, root, family = identity_pages[1], root_pages[1], families[1]
    assert identity["p9"] == (None,) and len(identity["p15"].encode()) > store._PAGE_BYTES
    assert root["p14"] == (None,) and len(root["p19"].encode()) > store._PAGE_BYTES
    assert family.family_sha256 == new_family_hash(request.definition, source)
    assert _calls(session, "child") == []


@pytest.mark.asyncio
@pytest.mark.parametrize("oversized", [False, True])
async def test_child_singletons_preserve_opaque_canonical_text_and_native_text_domain(oversized):
    request = _case_request()
    document = json.loads(request.definition.canonical)
    document["schema"]["children"][0]["child_key"] = ["child_id"]
    document["schema"]["children"][0]["fields"].append(
        {
            "id": "child_id",
            "slot": 7,
            "type": "string",
            "nullable": False,
        }
    )
    request = replace(request, definition=CustomImportDefinition.from_json(json.dumps(document)))
    root_family = _family(0, children=1)
    child_values_by_field = dict(
        root_family.children["rates"][0], child_id="x" * store._PAGE_BYTES if oversized else "a\u0000\u2603"
    )
    root_family = replace(root_family, children={"rates": (child_values_by_field,)})
    admitted = assemble_root_families(request.definition, [root_family.root], root_family.children)
    assert not admitted.rejections and not admitted.candidate_errors and len(admitted.families) == 1
    session = _GraphSession(request)
    await _bind(session, request)
    [family] = await graph.publish_families(
        session, request, _grant(session), _registry(), _packs(request, session, (root_family,)), (root_family,)
    )
    [page] = _calls(session, "child")
    assert page["p14"] == page["p16"] == page["p18"] == (None,)
    assert page["p22"] == root_key_document(request.definition, root_family.root)
    assert page["p23"] == child_key_document(request.definition, "rates", child_values_by_field)
    fields = store.fields_by_collection(request.definition)["rates"]
    assert page["p24"] == record_payload(fields, child_values_by_field)
    assert family.children[0].values_by_field == child_values_by_field
    if oversized:
        assert len(page["p23"].encode()) > store._PAGE_BYTES
    else:
        assert "\\u0000" in page["p23"] and "\u0000" not in page["p23"]


@pytest.mark.asyncio
async def test_middle_child_page_failure_escapes_whole_caller_transaction():
    request = _case_request()
    session = _GraphSession(request)
    await _bind(session, request)
    session.fail_kind, session.fail_page = "child", 2
    selected = (_family(0, children=300),)
    with pytest.raises(RuntimeError, match="synthetic family page failure"):
        await graph.publish_families(
            session, request, _grant(session), _registry(), _packs(request, session, selected), selected
        )
    assert session.kind_counts == {"identity": 1, "family_root": 1, "child": 2}
    assert "check_custom_import_materialization_authority" not in session.calls[-1][0]
    assert not any(hasattr(session, name) for name in ("add", "add_all", "begin", "commit", "rollback"))


@pytest.mark.asyncio
async def test_empty_graph_checks_authority_and_internal_direct_writers_are_removed():
    _, session, result = await _publish(())
    assert result == () and session.kind_counts == {}
    assert "check_custom_import_materialization_authority" in session.calls[-1][0]
    for name in (
        "publish_new_family",
        "copy_stored_family",
        "create_root_revision",
        "copy_root_revision",
        "create_family_revision",
        "publish_new_children",
        "copy_stored_children",
        "create_child_revision",
        "copy_child_revision",
        "add_family_child_membership",
        "root_record_for_values",
        "entity_binding_for_values",
    ):
        assert not hasattr(graph, name)


@pytest.mark.asyncio
async def test_wrong_authority_or_invalid_native_identity_cannot_fall_back():
    request = _case_request()
    session = _GraphSession(request)
    await _bind(session, request)
    selected = (_family(0),)
    with pytest.raises(CandidateRunnerError, match="authority differs"):
        await graph.publish_families(
            session,
            request,
            replace(_grant(session), execution_id=99),
            _registry(),
            _packs(request, session, selected),
            selected,
        )
    assert session.flushes == 0 and session.kind_counts == {}
    session.bad_identity = True
    with pytest.raises(CandidateRunnerError, match="returned identity differs"):
        await graph.publish_families(
            session, request, _grant(session), _registry(), _packs(request, session, selected), selected
        )
    assert "family_root" not in session.kind_counts


@pytest.mark.asyncio
@pytest.mark.parametrize("entity", [None, 1234567893])
async def test_nontext_entity_prevents_family_set_calls(entity):
    request = _case_request()
    session = _GraphSession(request)
    await _bind(session, request)
    source = _family(0)
    packs = _packs(request, session, (source,))
    source = replace(source, root=source.root | {"npi": entity})
    with pytest.raises(CandidateRunnerError, match="no string entity value"):
        await graph.publish_families(session, request, _grant(session), _registry(), packs, (source,))
    assert session.kind_counts == {}
    assert "check_custom_import_materialization_authority" not in session.calls[-1][0]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "duplicate", "message"),
    [
        ("identity", (2, 0), "root identities repeat"),
        ("family_root", (2, 0), "root revision identities repeat"),
        ("family_root", (3, 1), "root revision identities repeat"),
        ("child", (1, 0), "child returned identity differs"),
    ],
)
async def test_duplicate_native_ids_stop_family_writes(monkeypatch, kind, duplicate, message):
    request = _case_request()
    session = _GraphSession(request)
    await _bind(session, request)
    native_ids = session._page_ids

    def duplicate_ids(page_kind, parameters):
        returned_ids = native_ids(page_kind, parameters)
        if page_kind == kind:
            returned_ids[duplicate[0]] = returned_ids[duplicate[1]]
        return returned_ids

    monkeypatch.setattr(session, "_page_ids", duplicate_ids)
    selected = (_family(0, children=1), _family(1, children=1))
    with pytest.raises(CandidateRunnerError, match=message):
        await graph.publish_families(
            session, request, _grant(session), _registry(), _packs(request, session, selected), selected
        )
    assert list(session.kind_counts)[-1] == kind and session.kind_counts[kind] == 1
    assert "check_custom_import_materialization_authority" not in session.calls[-1][0]


@pytest.mark.asyncio
@pytest.mark.parametrize("identity", ["root", "entity"])
async def test_retained_identity_drift_stops_before_new_revisions(identity):
    request = _case_request()
    session = _GraphSession(request)
    await _bind(session, request)
    retained = _retained(request, session, _family(0))
    if identity == "root":
        session.root_ids[retained.root_record.logical_key_sha256] = 101
    else:
        session.entity_ids[retained.entity_binding.canonical_value] = 102
    with pytest.raises(CandidateRunnerError, match="retained identity differs"):
        await graph.publish_families(
            session, request, _grant(session), _registry(), _packs(request, session, (retained,)), (retained,)
        )
    assert session.kind_counts == {"identity": 1}
    assert retained.root_record.root_record_id == 91 and retained.entity_binding.entity_binding_id == 92


@pytest.mark.asyncio
@pytest.mark.parametrize("index", [0, 1])
async def test_replayed_root_ids_cannot_be_replaced(monkeypatch, index):
    request = _case_request()
    session = _GraphSession(request)
    await _bind(session, request)
    selected = (_family(0),)
    packs = _packs(request, session, selected)
    window = await store._authority(session)
    roots = list(store._root_rows(request, _grant(session), packs[None], selected))
    await store._persist_root_page(session, window, roots)
    original_ids = (roots[0].revision.root_revision_id, roots[0].family.family_revision_id)
    native_ids = session._page_ids

    def changed_ids(kind, parameters):
        returned_ids = native_ids(kind, parameters)
        if kind == "family_root":
            returned_ids[index] += 1
        return returned_ids

    monkeypatch.setattr(session, "_page_ids", changed_ids)
    with pytest.raises(CandidateRunnerError, match="root revision returned identity differs"):
        await store._persist_root_page(session, window, roots)
    assert (roots[0].revision.root_revision_id, roots[0].family.family_revision_id) == original_ids
    assert session.kind_counts == {"identity": 2, "family_root": 2}
