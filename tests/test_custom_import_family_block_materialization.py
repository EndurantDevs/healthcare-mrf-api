# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Canonical row/hash input vectors and interruptible full-family proof."""

import asyncio
import inspect
import itertools
import random
import struct
import threading
import time
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import inspect as sql_inspect
from sqlalchemy.dialects import postgresql

from db.models import custom_import as models
from process.custom_import import family_block_materialization as adapter
from process.custom_import import publication, runner_codec
from process.custom_import.family import RootFamily
from process.custom_import.family_block_binding import FamilyBlockBinding
from process.custom_import.family_block_codec import encode_family, encode_retained_family, verify_family
from process.custom_import.family_block_directory import iter_revision_memberships, storage_envelope_sha256
from process.custom_import.family_block_records import SparseRecord, record_parts
from process.custom_import.family_block_storage_codec import (
    iter_fragment_directory,
    stored_manifest,
    verify_stored_family,
)
from process.custom_import.materialization import TypedScalar
from tests.test_custom_import_family_block_storage import _storage_case


def _rows():
    case = _storage_case()
    layout, _limits, root, children, _frames, _directory, _members, manifest = case
    binding = FamilyBlockBinding(19, layout, layout.tables(19), {}, 7)
    generation = SimpleNamespace(
        dataset_id=1,
        definition_revision_id=2,
        schema_revision_id=3,
        execution_id=4,
        producing_fence=5,
        producing_token_sha256=b"t" * 32,
        generation_id=11,
        capture_bundle_id=19,
    )
    root_identity = models.CustomImportRootRecord(
        root_record_id=23,
        dataset_id=1,
        key_contract_sha256=runner_codec.root_key_contract_hash(layout.definition),
        logical_key_sha256=runner_codec.digest_text(
            "root-key", runner_codec.root_key_document(layout.definition, root.values_by_field)
        ),
        canonical_logical_key=runner_codec.root_key_document(layout.definition, root.values_by_field),
    )
    family = models.CustomImportFamilyRevision(
        family_revision_id=29,
        root_record_id=23,
        root_revision_id=19,
        entity_binding_id=31,
        family_sha256=manifest.family_sha256,
        child_count=len(children),
    )
    member = models.CustomImportGenerationFamily(generation_id=11, root_record_id=23, family_revision_id=29)
    entity = models.CustomImportEntityBinding(
        entity_binding_id=31, dataset_id=1, adapter_id="npi", canonical_value="1234567893", value_sha256=b"e" * 32
    )
    return case, binding, generation, root_identity, family, member, entity


def test_reconstructed_root_and_child_keep_original_v1_hash_documents():
    case, binding, generation, root_identity, family, member, entity = _rows()
    root = case[2]
    expected_root = models.CustomImportRootRevision(
        source_ordinal=7,
        canonical_payload=runner_codec.record_payload(binding.layout.definition.root_fields, root.values_by_field),
        payload_sha256=runner_codec.root_payload_hash(
            binding.layout.definition, RootFamily(("1234567893",), root.values_by_field, {})
        ),
    )
    actual_root = adapter.logical_root(binding, generation, root_identity, root)
    assert publication._materialization_document(actual_root) == publication._materialization_document(expected_root)
    assert publication._effective_output_revision_document(
        actual_root
    ) == publication._effective_output_revision_document(expected_root)
    for effective, framed in enumerate(adapter.family_frames((member, family, actual_root, root_identity, entity))):
        original = adapter._CapturedDigest()
        publication._add_family_revision_material(
            original, (member, family, expected_root, root_identity, entity), {}, effective_output=bool(effective)
        )
        assert framed == original.bytes()
    for child in case[3]:
        actual = adapter.logical_child(binding, generation, root_identity, "rates", child)
        expected = models.CustomImportChildRevision(
            collection_slot=7,
            source_ordinal=child.source_ordinal,
            canonical_parent_key=root_identity.canonical_logical_key,
            parent_key_sha256=root_identity.logical_key_sha256,
            canonical_child_key=runner_codec.child_key_document(
                binding.layout.definition, "rates", child.values_by_field
            ),
            child_key_sha256=runner_codec.child_key_hash(binding.layout.definition, "rates", child.values_by_field),
            canonical_payload=runner_codec.record_payload(
                binding.layout.definition.child_fields, child.values_by_field
            ),
            payload_sha256=runner_codec.child_payload_hash(binding.layout.definition, "rates", child.values_by_field),
        )
        assert publication._materialization_document(actual) == publication._materialization_document(expected)


def test_numeric_order_encoding_matches_native_numbers_and_nulls_last():
    numbers = [Decimal(text) for text in ("-100", "-1.21", "-1.2", "-1", "-0.00", "0", "0.01", "1.2", "1.21", "100")]
    assert sorted(reversed(numbers), key=adapter._order_cell) == numbers
    assert sorted([None, *numbers], key=adapter._order_cell)[-1] is None


@pytest.mark.parametrize("phase", ["_verify_records", "_objects", "storage_envelope_sha256", "membership"])
def test_full_proof_callback_interrupts_actual_pass_and_closes_every_spool(monkeypatch, phase):
    case = _storage_case(fragment_bytes=37)
    opened_spools = []
    import tempfile

    original = tempfile.SpooledTemporaryFile

    def tracked(*args, **kwargs):
        scratch_file = original(*args, **kwargs)
        opened_spools.append(scratch_file)
        return scratch_file

    class Interrupted(Exception):
        pass

    def check():
        frame = inspect.currentframe().f_back
        if frame.f_code.co_name == phase or (
            phase == "membership"
            and frame.f_code.co_name == "_verify_membership_identities"
            and "revision_id" in frame.f_locals
        ):
            raise Interrupted(phase)

    monkeypatch.setattr(tempfile, "SpooledTemporaryFile", tracked)
    layout, limits, _root, _children, frames, directory, members, manifest = case
    with pytest.raises(Interrupted):
        verify_stored_family(
            layout.definition,
            layout.collection_slots,
            zip(directory, frames, strict=True),
            members,
            manifest,
            limits=limits,
            check=check,
        )
    assert opened_spools and all(scratch_file.closed for scratch_file in opened_spools)


def test_noop_callback_keeps_exact_storage_and_semantic_hash_vectors():
    case = _storage_case(fragment_bytes=37)
    layout, limits, root, children, frames, directory, members, manifest = case
    calls = []
    check = lambda: calls.append(True)
    with verify_stored_family(
        layout.definition,
        layout.collection_slots,
        zip(directory, frames, strict=True),
        members,
        manifest,
        limits=limits,
        check=check,
    ) as verified:
        assert list(verified.records()) == [root]
        assert list(verified.records("rates")) == children
        assert verified.stored_manifest == manifest
    assert len(calls) > len(children)


class _AsyncRows:
    def __init__(self, entries):
        self.entries = iter(entries)
        self.closed = False

    def mappings(self):
        return self

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.entries)
        except StopIteration:
            raise StopAsyncIteration from None

    async def close(self):
        self.closed = True


def _native_fragment_entries(case):
    fragments = []
    for directory, frame in zip(case[5], case[4], strict=True):
        fragments.append(
            dict(
                family_revision_id=29,
                **dict(
                    zip(
                        (
                            "fragment_ordinal",
                            "raw_bytes",
                            "first_record_ordinal",
                            "starting_record_count",
                            "first_record_offset",
                            "continuation_bytes",
                        ),
                        directory,
                        strict=True,
                    )
                ),
                encoded_fragment=frame,
            )
        )
    return fragments


def _native_membership_entries(case):
    memberships = [
        dict(
            family_revision_id=29,
            **dict(
                zip(
                    (
                        "collection_slot",
                        "first_revision_id",
                        "last_revision_id",
                        "record_count",
                        "first_record_ordinal",
                        "encoded_membership",
                    ),
                    membership.cells(),
                    strict=True,
                )
            ),
        )
        for membership in case[6]
    ]
    return memberships


class _ProvisionalSession:
    """Explicit data/cursor fake; no native custody, proof or publication claim."""

    def __init__(self):
        from process.custom_import.materialization import DefinitionIdentity, selection_profile_models

        self.case, self.binding, self.generation, self.root, self.family, self.member, self.entity = _rows()
        self.info, self.results = {}, []
        self.transaction = SimpleNamespace(is_active=True)
        self.closed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.descriptor = SimpleNamespace(phase="closed", closed_at=self.closed_at)
        self.family_row = {name: None for name in self.binding.tables_by_role["family"].c.keys()}
        self.family_row.update(vars(self.case[-1]))
        self.family_row.update(family_revision_id=29, root_record_id=23, root_revision_id=19, entity_binding_id=31)
        self.fragments = _native_fragment_entries(self.case)
        self.memberships = _native_membership_entries(self.case)
        self.profiles = selection_profile_models(
            self.binding.layout.definition, identity=DefinitionIdentity(1, 2, 3), child_collection_slots={"rates": 7}
        )
        self.packs = [
            models.CustomImportPack(
                pack_id=identifier, stream_slot=slot, pack_ordinal=1, record_count=count, pack_sha256=b"p" * 32
            )
            for identifier, slot, count in ((31, 1, 1), (41, 2, len(self.case[3])))
        ]
        self.winner = models.CustomImportWinner(
            generation_id=11,
            dataset_id=1,
            definition_revision_id=2,
            schema_revision_id=3,
            profile_slot=1,
            entity_binding_id=31,
            family_revision_id=29,
            context_collection_slot=7,
            context_child_revision_id=self.case[3][0].revision_id,
            context_key_sha256=b"w" * 32,
        )
        self.winner_member = self.member

    def get_transaction(self):
        return self.transaction

    async def get(self, model, identity, *, populate_existing=False):
        from db.models.custom_import_family_block import CustomImportFamilyBlockLayout

        assert model is CustomImportFamilyBlockLayout and identity == 19
        assert populate_existing is True
        return self.descriptor

    def _result(self, entries):
        result = _AsyncRows(entries)
        self.results.append(result)
        return result

    async def stream(self, statement):
        sql = str(statement.compile(dialect=postgresql.dialect()))
        if "custom_import_block_fragment" in sql:
            return self._result(
                [
                    {name: entry[name] for name in self.binding.tables_by_role["fragment"].c.keys()}
                    for entry in self.fragments
                ]
            )
        if "custom_import_block_revision_run" in sql:
            return self._result(
                [
                    {name: entry[name] for name in self.binding.tables_by_role["revision_run"].c.keys()}
                    for entry in self.memberships
                ]
            )
        columns = tuple(self.family_row[name] for name in self.binding.tables_by_role["family"].c.keys())
        if "custom_import_winner" in sql:
            return self._result([(self.winner, self.entity, self.root, self.profiles[0], self.winner_member, *columns)])
        return self._result([(self.root, self.entity, self.member, *columns)])

    async def stream_scalars(self, statement):
        kind = sql_inspect(statement.column_descriptions[0]["entity"]).mapper.class_
        if kind is models.CustomImportPack:
            return self._result(self.packs)
        if kind is models.CustomImportRejection:
            return self._result([])
        assert kind is models.CustomImportSelectionProfile
        return self._result(self.profiles)


@pytest.mark.asyncio
async def test_provisional_reconstruction_exposes_all_counts_and_exact_original_sections(tmp_path):
    session = _ProvisionalSession()
    registered_roots = []
    limits = adapter.SourceFactLimits(32 * 1024**2, 2 * 1024**2, 2048)
    root = tmp_path / "materialization"
    with adapter.materialization_work(session, root, limits, registered_roots.append):
        async with adapter.reconstruct_sections(session, session.binding, session.generation) as sections:
            assert tuple(sections.counts[name] for name in adapter._COUNTS) == (1, 1, 1, 7, 1, 1, 0, 0)
            digests = [publication._new_digest(domain) for domain in ("materialization/v1", "effective-output/v1")]
            sections.hash_into(digests)
            expected_digests = [
                publication._new_digest(domain) for domain in ("materialization/v1", "effective-output/v1")
            ]
            root_revision = adapter.logical_root(session.binding, session.generation, session.root, session.case[2])
            for effective, digest in enumerate(expected_digests):
                publication._add_family_revision_material(
                    digest,
                    (session.member, session.family, root_revision, session.root, session.entity),
                    {},
                    effective_output=bool(effective),
                )
                for child_record in session.case[3]:
                    child = adapter.logical_child(
                        session.binding, session.generation, session.root, "rates", child_record
                    )
                    edge = models.CustomImportFamilyChild(family_revision_id=29, collection_slot=7)
                    publication._validate_and_add_family_child(
                        digest,
                        (session.member, edge, child, session.root),
                        {29: 7},
                        {},
                        set(),
                        effective_output=bool(effective),
                    )
                child = adapter.logical_child(
                    session.binding, session.generation, session.root, "rates", session.case[3][0]
                )
                document = publication._materialization_document(session.winner) | {
                    "context_child_key_sha256": publication._json_value(child.child_key_sha256),
                    "family_sha256": publication._json_value(session.family.family_sha256),
                    "root_key_sha256": publication._json_value(session.root.logical_key_sha256),
                }
                publication._add_digest_record(digest, "winner", document)
                publication._add_digest_record(
                    digest, "winner_binding", publication._materialization_document(session.entity)
                )
            assert [digest.digest() for digest in digests] == [digest.digest() for digest in expected_digests]
    assert registered_roots == [root.absolute()] and not root.exists()
    assert session.results and all(cursor.closed for cursor in session.results)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["spool", "cancel", "count", "proof"])
async def test_provisional_data_never_substitutes_for_integrity_or_automated_acceptance(tmp_path, monkeypatch, damage):
    session = _ProvisionalSession()
    root = tmp_path / "materialization"
    is_cancelled = False

    def check():
        if is_cancelled:
            raise RuntimeError("owner operation cancelled")

    with adapter.materialization_work(
        session, root, adapter.SourceFactLimits(32 * 1024**2, 2 * 1024**2, 2048), lambda _root: None, check=check
    ):
        async with adapter.reconstruct_sections(session, session.binding, session.generation) as sections:
            match damage:
                case "spool":
                    spool = sections.rows_by_section["family"].file
                    spool.seek(-1, 2)
                    final = spool.read(1)
                    spool.seek(-1, 2)
                    spool.write(bytes((final[0] ^ 1,)))
                case "cancel":
                    is_cancelled = True
                case "count":
                    sections.counts["root_count"] += 1
            if damage == "proof":
                bound = adapter.BlockMaterializationModels(session.binding, session.closed_at)
                monkeypatch.setattr(publication, "_finality_materialization_models", AsyncMock(return_value=bound))
                session.execute = AsyncMock(return_value=SimpleNamespace(one_or_none=lambda: None))
            with pytest.raises((publication.PublicationConflict, RuntimeError)):
                if damage == "proof":
                    await adapter.finalize_sections(session, session.generation, sections)
                else:
                    sections.hash_into(
                        (publication._new_digest("materialization/v1"), publication._new_digest("effective-output/v1"))
                    )
    assert not root.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "manual", "contract", "producer", "frozen", "state"])
async def test_automated_proof_keeps_exact_original_binding_without_literal_page62(damage):
    generation = _rows()[2]
    frozen = datetime(2026, 1, 1, tzinfo=timezone.utc)
    build = SimpleNamespace(
        **vars(generation),
        build_id=7,
        phase="verified",
        verified_at=frozen,
        source_frozen_at=frozen,
        graph_frozen_at=frozen,
        output_frozen_at=frozen,
    )
    proof = SimpleNamespace(
        build_id=7,
        generation_id=generation.generation_id,
        verification_contract="custom-import/build-structure/v1",
        manual_acceptance=None,
        verification_state="complete",
        scan_stage="complete",
        page_sequence=211,
        verified_at=frozen,
        source_frozen_at=frozen,
        graph_frozen_at=frozen,
        output_frozen_at=frozen,
    )
    match damage:
        case "manual":
            proof.manual_acceptance = {"contract": "custom-import/manual-acceptance/v1"}
        case "contract":
            proof.verification_contract = "custom-import/manual-acceptance/v1"
        case "producer":
            build.producing_fence += 1
        case "frozen":
            proof.output_frozen_at = None
        case "state":
            proof.verification_state = "scanning"
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(one_or_none=lambda: (build, proof))))
    if damage is None:
        assert await adapter.automated_proof(session, generation) is proof
    else:
        with pytest.raises(publication.PublicationConflict):
            await adapter.automated_proof(session, generation)
    assert session.execute.await_args.args[0].get_execution_options()["populate_existing"] is True


def test_independent_scalar_missing_null_actual_kind_and_native_scale_are_preserved():
    case, binding, generation, root, _family, _member, _entity = _rows()
    assert list(adapter.logical_scalars(binding, generation, root.root_record_id, None, case[2])) == []
    record = replace(case[2], scalars={"npi": TypedScalar("integer", "null")})
    logical = list(adapter.logical_scalars(binding, generation, root.root_record_id, None, record))[0]
    assert logical.field_type == "integer" and logical.value_state == "null" and logical.integer_value is None
    record = replace(
        case[3][0], scalars={"amount": TypedScalar("decimal", "value", decimal_value=Decimal("-0.0000000000001"))}
    )
    logical = list(adapter.logical_scalars(binding, generation, root.root_record_id, "rates", record))[0]
    assert publication._materialization_document(logical)["decimal_value"] == "0.000000000000"
    assert record.scalars["amount"].decimal_value.as_tuple() == Decimal("-0.0000000000001").as_tuple()


def _retained_record(definition, collection, family_record, parent):
    _, (_, key, payload) = record_parts(definition, collection, family_record, retained=False)
    kind = "root" if collection is None else "child"
    payload = payload + " "
    if collection is not None:
        key += " "
    sparse = SparseRecord(
        payload,
        key,
        runner_codec.digest_text(f"{kind}-payload", payload),
        runner_codec.digest_text(f"{kind}-key", key),
        None if collection is None else parent,
        None if collection is None else runner_codec.digest_text("root-key", parent),
    )
    return replace(family_record, values_by_field=None, sparse=sparse)


def _vary_records(index, is_sparse):
    case, binding, generation, root_identity, family, member, entity = _rows()
    definition = binding.layout.definition
    npi = str(1234567893 + index)
    root = replace(
        case[2],
        revision_id=19 + index * 100,
        values_by_field={"npi": npi, "display_name": f"Clinic {index}"},
        scalars={
            "display_name": TypedScalar("string", "value", string_value=f"Clinic {index}"),
            "npi": TypedScalar("string", "value", string_value=npi),
        },
    )
    child_records = [
        replace(
            child,
            revision_id=child.revision_id + index * 100,
            values_by_field={**child.values_by_field, "rate_npi": npi},
            scalars={
                "service_code": TypedScalar("string", "value", string_value=child.values_by_field["service_code"]),
                "amount": TypedScalar("decimal", "value", decimal_value=Decimal(index)),
            },
        )
        for child in case[3]
    ]
    semantic = runner_codec.new_family_hash(
        definition,
        RootFamily((npi,), root.values_by_field, {"rates": tuple(child.values_by_field for child in child_records)}),
    )
    key = runner_codec.root_key_document(definition, root.values_by_field)
    if is_sparse:
        root = _retained_record(definition, None, root, key)
        child_records = [_retained_record(definition, "rates", child, key) for child in child_records]
    child_records.sort(key=lambda child: record_parts(definition, "rates", child, retained=is_sparse)[1])
    root_identity.root_record_id += index
    root_identity.canonical_logical_key, root_identity.logical_key_sha256 = (
        key,
        runner_codec.digest_text("root-key", key),
    )
    family.family_revision_id += index
    family.root_record_id, family.root_revision_id, family.family_sha256 = (
        root_identity.root_record_id,
        root.revision_id,
        semantic,
    )
    member.root_record_id, member.family_revision_id = root_identity.root_record_id, family.family_revision_id
    return case, binding, generation, root_identity, family, member, entity, root, child_records


@contextmanager
def _verified_order_fixture(index, is_sparse, *, has_duplicate_child=False):
    bundle = _vary_records(index, is_sparse)
    binding, family = bundle[1], bundle[4]
    root, children = bundle[-2:]
    layout, limits = binding.layout, bundle[0][1]
    if has_duplicate_child:
        children.insert(1, replace(children[0], revision_id=29999))
        family.child_count += 1
        family.family_sha256 = runner_codec.new_family_hash(
            layout.definition,
            RootFamily(
                (root.values_by_field["npi"],),
                root.values_by_field,
                {"rates": tuple(child.values_by_field for child in children)},
            ),
        )
    encoder = encode_retained_family if is_sparse else encode_family
    with encoder(layout.definition, root, {"rates": children}, family.family_sha256, limits=limits) as encoded:
        fragments, manifest = tuple(encoded.fragments()), encoded.manifest
    with verify_family(layout.definition, fragments, manifest, limits=limits) as verified:
        directory_entries = tuple(
            iter_fragment_directory(
                verified, layout.collection_slots, (struct.unpack("!QQ", fragment[:16])[1] for fragment in fragments)
            )
        )
    memberships = tuple(iter_revision_memberships(0, [(root.revision_id, 0)], chunk_records=3)) + tuple(
        iter_revision_memberships(
            7, sorted((child.revision_id, ordinal) for ordinal, child in enumerate(children, 1)), chunk_records=3
        )
    )
    manifest = stored_manifest(manifest, directory_entries, memberships)
    family_row = vars(manifest) | {
        name: getattr(family, name)
        for name in ("family_revision_id", "root_record_id", "root_revision_id", "entity_binding_id")
    }
    with verify_stored_family(
        layout.definition,
        layout.collection_slots,
        zip(directory_entries, fragments, strict=True),
        memberships,
        manifest,
        limits=limits,
    ) as verified:
        yield bundle, family_row, verified


def _original_order(order_specs, by_model):
    return adapter._ordered(*(getattr(by_model[attribute.class_], attribute.key) for attribute in order_specs))


def _original_graph_entries(bundle, verified):
    _case, binding, generation, root, family, member, entity, *_records = bundle
    root_revision = adapter.logical_root(binding, generation, root, next(verified.records()))
    by_model = {
        models.CustomImportRootRecord: root,
        models.CustomImportRootRevision: root_revision,
        models.CustomImportFamilyRevision: family,
        models.CustomImportEntityBinding: entity,
    }
    entries_by_section = {name: [] for name in ("family", "child", "root_scalar", "child_scalar")}
    entries_by_section["family"].append(
        (
            _original_order(publication._family_material_order(), by_model),
            adapter.family_frames((member, family, root_revision, root, entity)),
        )
    )
    _original_scalar_entries(entries_by_section, by_model, binding, generation, root, None, next(verified.records()))
    for child_record in verified.records("rates"):
        child = adapter.logical_child(binding, generation, root, "rates", child_record)
        edge = models.CustomImportFamilyChild(family_revision_id=family.family_revision_id, collection_slot=7)
        by_model.update({models.CustomImportChildRevision: child, models.CustomImportFamilyChild: edge})
        entries_by_section["child"].append(
            (
                _original_order(publication._family_child_material_order(), by_model),
                adapter.child_frames((member, edge, child, root), family.child_count),
            )
        )
        _original_scalar_entries(entries_by_section, by_model, binding, generation, root, "rates", child_record)
    return entries_by_section


def _original_scalar_entries(entries_by_section, by_model, binding, generation, root, collection, family_record):
    section = "root_scalar" if collection is None else "child_scalar"
    ordering = (
        publication._root_scalar_material_order if collection is None else publication._child_scalar_material_order
    )
    scalar_model = models.CustomImportRootScalar if collection is None else models.CustomImportChildScalar
    for scalar in adapter.logical_scalars(binding, generation, root.root_record_id, collection, family_record):
        by_model[scalar_model] = scalar
        document_by_name = {
            "root_key_sha256": publication._json_value(root.logical_key_sha256),
            "scalar": publication._materialization_document(scalar),
        }
        if collection is not None:
            document_by_name["child_key_sha256"] = publication._json_value(
                by_model[models.CustomImportChildRevision].child_key_sha256
            )
        entries_by_section[section].append(
            (_original_order(ordering(), by_model), (adapter._record_frame(section, document_by_name),))
        )


@pytest.mark.parametrize("is_sparse", (False, True))
@pytest.mark.parametrize("seed", (4, 19))
def test_compact_keys_keep_original_exact_semantic_order_after_full_family_proof(tmp_path, is_sparse, seed):
    from contextlib import ExitStack

    with ExitStack() as stack:
        fixtures = [stack.enter_context(_verified_order_fixture(index, is_sparse)) for index in range(3)]
        binding, generation = fixtures[0][0][1:3]
        plan = adapter.MaterializationWork(
            tmp_path / "canonical", adapter.SourceFactLimits(64 * 1024**2, 2 * 1024**2, 256), lambda _path: None, None
        )
        sections = adapter.ReconstructedSections(binding, generation, plan, None)
        stack.callback(sections.close)
        random.Random(seed).shuffle(fixtures)
        expected_by_section = {name: [] for name in ("family", "child", "root_scalar", "child_scalar")}
        for bundle, family_row, verified in fixtures:
            adapter.add_verified_family(
                sections, generation, family_row, bundle[3], bundle[6], bundle[5], verified, lambda: None
            )
            for section, entries in _original_graph_entries(bundle, verified).items():
                expected_by_section[section].extend(entries)
        adapter._check_root_uniqueness(sections)
        adapter._complete_reconstruction(sections)
        for section, entries in expected_by_section.items():
            expected_frames = [frames for _key, frames in sorted(entries)]
            actual_frames = [frames for _key, frames in sections.ordered(section)]
            assert actual_frames == expected_frames
            assert sum(len(adapter._pack(key)) for key, _frames in sections.ordered(section)) < sum(
                len(adapter._pack(key)) for key, _frames in entries
            )


@pytest.mark.parametrize("damage", ("duplicate", "collision", "missing"))
def test_compact_sort_keys_refuse_unproven_or_repeated_root_prefixes_before_hash(tmp_path, damage):
    _case, binding, generation, *_rows_unused = _rows()
    plan = adapter.MaterializationWork(
        tmp_path / "canonical", adapter.SourceFactLimits(1024**2, 65536, 256), lambda _path: None, None
    )
    sections = adapter.ReconstructedSections(binding, generation, plan, None)
    try:
        sections.counts["root_count"] = 1 if damage == "missing" else 2
        if damage != "missing":
            sections.append("root_key", (b"c" * 32, b"k" * 32), ("same", 1, 1))
            sections.append(
                "root_key", (b"c" * 32, b"k" * 32), ("different" if damage == "collision" else "same", 2, 2)
            )
        with pytest.raises(publication.PublicationConflict):
            adapter._check_root_uniqueness(sections)
        with pytest.raises(publication.PublicationConflict):
            sections.hash_into((adapter._CapturedDigest(), adapter._CapturedDigest()))
    finally:
        sections.close()


@pytest.mark.asyncio
async def test_root_context_winner_requires_actual_selected_generation_family(tmp_path):
    session = _ProvisionalSession()
    session.winner.context_child_revision_id = None
    session.winner.context_collection_slot = 0
    session.profiles[0].context_collection_slot = None
    session.winner_member = None
    root = tmp_path / "materialization"
    with adapter.materialization_work(
        session, root, adapter.SourceFactLimits(32 * 1024**2, 2 * 1024**2, 2048), lambda _root: None
    ):
        with pytest.raises(publication.PublicationConflict, match="winner reference differs"):
            async with adapter.reconstruct_sections(session, session.binding, session.generation):
                pytest.fail("a root winner outside the selected generation cannot be materialized")
    assert not root.exists()
    assert all(cursor.closed for cursor in session.results)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ("producer", "closed_at", "oid", "owner", "proof_count", "transaction"))
async def test_reused_sections_recheck_exact_finality_scope_and_all_automated_counts(tmp_path, monkeypatch, damage):
    session = _ProvisionalSession()
    root = tmp_path / "materialization"
    plan_limits = adapter.SourceFactLimits(32 * 1024**2, 2 * 1024**2, 2048)
    with adapter.materialization_work(session, root, plan_limits, lambda _root: None):
        async with adapter.reconstruct_sections(session, session.binding, session.generation) as sections:
            binding = session.binding
            closed_at = session.closed_at
            match damage:
                case "producer":
                    session.generation.producing_token_sha256 = b"x" * 32
                case "closed_at":
                    closed_at = datetime(2026, 2, 1, tzinfo=timezone.utc)
                case "oid":
                    binding = replace(binding, oids_by_role={"family": 1024})
                case "owner":
                    binding = replace(binding, owner_oid=99)
                case "transaction":
                    session.transaction = SimpleNamespace(is_active=True)
            bound = adapter.BlockMaterializationModels(binding, closed_at)
            monkeypatch.setattr(publication, "_finality_materialization_models", AsyncMock(return_value=bound))
            proof = SimpleNamespace(**sections.counts)
            if damage == "proof_count":
                proof.child_scalar_count += 1
            monkeypatch.setattr(adapter, "automated_proof", AsyncMock(return_value=proof))
            monkeypatch.setattr(
                adapter, "_hash_sections", AsyncMock(side_effect=AssertionError("guard did not refuse"))
            )
            with pytest.raises(publication.PublicationConflict):
                await adapter.finalize_sections(session, session.generation, sections)
    assert not root.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage", (None, "missing_seal", "producer", "phase", "family", "index_count", "chain", "foreign_binding")
)
async def test_block_projection_finality_keeps_completed_manifest_and_seal_checks(monkeypatch, damage):
    from process.custom_import import query_projection_finality as finality
    from process.custom_import.query_projection_binding import QueryProjectionUnavailable
    from tests.test_custom_import_family_block_projection_binding import _binding, _custody
    from tests.test_custom_import_query_projection_evidence import _fixture_digest, _generation, _manifest, _seal
    from tests.test_custom_import_query_projection_storage import _definition

    definition = _definition()
    binding = _binding(definition)
    generation = _generation()
    evidence = _fixture_digest(definition).finish()
    manifest, seal = _manifest(definition, evidence, generation, **_custody(binding)), _seal(evidence, generation)
    match damage:
        case "missing_seal":
            for name in evidence.seal_values():
                setattr(seal, name, None)
        case "producer":
            seal.sealing_token_sha256 = b"x" * 32
        case "phase":
            manifest.phase = "child"
        case "family":
            manifest.family_id += 1
        case "index_count":
            manifest.index_count -= 1
        case "chain":
            manifest.digest_chain_sha256 = b"x" * 32
        case "foreign_binding":
            binding = object()
    session = SimpleNamespace(
        get=AsyncMock(return_value=seal), scalars=AsyncMock(return_value=SimpleNamespace(one_or_none=lambda: manifest))
    )
    legacy_lock = AsyncMock()
    monkeypatch.setattr(finality, "lock_projection_tables", legacy_lock)
    if damage is None:
        assert (
            await finality.load_generation_evidence(session, generation, definition, family_block_binding=binding)
            == evidence
        )
    else:
        with pytest.raises(QueryProjectionUnavailable):
            await finality.load_generation_evidence(session, generation, definition, family_block_binding=binding)
    legacy_lock.assert_not_awaited()


def test_full_codec_proof_cannot_make_duplicate_child_prefix_materializable(tmp_path):
    with _verified_order_fixture(0, False, has_duplicate_child=True) as (bundle, family_row, verified):
        binding, generation = bundle[1:3]
        plan = adapter.MaterializationWork(
            tmp_path / "canonical", adapter.SourceFactLimits(32 * 1024**2, 2 * 1024**2, 256), lambda _path: None, None
        )
        sections = adapter.ReconstructedSections(binding, generation, plan, None)
        try:
            with pytest.raises(publication.PublicationConflict, match="duplicate logical child key"):
                adapter.add_verified_family(
                    sections, generation, family_row, bundle[3], bundle[6], bundle[5], verified, lambda: None
                )
            with pytest.raises(publication.PublicationConflict):
                sections.hash_into((adapter._CapturedDigest(), adapter._CapturedDigest()))
        finally:
            sections.close()


def test_scalar_prefix_compaction_requires_unique_original_scalar_primary_key():
    case, binding, generation, root, *_unused = _rows()
    original = binding.layout.definition.fields_by_id
    fields_by_id = dict(original) | {
        "display_name": replace(original["display_name"], field_slot=original["npi"].field_slot)
    }
    altered = SimpleNamespace(layout=SimpleNamespace(definition=SimpleNamespace(fields_by_id=fields_by_id)))
    family_record = replace(
        case[2],
        scalars={
            "npi": TypedScalar("string", "value", string_value="1234567893"),
            "display_name": TypedScalar("string", "value", string_value="Synthetic"),
        },
    )
    with pytest.raises(publication.PublicationConflict, match="scalar declaration differs"):
        list(adapter.logical_scalars(altered, generation, root.root_record_id, None, family_record))


@pytest.mark.asyncio
async def test_operational_spool_exhaustion_closes_native_cursors_and_exact_work_root(tmp_path):
    import os

    from process.custom_import.family_block_codec import FamilyBlockResourceLimit
    from process.custom_import.family_shard_facts import SourceFactBudgetExceeded

    session = _ProvisionalSession()
    root = tmp_path / "materialization"
    registered_roots = []
    filesystem = os.statvfs(tmp_path)
    entry_bytes = 2 * max(512, filesystem.f_frsize or filesystem.f_bsize)
    # Admit the root and section replays, then refuse the first payload write.
    spool_bytes = entry_bytes * (1 + len(adapter._SECTIONS) + 7)
    with adapter.materialization_work(
        session, root, adapter.SourceFactLimits(spool_bytes, 2048, 256), registered_roots.append
    ):
        with pytest.raises((SourceFactBudgetExceeded, FamilyBlockResourceLimit)):
            async with adapter.reconstruct_sections(session, session.binding, session.generation):
                pytest.fail("operational exhaustion must not accept an incomplete snapshot")
    assert registered_roots == [root.absolute()] and not root.exists()
    assert session.results and all(cursor.closed for cursor in session.results)


def test_partial_spool_construction_closes_owned_files_and_registered_root(tmp_path, monkeypatch):
    _, binding, generation, *_unused = _rows()
    opened_spools, registered_roots = [], []
    original = adapter._Replay

    def allocate(work):
        if len(opened_spools) == 3:
            raise OSError("synthetic file exhaustion")
        replay = original(work)
        opened_spools.append(replay)
        return replay

    monkeypatch.setattr(adapter, "_Replay", allocate)
    root = tmp_path / "materialization"
    plan = adapter.MaterializationWork(
        root, adapter.SourceFactLimits(1024**2, 65536, 256), registered_roots.append, None
    )
    with pytest.raises(OSError, match="file exhaustion"):
        adapter.ReconstructedSections(binding, generation, plan, None)
    assert registered_roots == [root.absolute()] and not root.exists()
    assert all(spool.file.closed for spool in opened_spools)


def _batched_native_family(index):
    sparse = bool(index % 2)
    with _verified_order_fixture(index, sparse) as (bundle, family_row, verified):
        binding, _generation, root, _family, member, entity = bundle[1:7]
        encoder = encode_retained_family if sparse else encode_family
        with encoder(
            binding.layout.definition,
            bundle[-2],
            {"rates": bundle[-1]},
            family_row["family_sha256"],
            limits=bundle[0][1],
        ) as encoded:
            frames = tuple(encoded.fragments())
        directory_entries = tuple(
            iter_fragment_directory(
                verified, binding.layout.collection_slots, (struct.unpack("!QQ", frame[:16])[1] for frame in frames)
            )
        )
        members = tuple(iter_revision_memberships(0, [(bundle[-2].revision_id, 0)], chunk_records=3)) + tuple(
            iter_revision_memberships(
                7, sorted((child.revision_id, ordinal) for ordinal, child in enumerate(bundle[-1], 1)), chunk_records=3
            )
        )
        case = (*bundle[0][:4], frames, directory_entries, members, verified.stored_manifest)
        fragments, memberships = _native_fragment_entries(case), _native_membership_entries(case)
        for entry in (*fragments, *memberships):
            entry["family_revision_id"] = family_row["family_revision_id"]
        family_values = tuple(family_row.get(name) for name in binding.tables_by_role["family"].c.keys())
        return (root, entity, member, *family_values), fragments, memberships, _original_graph_entries(bundle, verified)


class _BatchedSession(_ProvisionalSession):
    def __init__(self, count):
        super().__init__()
        self.family_rows, self.fragments, self.memberships, self.expected = [], [], [], []
        for index in range(count):
            family, fragments, memberships, original = _batched_native_family(index)
            self.family_rows.append(family)
            self.fragments.extend(fragments)
            self.memberships.extend(memberships)
            self.expected.append(original)
        self.root, self.entity, self.member = self.family_rows[0][:3]
        self.winner_member = self.member
        self.family_row = dict(
            zip(self.binding.tables_by_role["family"].c.keys(), self.family_rows[0][3:], strict=True)
        )

    async def stream(self, statement):
        sql = str(statement.compile(dialect=postgresql.dialect()))
        if any(
            name in sql
            for name in ("custom_import_block_fragment", "custom_import_block_revision_run", "custom_import_winner")
        ):
            return await super().stream(statement)
        return self._result(self.family_rows)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "memory,chunk,expected_batches", [(1024**2, 256, (3,)), (1024**2, 2, (2, 1)), (512, 256, (1, 1, 1))]
)
async def test_family_batches_preserve_original_frames(tmp_path, monkeypatch, memory, chunk, expected_batches):
    session, observed = _BatchedSession(3), []
    original, owner = adapter._verify_family_batch, threading.get_ident()

    def verify(sections, batch, count):
        assert threading.get_ident() != owner
        observed.append((count, batch.file._rolled))
        return original(sections, batch, count)

    monkeypatch.setattr(adapter, "_verify_family_batch", verify)
    monkeypatch.setattr(publication, "_MATERIALIZATION_STREAM_CHUNK_SIZE", chunk)
    with adapter.materialization_work(
        session, tmp_path / "material", adapter.SourceFactLimits(64 * 1024**2, 2 * 1024**2, memory), lambda _path: None
    ):
        async with adapter.reconstruct_sections(session, session.binding, session.generation) as sections:
            assert sections.completed_counts == (3, 3, 3, 21, 1, 1, 6, 42)
            for section in ("family", "child", "root_scalar", "child_scalar"):
                expected = sorted(itertools.chain.from_iterable(row[section] for row in session.expected))
                assert [frames for _order, frames in sections.ordered(section)] == [
                    frames for _order, frames in expected
                ]
    assert tuple(count for count, _rolled in observed) == expected_batches
    if memory == 512:
        assert all(rolled for _count, rolled in observed)
    assert not (tmp_path / "material").exists() and all(result.closed for result in session.results)


def _mock_hash_prefixes(monkeypatch):
    from process.custom_import import query_projection_finality

    monkeypatch.setattr(publication, "_validated_source_bundle_digest", AsyncMock(return_value=b"s" * 32))
    monkeypatch.setattr(publication, "_add_generation_identity_material", AsyncMock())
    monkeypatch.setattr(publication, "_add_capture_material", AsyncMock())
    monkeypatch.setattr(publication, "_add_definition_shape_material", AsyncMock(return_value=1))
    monkeypatch.setattr(adapter, "_effective_prefix", AsyncMock(return_value=1))
    monkeypatch.setattr(
        query_projection_finality,
        "bind_generation_materialization",
        AsyncMock(side_effect=lambda _session, _generation, result, *_args, **_kwargs: result),
    )


def _interruptible_operation(original, signals, owner):
    def run(sections, *arguments):
        assert threading.get_ident() != owner
        live_cursors = tuple(cursor for cursor in signals.cursors if not cursor.closed)
        original(sections, *arguments)
        signals.entered.set()
        deadline = time.monotonic() + 10
        try:
            while time.monotonic() < deadline:
                sections.work.current()
                time.sleep(0.001)
            raise AssertionError("the owner did not cancel its CPU task")
        except adapter.CancellationRequested:
            signals.stopped.set()
            while not signals.release.wait(0.001):
                assert sections.work.root.exists()
                assert all(not replay.file.closed for replay in sections.rows_by_section.values())
                assert all(not cursor.closed for cursor in live_cursors)
                if time.monotonic() >= deadline:
                    raise AssertionError("the owner did not release the draining CPU task") from None
            raise
        finally:
            signals.drained.set()

    return run


async def _wait_signal(signal):
    async with asyncio.timeout(10):
        while not signal.is_set():
            await asyncio.sleep(0)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "operation", ("_verify_family_batch", "_check_section_references", "_winner_references", "_hash_canonical_sections")
)
async def test_cpu_cancellation_drains_before_cleanup(tmp_path, monkeypatch, operation):
    session, root = _ProvisionalSession(), tmp_path / "material"
    signals = SimpleNamespace(**{name: threading.Event() for name in ("entered", "stopped", "release", "drained")})
    signals.cursors = session.results
    _mock_hash_prefixes(monkeypatch)
    monkeypatch.setattr(
        adapter, operation, _interruptible_operation(getattr(adapter, operation), signals, threading.get_ident())
    )

    async def consume():
        with adapter.materialization_work(
            session, root, adapter.SourceFactLimits(32 * 1024**2, 2 * 1024**2, 2048), lambda _path: None
        ):
            async with adapter.reconstruct_sections(session, session.binding, session.generation) as sections:
                if operation == "_hash_canonical_sections":
                    await adapter._hash_sections(session, session.generation, session.binding, sections)

    task = asyncio.create_task(consume())
    try:
        await _wait_signal(signals.entered)
        for _heartbeat in range(3):
            await asyncio.sleep(0)
            assert not task.done() and root.exists()
        task.cancel()
        await _wait_signal(signals.stopped)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and not signals.drained.is_set() and root.exists()
    finally:
        signals.release.set()
        if not task.done():
            task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert signals.drained.is_set() and not root.exists()
    assert all(cursor.closed for cursor in session.results)


class _OwnerInfo(dict):
    def __init__(self):
        self.owner = threading.get_ident()
        super().__init__()

    def get(self, *arguments):
        assert threading.get_ident() == self.owner
        return super().get(*arguments)


@pytest.mark.asyncio
async def test_cpu_never_accesses_owner_session(tmp_path, monkeypatch):
    session, owner, calls = _ProvisionalSession(), threading.get_ident(), []
    session.info = _OwnerInfo()
    _mock_hash_prefixes(monkeypatch)
    original = adapter.add_verified_family

    def add(sections, generation, family, root, entity, member, verified, check):
        assert threading.get_ident() != owner
        assert root is not session.root and entity is not session.entity and member is not session.member
        calls.append(True)
        return original(sections, generation, family, root, entity, member, verified, check)

    def transaction():
        assert threading.get_ident() == owner
        return session.transaction

    monkeypatch.setattr(session, "get_transaction", transaction)
    monkeypatch.setattr(adapter, "add_verified_family", add)
    with adapter.materialization_work(
        session, tmp_path / "material", adapter.SourceFactLimits(32 * 1024**2, 2 * 1024**2, 2048), lambda _path: None
    ):
        async with adapter.reconstruct_sections(session, session.binding, session.generation) as sections:
            digests = tuple(
                publication._new_digest(domain)
                for domain in (publication._MATERIALIZATION_DOMAIN, publication._EFFECTIVE_OUTPUT_DOMAIN)
            )
            adapter._hash_canonical_sections(sections, digests)
            with adapter._finality_work_budget(session, sections):
                materialization = await adapter._hash_sections(session, session.generation, session.binding, sections)
            assert (materialization.materialization_sha256, materialization.effective_output_sha256) == tuple(
                digest.digest() for digest in digests
            )
    assert calls == [True]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage", ("transaction", "producer", "oids", "owner", "family", "layout", "tables", "columns")
)
@pytest.mark.parametrize("boundary", ("before", "after"))
async def test_cpu_scope_checked_at_both_boundaries(tmp_path, damage, boundary):
    session, calls = _ProvisionalSession(), []
    plan = adapter.MaterializationWork(
        tmp_path / "material",
        adapter.SourceFactLimits(32 * 1024**2, 2 * 1024**2, 2048),
        lambda _path: None,
        session.transaction,
    )
    sections = adapter.ReconstructedSections(session.binding, session.generation, plan, session.closed_at)

    def corrupt():
        if damage == "transaction":
            session.transaction = SimpleNamespace(is_active=True)
            return
        if damage == "producer":
            session.generation.execution_id += 1
            return
        if damage == "oids":
            sections.binding = replace(sections.binding, oids_by_role={"family": 999})
            return
        if damage == "owner":
            sections.binding = replace(sections.binding, owner_oid=999)
            return
        if damage == "family":
            sections.binding = replace(sections.binding, family_id=999)
            return
        if damage == "layout":
            sections.binding = replace(
                sections.binding, layout=replace(sections.binding.layout, collection_slots={"rates": 8})
            )
            return
        if damage == "columns":
            sections.binding = replace(sections.binding, columns_sha256_by_role={"family": b"x" * 32})
            return
        sections.binding = replace(sections.binding, tables_by_role=sections.binding.layout.tables(999))

    def operation():
        calls.append(True)
        corrupt()

    try:
        if boundary == "before":
            corrupt()
        with pytest.raises(publication.PublicationConflict, match="owner transaction or binding"):
            await adapter._cpu_sections(session, session.generation, sections, operation)
        assert calls == ([] if boundary == "before" else [True])
    finally:
        sections.close()


def test_deadline_capture_stays_current_without_session_access():
    session = SimpleNamespace(info={})
    key = publication._FINALITY_SCAN_WINDOW_KEY
    session.info[key] = publication._FinalityScanWindow(datetime.now(timezone.utc), time.monotonic() - 1)
    check = adapter._deadline_check(session, None)
    session.info = None
    with pytest.raises(publication.PublicationConflict, match="lease-bounded"):
        check()


@pytest.mark.asyncio
@pytest.mark.parametrize("role", ("fragments", "memberships"))
@pytest.mark.parametrize("edge", ("orphan", "trailing"))
async def test_family_batches_keep_cursor_scope(tmp_path, role, edge):
    session = _ProvisionalSession()
    entries = getattr(session, role)
    extra = dict(entries[0]) | {"family_revision_id": 28 if edge == "orphan" else 30}
    entries.insert(0 if edge == "orphan" else len(entries), extra)
    root = tmp_path / "material"
    with adapter.materialization_work(
        session, root, adapter.SourceFactLimits(32 * 1024**2, 2 * 1024**2, 2048), lambda _path: None
    ):
        with pytest.raises(publication.PublicationConflict, match=edge):
            async with adapter.reconstruct_sections(session, session.binding, session.generation):
                pytest.fail("native groups outside all selected families cannot complete")
    assert not root.exists() and all(cursor.closed for cursor in session.results)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ("fragment_end", "member_end", "header", "census"))
async def test_family_batch_requires_full_boundaries(tmp_path, monkeypatch, damage):
    session, original = _ProvisionalSession(), adapter._verify_family_batch

    def damaged(sections, batch, count):
        entries = list(batch.rows())
        if damage in ("fragment_end", "member_end"):
            role = "fragment" if damage == "fragment_end" else "revision_run"
            entries.remove((role, None))
        elif damage == "header":
            entries[0] = ("revision_run", entries[0][1])
        with adapter._Replay(sections.work) as broken:
            for entry in entries:
                broken.append(entry)
            original(sections, broken, count + int(damage == "census"))

    monkeypatch.setattr(adapter, "_verify_family_batch", damaged)
    root = tmp_path / "material"
    with adapter.materialization_work(
        session, root, adapter.SourceFactLimits(32 * 1024**2, 2 * 1024**2, 2048), lambda _path: None
    ):
        with pytest.raises(publication.PublicationConflict, match="batch"):
            async with adapter.reconstruct_sections(session, session.binding, session.generation):
                pytest.fail("a partial or changed framed batch cannot complete")
    assert not root.exists() and all(cursor.closed for cursor in session.results)


@pytest.mark.parametrize("replay_input", (True, False))
def test_shared_verifier_accepts_replays_and_iterators(tmp_path, replay_input):
    session = _ProvisionalSession()
    plan = adapter.MaterializationWork(
        tmp_path / "material",
        adapter.SourceFactLimits(32 * 1024**2, 2 * 1024**2, 2048),
        lambda _path: None,
        session.transaction,
    )
    sections = adapter.ReconstructedSections(session.binding, session.generation, plan, session.closed_at)
    try:
        with adapter._Replay(sections.work) as fragments, adapter._Replay(sections.work) as members:
            for entry in session.fragments:
                fragments.append(tuple(entry[name] for name in session.binding.tables_by_role["fragment"].c.keys()))
            for entry in session.memberships:
                members.append(tuple(entry[name] for name in session.binding.tables_by_role["revision_run"].c.keys()))
            with adapter._verify_group(
                sections,
                session.family_row,
                fragments if replay_input else fragments.rows(),
                members if replay_input else members.rows(),
                sections.work.current,
            ) as verified:
                assert list(verified.records()) == [session.case[2]]
                assert list(verified.records("rates")) == session.case[3]
                assert verified.stored_manifest == session.case[-1]
    finally:
        sections.close()
    assert not (tmp_path / "material").exists()
