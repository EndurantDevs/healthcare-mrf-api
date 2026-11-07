# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real original/codec files and identity joins; database identities are synthetic."""

import json
from contextlib import ExitStack, closing
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace

import pytest

from db.models import custom_import as models
from process.custom_import import family_block_materialization as reconstruction
from process.custom_import import family_shard_admission as admission_records
from process.custom_import import family_shard_original_validation as original_proof
from process.custom_import import family_shard_semantic_audit as claims_api
from process.custom_import import family_shard_source_values as source_values
from process.custom_import import publication, runner_codec
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family import RootFamily
from process.custom_import.family_block_binding import FamilyBlockBinding
from process.custom_import.family_block_codec import FamilyBlockLimits
from process.custom_import.family_block_directory import RevisionMembership
from process.custom_import.family_block_storage_codec import verify_stored_family
from process.custom_import.family_shard_output import FamilyOutputWork, build_family_output
from process.custom_import.family_shard_plan import PlanWork
from process.custom_import.family_shard_source_values import prepared_source_family_inputs
from process.custom_import.materialization import TypedScalar
from tests.test_custom_import_family_shard_original_validation import _LIMITS, _case
from tests.test_custom_import_family_shard_semantic_audit import _identity_models
from tests.test_custom_import_family_shard_source_plan import _child, _root
from tests.test_custom_import_family_shard_source_values import _definition
from tests.test_custom_import_family_shard_worker import _rows as _native_rows

pytestmark = pytest.mark.asyncio


def _nonquery_definition():
    document = json.loads(_definition(nullable=True).canonical)
    document["query"].pop("child")
    document["selection_profiles"] = [{"id": "default", "selection": [], "context_dimensions": []}]
    return CustomImportDefinition.from_mapping(document)


def _inputs(root, native):
    return prepared_source_family_inputs(
        native.context,
        native.admission,
        native.plan,
        work=PlanWork(root / "prepared", _LIMITS, lambda _path: None),
    )


def _sealed_claims(root, native, prepared):
    claims = claims_api.SourceRecordClaims(
        native.context, prepared, work=PlanWork(root / "claims", _LIMITS, lambda _path: None)
    )
    try:
        comparison = original_proof.prove_original_outputs(
            native.context,
            native.admission,
            native.plan,
            native.workers,
            native.keys,
            work=PlanWork(root / "claim-proof", _LIMITS, lambda _path: None),
            prepared=claims,
        )
    except BaseException:
        claims.close()
        raise
    return claims, comparison


def _modified_root(root, damage):
    if damage in ("root_payload", "root_pack", "root_scalar_missing"):
        root_values, root_scalars = dict(root.values_by_field), dict(root.scalars)
        if damage == "root_payload":
            root_values["score"] += 100
            root_scalars["score"] = TypedScalar("integer", "value", integer_value=root_values["score"])
        elif damage == "root_scalar_missing":
            del root_scalars["score"]
        root = replace(root, values_by_field=root_values, scalars=root_scalars)
        if damage == "root_pack":
            root = replace(root, pack_id=root.pack_id + 10000)
    return root


def _modified(family, definition, damage):
    """Rebuild internally valid family/storage hashes around an altered SOURCE row."""
    root = _modified_root(family.root, damage)
    child_records = list(family.children_by_collection["details"]())
    child = child_records[0]
    child_values, child_scalars = dict(child.values_by_field), dict(child.scalars)
    match damage:
        case "key":
            child_values["detail_id"] = "different-key"
            child_scalars["detail_id"] = TypedScalar("string", "value", string_value="different-key")
        case "payload":
            child_values["amount"] = Decimal("9.125")
            child_scalars["amount"] = TypedScalar("decimal", "value", decimal_value=Decimal("9.125"))
        case "null":
            child_scalars["amount"] = TypedScalar("decimal", "null")
        case "type":
            child_scalars["amount"] = TypedScalar("string", "value", string_value="1.25")
        case "scalar_missing":
            del child_scalars["amount"]
    child = replace(child, values_by_field=child_values, scalars=child_scalars)
    if damage == "pack":
        child = replace(child, pack_id=child.pack_id + 10000)
    elif damage == "ordinal":
        child = replace(child, source_ordinal=child.source_ordinal + 10)
    elif damage == "revision":
        child = replace(child, revision_id=child.revision_id + 10000)
    child_records[0] = child
    if damage == "missing":
        child_records.pop(0)
    elif damage == "extra":
        extra_values_by_field = dict(child.values_by_field, detail_id="additional-key")
        extra_scalars_by_name = dict(
            child.scalars, detail_id=TypedScalar("string", "value", string_value="additional-key")
        )
        child_records.append(
            replace(
                child,
                revision_id=child.revision_id + 20000,
                values_by_field=extra_values_by_field,
                scalars=extra_scalars_by_name,
            )
        )
    digest = runner_codec.new_family_hash(
        definition,
        RootFamily(
            tuple(root.values_by_field[name] for name in definition.root_logical_key),
            root.values_by_field,
            {"details": tuple(family_record.values_by_field for family_record in child_records)},
        ),
    )
    return replace(
        family,
        root=root,
        children_by_collection={"details": lambda: iter(child_records)},
        expected_family_sha256=digest,
    )


def _sections(root, native, prepared, claims, stack, *, damage=None, prefix=""):
    context, definition = native.context, native.context.request.definition
    layout, snapshot = context.layout, context.source.snapshot_family_id
    binding = FamilyBlockBinding(snapshot, layout, layout.tables(snapshot), {}, 7)
    generation, _build = _identity_models(native)
    sections = reconstruction.ReconstructedSections(
        binding,
        generation,
        reconstruction.MaterializationWork(root / (prefix + "material"), _LIMITS, lambda _path: None, None),
        None,
        source_record_claims=claims,
    )
    stack.callback(sections.close)
    reconstruction._source_claim_owner(binding, generation, claims)
    with closing(prepared.families()) as families:
        for plan, family in families:
            supplied = _modified(family, definition, damage)
            output = build_family_output(
                layout,
                snapshot,
                iter((supplied,)),
                {},
                work=FamilyOutputWork(
                    root / (prefix + "native-output"),
                    FamilyBlockLimits(32 * 1024**2, 1024**2, 4096, 256),
                    lambda _path: None,
                ),
                shard_id=0,
                partition_sha256=b"p" * 32,
                source_record_count=3,
            )
            with closing(output):
                _add_output_sections(sections, plan, family, generation, output)
    return sections


def _original_models(definition, plan, family, generation):
    root = models.CustomImportRootRecord(
        root_record_id=plan.root_record_id,
        dataset_id=generation.dataset_id,
        canonical_logical_key=plan.typed_key[0],
        logical_key_sha256=plan.typed_key[1],
        key_contract_sha256=runner_codec.root_key_contract_hash(definition),
    )
    entity_value = family.root.values_by_field[definition.entity_field]
    entity = models.CustomImportEntityBinding(
        entity_binding_id=family.entity_binding_id,
        dataset_id=generation.dataset_id,
        adapter_id="npi",
        canonical_value=entity_value,
        value_sha256=runner_codec.digest_text("entity:npi", entity_value),
    )
    member = models.CustomImportGenerationFamily(
        generation_id=generation.generation_id,
        dataset_id=generation.dataset_id,
        definition_revision_id=generation.definition_revision_id,
        schema_revision_id=generation.schema_revision_id,
        root_record_id=plan.root_record_id,
        family_revision_id=family.family_revision_id,
    )
    return root, entity, member


def _add_output_sections(sections, plan, family, generation, output):
    layout = sections.binding.layout
    rows_by_role = _native_rows(output)
    family_by_name = dict(
        zip(sections.binding.tables_by_role["family"].c.keys(), rows_by_role["family"][0], strict=True)
    )
    root, entity, member = _original_models(layout.definition, plan, family, generation)
    with verify_stored_family(
        layout.definition,
        layout.collection_slots,
        (((cells[1], cells[2], *cells[4:]), cells[3]) for cells in rows_by_role["fragment"]),
        (RevisionMembership(*cells[1:]) for cells in rows_by_role["revision_run"]),
        layout.manifest(family_by_name),
        limits=FamilyBlockLimits(32 * 1024**2, 1024**2, 4096, 256),
    ) as verified:
        reconstruction.add_verified_family(
            sections,
            generation,
            family_by_name,
            root,
            entity,
            member,
            verified,
            sections.work.current,
        )


async def test_complete_nonquery_source_claims_join_real_reconstruction_and_bind_source_proof(
    tmp_path, monkeypatch, record_property
):
    rows_by_stream = {"root_source": [[_root()]], "detail_source": [[_child(key="a"), _child(key="z", amount=None)]]}
    async with _case(tmp_path, monkeypatch, rows_by_stream, definition=_nonquery_definition()) as native:
        with _inputs(tmp_path, native) as prepared, ExitStack() as stack:
            counters = _source_read_counters(monkeypatch)
            claims, comparison = _sealed_claims(tmp_path, native, prepared)
            claimed_counts = len(counters.decodes), tuple(counters.reads)
            stack.callback(claims.close)
            expected_claims = tuple(claims.claims())
            assert len(expected_claims) == 3 and all(len(claim) == 14 for claim in expected_claims)
            assert {claim[11] for claim in expected_claims} == {"root", "child"}
            assert any(declaration[-1] for claim in expected_claims for declaration in claim[13])
            canonical_parts, renderings = reconstruction._canonical_parts, []

            def once(*args):
                renderings.append((args[1], args[2].revision_id))
                return canonical_parts(*args)

            monkeypatch.setattr(reconstruction, "_canonical_parts", once)
            sections = _sections(tmp_path, native, prepared, claims, stack)
            reconstruction._compare_source_claims(sections)
            assert len(renderings) == len(expected_claims) == len(set(renderings))
            assert sections.source_record_claims_sha256 == claims.sha256
            baseline = _sections(tmp_path, native, prepared, None, stack, prefix="baseline-")
            for section in reconstruction._SECTIONS:
                assert list(sections.ordered(section)) == list(baseline.ordered(section))
            assert comparison.record_claims_sha256 == claims.sha256 and comparison.semantic_audit is None
            counters.decodes.clear()
            counters.reads.clear()
            without = original_proof.prove_original_outputs(
                native.context,
                native.admission,
                native.plan,
                native.workers,
                native.keys,
                work=PlanWork(tmp_path / "original-proof", _LIMITS, lambda _path: None),
                prepared=prepared,
            )
            assert comparison.sha256 != without.sha256 and comparison.counts == without.counts
            _check_read_counts(record_property, counters, claimed_counts)


def _source_read_counters(monkeypatch):
    counters = SimpleNamespace(decodes=[], reads=[])
    decode_record, original_rows = admission_records.decode_record, source_values._SourceFamily._rows

    def source_decode(*args):
        counters.decodes.append(True)
        return decode_record(*args)

    def observed_rows(family, collection):
        counters.reads.append("open")
        with closing(original_rows(family, collection)) as records:
            for record in records:
                counters.reads.append("record")
                yield record

    monkeypatch.setattr(admission_records, "decode_record", source_decode)
    monkeypatch.setattr(source_values._SourceFamily, "_rows", observed_rows)
    return counters


def _check_read_counts(record_property, counters, claimed):
    source_decodes, source_reads = claimed
    assert len(counters.decodes) == source_decodes and tuple(counters.reads) == source_reads
    record_property("claimed_original_decodes", source_decodes)
    record_property("baseline_original_decodes", len(counters.decodes))
    record_property("claimed_child_replay_opens", source_reads.count("open"))
    record_property("baseline_child_replay_opens", counters.reads.count("open"))
    record_property("claimed_child_replay_records", source_reads.count("record"))
    record_property("baseline_child_replay_records", counters.reads.count("record"))


@pytest.mark.parametrize(
    "damage",
    (
        "key",
        "payload",
        "pack",
        "ordinal",
        "revision",
        "null",
        "type",
        "scalar_missing",
        "missing",
        "extra",
        "root_payload",
        "root_pack",
        "root_scalar_missing",
    ),
)
async def test_self_consistent_nonquery_native_changes_refuse_original_identity_claims(tmp_path, monkeypatch, damage):
    rows_by_stream = {"root_source": [[_root()]], "detail_source": [[_child(key="a"), _child(key="z")]]}
    async with _case(tmp_path, monkeypatch, rows_by_stream, definition=_nonquery_definition()) as native:
        with _inputs(tmp_path, native) as prepared, ExitStack() as stack:
            claims, _comparison = _sealed_claims(tmp_path, native, prepared)
            stack.callback(claims.close)
            sections = _sections(tmp_path, native, prepared, claims, stack, damage=damage)
            if damage not in ("scalar_missing", "missing", "extra", "root_scalar_missing"):
                assert sections.counts["family_child_count"] == claims.counts["family_child_count"]
                assert sections.counts["child_scalar_count"] == claims.counts["child_scalar_count"]
            with pytest.raises(publication.PublicationConflict, match="SOURCE"):
                reconstruction._compare_source_claims(sections)


@pytest.mark.parametrize("reader", ("membership", "query"))
async def test_every_existing_audit_reader_refuses_unsupported_claim_extensions(tmp_path, monkeypatch, reader):
    async with _case(tmp_path, monkeypatch) as native:
        with _inputs(tmp_path, native) as prepared:
            claims, _comparison = _sealed_claims(tmp_path, native, prepared)
            try:
                with closing(claims.claims()) as records:
                    row = next(records) + ("unsupported",)
                _refuse_extended_claim(reader, row)
            finally:
                claims.close()


def _refuse_extended_claim(reader, claim):
    with pytest.raises(ValueError, match="claim shape"):
        if reader == "membership":
            claims_api._equal_members(iter(()), iter((claim,)), SimpleNamespace(current=lambda: None))
        else:
            tuple(claims_api._query_claims(iter(()), iter((claim,)), SimpleNamespace(current=lambda: None)))


def _malformed_claim(claim, damage):
    claim_cells = list(claim)
    match damage:
        case "extra":
            claim_cells.append("extension")
        case "kind":
            claim_cells[11] = "unsupported"
        case "collection":
            claim_cells[12] = "unsupported"
        case "source_bool":
            claim_cells[5] = False
        case "duplicate_scalar":
            claim_cells[13] = (*claim_cells[13], claim_cells[13][-1])
        case _:
            declarations = list(claim_cells[13])
            declaration_cells = list(declarations[0])
            declaration_cells[3 if damage == "slot_bool" else 4] = False if damage == "slot_bool" else "unsupported"
            declarations[0] = tuple(declaration_cells)
            claim_cells[13] = tuple(declarations)
    return tuple(claim_cells)


@pytest.mark.parametrize(
    "damage", ("extra", "kind", "collection", "source_bool", "slot_bool", "type", "duplicate_scalar")
)
async def test_full_claim_contract_refuses_unsupported_cells_before_a_join(tmp_path, monkeypatch, damage):
    async with _case(tmp_path, monkeypatch) as native:
        with _inputs(tmp_path, native) as prepared:
            claims, _comparison = _sealed_claims(tmp_path, native, prepared)
            try:
                with closing(claims.claims()) as records:
                    malformed = _malformed_claim(next(records), damage)
                with pytest.raises(ValueError):
                    claims_api.checked_record_claim(malformed)
            finally:
                claims.close()


async def test_unfinished_claims_refuse_reads_and_preserve_original_custody(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        with _inputs(tmp_path, native) as prepared:
            claims = claims_api.SourceRecordClaims(
                native.context, prepared, work=PlanWork(tmp_path / "claims", _LIMITS, lambda _path: None)
            )
            try:
                assert claims.sha256 is None and not claims.is_complete
                with pytest.raises(ValueError, match="complete original EOF"):
                    tuple(claims.claims())
                with pytest.raises(ValueError, match="selected family coverage"):
                    claims.seal(dict(claims.counts))
            finally:
                claims.close()
            prepared.check_scope(native.context, native.admission, native.plan)
            assert prepared.work.root.exists() and not (tmp_path / "claims").exists()
