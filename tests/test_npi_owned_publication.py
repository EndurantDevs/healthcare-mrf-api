# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Ownership-only native output cannot borrow legacy publication authority."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from process import npi_owned_publication as owned
from process import npi_result_archive as archive
from process.npi_canonical_publication import NpiCanonicalPublicationInput, build_npi_canonical_publication_receipt


def _output_content():
    dataset_id = uuid4()
    metadata, metadata_sha = archive._source_metadata({"run_id": "run_" + "a" * 32, "evidence": "b" * 64})
    tables = tuple(
        archive.NpiTableReceipt(model.__name__, model.__tablename__, "c" * 64, 1)
        for model in archive.npi_archive_models(canonical=True)
    )
    manifest = archive.NpiResultManifest(
        tables,
        metadata,
        metadata_sha,
        archive._schema_digest(tables),
        "legacy-manual",
        None,
        None,
        archive.MODEL_CONTRACT,
    )
    pairs = tuple((name, index + 100) for index, name in enumerate(archive.npi_archive_names(canonical=True)))
    ownership = archive.NpiStageOwnership(
        dataset_id, archive.npi_stage_schema(dataset_id), 30, tuple(sorted(pairs)), (), None, (), ()
    )
    validation_by_field = {
        "contract": archive.VALIDATION_CONTRACT,
        "package_id": "d" * 64,
        "profile_contract": archive.MODEL_CONTRACT,
        "stage_schema": ownership.schema_name,
        "stage_schema_oid": ownership.schema_oid,
        "relation_oids": [list(pair) for pair in ownership.relation_oids],
        "sealed_owner_oid": 40,
        "manifest_sha256": archive._manifest_digest(manifest),
        "tables": [table.as_dict() for table in tables],
    }
    validation = archive.validate_npi_validation_receipt(
        {**validation_by_field, "validation_sha256": archive._validation_digest(validation_by_field)}
    )
    return ownership, manifest, validation, pairs


def _publication_session(transaction):
    session = SimpleNamespace(
        in_transaction=lambda: True, get_transaction=lambda: transaction, transaction_id="synthetic-transaction"
    )

    async def scalar(statement, *_arguments):
        if "pg_current_xact_id()" in str(statement):
            return session.transaction_id
        return "FROM pg_catalog.pg_roles" in str(statement)

    session.scalar = AsyncMock(side_effect=scalar)
    return session


@pytest.fixture
def publication(monkeypatch):
    ownership, manifest, validation, pairs = _output_content()
    dataset_id = ownership.dataset_id
    events = []
    transaction = SimpleNamespace(is_active=True)
    session = _publication_session(transaction)

    def boundary(name, value=None):
        async def call(*_arguments, **_options):
            events.append(name)
            return value

        return AsyncMock(side_effect=call)

    for name, boundary_value in (
        ("_lock_and_verify_activation", None),
        ("_verify_stage_owner", None),
        ("_preserve_npi_input_reads", None),
        ("_rotate_relations", "npi_predecessor_" + dataset_id.hex),
        ("_relation_pairs", pairs),
    ):
        monkeypatch.setattr(archive, name, boundary(name, boundary_value))
    for name in ("install_npi_stage_mutation_guards", "merge_canonical_contribution", "_remove_npi_clone_freeze"):
        monkeypatch.setattr(archive, name, lambda *_args, **_kwargs: pytest.fail("legacy publication path executed"))
    monkeypatch.setattr(owned, "read_npi_result_generation_authority", boundary("current", object()))
    generation = object()
    monkeypatch.setattr(owned, "_publish_generation", boundary("generation", generation))
    publication = build_npi_canonical_publication_receipt(
        NpiCanonicalPublicationInput(
            "synthetic-run",
            "synthetic-run:" + "a" * 32,
            "2026-01-01T00:00:00.000000+00:00",
            "penpc1_" + "b" * 43,
            "2026-01-01",
            tuple(dict(pairs)[name] for name in archive.RELATION_NAMES),
            (1,) * 6,
        ),
        publication_generation=1,
        created_at="2026-01-01T00:00:00.000000+00:00",
    )
    options_by_name = dict(
        ownership=ownership,
        manifest=manifest,
        incumbent=archive.NpiIncumbent("serving", tuple((name, index + 200) for index, (name, _) in enumerate(pairs))),
        validation_receipt=validation,
        authority=owned.NpiOwnershipPublication(
            archive.NpiCutoverAuthority("d" * 64, 40, 40, "manual"),
            boundary("custody"),
            boundary("canonical", publication),
            boundary("settle"),
        ),
    )
    return SimpleNamespace(
        session=session, options=options_by_name, events=events, generation=generation, canonical=publication
    )


@pytest.mark.asyncio
async def test_manual_owned_adoption_keeps_hooks_out_and_settles_after_real_generation(publication):
    state = publication
    receipt = await owned.activate_ownership_sealed_npi_stage(state.session, **state.options)
    assert state.events == [
        "_lock_and_verify_activation",
        "_verify_stage_owner",
        "custody",
        "current",
        "_preserve_npi_input_reads",
        "_rotate_relations",
        "_relation_pairs",
        "canonical",
        "generation",
        "settle",
    ]
    assert receipt.adopted_authority is state.generation
    state.options["authority"].on_activated.assert_awaited_once_with(state.session, receipt)
    assert state.options["manifest"].source_serving_generation is None


@pytest.mark.asyncio
async def test_missing_canonical_capability_refuses_before_catalog_work(publication):
    state = publication
    state.options["authority"] = replace(state.options["authority"], attach_canonical=None)
    with pytest.raises(archive.NpiResultArchiveError, match="capabilities are unavailable"):
        await owned.activate_ownership_sealed_npi_stage(state.session, **state.options)
    assert state.events == []


@pytest.mark.asyncio
async def test_failed_custody_cannot_rotate_or_settle(publication):
    state = publication
    state.options["authority"].verify_custody.side_effect = RuntimeError("custody refused")
    with pytest.raises(RuntimeError, match="custody refused"):
        await owned.activate_ownership_sealed_npi_stage(state.session, **state.options)
    assert state.events == ["_lock_and_verify_activation", "_verify_stage_owner"]
    state.options["authority"].attach_canonical.assert_not_awaited()
    state.options["authority"].on_activated.assert_not_awaited()


@pytest.mark.asyncio
async def test_callback_cannot_replace_the_original_transaction(publication):
    state = publication

    async def changed(*_args):
        state.session.get_transaction = lambda: SimpleNamespace(is_active=True)

    state.options["authority"] = replace(state.options["authority"], verify_custody=changed)
    with pytest.raises(archive.NpiResultArchiveError, match="transaction changed"):
        await owned.activate_ownership_sealed_npi_stage(state.session, **state.options)
    assert "_rotate_relations" not in state.events


@pytest.mark.asyncio
async def test_callback_cannot_replace_underlying_database_transaction(publication):
    state = publication

    async def changed(*_arguments):
        state.session.transaction_id = "replacement-transaction"

    state.options["authority"] = replace(state.options["authority"], verify_custody=changed)
    with pytest.raises(archive.NpiResultArchiveError, match="transaction changed"):
        await owned.activate_ownership_sealed_npi_stage(state.session, **state.options)
    assert "_rotate_relations" not in state.events


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["authority", "expected_stage_owner_oid"])
async def test_automatic_or_builder_handback_cannot_select_owned_activation(publication, field):
    state = publication
    state.options["authority"] = replace(
        state.options["authority"],
        cutover=replace(state.options["authority"].cutover, **{field: "automatic" if field == "authority" else 41}),
    )
    with pytest.raises(archive.NpiResultArchiveError, match="ownership-only activation authority differs"):
        await owned.activate_ownership_sealed_npi_stage(state.session, **state.options)
    assert state.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["sequence_oids", "freeze_function_oid"])
async def test_legacy_sequence_or_hook_custody_is_not_a_new_output(publication, field):
    state = publication
    value = (("npi_npi_seq", 55, "npi", "npi"),) if field == "sequence_oids" else 55
    state.options["ownership"] = replace(state.options["ownership"], **{field: value})
    with pytest.raises(archive.NpiResultArchiveError, match="ownership-only activation authority differs"):
        await owned.activate_ownership_sealed_npi_stage(state.session, **state.options)
    assert state.events == []


@pytest.mark.asyncio
async def test_canonical_attachment_failure_cannot_record_native_success(publication):
    state = publication
    state.options["authority"].attach_canonical.side_effect = RuntimeError("attachment refused")
    with pytest.raises(RuntimeError, match="attachment refused"):
        await owned.activate_ownership_sealed_npi_stage(state.session, **state.options)
    assert "generation" not in state.events and "settle" not in state.events


@pytest.mark.asyncio
async def test_untyped_canonical_receipt_cannot_record_native_success(publication):
    state = publication
    state.options["authority"] = replace(
        state.options["authority"], attach_canonical=AsyncMock(return_value={"contract": "unknown"})
    )
    with pytest.raises(RuntimeError):
        await owned.activate_ownership_sealed_npi_stage(state.session, **state.options)
    assert "generation" not in state.events and "settle" not in state.events


@pytest.mark.asyncio
async def test_precreate_removes_only_native_generated_npi_identifier(monkeypatch):
    dataset_id = uuid4()
    initial = archive.NpiStageOwnership(
        dataset_id, archive.npi_stage_schema(dataset_id), 30, (), (("npi_npi_seq", 55, "npi", "npi"),), None, (), ()
    )
    final = replace(initial, sequence_oids=())
    monkeypatch.setattr(archive, "precreate_npi_restore", AsyncMock(return_value=initial))
    monkeypatch.setattr(archive, "capture_npi_stage_ownership", AsyncMock(return_value=final))
    session = SimpleNamespace(execute=AsyncMock())
    assert await owned.precreate_npi_output(session, dataset_id=dataset_id) is final
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert statements == [
        f'ALTER TABLE "{initial.schema_name}"."npi" ALTER COLUMN "npi" DROP DEFAULT',
        f'DROP SEQUENCE "{initial.schema_name}"."npi_npi_seq" RESTRICT',
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("transaction", [None, SimpleNamespace(is_active=False)])
async def test_unavailable_transaction_refuses_before_custody_callback(publication, transaction):
    state = publication
    state.session.get_transaction = lambda: transaction
    with pytest.raises(archive.NpiResultArchiveError, match="transaction is unavailable"):
        await owned.activate_ownership_sealed_npi_stage(state.session, **state.options)
    state.options["authority"].verify_custody.assert_not_awaited()
    state.options["authority"].attach_canonical.assert_not_awaited()
    state.options["authority"].on_activated.assert_not_awaited()
    assert "_rotate_relations" not in state.events


@pytest.mark.asyncio
async def test_callback_cannot_close_the_original_transaction(publication):
    state = publication
    transaction = state.session.get_transaction()

    async def close_transaction(*_arguments):
        transaction.is_active = False

    state.options["authority"] = replace(state.options["authority"], verify_custody=close_transaction)
    with pytest.raises(archive.NpiResultArchiveError, match="transaction changed"):
        await owned.activate_ownership_sealed_npi_stage(state.session, **state.options)
    assert "_rotate_relations" not in state.events
    state.options["authority"].on_activated.assert_not_awaited()


@pytest.mark.asyncio
async def test_foreign_sequence_refuses_before_output_ddl(monkeypatch):
    ownership, _manifest, _validation, _pairs = _output_content()
    ownership = replace(ownership, sequence_oids=(("unexpected_sequence", 55, "npi", "npi"),))
    monkeypatch.setattr(archive, "precreate_npi_restore", AsyncMock(return_value=ownership))
    session = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(archive.NpiResultArchiveError, match="output sequence differs"):
        await owned.precreate_npi_output(session, dataset_id=ownership.dataset_id)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("relation_oids", ()),
        ("sequence_oids", (("unexpected_sequence", 55, "npi", "npi"),)),
        ("freeze_function_oid", 55),
        ("freeze_trigger_oids", (55,)),
        ("freeze_catalog_versions", ((55, "1", "(0,1)"),)),
    ],
)
async def test_invalid_output_catalog_refuses_before_native_inspection(monkeypatch, publication, field, value):
    ownership = replace(publication.options["ownership"], **{field: value})
    inspect = AsyncMock()
    monkeypatch.setattr(archive, "verify_npi_stage_ownership", inspect)
    with pytest.raises(archive.NpiResultArchiveError, match="catalog ownership differs"):
        await owned.require_npi_output_catalog(
            publication.session, ownership, executable_roles=(), protected_owner_oid=40, expected_owner_oid=40
        )
    inspect.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["ownership", "incumbent"])
async def test_untyped_activation_identity_refuses_before_catalog_work(publication, field):
    state = publication
    state.options[field] = None
    with pytest.raises(archive.NpiResultArchiveError, match="activation ownership is invalid"):
        await owned.activate_ownership_sealed_npi_stage(state.session, **state.options)
    assert state.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["ownership", "incumbent"])
async def test_inexact_native_family_order_refuses_before_adoption(publication, field):
    state = publication
    value = state.options[field]
    state.options[field] = replace(value, relation_oids=tuple(reversed(value.relation_oids)))
    with pytest.raises(archive.NpiResultArchiveError, match="direct output contract differs"):
        await owned.activate_ownership_sealed_npi_stage(state.session, **state.options)
    assert state.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("evidence", [(None, False), (True, True), (True, None)])
async def test_unprotected_or_unknown_output_custody_refuses_rotation(publication, evidence):
    state = publication
    state.session.scalar = AsyncMock(side_effect=evidence)
    with pytest.raises(archive.NpiResultArchiveError, match="output custody differs"):
        await owned.activate_ownership_sealed_npi_stage(state.session, **state.options)
    assert "_rotate_relations" not in state.events
    state.options["authority"].verify_custody.assert_not_awaited()
    state.options["authority"].on_activated.assert_not_awaited()


@pytest.mark.asyncio
async def test_postrotation_relation_drift_refuses_canonical_and_terminal_callbacks(monkeypatch, publication):
    state = publication
    pairs = state.options["ownership"].relation_oids
    monkeypatch.setattr(archive, "_relation_pairs", AsyncMock(return_value=pairs[:-1]))
    with pytest.raises(archive.NpiResultArchiveError, match="adopted output identity differs"):
        await owned.activate_ownership_sealed_npi_stage(state.session, **state.options)
    state.options["authority"].attach_canonical.assert_not_awaited()
    state.options["authority"].on_activated.assert_not_awaited()
    assert "generation" not in state.events


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["relation_oids", "row_counts"])
async def test_valid_but_foreign_canonical_output_cannot_publish_generation(publication, field):
    state = publication
    current = state.canonical
    arguments_by_field = {name: getattr(current, name) for name in NpiCanonicalPublicationInput._fields}
    arguments_by_field[field] = tuple(value + 1 for value in arguments_by_field[field])
    foreign = build_npi_canonical_publication_receipt(
        NpiCanonicalPublicationInput(**arguments_by_field),
        publication_generation=current.publication_generation,
        created_at=current.created_at,
    )
    state.options["authority"] = replace(state.options["authority"], attach_canonical=AsyncMock(return_value=foreign))
    with pytest.raises(archive.NpiResultArchiveError, match="canonical attachment output differs"):
        await owned.activate_ownership_sealed_npi_stage(state.session, **state.options)
    assert "generation" not in state.events
    state.options["authority"].on_activated.assert_not_awaited()
