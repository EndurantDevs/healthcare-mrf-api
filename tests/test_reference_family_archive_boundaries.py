# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reject drifted source and restored ownership before publication."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import reference_family_archive as archive
from tests.test_reference_family_archive import _incumbent, _manifest, _ownership, _serving_generation


@pytest.mark.parametrize("authority,generation", [("unknown", None), ("tracked-generation", None), ("manual-only", 1)])
def test_manifest_serialization_cannot_invent_generation_authority(authority, generation):
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="authority is invalid"):
        replace(_manifest(), publication_authority=authority, source_serving_generation=generation).as_dict()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["relation", "columns", "primary-key", "payload-key"])
async def test_restored_canonical_address_requires_exact_identity(monkeypatch, failure):
    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(return_value=None if failure == "relation" else 10))
    columns = [
        {"attname": name, "type": kind, "attnotnull": True, "default_expression": None}
        for name, kind in [("address_key", "uuid"), ("payload", "jsonb")]
    ]
    monkeypatch.setattr(
        archive.catalog_identity, "_catalog_columns", AsyncMock(return_value=[] if failure == "columns" else columns)
    )
    session = SimpleNamespace(scalar=AsyncMock(side_effect=[0 if failure == "primary-key" else 1, 1]))
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="MRF canonical auxiliary"):
        await archive._restored_mrf_auxiliary_identity(session, "synthetic", {"archive_name": "address_archive_v2"})


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["relation", "publication"])
async def test_canonical_source_requires_both_relation_and_publication(monkeypatch, missing):
    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(return_value=None if missing == "relation" else 10))
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="source or publication receipt"):
        await archive._mrf_auxiliary_receipt(
            object(), "synthetic", is_source=True, publication=None if missing == "publication" else {}
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("schema,expected", [("tiger", True), ("synthetic", False)])
async def test_tiger_generation_authority_is_native_schema_only(schema, expected):
    assert (
        await archive._has_source_generation_authority(object(), archive.reference_family_spec("tiger"), schema)
        is expected
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["missing", "wrong-column", "wrong-sequence", "duplicate"])
async def test_sequence_identity_rejects_absent_or_ambiguous_defaults(monkeypatch, failure):
    columns = [{"attname": "id", "default_expression": "nextval('synthetic.owned_seq'::regclass)"}]
    if failure == "missing":
        columns[0]["default_expression"] = None
    if failure == "wrong-column":
        columns[0]["attname"] = "unrelated"
    if failure == "wrong-sequence":
        columns[0]["default_expression"] = "nextval('synthetic.other_seq'::regclass)"
    if failure == "duplicate":
        columns.append(dict(columns[0]))
    monkeypatch.setattr(archive, "_OWNED_SEQUENCES", {"synthetic": (("owned_seq", "owned_table", "id"),)})
    monkeypatch.setattr(archive, "_source_owned_sequence", AsyncMock(return_value="owned_seq"))
    monkeypatch.setattr(archive.catalog_identity, "_catalog_columns", AsyncMock(return_value=columns))
    monkeypatch.setattr(archive.catalog_identity, "_catalog_constraints", AsyncMock(return_value=[]))
    monkeypatch.setattr(archive.catalog_identity, "_catalog_indexes", AsyncMock(return_value=[]))
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="owned sequence default"):
        await archive._family_schema_identity(object(), "synthetic", 10, "synthetic", "owned_table")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "records", [[], [{"sequence_name": "bad-name"}], [{"sequence_name": "a"}, {"sequence_name": "b"}]]
)
async def test_owned_sequence_requires_one_exact_catalog_record(records):
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(mappings=lambda: records)))
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="owned sequence is unavailable"):
        await archive._source_owned_sequence(session, 10, "id")


@pytest.mark.parametrize("auxiliary", [None, {}, {"table_name": "other"}])
def test_canonical_auxiliary_receipt_is_mandatory_and_closed(auxiliary):
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="auxiliary receipt is invalid"):
        archive._validate_mrf_auxiliary_receipt(auxiliary)


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["untracked", "incomplete", "drifted", "invalid-snapshot"])
async def test_capture_rejects_incomplete_or_drifted_source_generation(monkeypatch, state):
    @asynccontextmanager
    async def bounded(_session):
        yield

    session = SimpleNamespace(
        in_transaction=lambda: True,
        execute=AsyncMock(
            return_value=SimpleNamespace(
                scalar_one=lambda: "bad snapshot" if state == "invalid-snapshot" else "00000001-00000001-1"
            )
        ),
    )
    monkeypatch.setattr(archive, "_bounded_capture", bounded)
    monkeypatch.setattr(archive, "_lock_source_family", AsyncMock())
    monkeypatch.setattr(archive, "_has_source_generation_authority", AsyncMock(return_value=state != "untracked"))
    authority = SimpleNamespace(
        serving_generation=None if state == "incomplete" else _serving_generation(), relation_oids=(99,)
    )
    monkeypatch.setattr(archive, "read_reference_family_result_generation_authority", AsyncMock(return_value=authority))
    monkeypatch.setattr(
        archive,
        "current_reference_family_relation_oids",
        AsyncMock(return_value=(99,) if state == "invalid-snapshot" else (10,)),
    )
    manifest = AsyncMock(return_value=_manifest())
    monkeypatch.setattr(archive, "_family_manifest", manifest)
    arguments = dict(
        importer_id="places-zcta",
        schema_name="synthetic",
        source_metadata={"release": "synthetic"},
        configure_isolation=False,
    )
    if state == "untracked":
        result = await archive._capture_reference_family_source(session, **arguments)
        assert result.manifest == _manifest()
        assert manifest.await_args.kwargs["source_serving_generation"] is None
    else:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="source generation|source snapshot"):
            await archive._capture_reference_family_source(session, **arguments)
        assert manifest.await_count == int(state == "invalid-snapshot")


@pytest.mark.asyncio
@pytest.mark.parametrize("wrong_scope", [False, True])
async def test_sequence_rebase_locks_and_verifies_retained_ownership(monkeypatch, wrong_scope):
    ownership = replace(_ownership("tiger"), sequence_oids=(("zcta5_gid_seq", 99, "zcta5", "gid"),))
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(), scalar=AsyncMock(return_value=17))
    lock = AsyncMock()
    verify = AsyncMock()
    monkeypatch.setattr(archive, "_lock_family", lock)
    monkeypatch.setattr(archive, "verify_reference_family_stage_ownership", verify)
    if wrong_scope:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="ownership scope differs"):
            await archive._rebase_owned_sequences(session, "other", "tiger", ownership=ownership)
        lock.assert_not_awaited()
    else:
        await archive._rebase_owned_sequences(session, ownership.schema_name, "tiger", ownership=ownership)
        verify.assert_awaited_once_with(session, ownership)
        assert session.execute.await_args.args[1] == {"sequence": "99", "value": 17, "called": True}
        assert "SELECT last_value" in str(session.execute.await_args_list[0].args[0])


@pytest.mark.asyncio
async def test_capture_rejects_missing_mrf_auxiliary_stage(monkeypatch):
    monkeypatch.setattr(archive, "_schema_oid", AsyncMock(return_value=10))
    monkeypatch.setattr(
        archive,
        "_relation_oid",
        AsyncMock(side_effect=lambda _session, _schema, name: None if name == archive.STAGE_TABLE else 11),
    )
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="auxiliary relation is missing"):
        await archive.capture_reference_family_stage_ownership(
            SimpleNamespace(in_transaction=lambda: True), importer_id="mrf", dataset_id=_ownership().dataset_id
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["archive-name", "archive-missing", "invalid-source", "strict-bits"])
async def test_canonical_merge_rejects_invalid_sources_and_preserves_strict_bits(monkeypatch, failure):
    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(return_value=None if failure == "archive-missing" else 10))
    monkeypatch.setattr(archive, "_lock_family", AsyncMock())
    session = SimpleNamespace(
        scalar=AsyncMock(side_effect=[0, int(failure == "invalid-source"), True]), execute=AsyncMock()
    )
    auxiliary = {"archive_name": "other" if failure == "archive-name" else archive.archive_table_name()}
    if failure != "strict-bits":
        with pytest.raises(
            archive.ReferenceFamilyArchiveError, match="archive name|archive is unavailable|source contribution"
        ):
            await archive._merge_mrf_canonical_address(session, _ownership("mrf"), "synthetic", auxiliary)
        session.execute.assert_not_awaited()
    else:
        await archive._merge_mrf_canonical_address(session, _ownership("mrf"), "synthetic", auxiliary)
        statements = [str(call.args[0]) for call in session.execute.await_args_list]
        assert "strict_source_bits=target.strict_source_bits |" in statements[2]
        assert statements[-1].startswith("DROP TABLE")


@pytest.mark.asyncio
async def test_validated_cutover_rejects_post_rotation_oid_drift(monkeypatch):
    monkeypatch.setattr(archive, "_rotate_family_relations", AsyncMock(return_value=None))
    monkeypatch.setattr(archive, "_drop_empty_stage_schema", AsyncMock())
    monkeypatch.setattr(archive, "_incumbent_pairs", AsyncMock(return_value=[("pricing_places_zcta", 99)]))
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="activated relation OID differs"):
        await archive._complete_validated_stage_activation(
            object(),
            archive.reference_family_spec("places-zcta"),
            _ownership(),
            SimpleNamespace(schema_name="synthetic"),
            _manifest(),
        )


def test_declared_family_cannot_repeat_a_table(monkeypatch):
    spec = archive.reference_family_spec("places-zcta")
    monkeypatch.setitem(archive._SPECS, "places-zcta", replace(spec, model_types=spec.model_types * 2))
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="model declaration is invalid"):
        archive.reference_family_spec("places-zcta")


@pytest.mark.asyncio
async def test_export_preparation_failure_does_not_claim_stage_cleanup(monkeypatch):
    prepare = AsyncMock(side_effect=RuntimeError("preparation failed"))
    cleanup = AsyncMock()
    monkeypatch.setattr(archive, "prepare_reference_family_archive_source", prepare)
    monkeypatch.setattr(archive, "_shielded_cleanup", cleanup)
    with pytest.raises(RuntimeError, match="preparation failed"):
        await archive.export_reference_family_archive(
            object(),
            importer_id="places-zcta",
            schema_name="synthetic",
            source_metadata={"release": "synthetic"},
            dataset_id=_ownership().dataset_id,
            archive_copy=AsyncMock(),
        )
    cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancelled_cleanup_finishes_before_propagating_cancellation(monkeypatch):
    entered = asyncio.Event()
    release = asyncio.Event()
    finished = asyncio.Event()

    @asynccontextmanager
    async def transaction():
        yield

    @asynccontextmanager
    async def sessions():
        yield SimpleNamespace(begin=transaction)

    async def cleanup(_session, _owned):
        entered.set()
        await release.wait()
        finished.set()

    monkeypatch.setattr(archive, "cleanup_reference_family_stage", cleanup)
    task = asyncio.create_task(archive._shielded_cleanup(sessions, _ownership()))
    try:
        await asyncio.wait_for(entered.wait(), timeout=1)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=1)
        assert finished.is_set()
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_mrf_activation_rechecks_table_receipts_after_rotation(monkeypatch):
    owned = _ownership("mrf")
    monkeypatch.setattr(archive, "_incumbent_pairs", AsyncMock(return_value=owned.relation_oids))
    monkeypatch.setattr(archive, "_table_receipt", AsyncMock(return_value=None))
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="activated receipt differs"):
        await archive._activation_receipt(
            object(), archive.reference_family_spec("mrf"), owned, _incumbent("mrf"), _manifest(), (), None
        )
