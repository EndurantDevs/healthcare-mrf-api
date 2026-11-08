# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed executor identity, operator lifecycle and retained cleanup boundaries."""

import asyncio
import base64
import hashlib
import os
import sys
from contextlib import asynccontextmanager
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_profile_failed_cleanup as cleanup
from tests.test_provider_directory_profile_failed_cleanup import (
    NOW,
    _completed_history_fixture,
    authorization_fixture,
    initial_authorization_fixture,
    legacy_authorization_fixture,
    sign,
)


@pytest.fixture
def executor_tree(tmp_path):
    source_bytes_by_path = {
        "requirements.txt": b"synthetic-package==1.0\n",
        "process/__init__.py": b"from . import helper\n",
        "process/provider_directory_profile_failed_cleanup.py": (
            b"import os\nfrom process import helper\nfrom .helper import value\nfrom process.helper import *\n"
        ),
        "process/provider_directory_fhir.py": b"from db.connection import connect\n",
        "process/helper.py": b"from process import provider_directory_profile_failed_cleanup\nvalue = 1\n",
        "db/__init__.py": b"",
        "db/connection.py": b"from process.helper import value\n",
    }
    for name, content in source_bytes_by_path.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    return tmp_path, source_bytes_by_path


def test_executor_manifest_binds_transitive_imports_once_and_tracks_reachable_changes(executor_tree):
    root, source_bytes_by_path = executor_tree
    expected_by_path = {
        name: hashlib.sha256(content).hexdigest() for name, content in sorted(source_bytes_by_path.items())
    }
    assert cleanup._executor_source_manifest(root) == expected_by_path
    helper = root / "process/helper.py"
    helper.write_bytes(b"value = 2\n")
    changed = cleanup._executor_source_manifest(root)
    assert changed.keys() == expected_by_path.keys()
    assert [name for name in expected_by_path if changed[name] != expected_by_path[name]] == ["process/helper.py"]


def test_executor_import_closure_resolves_nested_relative_packages_without_external_code(tmp_path):
    names = ("process/__init__.py", "process/nested/__init__.py", "process/helper.py", "db/connection.py")
    for name in names:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
    source = b"from .. import helper\nfrom db.connection import *\nimport json\nimport process.missing\nvalue = 1\n"
    assert cleanup._executor_local_import_paths(tmp_path, "process/nested/entry.py", source) == sorted(
        name for name in names if name != "process/nested/__init__.py"
    )


@pytest.mark.parametrize("change", ["content", "mode", "inode"])
def test_executor_manifest_refuses_real_changes_during_read_and_closes_descriptor(monkeypatch, executor_tree, change):
    root, _sources = executor_tree
    path = root / "requirements.txt"
    source_inode = path.stat().st_ino
    original_read, descriptors = os.read, []

    def changed_read(descriptor, size):
        content = original_read(descriptor, size)
        if not descriptors and os.fstat(descriptor).st_ino == source_inode:
            descriptors.append(descriptor)
            if change == "content":
                path.write_bytes(content + b"\n")
            elif change == "mode":
                path.chmod(0o400)
            else:
                replacement = path.with_name("replacement.txt")
                replacement.write_bytes(content)
                replacement.replace(path)
        return content

    monkeypatch.setattr(os, "read", changed_read)
    with pytest.raises(RuntimeError, match="failed_profile_cleanup_executor_source_changed"):
        cleanup._executor_source_manifest(root)
    with pytest.raises(OSError):
        os.fstat(descriptors[0])


@pytest.mark.parametrize("change", ["symlink", "oversized"])
def test_executor_manifest_refuses_unbounded_or_indirect_source(executor_tree, change):
    root, _sources = executor_tree
    path = root / "requirements.txt"
    if change == "symlink":
        target = root / "external.txt"
        target.write_bytes(b"preserve this source\n")
        path.unlink()
        path.symlink_to(target)
        with pytest.raises(OSError):
            cleanup._executor_source_manifest(root)
        assert target.read_bytes() == b"preserve this source\n"
    else:
        path.write_bytes(b"#" * (8 * 1024 * 1024 + 1))
        with pytest.raises(RuntimeError, match="failed_profile_cleanup_executor_source_unsupported"):
            cleanup._executor_source_manifest(root)


@pytest.mark.asyncio
async def test_observed_executor_identity_binds_runtime_and_import_manifest_separately(monkeypatch, executor_tree):
    from process import provider_directory_profile_runtime_observation as observation

    root, _sources = executor_tree
    manifest = cleanup._executor_source_manifest(root)
    runtime_by_field = {"version": "synthetic", "database_oid": 123}
    observe = AsyncMock(return_value=runtime_by_field)
    monkeypatch.setattr(observation, "observe_profile_runtime", observe)
    monkeypatch.setattr(cleanup, "_executor_source_manifest", lambda _root: manifest)
    database = object()
    identity = await cleanup.observed_executor_identity(SimpleNamespace(db=database))
    assert identity == {
        "runtime_sha256": cleanup.digest(runtime_by_field, cleanup.CONTRACT + ".runtime"),
        "release_digest": cleanup.digest(manifest, cleanup.CONTRACT + ".executor-release"),
    }
    observe.assert_awaited_once_with(database)
    manifest["requirements.txt"] = "f" * 64
    changed = await cleanup.observed_executor_identity(SimpleNamespace(db=database))
    assert changed["runtime_sha256"] == identity["runtime_sha256"]
    assert changed["release_digest"] != identity["release_digest"]


@pytest.fixture
def operator_mocks(monkeypatch):
    envelope, _trust, _key = authorization_fixture()
    fhir = SimpleNamespace(db=SimpleNamespace(connect=AsyncMock(), disconnect=AsyncMock()))
    monkeypatch.setitem(sys.modules, "process.provider_directory_fhir", fhir)
    mocks = SimpleNamespace(
        fhir=fhir,
        envelope=envelope,
        identity=AsyncMock(return_value=envelope["authorization"]["executor_identity"]),
        inspect=AsyncMock(return_value={"state": "failed"}),
        execute=AsyncMock(return_value={"operation_id": "operation-a"}),
        reconcile=AsyncMock(return_value={"operation_id": "operation-a"}),
        read=Mock(return_value=(b"synthetic", envelope)),
    )
    for name, replacement in (
        ("observed_executor_identity", mocks.identity),
        ("inspect_failed_profile_cleanup", mocks.inspect),
        ("execute_failed_profile_cleanup", mocks.execute),
        ("reconcile_failed_profile_cleanup", mocks.reconcile),
        ("_protected_json_file", mocks.read),
    ):
        monkeypatch.setattr(cleanup, name, replacement)
    return mocks


def operator_arguments(mode, **changes):
    arguments_by_field = dict(
        mode=mode, build_id=None, owner_run_id=None, published_run_id=None, private_input_file=None
    )
    arguments_by_field.update(changes)
    return SimpleNamespace(**arguments_by_field)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["inspect", "execute", "reconcile"])
async def test_operator_dispatch_preserves_identity_and_disconnects(operator_mocks, mode):
    mocks = operator_mocks
    fields = (
        {"build_id": "build-a", "owner_run_id": "run-a"}
        if mode == "inspect"
        else {"private_input_file": "authority.json"}
    )
    result = await cleanup._operator(operator_arguments(mode, **fields))
    mocks.fhir.db.connect.assert_awaited_once_with()
    mocks.fhir.db.disconnect.assert_awaited_once_with()
    if mode == "inspect":
        assert result == {"state": "failed", "executor_identity": mocks.identity.return_value}
        mocks.inspect.assert_awaited_once_with(
            mocks.fhir, build_id="build-a", owner_run_id="run-a", published_run_id=None
        )
        mocks.read.assert_not_called()
    elif mode == "execute":
        mocks.execute.assert_awaited_once_with(
            mocks.fhir, mocks.envelope, cleanup_trust=None, executor_identity=mocks.identity.return_value
        )
    else:
        mocks.reconcile.assert_awaited_once_with(mocks.fhir, mocks.envelope, cleanup_trust=None)
    if mode != "inspect":
        assert result == {"operation_id": "operation-a"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode,fields",
    [
        ("inspect", {}),
        ("inspect", {"build_id": "build-a"}),
        ("inspect", {"owner_run_id": "run-a"}),
        ("inspect", {"build_id": "build-a", "owner_run_id": "run-a", "private_input_file": "authority.json"}),
        ("execute", {}),
        ("execute", {"private_input_file": "authority.json", "build_id": "build-a"}),
        ("execute", {"private_input_file": "authority.json", "owner_run_id": "run-a"}),
        ("reconcile", {"private_input_file": "authority.json", "published_run_id": "run-published"}),
    ],
)
async def test_operator_refuses_mixed_arguments_before_dispatch_and_disconnects(operator_mocks, mode, fields):
    with pytest.raises(RuntimeError, match="failed_profile_cleanup_arguments_invalid"):
        await cleanup._operator(operator_arguments(mode, **fields))
    operator_mocks.read.assert_not_called()
    for handler in (operator_mocks.inspect, operator_mocks.execute, operator_mocks.reconcile):
        handler.assert_not_awaited()
    operator_mocks.fhir.db.disconnect.assert_awaited_once_with()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["identity", "read", "execute", "reconcile", "inspect"])
async def test_operator_disconnects_when_measurement_input_or_action_fails(operator_mocks, phase):
    failure = RuntimeError("synthetic refusal")
    getattr(operator_mocks, phase).side_effect = failure
    mode = phase if phase in {"inspect", "reconcile"} else "execute"
    fields = (
        {"build_id": "build-a", "owner_run_id": "run-a"}
        if mode == "inspect"
        else {"private_input_file": "authority.json"}
    )
    with pytest.raises(RuntimeError) as raised:
        await cleanup._operator(operator_arguments(mode, **fields))
    assert raised.value is failure
    operator_mocks.fhir.db.disconnect.assert_awaited_once_with()


@pytest.mark.parametrize("arguments", [[], ["delete"], ["inspect", "--unknown"]])
def test_cli_rejects_unsupported_actions_without_starting_operator(monkeypatch, capsys, arguments):
    operator = AsyncMock()
    monkeypatch.setattr(cleanup, "_operator", operator)
    monkeypatch.setattr(sys, "argv", ["cleanup", *arguments])
    with pytest.raises(SystemExit) as raised:
        cleanup.main()
    assert raised.value.code == 2
    assert "usage:" in capsys.readouterr().err
    operator.assert_not_called()


def set_nested(values, path, replacement):
    for field in path[:-1]:
        values = values[field]
    values[path[-1]] = replacement


@pytest.mark.parametrize(
    "path,replacement,reason",
    [
        (("issued_at",), None, "timestamp_invalid"),
        (("issued_at",), NOW.replace(tzinfo=None).isoformat(), "timestamp_invalid"),
        (("authorization_id",), "invalid identity", "identity_invalid"),
        (("attestor_release_digest",), "A" * 64, "digest_invalid"),
        (("checkpoint", "authority_revision"), True, "integer_invalid"),
        (("checkpoint", "build_id"), "build-a", "build_invalid"),
        (("database", "tablespaces"), [], "tablespaces_invalid"),
        (("database", "tablespaces", 0, "usage"), "temp", "tablespaces_invalid"),
        (("stages", 0, "role"), "profile", "stages_invalid"),
        (("limits", "lock_timeout_ms"), 30001, "timeout_invalid"),
        (("volumes",), [], "volumes_invalid"),
        (("volumes", 0, "reserved_bytes"), 1, "volume_limit_invalid"),
        (("volumes", 0, "available_after_all_reservations_bytes"), 10**10 + 1, "volume_remaining_invalid"),
        (("volumes", 0, "available_bytes"), 10**10 + 1, "colocated_volume_invalid"),
        (("database", "tablespaces", 0, "volume_digest"), "b" * 64, "tablespace_volume_invalid"),
        (
            ("observations", "accounted_reservation_ids"),
            ["reservation-a", "reservation-a"],
            "accounting_identities_invalid",
        ),
    ],
)
def test_signed_authority_refuses_invalid_closed_coordinates(path, replacement, reason):
    envelope, trust, key = authorization_fixture()
    set_nested(envelope["authorization"], path, replacement)
    with pytest.raises(RuntimeError, match="failed_profile_cleanup_" + reason):
        cleanup.validate_authorization(sign(envelope["authorization"], key), trust=trust, now=NOW)


@pytest.mark.parametrize("change", ["binding", "database", "tablespaces", "volumes", "duplicate_key", "release"])
def test_independent_trust_rejects_authority_from_a_different_epoch(change):
    envelope, trust, _key = authorization_fixture()
    reason_by_change = {
        "binding": "trust_binding_invalid",
        "database": "trust_database_invalid",
        "tablespaces": "trust_tablespaces_invalid",
        "volumes": "trust_volumes_invalid",
        "duplicate_key": "trust_key_invalid",
        "release": "trust_key_invalid",
    }
    if change == "binding":
        trust.environment_id = "environment-b"
    elif change == "database":
        trust.database_oid += 1
    elif change in {"tablespaces", "volumes"}:
        setattr(trust, change, ())
    elif change == "duplicate_key":
        trust.keys = trust.keys * 2
    else:
        trust.keys[0].attestor_release_digest = "e" * 64
    with pytest.raises(RuntimeError, match="failed_profile_cleanup_" + reason_by_change[change]):
        cleanup.validate_authorization(envelope, trust=trust, now=NOW)


@pytest.mark.parametrize("change", ["noncanonical_signature", "changed_signed_body", "malformed_public_key"])
def test_signature_refuses_equivalent_encoding_or_unverified_bytes(change):
    envelope, trust, _key = authorization_fixture()
    if change == "noncanonical_signature":
        alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
        signature = envelope["signature"]
        envelope["signature"] = signature[:-1] + alphabet[alphabet.index(signature[-1]) + 1]
        assert base64.urlsafe_b64decode(signature + "==") == base64.urlsafe_b64decode(envelope["signature"] + "==")
    elif change == "changed_signed_body":
        envelope["authorization"]["operation_id"] = "operation-b"
    else:
        trust.keys[0].public_key = b"invalid"
    with pytest.raises(RuntimeError, match="failed_profile_cleanup_signature_invalid"):
        cleanup.validate_authorization(envelope, trust=trust, now=NOW)


@pytest.mark.parametrize(
    "path,replacement,reason",
    [
        (("publication", "serving_state"), "multiple", "publication_invalid"),
        (("publication", "targets"), [], "publication_invalid"),
        (("publication", "targets", 0, "role"), "profile", "publication_invalid"),
        (("physical", "stage_dependencies"), [], "dependencies_invalid"),
        (("physical", "stage_dependencies", 0, "role"), "profile", "dependencies_invalid"),
        (("physical", "postgres", "database_oid"), 123, "physical_database_changed"),
        (("physical", "postgres", "postgres_full_page_writes"), 1, "physical_invalid"),
        (("checkpoint", "lineage_kind"), "unknown", "legacy_lineage_partial"),
    ],
)
def test_signed_legacy_cleanup_refuses_unknown_physical_and_publication_witnesses(path, replacement, reason):
    envelope, trust, key = legacy_authorization_fixture()
    set_nested(envelope["authorization"], path, replacement)
    with pytest.raises(RuntimeError, match="failed_profile_cleanup_" + reason):
        cleanup.validate_authorization(sign(envelope["authorization"], key), trust=trust, now=NOW)


def test_legacy_complete_lineage_is_retained_as_signed_authority():
    envelope, trust, key = legacy_authorization_fixture()
    checkpoint = envelope["authorization"]["checkpoint"]
    checkpoint.update(lineage_kind="complete", selection_proof_id="proof-a", authority_revision=1, control_generation=2)
    assert (
        cleanup.validate_authorization(sign(envelope["authorization"], key), trust=trust, now=NOW)["checkpoint"]
        == checkpoint
    )


@pytest.mark.parametrize(
    "field", ["operation_id", "nonce", "expires_at", "max_operation_deadline", "authorization_json"]
)
def test_retained_claim_requires_exact_identity_and_canonical_authorization(field):
    envelope, _trust, _checkpoint, claim, _receipt = _completed_history_fixture()
    assert cleanup._claim_envelope(claim) == envelope
    if field.endswith("_at") or field == "max_operation_deadline":
        claim[field] += timedelta(microseconds=1)
    else:
        claim[field] += " "
    with pytest.raises(RuntimeError, match="failed_profile_cleanup_claim_corrupt"):
        cleanup._claim_envelope(claim)


@pytest.mark.parametrize(
    "change,reason",
    [
        ("variant", "completion_variant_changed"),
        ("operation", "completion_binding_invalid"),
        ("stages", "completion_binding_invalid"),
        ("timestamp", "completion_timestamp_invalid"),
        ("preclaim", "completion_timestamp_invalid"),
        ("wal", "completion_wal_invalid"),
        ("noncanonical", "completion_invalid"),
    ],
)
def test_completion_refuses_tampered_receipts_and_cannot_advance_claimed_time(change, reason):
    envelope, _trust, checkpoint, claim, receipt = _completed_history_fixture()
    match change:
        case "variant":
            receipt.update(contract_id=cleanup.LEGACY_RECEIPT_CONTRACT, variant="legacy_full_swap")
        case "operation":
            receipt["operation_id"] = "operation-b"
        case "stages":
            receipt["disposed_stages"] = list(reversed(receipt["disposed_stages"]))
        case "timestamp":
            checkpoint["updated_at"] += timedelta(microseconds=1)
        case "preclaim":
            checkpoint["updated_at"] = NOW - timedelta(microseconds=1)
            receipt["completed_at"] = checkpoint["updated_at"].isoformat()
        case "wal":
            receipt["wal_precommit_bytes"] = envelope["authorization"]["limits"]["wal_bytes"] + 1
    suffix = (
        cleanup.completion_suffix(receipt)
        if change != "noncanonical"
        else cleanup.MARKER + cleanup.json.dumps(receipt) + "]"
    )
    checkpoint["last_error"] = "original failure" + suffix
    with pytest.raises(RuntimeError, match="failed_profile_cleanup_" + reason):
        cleanup.validate_completion_preimage(checkpoint, claim)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change,reason",
    [
        ("empty", "drop_dependency_bound_unsupported"),
        ("oversized", "drop_dependency_bound_unsupported"),
        ("kind", "drop_object_kind_unsupported"),
        ("external", "drop_external_dependency"),
        ("relations", "drop_dependency_bound_unsupported"),
        ("shape", "drop_catalog_shape_unsupported"),
        ("rows", "drop_catalog_bound_unsupported"),
        ("wal", "drop_catalog_wal_exceeded"),
    ],
)
async def test_drop_closure_rejects_unbounded_or_external_catalog_mutations(monkeypatch, change, reason):
    objects = [] if change == "empty" else [(999 if change == "kind" else 1259, 17000, 0)]
    if change == "oversized":
        objects *= 129
    fhir = SimpleNamespace(
        db=SimpleNamespace(
            all=AsyncMock(return_value=objects),
            scalar=AsyncMock(side_effect=[change == "external", 33 if change == "relations" else 1, change == "shape"]),
            status=AsyncMock(),
        )
    )
    catalogs = [
        {
            "deleted_rows": 8193 if change == "rows" else 1,
            "wal_upper_bytes": 32 * 1024 * 1024 + 1 if change == "wal" else 1,
        }
    ]
    monkeypatch.setattr(cleanup, "_catalog_deletion_manifest", AsyncMock(return_value=catalogs))
    with pytest.raises(RuntimeError, match="failed_profile_cleanup_" + reason):
        await cleanup._stage_dependency_manifest(fhir, "synthetic", {"role": "evidence", "oid": 17000})
    fhir.db.status.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("claim_kind", ["missing", "different"])
async def test_disposal_requires_exact_visible_claim_before_any_stage_drop(monkeypatch, claim_kind):
    envelope, _trust, checkpoint, claim, _receipt = _completed_history_fixture()
    if claim_kind == "different":
        different, _trust, _key = authorization_fixture()
        claim["authorization_json"] = cleanup.canonical(different["authorization"])
        claim["authorization_sha256"] = cleanup.digest(different)
        claim["signature"] = different["signature"]
        claim["checkpoint_preimage_sha256"] = different["authorization"]["checkpoint"]["preimage_sha256"]
    row = None if claim_kind == "missing" else SimpleNamespace(_mapping=claim)
    fhir = SimpleNamespace(
        db=SimpleNamespace(first=AsyncMock(return_value=row), status=AsyncMock()),
        _unscoped_qt=lambda schema, name: schema + "." + name,
    )
    tail = AsyncMock()
    monkeypatch.setattr(cleanup, "_receipt_tail_bound", tail)
    with pytest.raises(RuntimeError, match="failed_profile_cleanup_claim_not_visible"):
        await cleanup._dispose_claimed_stages(fhir, "synthetic", envelope, checkpoint, "0/100")
    tail.assert_not_awaited()
    fhir.db.status.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_disposal_keeps_original_error_and_drains_canceled_reconciliation(monkeypatch):
    original = RuntimeError("synthetic disposal refusal")
    started, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def reconcile(*_arguments):
        started.set()
        await release.wait()
        finished.set()
        raise RuntimeError("synthetic readback refusal")

    monkeypatch.setattr(cleanup, "_disposal_transaction", AsyncMock(side_effect=original))
    monkeypatch.setattr(cleanup, "_reconcile_under_guards", reconcile)
    task = asyncio.create_task(cleanup._dispose_and_reconcile_under_guards(object(), {}, None))
    try:
        await asyncio.wait_for(started.wait(), 5)
        task.cancel()
        release.set()
        with pytest.raises(RuntimeError) as raised:
            await task
        assert raised.value is original
        assert task.done() and finished.is_set()
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change,reason",
    [
        ("checkpoint_missing", "checkpoint_missing"),
        ("owner_missing", "owner_missing"),
        ("owner", "owner_not_failed_terminal"),
        ("state", "owner_not_failed_terminal"),
        ("importer", "owner_not_failed_terminal"),
        ("status", "owner_not_failed_terminal"),
        ("finished", "owner_not_failed_terminal"),
        ("variant", "variant_changed"),
        ("affected", "legacy_affected_stage_present"),
        ("unknown", "historical_geometry_unsupported"),
    ],
)
async def test_locked_cleanup_requires_exact_failed_terminal_owner(change, reason):
    checkpoint_by_field = dict(owner_run_id="run-a", state="failed", materialization_mode="source_delta")
    owner_by_field = dict(importer="provider-directory-fhir", status="failed", finished_at=NOW)
    if change in {"owner", "state"}:
        checkpoint_by_field["owner_run_id" if change == "owner" else "state"] = "changed"
    elif change in {"importer", "status", "finished"}:
        owner_by_field[{"finished": "finished_at"}.get(change, change)] = None if change == "finished" else "changed"
    elif change == "affected":
        checkpoint_by_field.update(
            materialization_mode="full_swap",
            capacity_geometry_status="legacy_unavailable",
            affected_npi_stage="affected_stage",
            affected_npi_stage_oid=17002,
        )
    elif change == "unknown":
        checkpoint_by_field["materialization_mode"] = "unknown"
    database_rows = [
        None if change == "checkpoint_missing" else SimpleNamespace(_mapping=checkpoint_by_field),
        None if change == "owner_missing" else SimpleNamespace(_mapping=owner_by_field),
    ]
    fhir = SimpleNamespace(
        db=SimpleNamespace(first=AsyncMock(side_effect=database_rows), status=AsyncMock()),
        _provider_directory_profile_checkpoint_ref=lambda schema: schema + ".checkpoint",
        _unscoped_qt=lambda schema, name: schema + "." + name,
    )
    with pytest.raises(RuntimeError, match="failed_profile_cleanup_" + reason):
        await cleanup._checkpoint_and_owner(
            fhir,
            "synthetic",
            "build-a",
            "run-a",
            locked=True,
            variant="legacy_full_swap" if change == "variant" else None,
        )
    fhir.db.status.assert_not_awaited()
    assert all("FOR UPDATE NOWAIT" in call.args[0] for call in fhir.db.first.await_args_list)


@pytest.mark.parametrize("change", ["params", "error", "selection", "partial"])
def test_legacy_checkpoint_coordinates_require_complete_original_owner_preimage(change):
    envelope, _trust, _key = legacy_authorization_fixture()
    checkpoint_by_field = dict(envelope["authorization"]["checkpoint"], last_error="original failure")
    params = {} if change != "params" else []
    if change == "error":
        checkpoint_by_field["last_error"] = None
    elif change == "selection":
        params["provider_directory_profile_selection_attestation"] = ["proof-a"]
    elif change == "partial":
        params["provider_directory_profile_generation"] = 2
    owner_by_field = {"params": cleanup.canonical(params)}
    reason = "legacy_owner_preimage_invalid" if change in {"params", "error"} else "legacy_lineage_partial"
    with pytest.raises(RuntimeError, match="failed_profile_cleanup_" + reason):
        cleanup._legacy_checkpoint_coordinates(checkpoint_by_field, owner_by_field)


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy", [False, True])
async def test_stage_storage_change_refuses_disposal_without_a_legacy_absence_witness(legacy):
    roles = ("evidence", "profile") if legacy else ("evidence", "profile", "affected_npi")
    checkpoint_by_field = dict(
        materialization_mode="full_swap" if legacy else "source_delta", capacity_geometry_status="legacy_unavailable"
    )
    for position, role in enumerate(roles):
        checkpoint_by_field[role + "_stage_oid"] = 17000 + position
        checkpoint_by_field[role + "_stage_storage_fingerprint"] = "a" * 64
    fhir = SimpleNamespace(
        db=SimpleNamespace(status=AsyncMock()),
        _validated_profile_checkpoint_stage_names=lambda _checkpoint: tuple(role + "_stage" for role in roles),
        _provider_directory_profile_stage_relation_identity=AsyncMock(return_value=(17000, "r", "p")),
        _provider_directory_profile_stage_storage_fingerprint=AsyncMock(return_value="b" * 64),
    )
    with pytest.raises(RuntimeError, match="failed_profile_cleanup_stage_storage_changed"):
        await cleanup._stage_manifest(fhir, "synthetic", checkpoint_by_field, locked=False)
    fhir.db.status.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("authority", ["capacity", "transaction", "executor"])
async def test_executor_refuses_borrowed_authority_or_changed_identity_before_claim_lookup(monkeypatch, authority):
    envelope, trust, _key = authorization_fixture()
    fhir = SimpleNamespace(
        db=SimpleNamespace(_transaction_binding=lambda: object() if authority == "transaction" else None),
        _provider_directory_profile_capacity_admission=lambda: object() if authority == "capacity" else None,
    )
    read_claim = AsyncMock()
    monkeypatch.setattr(cleanup, "_read_claim", read_claim)
    reason = {
        "capacity": "active_capacity_context",
        "transaction": "fresh_transaction_required",
        "executor": "executor_identity_changed",
    }[authority]
    with pytest.raises(RuntimeError, match="failed_profile_cleanup_" + reason):
        await cleanup.execute_failed_profile_cleanup(fhir, envelope, cleanup_trust=trust, executor_identity={})
    read_claim.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["already_spent", "lost_ack_missing", "lost_ack_exact", "unresolved"])
async def test_spent_claim_transaction_requires_exact_durable_readback(monkeypatch, case):
    envelope, _trust, _checkpoint, claim, _receipt = _completed_history_fixture()
    commit_error = RuntimeError("synthetic commit acknowledgement lost")

    @asynccontextmanager
    async def begin():
        yield
        if case.startswith("lost_ack"):
            raise commit_error

    session = SimpleNamespace(
        begin=begin,
        execute=AsyncMock(side_effect=[None, None, SimpleNamespace(rowcount=0 if case == "already_spent" else 1)]),
    )

    @asynccontextmanager
    async def session_scope():
        yield session

    fhir = SimpleNamespace(
        db=SimpleNamespace(session=session_scope), _unscoped_qt=lambda schema, name: schema + "." + name
    )
    readback = AsyncMock(return_value=claim if case == "lost_ack_exact" else None)
    monkeypatch.setattr(cleanup, "_read_claim", readback)
    if case == "lost_ack_exact":
        assert await cleanup._commit_spent_claim(fhir, "synthetic", envelope) == claim
        assert readback.await_count == 2
    else:
        reason = {
            "already_spent": "authorization_already_spent",
            "lost_ack_missing": "acknowledgement lost",
            "unresolved": "claim_commit_unresolved",
        }[case]
        with pytest.raises(RuntimeError, match=reason) as raised:
            await cleanup._commit_spent_claim(fhir, "synthetic", envelope)
        if case == "lost_ack_missing":
            assert raised.value is commit_error
    assert "INSERT INTO" in str(session.execute.await_args_list[-1].args[0])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case,reason",
    [
        ("wal", "observed_wal_exceeded"),
        ("error", "complete_error_exceeds_budget"),
        ("update", "checkpoint_update_changed"),
    ],
)
async def test_disposal_refuses_exceeded_completion_bounds(monkeypatch, case, reason):
    envelope, _trust, checkpoint, claim, _receipt = _completed_history_fixture()
    checkpoint.update(owner_run_id="run-a", last_error="original failure")
    if case == "error":
        checkpoint["last_error"] = "x" * cleanup.MAX_BYTES
    observation_by_field = dict(completed_at=NOW + timedelta(seconds=1), lsn="0/200", bytes=256)
    if case == "wal":
        observation_by_field["bytes"] = envelope["authorization"]["limits"]["wal_bytes"] + 1
    fhir = SimpleNamespace(
        db=SimpleNamespace(
            first=AsyncMock(
                side_effect=[SimpleNamespace(_mapping=claim), SimpleNamespace(_mapping=observation_by_field)]
            ),
            status=AsyncMock(return_value=0),
        ),
        _unscoped_qt=lambda schema, name: schema + "." + name,
        _provider_directory_profile_checkpoint_ref=lambda schema: schema + ".checkpoint",
        _validated_profile_checkpoint_stage_names=lambda _checkpoint: (
            "evidence_stage",
            "profile_stage",
            "affected_stage",
        ),
    )
    monkeypatch.setattr(cleanup, "_receipt_tail_bound", AsyncMock(return_value=(0, False)))
    with pytest.raises(RuntimeError, match="failed_profile_cleanup_" + reason):
        await cleanup._dispose_claimed_stages(fhir, "synthetic", envelope, checkpoint, "0/100")
    statements = [call.args[0] for call in fhir.db.status.await_args_list]
    assert statements[:3] == [
        "DROP TABLE synthetic." + name for name in ("evidence_stage", "profile_stage", "affected_stage")
    ]
    assert len(statements) == (4 if case == "update" else 3)


@pytest.mark.asyncio
async def test_disposal_deadline_refuses_before_opening_transaction(monkeypatch):
    envelope, trust, _key = authorization_fixture()
    envelope["authorization"]["max_operation_deadline"] = NOW.isoformat()
    transaction = Mock()
    fhir = SimpleNamespace(_schema=lambda: "synthetic", db=SimpleNamespace(transaction=transaction))
    monkeypatch.setattr(cleanup, "_now_utc", lambda: NOW)
    with pytest.raises(RuntimeError, match="failed_profile_cleanup_deadline_reached"):
        await cleanup._disposal_transaction(fhir, envelope, trust)
    transaction.assert_not_called()


@pytest.mark.asyncio
async def test_disposed_checkpoint_requires_durable_claim_after_valid_marker():
    _envelope, _trust, checkpoint, _claim, _receipt = _completed_history_fixture()
    fhir = SimpleNamespace(
        db=SimpleNamespace(first=AsyncMock(return_value=None)), _unscoped_qt=lambda schema, name: schema + "." + name
    )
    with pytest.raises(RuntimeError, match="failed_profile_cleanup_claim_missing"):
        await cleanup.validate_disposed_checkpoint(fhir, "synthetic", checkpoint)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case,reason",
    [
        ("missing", "existing_serving_prerequisite_missing"),
        ("stage", "stage_is_serving"),
        ("delta", "historical_delta_publication"),
        ("cms", "historical_cms_publication"),
    ],
)
async def test_disposal_refuses_live_or_previously_published_stages(case, reason):
    envelope, _trust, _key = authorization_fixture()
    coordinates = envelope["authorization"]["checkpoint"]
    checkpoint_by_field = {
        role + "_stage_oid": stage["oid"]
        for role, stage in zip(
            ("evidence", "profile", "affected_npi"), envelope["authorization"]["stages"], strict=True
        )
    }
    serving_by_field = {"evidence_target_oid": 17000 if case == "stage" else 18000, "profile_target_oid": 18001}
    serving = None if case == "missing" else SimpleNamespace(_mapping=serving_by_field)
    fhir = SimpleNamespace(
        db=SimpleNamespace(
            first=AsyncMock(return_value=serving),
            scalar=AsyncMock(side_effect=[None, None, case == "delta", True]),
            status=AsyncMock(),
        ),
        _unscoped_qt=lambda schema, name: schema + "." + name,
        _provider_directory_profile_serving_generation_ref=lambda schema: schema + ".serving",
        _profile_serving_state_from_row=Mock(),
    )
    with pytest.raises(RuntimeError, match="failed_profile_cleanup_" + reason):
        await cleanup._exclude_publication(fhir, "synthetic", checkpoint_by_field, coordinates)
    assert all(call.args[0].startswith("LOCK TABLE ") for call in fhir.db.status.await_args_list)
    fhir._profile_serving_state_from_row.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "position,reason", [(0, "active_competing_owner"), (1, "active_competing_checkpoint"), (2, "unresolved_preflight")]
)
async def test_disposal_refuses_competing_work_and_unspent_preflight(position, reason):
    fhir = SimpleNamespace(
        db=SimpleNamespace(scalar=AsyncMock(side_effect=[index == position for index in range(3)])),
        _unscoped_qt=lambda schema, name: schema + "." + name,
        _provider_directory_profile_checkpoint_ref=lambda schema: schema + ".checkpoint",
        _profile_capacity_preflight_receipt_ref=lambda schema: schema + ".preflight",
        _PROFILE_ACTIVE_RUN_STATUSES=("queued", "running"),
    )
    with pytest.raises(RuntimeError, match="failed_profile_cleanup_" + reason):
        await cleanup._exclude_competitors(fhir, "synthetic", "run-a")
    assert fhir.db.scalar.await_count == position + 1
    assert fhir.db.scalar.await_args_list[0].kwargs["owner"] == "run-a"


@pytest.mark.asyncio
async def test_invalid_json_completion_marker_refuses_before_database_lookup():
    fhir = SimpleNamespace(db=SimpleNamespace(first=AsyncMock()))
    with pytest.raises(RuntimeError, match="failed_profile_cleanup_completion_invalid"):
        await cleanup.validate_disposed_checkpoint(fhir, "synthetic", {"last_error": cleanup.MARKER + "{]"})
    fhir.db.first.assert_not_awaited()


@pytest.mark.parametrize(
    "case,reason",
    [
        ("size", "authorization_too_large"),
        ("accounting", "reservation_accounting_invalid"),
        ("signature", "signature_invalid"),
    ],
)
def test_cleanup_authority_enforces_envelope_and_shared_volume_bounds(case, reason):
    envelope, trust, key = authorization_fixture()
    if case == "size":
        envelope["authorization"]["observations"]["accounted_reservation_ids"] = sorted(
            ["reservation-a", *(f"reservation-{index:06d}" for index in range(4000))]
        )
    elif case == "accounting":
        for volume in envelope["authorization"]["volumes"]:
            volume["available_after_all_reservations_bytes"] = volume["available_bytes"]
    envelope = sign(envelope["authorization"], key)
    if case == "signature":
        envelope["signature"] = None
    with pytest.raises(RuntimeError, match="failed_profile_cleanup_" + reason):
        cleanup.validate_authorization(envelope, trust=trust, now=NOW)


@pytest.mark.parametrize(
    "case,reason",
    [
        ("target", "initial_target_changed"),
        ("layout", "initial_checkpoint_layout_changed"),
        ("stage", "stage_is_serving"),
    ],
)
def test_initial_cleanup_request_binds_original_target_layout_and_nonstage_objects(case, reason):
    from process import provider_directory_profile_capacity as capacity

    envelope, _trust, _key, geometry = initial_authorization_fixture(with_geometry=True)
    body = envelope["authorization"]
    if case == "target":
        geometry["evidence_target_oid"] += 1000
        body["checkpoint"]["capacity_geometry_hash"] = capacity.capacity_geometry_hash(
            capacity.validated_capacity_geometry(geometry)
        )
    elif case == "layout":
        body["physical"]["checkpoint_layout"]["oid"] += 1
    else:
        body["stages"][0]["oid"] = body["database"]["database_oid"]
    with pytest.raises(RuntimeError, match="failed_profile_cleanup_" + reason):
        cleanup._validate_initial_request(dict(body, capacity_geometry=geometry))
