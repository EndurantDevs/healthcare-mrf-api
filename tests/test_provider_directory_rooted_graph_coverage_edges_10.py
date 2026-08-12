# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed edge coverage for rooted graph persistence boundaries."""

from __future__ import annotations

from datetime import UTC, datetime
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import db.connection as db_connection
from process import provider_directory_rooted_graph_identity as graph_identity
from process import provider_directory_rooted_graph_publication as publication
from process import (
    provider_directory_rooted_graph_publication_materialization as materialization,
)
from process import (
    provider_directory_rooted_graph_publication_readiness_store as readiness_store,
)
from process import (
    provider_directory_rooted_graph_publication_store_support as publication_support,
)
from process import provider_directory_rooted_graph_registration as registration
from process import provider_directory_rooted_graph_result_contract as result_contract
from process import provider_directory_rooted_graph_result_store as result_store
from process import (
    provider_directory_rooted_graph_result_validation as result_validation,
)
from process import provider_directory_rooted_graph_store as graph_store
from process import provider_directory_rooted_graph_twin_store as twin_store
from process.provider_directory_rooted_graph_result_contract import _sha256_text
from tests.provider_directory_rooted_graph_publication_test_support import (
    exact_current,
    sealed_roots,
)
from tests.test_provider_directory_rooted_graph_registration import (
    _RegistrationDatabase,
)
from tests.test_provider_directory_rooted_graph_result_boundaries import (
    _direct_claim,
)
from tests.test_provider_directory_rooted_graph_store import (
    _census_claim_row,
    _Database,
)
from tests.test_provider_directory_rooted_graph_store_contract import (
    API_BASE,
    _identity,
    _scope,
)
from tests.test_provider_directory_rooted_graph_twin_store_boundaries import (
    _attempt_and_admission,
    _record,
    _root_row,
    _ScriptedDatabase,
)


def test_registration_scalar_and_decoder_edges_fail_closed(monkeypatch) -> None:
    """Malformed local values are rejected before any registry write."""

    with pytest.raises(ValueError, match="registration_result_invalid"):
        registration.ProviderDirectoryRootedGraphRegistrationResult(
            source_id="invalid",
            endpoint_id=registration.PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID,
            endpoint_created=False,
            source_created=False,
        )

    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "bad-schema")
    with pytest.raises(registration.ProviderDirectoryRootedGraphRegistrationError):
        registration._schema_name()

    assert registration._row_fields(None) == {}
    with pytest.raises(registration.ProviderDirectoryRootedGraphRegistrationError):
        registration._row_fields(object())
    with pytest.raises(registration.ProviderDirectoryRootedGraphRegistrationError):
        registration._json_object("{")
    with pytest.raises(registration.ProviderDirectoryRootedGraphRegistrationError):
        registration._json_object("[]")
    with pytest.raises(registration.ProviderDirectoryRootedGraphRegistrationError):
        registration._is_inserted(True)


@pytest.mark.asyncio
async def test_registration_endpoint_and_source_rows_reject_absence_and_drift() -> None:
    """Each persisted half of the immutable registry pair is independently checked."""

    expected_identity = registration.provider_directory_rooted_graph_endpoint_identity()
    missing_database = _RegistrationDatabase()
    with pytest.raises(registration.ProviderDirectoryRootedGraphRegistrationError):
        await registration._validate_endpoint(missing_database, expected_identity)
    with pytest.raises(registration.ProviderDirectoryRootedGraphRegistrationError):
        await registration._validate_source(
            missing_database,
            expected_identity.endpoint_id,
        )

    malformed_database = _RegistrationDatabase()
    await registration._has_created_endpoint(malformed_database, expected_identity)
    malformed_database.endpoints[expected_identity.endpoint_id][
        "canonical_api_base"
    ] = "not-a-url"
    with pytest.raises(registration.ProviderDirectoryRootedGraphRegistrationError):
        await registration._validate_endpoint(malformed_database, expected_identity)

    metadata_database = _RegistrationDatabase()
    await registration._has_created_endpoint(metadata_database, expected_identity)
    metadata_database.endpoints[expected_identity.endpoint_id]["metadata_json"][
        "manual_only"
    ] = False
    with pytest.raises(registration.ProviderDirectoryRootedGraphRegistrationError):
        await registration._validate_endpoint(metadata_database, expected_identity)


@pytest.mark.asyncio
async def test_registration_default_database_path_remains_test_local(
    monkeypatch,
) -> None:
    """The optional database path resolves the injected process-local registry."""

    database = _RegistrationDatabase()
    monkeypatch.setattr(db_connection, "db", database)

    result = await registration.register_provider_directory_rooted_graph_source()

    assert result.created is True
    assert result.source_id in database.sources


def test_scope_identity_rejects_bounds_shape_and_disappearing_keys() -> None:
    """Scope validation handles ordinary and adversarial Mapping failures."""

    with pytest.raises(ValueError, match="max_work_items_invalid"):
        graph_identity._bounded_positive_count(0, 1, "max_work_items")
    with pytest.raises(ValueError, match="scope_identity_invalid"):
        graph_identity._validated_scope_by_field({})

    scope = _scope()
    value_by_field = {
        field_name: getattr(scope, field_name)
        for field_name in graph_identity._SCOPE_FIELD_NAMES
    }

    class _DisappearingField(dict[str, object]):
        def __getitem__(self, key: str) -> object:
            if key == "root_source_id":
                raise KeyError(key)
            return super().__getitem__(key)

    with pytest.raises(ValueError, match="scope_identity_invalid"):
        graph_identity._validated_scope_by_field(_DisappearingField(value_by_field))


def test_twin_row_decoders_reject_constructor_level_drift() -> None:
    """Structurally present rows still must satisfy immutable twin contracts."""

    baseline, _candidate, attempt, admission = _attempt_and_admission()
    malformed_root = _root_row(baseline)
    malformed_root["acquisition_id"] = "invalid"
    with pytest.raises(twin_store.ProviderDirectoryRootedGraphTwinError):
        twin_store._root_from_row(malformed_root)

    with pytest.raises(twin_store.ProviderDirectoryRootedGraphTwinError):
        twin_store._attempt_from_row(None)
    malformed_attempt = _record(attempt)
    malformed_attempt["attempt_id"] = "invalid"
    with pytest.raises(twin_store.ProviderDirectoryRootedGraphTwinError):
        twin_store._attempt_from_row(malformed_attempt)

    malformed_admission = _record(admission)
    malformed_admission["admission_id"] = "invalid"
    with pytest.raises(twin_store.ProviderDirectoryRootedGraphTwinError):
        twin_store._admission_from_row(malformed_admission)


@pytest.mark.asyncio
async def test_twin_admission_rejects_a_missing_persisted_admission(
    monkeypatch,
) -> None:
    """A matched attempt cannot succeed without its exact admission row."""

    roots = sealed_roots()
    stored_by_kind: dict[str, object] = {}

    async def lock_current(_database):
        return exact_current()

    async def lock_roots(_database, _acquisition_ids):
        return roots

    async def insert_attempt(_database, attempt):
        stored_by_kind["attempt"] = attempt

    async def read_attempt(_database, _attempt_id):
        return stored_by_kind["attempt"]

    async def ignore_insert(_database, _admission):
        return None

    async def missing_admission(_database, _publication_acquisition_id):
        return None

    monkeypatch.setattr(twin_store, "_lock_logical_current", lock_current)
    monkeypatch.setattr(twin_store, "_lock_roots", lock_roots)
    monkeypatch.setattr(twin_store, "_insert_attempt", insert_attempt)
    monkeypatch.setattr(twin_store, "_read_attempt", read_attempt)
    monkeypatch.setattr(twin_store, "_insert_authority", ignore_insert)
    monkeypatch.setattr(twin_store, "_read_admission", missing_admission)
    monkeypatch.setattr(twin_store, "_require_exact", lambda *_arguments: None)
    database = _ScriptedDatabase(scalars=(datetime(2026, 8, 10, tzinfo=UTC),))

    with pytest.raises(twin_store.ProviderDirectoryRootedGraphTwinError) as caught:
        await twin_store.admit_provider_directory_rooted_graph_twins(
            roots[0].acquisition_id,
            roots[1].acquisition_id,
            database=database,
        )

    assert caught.value.code == "state"


@pytest.mark.asyncio
async def test_required_twin_rejects_invalid_id_and_unmatched_attempt(
    monkeypatch,
) -> None:
    """Read-side admission proof rejects malformed and unmatched authorities."""

    with pytest.raises(ValueError, match="acquisition_id_invalid"):
        await twin_store.require_provider_directory_rooted_graph_admission(
            "invalid",
            database=_ScriptedDatabase(),
        )

    _baseline, _candidate, _attempt, admission = _attempt_and_admission()
    mismatched_roots = (
        sealed_roots()[0],
        sealed_roots(second_resource_hash="e" * 64)[1],
    )
    mismatched_attempt = twin_store.build_provider_directory_rooted_graph_twin_attempt(
        *mismatched_roots,
        attempted_at=admission.admitted_at,
    )

    async def read_admission(_database, _publication_acquisition_id):
        return admission

    async def read_attempt(_database, _attempt_id):
        return mismatched_attempt

    async def lock_roots(_database, _acquisition_ids):
        return mismatched_roots

    monkeypatch.setattr(twin_store, "_read_admission", read_admission)
    monkeypatch.setattr(twin_store, "_read_attempt", read_attempt)
    monkeypatch.setattr(twin_store, "_lock_roots", lock_roots)
    with pytest.raises(twin_store.ProviderDirectoryRootedGraphTwinError) as caught:
        await twin_store.require_provider_directory_rooted_graph_admission(
            admission.publication_acquisition_id,
            database=_ScriptedDatabase(),
        )
    assert caught.value.code == "state"


@pytest.mark.asyncio
async def test_census_claim_boundaries_reject_invalid_inputs_and_rows(
    monkeypatch,
) -> None:
    """Census admission remains bounded when no work or bad anchors are returned."""

    identity = _identity()
    with pytest.raises(ValueError, match="identity_invalid"):
        await graph_store.claim_provider_directory_rooted_graph_census(
            "invalid",
            database=_Database(),
        )
    with pytest.raises(ValueError, match="lease_invalid"):
        await graph_store.claim_provider_directory_rooted_graph_census(
            identity,
            lease_seconds=1,
            database=_Database(),
        )

    async def no_claim(*_arguments, **_keywords):
        return None, ()

    monkeypatch.setattr(graph_store, "_admit_and_claim_census", no_claim)
    assert (
        await graph_store.claim_provider_directory_rooted_graph_census(
            identity,
            database=_Database(),
        )
        is None
    )

    async def invalid_references(*_arguments, **_keywords):
        return _census_claim_row(identity), ("invalid",)

    monkeypatch.setattr(
        graph_store,
        "_admit_and_claim_census",
        invalid_references,
    )
    with pytest.raises(graph_store.ProviderDirectoryRootedGraphStoreError):
        await graph_store.claim_provider_directory_rooted_graph_census(
            identity,
            database=_Database(),
        )


@pytest.mark.asyncio
async def test_existing_census_work_is_claimed_without_duplicate_insert(
    monkeypatch,
) -> None:
    """An existing census row skips admission-time reinsertion."""

    identity = _identity()

    async def closure_fields(_database, _identity):
        return {
            "canonical_api_base": API_BASE,
            "census_count": 1,
            "root_network_references": (),
        }

    async def claimed_row(*_arguments, **_keywords):
        return {"claim": "synthetic"}

    set_action = AsyncMock()
    insert_work = AsyncMock()
    monkeypatch.setattr(graph_store, "_root_closure_fields", closure_fields)
    monkeypatch.setattr(graph_store, "_claim_work_row", claimed_row)
    monkeypatch.setattr(graph_store, "set_store_action", set_action)
    monkeypatch.setattr(graph_store, "insert_work_spec", insert_work)

    claimed, references = await graph_store._admit_and_claim_census(
        _Database(),
        identity,
        "f" * 64,
        300,
    )

    assert claimed == {"claim": "synthetic"}
    assert references == ()
    insert_work.assert_not_awaited()


def test_publication_helper_type_and_schema_boundaries(monkeypatch) -> None:
    """Publication metadata and schema helpers reject non-contract values."""

    assert (
        publication._has_valid_publication_metadata_inputs(
            object(),
            object(),
            None,
            {},
        )
        is False
    )

    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "runtime_schema")
    monkeypatch.setenv("DB_SCHEMA", "legacy_schema")
    with pytest.raises(readiness_store.ProviderDirectoryRootedGraphPublicationError):
        readiness_store._schema()

    for module, function in (
        (materialization, materialization._schema),
        (readiness_store, readiness_store._schema),
        (publication_support, lambda: publication_support.publication_table("row")),
    ):
        monkeypatch.delenv("DB_SCHEMA", raising=False)
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "bad-schema")
        with pytest.raises(module.ProviderDirectoryRootedGraphPublicationError):
            function()


def test_materialization_rejects_normalizer_identity_drift(monkeypatch) -> None:
    """Normalized resource identity must equal the sealed graph key."""

    monkeypatch.setattr(
        materialization,
        "_raw_graph_resource",
        lambda _fields, _key: {
            "resourceType": "Organization",
            "id": "org.synthetic-1",
        },
    )
    monkeypatch.setattr(
        materialization,
        "materialize_provider_directory_dataset_fhir_resource",
        lambda **_keywords: object(),
    )
    monkeypatch.setattr(
        materialization,
        "_resource_record",
        lambda *_arguments: {
            "resource_type": "Location",
            "resource_id": "org.synthetic-1",
        },
    )
    identity = SimpleNamespace(
        source_id="synthetic-source",
        dataset_id="synthetic-dataset",
        semantic_projection_as_of="2026-08-10T00:00:00+00:00",
        root_dataset_id="synthetic-root",
        publication_acquisition_id="pdrga_" + "a" * 48,
    )
    field_by_name = {
        "resource_type": "Organization",
        "resource_id": "org.synthetic-1",
    }

    with pytest.raises(materialization.ProviderDirectoryRootedGraphPublicationError):
        materialization._materialized_graph_pair(
            field_by_name,
            identity,
            "synthetic-publication-run",
        )


def test_result_contract_and_claim_binding_reject_advertised_total_drift() -> None:
    """Direct and exact-search totals cannot claim a different row count."""

    direct_shape = SimpleNamespace(kind=result_contract.ROOTED_GRAPH_QUERY_DIRECT_READ)
    with pytest.raises(ValueError, match="result_invalid"):
        result_contract._normalized_inputs(direct_shape, [], set(), 0, 1)

    exact_claim = SimpleNamespace(
        resource_type="PractitionerRole",
        reference_type="Practitioner",
        reference_id="practitioner.synthetic-1",
        closure_scope="root",
    )
    with pytest.raises(ValueError, match="result_invalid"):
        result_validation._validate_exact_result_binding(
            exact_claim,
            SimpleNamespace(advertised_total=1, resources=()),
        )


@pytest.mark.asyncio
async def test_missing_terminalization_rejects_a_lost_lease() -> None:
    """A direct-read absence cannot terminalize after ownership is lost."""

    claim = _direct_claim()
    response_text = json.dumps(
        {
            "resourceType": "OperationOutcome",
            "issue": [{"severity": "error", "code": "not-found"}],
        },
        separators=(",", ":"),
    )
    with pytest.raises(graph_store.ProviderDirectoryRootedGraphStoreError) as caught:
        await result_store.complete_provider_directory_rooted_graph_missing(
            claim,
            missing_http_status=404,
            missing_response_sha256=_sha256_text(response_text),
            missing_response_bytes=len(response_text.encode("utf-8")),
            missing_response_json_text=response_text,
            database=_Database(status_counts=(0,)),
        )

    assert caught.value.code == "lease_lost"
