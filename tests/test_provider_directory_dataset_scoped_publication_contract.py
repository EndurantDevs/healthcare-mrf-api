# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed-boundary tests for exact Provider Directory current datasets."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from process import provider_directory_dataset_scoped_publication as publication
from process.provider_directory_dataset_scoped_publication import (
    ExactCurrentDataset,
    LEGACY_PRACTITIONER_VARIANT,
    ProviderDirectoryDatasetScopedPublicationError,
    ROOTED_COMBINED_VARIANT,
    exact_current_matches_root,
    exact_uhc_dataset_pair,
    lock_exact_current_dataset,
    supersede_exact_current_dataset,
)
from process.provider_directory_rooted_graph_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_COMBINED_ROOT_PUBLICATION_CONTRACT_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_LEGACY_ROOT_PUBLICATION_CONTRACT_ID,
)
from process.provider_directory_rooted_graph_source_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256,
)
from process.uhc_flex_official_cohort_contract import (
    UHC_FLEX_OFFICIAL_AUTHORITY_ID,
)
from process.uhc_flex_practitioner_registration import (
    uhc_flex_practitioner_endpoint_identity,
)


def _legacy_current() -> ExactCurrentDataset:
    pair = exact_uhc_dataset_pair()
    return ExactCurrentDataset(
        dataset_id="pdufpd_" + "1" * 48,
        endpoint_id=pair.legacy_endpoint_id,
        source_id=pair.legacy_source_id,
        root_source_id=pair.legacy_source_id,
        root_endpoint_id=pair.legacy_endpoint_id,
        acquisition_source_id=pair.rooted_source_id,
        acquisition_endpoint_id=pair.rooted_endpoint_id,
        practitioner_origin_source_id=pair.legacy_source_id,
        practitioner_origin_endpoint_id=pair.legacy_endpoint_id,
        source_authority_id=UHC_FLEX_OFFICIAL_AUTHORITY_ID,
        endpoint_signature_sha256=(
            PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256
        ),
        dataset_hash="2" * 64,
        resource_count=3,
        practitioner_resource_count=3,
        root_content_proof_sha256="3" * 64,
        root_cohort_id="reviewed-cohort",
        cohort_complete=True,
        retry_exhausted_count=0,
        semantic_projection_as_of="2026-08-10",
        operation_key="4" * 64,
        acquisition_root_run_id="pdufpar_" + "5" * 48,
        variant=LEGACY_PRACTITIONER_VARIANT,
        root_publication_contract_id=(
            PROVIDER_DIRECTORY_ROOTED_GRAPH_LEGACY_ROOT_PUBLICATION_CONTRACT_ID
        ),
    )


def _header_for(current: ExactCurrentDataset) -> dict[str, object]:
    header_by_column = {
        field_name: getattr(current, field_name)
        for field_name in current.__dataclass_fields__
    }
    header_by_column.update(status="published", is_current=True)
    return header_by_column


def _parent_for(current: ExactCurrentDataset) -> dict[str, object]:
    return {
        "dataset_id": current.dataset_id,
        "endpoint_id": current.endpoint_id,
        "dataset_hash": current.dataset_hash,
        "resource_count": current.resource_count,
        "status": "published",
        "is_current": True,
    }


@pytest.mark.parametrize(
    ("field_name", "forged_value"),
    (
        ("variant", []),
        ("root_publication_contract_id", None),
        ("endpoint_id", None),
        ("practitioner_origin_endpoint_id", None),
        ("endpoint_signature_sha256", None),
        ("dataset_hash", None),
        ("root_content_proof_sha256", None),
        ("semantic_projection_as_of", None),
        ("operation_key", None),
    ),
)
def test_exact_current_rejects_forged_types_cleanly(
    field_name: str,
    forged_value: object,
) -> None:
    with pytest.raises(
        ValueError,
        match="provider_directory_exact_current_dataset_invalid",
    ):
        replace(_legacy_current(), **{field_name: forged_value})


def test_exact_current_rejects_cross_variant_publication_contract() -> None:
    with pytest.raises(
        ValueError,
        match="provider_directory_exact_current_dataset_invalid",
    ):
        replace(
            _legacy_current(),
            root_publication_contract_id=(
                PROVIDER_DIRECTORY_ROOTED_GRAPH_COMBINED_ROOT_PUBLICATION_CONTRACT_ID
            ),
        )


def test_exact_current_requires_variant_specific_root_run_id() -> None:
    legacy = _legacy_current()
    with pytest.raises(
        ValueError,
        match="provider_directory_exact_current_dataset_invalid",
    ):
        replace(
            legacy,
            acquisition_root_run_id="pdrgpr_" + "5" * 48,
        )

    pair = exact_uhc_dataset_pair()
    rooted = replace(
        legacy,
        dataset_id="pdrgpd_" + "1" * 48,
        endpoint_id=pair.rooted_endpoint_id,
        source_id=pair.rooted_source_id,
        root_source_id=pair.rooted_source_id,
        root_endpoint_id=pair.rooted_endpoint_id,
        variant=ROOTED_COMBINED_VARIANT,
        root_publication_contract_id=(
            PROVIDER_DIRECTORY_ROOTED_GRAPH_COMBINED_ROOT_PUBLICATION_CONTRACT_ID
        ),
        acquisition_root_run_id="pdrgpr_" + "5" * 48,
    )
    assert rooted.acquisition_root_run_id.startswith("pdrgpr_")
    with pytest.raises(
        ValueError,
        match="provider_directory_exact_current_dataset_invalid",
    ):
        replace(
            rooted,
            acquisition_root_run_id="pdufpar_" + "5" * 48,
        )


def test_exact_current_carries_only_consistent_retry_exhaustion() -> None:
    partial = replace(
        _legacy_current(),
        cohort_complete=False,
        retry_exhausted_count=8,
    )
    assert partial.retry_exhausted_count == 8
    for changes in (
        {"cohort_complete": True},
        {"retry_exhausted_count": 0},
        {"retry_exhausted_count": -1},
    ):
        with pytest.raises(
            ValueError,
            match="provider_directory_exact_current_dataset_invalid",
        ):
            replace(partial, **changes)


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    (
        ("operation_key", "g" * 64),
        ("semantic_projection_as_of", "2026-02-30"),
        ("root_cohort_id", ""),
        ("root_cohort_id", "c" * 129),
    ),
)
def test_exact_current_keeps_content_lineage_validation(
    field_name: str,
    invalid_value: str,
) -> None:
    with pytest.raises(
        ValueError,
        match="provider_directory_exact_current_dataset_invalid",
    ):
        replace(_legacy_current(), **{field_name: invalid_value})


def test_exact_current_root_match_binds_publication_contract() -> None:
    current = _legacy_current()
    identity = SimpleNamespace(
        root_dataset_variant=current.variant,
        root_publication_contract_id=current.root_publication_contract_id,
        root_source_id=current.root_source_id,
        root_endpoint_id=current.root_endpoint_id,
        acquisition_source_id=current.acquisition_source_id,
        acquisition_endpoint_id=current.acquisition_endpoint_id,
        source_authority_id=current.source_authority_id,
        endpoint_signature_sha256=current.endpoint_signature_sha256,
        root_dataset_id=current.dataset_id,
        root_dataset_hash=current.dataset_hash,
        root_content_proof_sha256=current.root_content_proof_sha256,
        root_cohort_id=current.root_cohort_id,
        root_resource_count=current.practitioner_resource_count,
    )

    assert exact_current_matches_root(current, identity) is True
    identity.root_publication_contract_id = (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_COMBINED_ROOT_PUBLICATION_CONTRACT_ID
    )
    assert exact_current_matches_root(current, identity) is False


class _LegacyCurrentDatabase:
    def __init__(self, current: ExactCurrentDataset) -> None:
        self.current = current
        self.identity_valid = True
        self.readiness = True
        self.header_reads: list[str] = []
        self.status_writes: list[str] = []

    async def all(self, statement: str, **_parameters: object) -> list[dict]:
        pair = exact_uhc_dataset_pair()
        if "provider_directory_source" in statement:
            legacy_signature = (
                uhc_flex_practitioner_endpoint_identity().endpoint_signature_hash
            )
            return [
                {
                    "source_id": pair.legacy_source_id,
                    "endpoint_id": pair.legacy_endpoint_id,
                    "source_api_base": PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE,
                    "endpoint_api_base": PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE,
                    "source_authority_id": UHC_FLEX_OFFICIAL_AUTHORITY_ID,
                    "endpoint_authority_id": UHC_FLEX_OFFICIAL_AUTHORITY_ID,
                    "endpoint_signature_sha256": legacy_signature,
                },
                {
                    "source_id": pair.rooted_source_id,
                    "endpoint_id": pair.rooted_endpoint_id,
                    "source_api_base": PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE,
                    "endpoint_api_base": PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE,
                    "source_authority_id": UHC_FLEX_OFFICIAL_AUTHORITY_ID,
                    "endpoint_authority_id": UHC_FLEX_OFFICIAL_AUTHORITY_ID,
                    "endpoint_signature_sha256": (
                        PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256
                    ),
                },
            ]
        if "provider_directory_endpoint_dataset" in statement:
            return [
                {
                    "dataset_id": self.current.dataset_id,
                    "endpoint_id": self.current.endpoint_id,
                    "dataset_hash": self.current.dataset_hash,
                    "resource_count": self.current.resource_count,
                    "status": "published",
                    "is_current": True,
                }
            ]
        raise AssertionError("unexpected all query")

    async def first(self, statement: str, **_parameters: object) -> dict | None:
        self.header_reads.append(statement)
        assert (
            "header.publication_contract_id"
            "\n                       AS root_publication_contract_id"
        ) in statement
        if "provider_directory_uhc_flex_practitioner_dataset" in statement:
            current = self.current
            return {
                "dataset_id": current.dataset_id,
                "endpoint_id": current.endpoint_id,
                "source_id": current.source_id,
                "root_publication_contract_id": (current.root_publication_contract_id),
                "root_source_id": current.root_source_id,
                "root_endpoint_id": current.root_endpoint_id,
                "acquisition_source_id": current.acquisition_source_id,
                "acquisition_endpoint_id": current.acquisition_endpoint_id,
                "practitioner_origin_source_id": (
                    current.practitioner_origin_source_id
                ),
                "practitioner_origin_endpoint_id": (
                    current.practitioner_origin_endpoint_id
                ),
                "source_authority_id": current.source_authority_id,
                "endpoint_signature_sha256": (current.endpoint_signature_sha256),
                "status": "published",
                "is_current": True,
                "dataset_hash": current.dataset_hash,
                "resource_count": current.resource_count,
                "practitioner_resource_count": (current.practitioner_resource_count),
                "root_content_proof_sha256": (current.root_content_proof_sha256),
                "root_cohort_id": current.root_cohort_id,
                "cohort_complete": current.cohort_complete,
                "retry_exhausted_count": current.retry_exhausted_count,
                "semantic_projection_as_of": (current.semantic_projection_as_of),
                "operation_key": current.operation_key,
                "acquisition_root_run_id": current.acquisition_root_run_id,
            }
        if "provider_directory_rooted_graph_dataset" in statement:
            return None
        raise AssertionError("unexpected first query")

    async def scalar(self, statement: str, **_parameters: object) -> object:
        if "_dataset_ready" in statement:
            return self.readiness
        if "_dataset_valid" in statement:
            return self.identity_valid
        raise AssertionError("unexpected scalar query")

    async def status(self, statement: str, **_parameters: object) -> int:
        self.status_writes.append(statement)
        return 1


@pytest.mark.asyncio
async def test_lock_exact_current_reads_and_returns_root_publication_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    expected = _legacy_current()
    database = _LegacyCurrentDatabase(expected)

    current = await lock_exact_current_dataset(
        database,
        pair=exact_uhc_dataset_pair(),
    )

    assert current == expected
    assert len(database.header_reads) == 2


@pytest.mark.asyncio
async def test_supersede_reloads_and_rejects_forged_current_capability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    actual = _legacy_current()
    forged = replace(actual, operation_key="9" * 64)
    database = _LegacyCurrentDatabase(actual)

    with pytest.raises(
        ProviderDirectoryDatasetScopedPublicationError,
    ) as caught:
        await supersede_exact_current_dataset(database, forged)

    assert caught.value.code == "foreign_current"
    assert database.status_writes == []


def test_store_local_schema_row_variant_and_projection_boundaries(monkeypatch) -> None:
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "exact")
    monkeypatch.setenv("DB_SCHEMA", "other")
    with pytest.raises(ProviderDirectoryDatasetScopedPublicationError):
        publication._schema_name()
    monkeypatch.delenv("DB_SCHEMA")
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "bad-name")
    with pytest.raises(ProviderDirectoryDatasetScopedPublicationError):
        publication._schema_name()
    with pytest.raises(ProviderDirectoryDatasetScopedPublicationError):
        publication._row_fields(object())
    assert publication._variant_table_and_ready(ROOTED_COMBINED_VARIANT) == (
        publication.ROOTED_DATASET_TABLE,
        publication.ROOTED_READY_FUNCTION,
    )
    with pytest.raises(ProviderDirectoryDatasetScopedPublicationError):
        publication._variant_table_and_ready("generic")
    assert (
        publication._projection_text(SimpleNamespace(isoformat=lambda: "day")) == "day"
    )
    with pytest.raises(ProviderDirectoryDatasetScopedPublicationError):
        publication._projection_text(None)


@pytest.mark.asyncio
async def test_registry_and_header_selection_reject_drift_and_split_brain(
    monkeypatch,
) -> None:
    current = _legacy_current()
    database = _LegacyCurrentDatabase(current)
    registry_by_coordinate = await publication._locked_registry_by_coordinate(
        database,
        exact_uhc_dataset_pair(),
    )
    publication._validate_pair_registry(
        registry_by_coordinate,
        exact_uhc_dataset_pair(),
    )
    registry_by_coordinate.pop(next(iter(registry_by_coordinate)))
    with pytest.raises(ProviderDirectoryDatasetScopedPublicationError) as drift:
        publication._validate_pair_registry(
            registry_by_coordinate,
            exact_uhc_dataset_pair(),
        )
    assert drift.value.code == "source_drift"

    async def legacy_header(_database, _pair):
        return _header_for(current)

    async def rooted_header(_database, _pair):
        return _header_for(current)

    monkeypatch.setattr(publication, "_locked_legacy_header", legacy_header)
    monkeypatch.setattr(publication, "_locked_rooted_header", rooted_header)
    with pytest.raises(ProviderDirectoryDatasetScopedPublicationError) as split:
        await publication._locked_current_header(database, exact_uhc_dataset_pair())
    assert split.value.code == "both_current"


@pytest.mark.asyncio
async def test_lock_exact_current_rejects_foreign_parent_shapes(monkeypatch) -> None:
    current = _legacy_current()

    async def lock_registry(_database, _pair):
        return None

    async def no_header(_database, _pair):
        return None

    monkeypatch.setattr(publication, "_lock_pair_registry", lock_registry)
    monkeypatch.setattr(publication, "_locked_current_header", no_header)

    async def no_parents(_database, _pair):
        return {}

    monkeypatch.setattr(publication, "_locked_parent_by_id", no_parents)
    assert (
        await lock_exact_current_dataset(object(), pair=exact_uhc_dataset_pair())
        is None
    )

    async def foreign_parent(_database, _pair):
        return {current.dataset_id: _parent_for(current)}

    monkeypatch.setattr(publication, "_locked_parent_by_id", foreign_parent)
    with pytest.raises(ProviderDirectoryDatasetScopedPublicationError) as foreign:
        await lock_exact_current_dataset(object(), pair=exact_uhc_dataset_pair())
    assert foreign.value.code == "foreign_current"
    with pytest.raises(ProviderDirectoryDatasetScopedPublicationError):
        await lock_exact_current_dataset(object(), pair=object())
