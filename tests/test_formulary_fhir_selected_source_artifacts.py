# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from unittest.mock import Mock

import pytest

from process.formulary_fhir import source_artifacts
from process.formulary_fhir.uhc_source_artifacts import identities_from_uhc_drug_catalog
from tests.test_formulary_fhir_source_artifacts import SOURCE_ID
from tests.test_formulary_fhir_source_artifacts import _catalog
from tests.test_formulary_fhir_source_artifacts import _database
from tests.test_formulary_fhir_source_artifacts import _row
from tests.test_formulary_fhir_source_artifacts import _set_row


@pytest.mark.asyncio
async def test_selected_set_loads_only_verified_ids_and_keeps_full_census(monkeypatch):
    identities = identities_from_uhc_drug_catalog(SOURCE_ID, _catalog())
    selected_ids = tuple(identity.source_file_id for identity in identities[1:])
    database = _database()
    database.first.return_value = _set_row(identities)
    database.all.return_value = [
        _row(identity, status="verified", artifact_index=index) if index else _row(identity)
        for index, identity in enumerate(identities)
    ]
    verify_retained = Mock()
    monkeypatch.setattr(source_artifacts, "_verify_retained_source_artifact", verify_retained)

    selected = await source_artifacts.load_selected_source_artifact_set(
        identities,
        selected_source_file_ids=selected_ids,
        require_unselected_pending=True,
        database=database,
    )

    assert tuple(artifact.identity.source_file_id for artifact in selected.artifacts) == selected_ids
    assert len(selected.artifacts) == verify_retained.call_count == 47
