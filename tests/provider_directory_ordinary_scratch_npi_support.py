# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real signed ordinary plans with host-only scratch custody observations."""

import importlib
import json
from contextlib import contextmanager
from contextvars import ContextVar
from types import SimpleNamespace
from unittest.mock import AsyncMock

from process import provider_directory_cms_preparation as preparation
from process import provider_directory_ordinary_scratch_npi as repair
from process.provider_directory_artifact_bundle_preparation import CapturedServingInputs
from tests.provider_directory_serving_capacity_signing_test_support import serving_case, verify_serving_envelope

fhir = importlib.import_module("process.provider_directory_fhir")


def scratch_case(database=None):
    signed = serving_case()
    if database is None:
        database = SimpleNamespace(engine=object(), _transaction_binding=lambda: None)
    directory = SimpleNamespace(
        db=database,
        _schema=lambda: "fixture",
        _q=fhir._q,
        _unscoped_qt=fhir._unscoped_qt,
        _clean_source_id_list=fhir._clean_source_id_list,
        _pagination_checkpoint_row_mapping=lambda row: row,
        ProviderDirectorySource=fhir.ProviderDirectorySource,
        RESOURCE_MODELS=fhir.RESOURCE_MODELS,
        RESOURCE_TYPES_BY_MODEL=fhir.RESOURCE_TYPES_BY_MODEL,
        CMS_NPD_SOURCE_ID=fhir.CMS_NPD_SOURCE_ID,
        _ArtifactScopeMaterializationPlan=fhir._ArtifactScopeMaterializationPlan,
    )
    fence = fhir.ProviderDirectoryArtifactDatasetFence(
        tuple(
            fhir.ProviderDirectoryArtifactDataset(
                source_id=dataset.source_id,
                endpoint_id="endpoint-" + dataset.source_id,
                dataset_id=dataset.dataset_id,
                evidence_run_id=dataset.evidence_run_id,
                selected_resources=dataset.artifact_resources,
            )
            for dataset in signed.fence.datasets
        )
    )
    capture = CapturedServingInputs(
        database,
        database.engine,
        signed.execution,
        fence,
        json.dumps(
            dict(
                scope=signed.plan.payload["ordinary_scope"],
                affected_scope=dict(refresh_source_ids=list(fence.source_ids)),
            )
        ),
        "{}",
    )
    admission = preparation.NonprofileAdmission(
        verify_serving_envelope(signed.envelope), signed.plan, AsyncMock(), captured_inputs=capture
    )
    admission._started = True
    names = preparation._scope_plan(directory).relation_by_table
    oid_by_root = {root: ordinal for ordinal, root in enumerate(sorted(names), 20)}
    admission._relations = {("fixture", name): oid_by_root[root] for root, name in names.items()}
    return SimpleNamespace(
        directory=directory, admission=admission, capture=capture, fence=fence, names=names, oids=oid_by_root
    )


@contextmanager
def issued(case):
    context_defaults_by_name = dict(
        _PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION=None,
        _PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION=case.capture._execution,
        _PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE=case.fence,
        _PROVIDER_DIRECTORY_ARTIFACT_RESOURCE_SCOPE_SOURCE_IDS=tuple(case.fence.source_ids),
        _PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES=dict(case.names),
        _PROVIDER_DIRECTORY_ARTIFACT_OWNED_RELATION_OIDS=dict(case.oids),
    )
    for name, value in context_defaults_by_name.items():
        setattr(case.directory, name, ContextVar(name, default=value))
    token = preparation._ACTIVE.set(case.admission)
    try:
        with repair.ordinary_scratch_scope(case.directory, case.admission, case.fence, case.names, case.oids):
            yield case
    finally:
        preparation._ACTIVE.reset(token)
