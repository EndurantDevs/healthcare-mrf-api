# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic-only helpers for rooted-graph HTTP and runtime tests."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import replace
import json
from typing import Any, AsyncIterator

from process.provider_directory_rooted_graph_acquisition import (
    ProviderDirectoryRootedGraphInputSnapshot,
)
from process.provider_directory_rooted_graph_identity import (
    build_provider_directory_rooted_graph_scope,
)
from process.provider_directory_rooted_graph_store_contract import (
    ProviderDirectoryRootedGraphAcquisitionIdentity,
    ProviderDirectoryRootedGraphWorkClaim,
    _canonical_json,
    _sha256_text,
    build_provider_directory_rooted_graph_acquisition_identity,
)
from process.provider_directory_rooted_graph_source_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_AUTHORITY_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
)


API_BASE = "https://directory.synthetic.test/fhir/R4"
ENDPOINT_ID = PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID
ROOT_ENDPOINT_ID = "9" * 64
ROOT_SOURCE_ID = "synthetic-practitioner-root"
ACQUISITION_SOURCE_ID = PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID
SOURCE_AUTHORITY_ID = PROVIDER_DIRECTORY_ROOTED_GRAPH_AUTHORITY_ID
ENDPOINT_SIGNATURE = PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256
DATASET_HASH = "c" * 64
ROOT_PROOF = "d" * 64
DATASET_ID = "synthetic-practitioner-dataset-1"
COHORT_ID = "synthetic-practitioner-cohort-1"
INTENT_ID = "pdrgi_" + "e" * 48


def identity(role: str = "baseline") -> ProviderDirectoryRootedGraphAcquisitionIdentity:
    scope = build_provider_directory_rooted_graph_scope(
        root_dataset_variant="uhc_flex_practitioner",
        root_publication_contract_id=(
            "healthporta.provider-directory.uhc-flex-practitioner-"
            "dataset-publication.v1"
        ),
        root_source_id=ROOT_SOURCE_ID,
        root_endpoint_id=ROOT_ENDPOINT_ID,
        acquisition_source_id=ACQUISITION_SOURCE_ID,
        acquisition_endpoint_id=ENDPOINT_ID,
        source_authority_id=SOURCE_AUTHORITY_ID,
        root_dataset_id=DATASET_ID,
        root_dataset_hash=DATASET_HASH,
        root_content_proof_sha256=ROOT_PROOF,
        root_resource_count=1,
    )
    return build_provider_directory_rooted_graph_acquisition_identity(
        scope,
        root_cohort_id=COHORT_ID,
        endpoint_signature_sha256=ENDPOINT_SIGNATURE,
        acquisition_role=role,
        run_id="pdrgr_" + ("1" if role == "baseline" else "2") * 48,
        dataset_intent_id=INTENT_ID,
    )


def snapshot(
    role: str = "baseline",
    *,
    status: str = "building",
) -> ProviderDirectoryRootedGraphInputSnapshot:
    root = identity(role)
    return ProviderDirectoryRootedGraphInputSnapshot(
        api_base=PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE,
        root_dataset_variant=root.root_dataset_variant,
        root_publication_contract_id=root.root_publication_contract_id,
        root_source_id=root.root_source_id,
        root_endpoint_id=root.root_endpoint_id,
        acquisition_source_id=root.acquisition_source_id,
        acquisition_endpoint_id=root.acquisition_endpoint_id,
        source_authority_id=root.source_authority_id,
        endpoint_signature_sha256=root.endpoint_signature_sha256,
        root_dataset_id=root.root_dataset_id,
        root_dataset_hash=root.root_dataset_hash,
        root_content_proof_sha256=root.root_content_proof_sha256,
        root_resource_count=root.root_resource_count,
        root_cohort_id=root.root_cohort_id,
        max_work_items=root.max_work_items,
        max_resource_rows=root.max_resource_rows,
        max_edge_rows=root.max_edge_rows,
        max_payload_bytes=root.max_payload_bytes,
        acquisition_status=status,
    )


def claim_for_query(
    query: Any,
    *,
    role: str = "baseline",
    closure_scope: str = "root",
    attempt: int = 1,
) -> ProviderDirectoryRootedGraphWorkClaim:
    root = identity(role)
    query_identity = query.identity_document()
    identity_json = _canonical_json(query_identity)
    reference = query_identity["reference"]
    reference_type, reference_id = (
        reference.split("/", 1) if reference is not None else (None, None)
    )
    return ProviderDirectoryRootedGraphWorkClaim(
        acquisition_id=root.acquisition_id,
        scope_id=root.scope_id,
        query_id=query.query_id(root.scope_id),
        query_identity_sha256=_sha256_text(identity_json),
        kind=query.kind,
        resource_type=query.resource_type,
        reference_type=reference_type,
        reference_id=reference_id,
        closure_scope=closure_scope,
        attempt=attempt,
        lease_token="f" * 64,
    )


def replay_claim(
    claim: ProviderDirectoryRootedGraphWorkClaim,
    attempt: int,
) -> ProviderDirectoryRootedGraphWorkClaim:
    return replace(claim, attempt=attempt, lease_token=hex(attempt)[2:] * 64)


def bundle(
    resources: list[dict[str, Any]],
    *,
    total: int | None = None,
    next_url: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "resourceType": "Bundle",
        "type": "searchset",
        "entry": [
            {"search": {"mode": "match"}, "resource": resource}
            for resource in resources
        ],
    }
    if total is not None:
        payload["total"] = total
    if next_url is not None:
        payload["link"] = [{"relation": "next", "url": next_url}]
    return payload


def missing_outcome(status: int) -> dict[str, object]:
    """Return a strict synthetic direct-read absence witness."""

    if status == 404:
        issues = [
            {"severity": "error", "code": "processing"},
            {"severity": "information", "code": "informational"},
        ]
    else:
        issues = [{"severity": "error", "code": "deleted"}]
    return {
        "resourceType": "OperationOutcome",
        "issue": issues,
    }


def missing_outcome_json_text(status: int) -> str:
    return json.dumps(missing_outcome(status), separators=(",", ":"))


class FakeContent:
    def __init__(self, body: bytes, error: BaseException | None = None) -> None:
        self.body = body
        self.error = error

    async def iter_chunked(self, _chunk_size: int):
        if self.error is not None:
            raise self.error
        midpoint = max(1, len(self.body) // 2)
        yield self.body[:midpoint]
        yield self.body[midpoint:]


class FakeResponse:
    def __init__(
        self,
        request_url: str,
        payload: object = None,
        *,
        body: bytes | None = None,
        status: int = 200,
        headers: dict[str, str] | None = None,
        response_url: str | None = None,
        stream_error: BaseException | None = None,
    ) -> None:
        encoded_body = (
            body
            if body is not None
            else json.dumps(payload, separators=(",", ":")).encode("utf-8")
        )
        self.status = status
        self.url = response_url or request_url
        self.headers = headers or {
            "Content-Type": "application/fhir+json; charset=utf-8",
            "Content-Encoding": "identity",
            "Content-Length": str(len(encoded_body)),
        }
        self.content = FakeContent(encoded_body, stream_error)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = list(responses)
        self.requests: list[tuple[str, dict[str, Any]]] = []

    def get(self, request_url: str, **kwargs: Any) -> FakeResponse:
        request_text = str(request_url)
        self.requests.append((request_text, kwargs))
        if not self.responses:
            raise AssertionError("unexpected rooted-graph HTTP request")
        response = self.responses.pop(0)
        if response.url == "__REQUEST_URL__":
            response.url = request_text
        return response


class SessionLedger:
    def __init__(self) -> None:
        self.opened: list[int] = []
        self.closed: list[int] = []

    @asynccontextmanager
    async def scope(self, connection_limit: int) -> AsyncIterator[object]:
        session_id = len(self.opened) + 1
        self.opened.append(session_id)
        try:
            yield {"session_id": session_id, "limit": connection_limit}
        finally:
            self.closed.append(session_id)


__all__ = (
    "bundle",
    "claim_for_query",
    "identity",
    "missing_outcome",
    "replay_claim",
    "snapshot",
    "FakeResponse",
    "FakeSession",
    "SessionLedger",
    "API_BASE",
    "ACQUISITION_SOURCE_ID",
    "COHORT_ID",
    "DATASET_HASH",
    "DATASET_ID",
    "ENDPOINT_ID",
    "ENDPOINT_SIGNATURE",
    "INTENT_ID",
    "ROOT_PROOF",
    "ROOT_ENDPOINT_ID",
    "ROOT_SOURCE_ID",
    "SOURCE_AUTHORITY_ID",
)
