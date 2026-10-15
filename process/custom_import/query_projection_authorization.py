# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""One purpose-bound projection operation; request data carries no rows or SQL."""

from dataclasses import asdict, dataclass, field

from process.custom_import import admission_authorization as admission

PROJECTION_PATH = admission.PROJECTION_PATH
PROJECTION_PERMIT_CONTRACT = admission.PROJECTION_PERMIT_CONTRACT
_BODY_FIELDS = frozenset(
    {
        "build_id",
        "execution_id",
        "fence",
        "expected_phase",
        "expected_after_entity_binding_id",
        "expected_after_selected_value",
        "expected_after_child_key_sha256",
        "expected_index_count",
        "expected_proof_count",
    }
)


@dataclass(frozen=True, slots=True)
class ProjectionPermit(admission.AdmissionPermit):
    pass


@dataclass(frozen=True, slots=True)
class ProjectionBatchPins:
    build_id: int
    execution_id: int
    fence: int
    expected_phase: str
    expected_after_entity_binding_id: int
    expected_after_selected_value: int | None
    expected_after_child_key_sha256: str | None
    expected_index_count: int
    expected_proof_count: int = 0


@dataclass(frozen=True, slots=True)
class VerifiedProjection:
    permit: ProjectionPermit
    pins: ProjectionBatchPins
    token: admission.LeaseToken = field(repr=False)


def batch_pins(document):
    """Require a complete retained context cursor, including signed selections."""

    if type(document) is not dict or document.keys() != _BODY_FIELDS:
        raise admission._fail()
    for name in ("build_id", "execution_id", "fence"):
        admission._positive_id(document[name])
    entity = admission._positive_id(document["expected_after_entity_binding_id"], minimum=0)
    phase = document["expected_phase"]
    if type(phase) is not str or phase not in {
        "root",
        "child",
        "indexes",
        "verify_root",
        "verify_child",
        "digest_root",
        "digest_child",
        "complete",
    }:
        raise admission._fail()
    index_count = document["expected_index_count"]
    admission._positive_id(document["expected_proof_count"], minimum=0)
    if phase in {"root", "child", "indexes"} and document["expected_proof_count"]:
        raise admission._fail()
    if type(index_count) is not int or not 0 <= index_count <= 33 or phase in {"root", "child"} and index_count:
        raise admission._fail()
    selected, child_key = document["expected_after_selected_value"], document["expected_after_child_key_sha256"]
    if selected is not None and (type(selected) is not int or not -(2**63) <= selected < 2**63):
        raise admission._fail()
    if child_key is not None and (type(child_key) is not str or admission._SHA256.fullmatch(child_key) is None):
        raise admission._fail()
    if (entity == 0 and (selected is not None or child_key is not None)) or (entity > 0 and selected is None):
        raise admission._fail()
    if (phase == "root" and child_key is not None) or (phase == "child" and entity > 0 and child_key is None):
        raise admission._fail()
    return ProjectionBatchPins(**document)


def verify_request(*, headers, body, method, path, query_string, trusted_now, expected_origin, keyring):
    """Require the dedicated purpose before accepting the retained context cursor."""

    permit, token = admission._verify_authority(
        headers=headers,
        method=method,
        path=path,
        query_string=query_string,
        trusted_now=trusted_now,
        expected_origin=expected_origin,
        keyring=keyring,
        purpose="projection",
    )
    return VerifiedProjection(ProjectionPermit(**asdict(permit)), batch_pins(admission._object(body, 512)), token)
