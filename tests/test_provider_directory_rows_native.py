# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native PractitionerRole row preparation is byte-identical to the Python chain, or defers to it."""

from __future__ import annotations

import importlib
import importlib.util
import inspect
import json
import os
import random
import re

import pytest

from tests import provider_directory_rows_cases as cases

if importlib.util.find_spec("ptg2_address_canon") is None:
    pytest.importorskip("ptg2_address_canon", exc_type=ModuleNotFoundError)
ptg2_address_canon = importlib.import_module("ptg2_address_canon")

seam = importlib.import_module("process.provider_directory_rows_native")
directory = importlib.import_module("process.provider_directory_fhir")
role_merge = importlib.import_module("process.provider_directory_role_merge")
storage = importlib.import_module("process.provider_directory_generic_source_storage")
grouping = importlib.import_module("process.provider_directory_bulk_grouping")
bulk_prepare = importlib.import_module("process.provider_directory_bulk_prepare")

CONTRACTS = ("semantic_content_v4", "semantic_content_v3", "transport_neutral_v2", "transport_bound_v1")
LOG_IDENTITY = directory._bulk_capability_log_identity("https://Bulk.Example.test/out/file_1.ndjson?sig=abc")
GENERATED_LINES = int(os.environ.get("PROVIDER_DIRECTORY_ROWS_DIFFERENTIAL_LINES", "4000"))


def _compiled_export(name):
    export = getattr(ptg2_address_canon, name)
    assert inspect.isbuiltin(export), f"installed {name} must be compiled"
    return export


def _context(contract, merge_roles, strict=False, resource_type="PractitionerRole", dataset_id="dataset-1"):
    return bulk_prepare.PrepareContext(
        source_id="synthetic-source",
        resource_type=resource_type,
        run_id="run-1",
        log_identity=LOG_IDENTITY,
        dataset_id=dataset_id,
        resource_hash_contract=contract,
        semantic_projection_as_of="2026-01-02",
        merge_roles=merge_roles,
        strict=strict,
    )


def _native_context_by_name(contract=CONTRACTS[0]):
    return {
        "source_id": "synthetic-source",
        "dataset_id": "dataset-1",
        "resource_hash_contract": contract,
        "merge_roles": False,
        "resource_url": None,
        "self_url": None,
        "fetch_url": None,
        "fetch_mode": None,
        "payload_text": False,
        "strict": False,
    }


def _typed(value):
    """Order- and type-exact view: equal only when keys, order, types and values all match."""

    if isinstance(value, dict):
        return (type(value).__name__, tuple((key, _typed(item)) for key, item in value.items()))
    if isinstance(value, list):
        return ("list", tuple(_typed(item) for item in value))
    return (type(value).__name__, value)


def _reference(lines, context):
    try:
        return bulk_prepare.prepare_lines(list(lines), context), None
    except Exception as error:  # the seam must raise the same
        return None, error


def assert_batch_parity(lines, contract, merge_roles, strict=False, resource_type="PractitionerRole"):
    """Compare the seam with the Python chain field by field; return the native refusal reason, if any."""

    context = _context(contract, merge_roles, strict, resource_type)
    expected, error = _reference(lines, context)
    if error is not None:
        with pytest.raises(type(error), match=re.escape(str(error))):
            seam.prepare_directory_rows(lines, context, payload_text=True)
        return "python_raises"
    prepared = seam.prepare_directory_rows(lines, context, payload_text=True)
    assert (prepared.error, prepared.rows_fetched) == (expected.error, expected.rows_fetched)
    assert _typed(prepared.rows) == _typed(expected.rows)
    if prepared.payload_texts is not None:
        assert prepared.unsupported_reason is None
        assert prepared.payload_texts == [storage._encoded(item["payload_json"]) for item in expected.rows]
        for prepared_row, expected_row in zip(prepared.rows, expected.rows, strict=True):
            assert role_merge.merged_role_hash_components(
                prepared_row["payload_json"]
            ) == role_merge.merged_role_hash_components(expected_row["payload_json"])
        return None
    return prepared.unsupported_reason or "not_native"


def test_exports_are_compiled_with_matching_version():
    _compiled_export("prepare_provider_directory_rows_v1")
    assert _compiled_export("provider_directory_rows_version")() == seam.ROWS_CONTRACT_VERSION


@pytest.mark.parametrize("contract", CONTRACTS)
@pytest.mark.parametrize("merge_roles", [False, True])
def test_every_fixture_role_matches_python(contract, merge_roles):
    lines = [json.dumps(resource).encode() for resource in cases.base_roles()]
    reasons = [assert_batch_parity([line], contract, merge_roles) for line in lines]
    assert_batch_parity(lines, contract, merge_roles)
    assert len(cases.harvested_roles()) >= 20
    assert reasons.count(None) >= len(lines) - 4


def test_plan_net_batch_with_network_siblings_stays_native():
    resources = [cases._plan_net_role("role-1", "net-a"), cases._plan_net_role("role-2", "net-b")]
    lines = [json.dumps(resource).encode() for resource in resources]
    for contract in CONTRACTS:
        for merge_roles in (False, True):
            assert assert_batch_parity(lines * 2, contract, merge_roles) is None
    prepared = seam.prepare_directory_rows(lines, _context(CONTRACTS[0], True))
    assert len(prepared.rows) == 1
    assert type(prepared.rows[0]["payload_json"]) is role_merge._HashedRole
    assert prepared.rows[0]["payload_json"]["network_refs"] == [
        "Organization/nested",
        "Organization/net-a",
        "Organization/net-b",
    ]


def test_generated_batches_match_python_byte_for_byte():
    reason_counts_by_name: dict[str, int] = {}
    rng = random.Random(7)
    for batch in cases.generated_batches(20261119, GENERATED_LINES):
        reason = assert_batch_parity(batch, rng.choice(CONTRACTS), rng.random() < 0.7, strict=rng.random() < 0.3)
        reason_counts_by_name[str(reason)] = reason_counts_by_name.get(str(reason), 0) + 1
    assert reason_counts_by_name.get("None", 0) > 0


EXACT_LINES = (
    b"",
    b"  \t ",
    b'\xef\xbb\xbf{"resourceType": "PractitionerRole", "id": "bom"}',
    b'{"resourceType": "PractitionerRole", "id": "dup", "telecom": [{"b": 1, "a": 2, "b": -0}], "id": "last"}',
    b'{"resourceType": "PractitionerRole", "id": 12345678901234567890123, "telecom": [{"rank": 10}]}',
    b'{"resourceType": "PractitionerRole", "id": true, "active": "yes"}',
    ('{"resourceType": "PractitionerRole", "id": "%s"}' % ("a" * 255 + " b")).encode(),
    '{"resourceType": "PractitionerRole", "id": " \\u3000caf\\u00e9\\ud83d\\ude00\\u0085 "}'.encode(),
    b'{"resourceType": "PractitionerRole", "practitioner": [{"reference": "\\u001cPractitioner/1\\u001f"}]}',
    b'{"resourceType": "PractitionerRole", "id": "zero", "identifier": [{"system": "npi", "value": "0000000000"}]}',
    b'{"resourceType": "PractitionerRole", "id": "port", "meta": {"source": "http://host:0443/x"}}',
    b'{"resourceType": "PractitionerRole", "id": "wide", "x": ' + b"1" * 640 + b"}",
)


@pytest.mark.parametrize("line", EXACT_LINES)
def test_reproduced_edge_lines_stay_native(line):
    assert assert_batch_parity([line], CONTRACTS[0], True) is None


HAZARDS = (
    ([b'{"resourceType": "PractitionerRole", "id": NaN}'], "non_finite_number"),
    ([b'{"resourceType": "PractitionerRole", "x": -Infinity}'], "non_finite_number"),
    ([b'{"resourceType": "PractitionerRole", "id": "\\ud800"}'], "lone_surrogate"),
    ([b'{"resourceType": "PractitionerRole", "id": "bad\xff"}'], "invalid_utf8"),
    ([b'{"resourceType": "PractitionerRole"} x'], "invalid_json"),
    ([b"[1]"], "line_not_object"),
    ([b'{"resourceType": "Practitioner"}'], "line_resource_type"),
    ([b'{"resourceType": "PractitionerRole", "telecom": [{"rank": 1.0}]}'], "float_value"),
    ([b'{"resourceType": "PractitionerRole", "id": 1e2}'], "text_repr"),
    ([b'{"resourceType": "PractitionerRole", "id": ["a"]}'], "text_repr"),
    ([b'{"resourceType": "PractitionerRole", "telecom": 5}'], "not_iterable"),
    ([b'{"resourceType": "PractitionerRole", "meta": {"source": "http://[::1]/x"}}'], "url_network_location"),
    ([b'{"resourceType": "PractitionerRole", "meta": {"source": "https://h/\\u212aey/v"}}'], "unicode_lowercase"),
    (
        [b'{"resourceType": "PractitionerRole", "identifier": [{"system": "npi", "value": "\\u0661"}]}'],
        "unicode_identifier",
    ),
    (
        [
            b'{"resourceType": "PractitionerRole", "id": "same", "active": true}',
            b'{"resourceType": "PractitionerRole", "id": "same", "active": false}',
        ],
        "payload_conflict",
    ),
    # Past the lowest digit limit an interpreter can be given, Python decides.
    ([b'{"resourceType": "PractitionerRole", "id": "wide", "x": ' + b"1" * 641 + b"}"], "integer_digits"),
    (
        [b'{"resourceType": "PractitionerRole", "meta": {"source": "http://host:' + b"0" * 640 + b'80/x"}}'],
        "port_digits",
    ),
)


@pytest.mark.parametrize(("lines", "reason"), HAZARDS)
@pytest.mark.parametrize("strict", [False, True])
def test_hazards_are_refused_and_python_result_returned(lines, reason, strict):
    native = _compiled_export("prepare_provider_directory_rows_v1")
    outcome = native(lines, "PractitionerRole", _native_context_by_name())
    assert tuple(outcome) == ("unsupported", reason)
    assert assert_batch_parity(lines, CONTRACTS[0], False, strict=strict) in {reason, "python_raises"}


def test_unported_resource_type_uses_python_chain():
    lines = [b'{"resourceType": "Practitioner", "id": "p1", "name": [{"family": "Doe"}]}']
    assert assert_batch_parity(lines, CONTRACTS[0], False, resource_type="Practitioner") == "not_native"
    native = _compiled_export("prepare_provider_directory_rows_v1")
    refusal = native(lines, "Practitioner", _native_context_by_name())
    assert tuple(refusal) == ("unsupported", "resource_type_not_ported")


def test_missing_dataset_and_bad_contract_follow_python():
    line = json.dumps(cases._plan_net_role("role-1", "net-a")).encode()
    prepared = seam.prepare_directory_rows([line], _context(CONTRACTS[0], False, dataset_id=None))
    assert (prepared.rows, prepared.rows_fetched, prepared.unsupported_reason) == ([], 1, None)
    with pytest.raises(ValueError, match="provider_directory_resource_hash_contract_invalid"):
        seam.prepare_directory_rows([line], _context("unknown_contract", False))


def test_version_mismatch_raises_instead_of_falling_back(monkeypatch):
    monkeypatch.setattr(ptg2_address_canon, "provider_directory_rows_version", lambda: "provider_directory_rows_v0")
    with pytest.raises(RuntimeError, match="provider_directory_rows_native_version_mismatch"):
        seam.prepare_directory_rows([b"{}"], _context(CONTRACTS[0], False))


def test_fold_merged_roles_equals_env_opt_in(monkeypatch):
    lines = [json.dumps(cases._plan_net_role(f"role-{index}", f"net-{index}")).encode() for index in range(3)]
    expected, _ = _reference(lines, _context(CONTRACTS[0], False))
    parsed_rows = [{"source_id": "synthetic-source"}]
    monkeypatch.setenv(role_merge.MERGED_ROLE_SOURCES_ENV, "synthetic-source")
    from_env = role_merge.merged_role_rows(expected.rows, parsed_rows, CONTRACTS[0])
    assert _typed(from_env) == _typed(role_merge.fold_merged_roles(expected.rows, CONTRACTS[0]))
    monkeypatch.setenv(role_merge.MERGED_ROLE_SOURCES_ENV, "other-source")
    assert role_merge.merged_role_rows(expected.rows, parsed_rows, CONTRACTS[0]) is expected.rows


def _python_row_lines(lines, context):
    """The per-line mode with every line run through today's Python chain."""

    native_export = seam._native_export
    seam._native_export = lambda name: None
    try:
        return seam.prepare_directory_row_lines(lines, context), None
    except Exception as error:  # the native mode must raise the same
        return None, error
    finally:
        seam._native_export = native_export


def assert_row_lines_parity(lines, contract=CONTRACTS[0], strict=False):
    """Per line: equal role id and the very group line Python writes; return the fallback counts by reason."""

    context = _context(contract, True, strict)
    expected, error = _python_row_lines(lines, context)
    if error is not None:
        with pytest.raises(type(error), match=re.escape(str(error))):
            seam.prepare_directory_row_lines(lines, context)
        return {"python_raises": len(lines)}
    prepared = seam.prepare_directory_row_lines(lines, context)
    assert (prepared.error, prepared.rows_fetched) == (expected.error, expected.rows_fetched)
    assert prepared.resource_ids == expected.resource_ids
    assert prepared.encoded_lines == expected.encoded_lines
    assert [json.loads(line) for line in prepared.encoded_lines] == [
        json.loads(line) for line in expected.encoded_lines
    ]
    return prepared.fallback_counts_by_reason


def test_row_lines_match_single_line_python_chain():
    fallback_counts_by_reason: dict[str, int] = {}
    rng = random.Random(3)
    fixture_lines = [json.dumps(resource).encode() for resource in cases.base_roles()]
    batches = [fixture_lines, *cases.generated_batches(20261119, GENERATED_LINES)]
    for batch in batches:
        reasons_by_name = assert_row_lines_parity(batch, rng.choice(CONTRACTS), strict=rng.random() < 0.3)
        for reason, count in reasons_by_name.items():
            fallback_counts_by_reason[reason] = fallback_counts_by_reason.get(reason, 0) + count
    assert fallback_counts_by_reason.get("float_value", 0) > 0


def test_row_lines_fall_back_per_line_and_stop_at_batch_errors():
    good = json.dumps(cases._plan_net_role("role-1", "net-a")).encode()
    lines = [good, b'{"resourceType": "PractitionerRole", "id": NaN}', b"", b"[1]", good]
    prepared = seam.prepare_directory_row_lines(lines, _context(CONTRACTS[0], True))
    assert prepared.fallback_counts_by_reason == {"non_finite_number": 1}
    assert (len(prepared.encoded_lines), prepared.rows_fetched, prepared.error) == (3, 3, None)
    strict = seam.prepare_directory_row_lines(lines, _context(CONTRACTS[0], True, strict=True))
    assert (len(strict.encoded_lines), strict.rows_fetched, strict.error) == (2, 2, "invalid_ndjson")
    for case_lines in (lines, [good, b"{"], [good, b'{"resourceType": "Practitioner"}']):
        for strict_mode in (False, True):
            assert_row_lines_parity(case_lines, strict=strict_mode)


def _reduced(tmp_path, name, prepared_batches):
    """Group prepared batches the way the grouped load does, then reduce every partition."""

    root = tmp_path / name
    for output_index, prepared in enumerate(prepared_batches):
        writer = grouping.GroupWriter(root, output_index, 4)
        if isinstance(prepared, seam.PreparedRowLines):
            writer.write_encoded(prepared.resource_ids, prepared.encoded_lines)
        else:
            writer.write(prepared.rows)
        writer.finish(prepared.rows_fetched)
    indexes = range(len(prepared_batches))
    return [
        reduced_row
        for partition in range(4)
        for reduced_row in grouping.reduce_partition(root, partition, indexes, CONTRACTS[0], 4)
    ]


def _role_line(role_id, network, last_updated="2026-03-04T05:06:07Z"):
    role = cases._plan_net_role(role_id, network)
    role["meta"]["lastUpdated"] = last_updated
    role["identifier"][1]["value"] = "shared"
    return json.dumps(role).encode()


def test_single_line_union_equals_batch_path_union(tmp_path):
    batches = [
        [_role_line("r1", "n1"), _role_line("r2", "n2"), _role_line("r1", "n1", "2026-05-01T00:00:00Z")],
        [_role_line("r3", "n3"), _role_line("r1", "n1", "2025-01-01T00:00:00Z"), _role_line("r4", "n1")],
        *cases.generated_batches(5, 2000),
    ]
    context = _context(CONTRACTS[0], True)
    batch_results = []
    line_results = []
    for batch in batches:
        expected, error = _reference(batch, context)
        prepared, line_error = _python_row_lines(batch, context)
        if error is not None or line_error is not None or expected.error is not None or prepared.error is not None:
            continue
        batch_results.append(expected)
        line_results.append(seam.prepare_directory_row_lines(batch, context))
    assert sum(len(item.encoded_lines) for item in line_results) > sum(len(item.rows) for item in batch_results) > 100
    assert _typed(_reduced(tmp_path, "lines", line_results)) == _typed(_reduced(tmp_path, "batches", batch_results))


def test_one_id_with_and_without_meta_stays_two_roles():
    bare_role_by_field = cases._plan_net_role("r1", "n1")
    bare_role_by_field.pop("meta")
    dated_role_by_field = {**bare_role_by_field, "meta": {"lastUpdated": "2026-03-04T05:06:07Z"}}
    lines = [json.dumps(bare_role_by_field).encode(), json.dumps(dated_role_by_field).encode()]
    context = _context(CONTRACTS[0], True)
    # Equal content, so one batch keeps one sighting; the merged-role id still differs by Meta being absent.
    assert len(bulk_prepare.prepare_lines(lines, context).rows) == 1
    role_ids_across_batches = [bulk_prepare.prepare_lines([line], context).rows[0]["resource_id"] for line in lines]
    prepared = seam.prepare_directory_row_lines(lines, context)
    assert prepared.resource_ids == role_ids_across_batches and len(set(role_ids_across_batches)) == 2
    assert assert_row_lines_parity(lines) == {}  # the same lines with the compiled preparer off


def test_conflicting_native_role_is_refused_at_completion_not_grouping(tmp_path):
    lines = [_role_line("r1", "n1"), _role_line("r1", "n1").replace(b'"active": true', b'"active": false')]
    context = _context(CONTRACTS[0], True)
    with pytest.raises(ValueError, match="provider_directory_resource_payload_conflict"):
        bulk_prepare.prepare_lines(lines, context)
    reduced_rows = _reduced(tmp_path, "lines", [seam.prepare_directory_row_lines(lines, context)])
    # require_one_content_per_native_role: one source role identity (24 digits) with two contents refuses completion.
    contents_by_native_role: dict[str, set[str]] = {}
    for reduced_row in reduced_rows:
        for native_role in reduced_row["payload_json"]["native_roles"]:
            contents_by_native_role.setdefault(native_role[:24], set()).add(native_role)
    assert [len(contents) for contents in contents_by_native_role.values()] == [2]
