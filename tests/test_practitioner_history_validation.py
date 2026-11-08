# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure contract/driver fixtures; native adoption remains a separate gate."""

import asyncio
import json
from copy import deepcopy
from dataclasses import asdict
from hashlib import sha256
from pathlib import Path

import pytest

from process import uhc_flex_practitioner_history_validation as history
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.uhc_canonical_proof import (
    ProviderDirectoryContentProofBuilder,
    UhcCanonicalMaterializationIdentity,
    UhcCanonicalNpiProof,
    bind_uhc_canonical_content_proof,
    canonical_materialization_proof,
)
from process.uhc_flex_official_cohort_contract import build_uhc_flex_official_cohort
from process.uhc_flex_practitioner_contract import UHC_FLEX_PRACTITIONER_QUERY_CONTRACT_ID
from process.uhc_flex_practitioner_query import (
    UHCFlexPractitionerQueryError,
    UHCFlexPractitionerQueryResult,
    validate_uhc_flex_practitioner_search_bundle,
)
from process.uhc_flex_practitioner_store_contract import (
    UHCFlexPractitionerWorkClaim,
    canonical_resource_fields_list,
    terminal_record_sha256,
)
from process.uhc_provider_file_source_identity import UHC_PROVIDER_FILE_SOURCE_ID


@pytest.fixture
def policy():
    return GenericSourcePolicy("sample", "snapshot_owner", "snapshot_publisher", ("application",), ("application",))


@pytest.fixture
def family(policy):
    roots_by_name = {name: dict(oid=100 + index, parent_oid=None) for index, name in enumerate(history._FIXED)}
    inputs_by_name = dict(
        header=dict(name=history._SOURCE_HEADER, oid=301, columns=["fixture"], catalog={"fixture": True}),
        family=[
            dict(
                relname=history._SOURCE_RESOURCE,
                oid=302,
                relkind="p",
                parents=[],
                direct_acl_closed=True,
                readers_closed=True,
                native_checks=True,
            )
        ],
    )
    inputs_by_name["headers"] = {history._SOURCE_HEADER: inputs_by_name["header"]}
    return dict(
        schema=policy.schema,
        roots=roots_by_name,
        members=[],
        acquisitions={history._WORK: {}, history._RESOURCE: {}},
        source_inputs=inputs_by_name,
    )


def member(family, *, phase="loading", resource=False, attached=False):
    acquisition = "pdufpa_" + "a" * 48
    root = history._RESOURCE if resource else history._WORK
    prefix = "pd_uhc_flex_pr_" if resource else "pd_uhc_flex_pw_"
    member_by_field = dict(
        name=prefix + sha256(acquisition.encode()).hexdigest()[:40],
        root=root,
        oid=220 if resource else 210,
        parent_oid=family["roots"][root]["oid"] if attached else None,
        phase=phase,
        acquisition_id=acquisition,
    )
    family["members"].append(member_by_field)
    family["acquisitions"][root][acquisition] = "sealed" if phase == "sealed" else "building"
    return member_by_field


class NativeFixture:
    """Model driver control flow only; it does not execute or validate SQL."""

    def __init__(self, family, *, result=None, pages=()):
        self.family, self.statements = family, []
        self.rows = [
            dict(name=name, oid=entry["oid"], owner=99, parent_oid=entry["parent_oid"], locked=True)
            for name, entry in family["roots"].items()
        ]
        self.rows.extend(
            dict(name=value["name"], oid=value["oid"], owner=99, parent_oid=value["parent_oid"], locked=True)
            for value in family["members"]
        )
        self.rows.extend(
            dict(name=name, oid=entry["oid"], owner=99, parent_oid=entry["parent_oid"], locked=False, read_locked=True)
            for name, entry in history._source_scope(family).items()
        )
        for entry in self.rows:
            entry.setdefault("read_locked", True)
        self.rows.sort(key=lambda value: value["oid"])
        self.result = {key: 0 for key in (*history._COUNTS, *history._INVALID)} | (result or {})
        self.pages, self.admin, self.transaction, self.xid = list(pages), True, True, "1001"
        self.replaced, self.fail_close, self.cancel = False, False, False
        self.cohorts = []
        self.next_xid = None

    def is_in_transaction(self):
        return self.transaction

    async def fetchval(self, statement, *arguments):
        self.statements.append((statement, arguments))
        return 99

    async def fetchrow(self, statement, *arguments):
        self.statements.append((statement, arguments))
        if statement.startswith("SELECT session_user"):
            return dict(
                session="native_admin", current="native_admin", backend=123, xid=self.xid, native_admin=self.admin
            )
        return self.result

    async def fetch(self, statement, *arguments):
        self.statements.append((statement, arguments))
        if statement.startswith("SELECT pg_catalog.to_jsonb(h)"):
            return self.cohorts
        if statement.startswith("FETCH"):
            if self.cancel:
                raise asyncio.CancelledError
            return self.pages.pop(0) if self.pages else []
        rows = deepcopy(self.rows)
        if self.replaced and sum("pg_catalog.pg_class" in sql for sql, _ in self.statements) > 1:
            rows[0]["oid"] += 1
        return rows

    async def execute(self, statement):
        self.statements.append((statement, ()))
        if statement.startswith("CLOSE") and self.fail_close:
            raise RuntimeError("transaction aborted")
        if statement.startswith("CLOSE") and self.next_xid:
            self.xid = self.next_xid
        return "OK"


def payload_rows(*texts):
    return [dict(payload_json_text=value, bytes=len(value.encode("utf-8"))) for value in texts]


def canonical_result(*ids):
    payloads = [
        dict(
            resourceType="Practitioner",
            id=value,
            identifier=[dict(system="http://hl7.org/fhir/sid/us-npi", value="1234567893")],
            name=[dict(text="\u00c9lodie \U0001f331")],
            extension=[dict(url="sample", valueDecimal=-1.25e-8)],
        )
        for value in ids
    ]
    return validate_uhc_flex_practitioner_search_bundle(
        1234567893,
        dict(
            resourceType="Bundle",
            type="searchset",
            total=len(payloads),
            entry=[dict(resource=value) for value in payloads],
        ),
    )


@pytest.fixture
def cohort_records():
    """Use actual pure issuers; these documents confer no native custody."""
    source_id = UHC_PROVIDER_FILE_SOURCE_ID
    builder = ProviderDirectoryContentProofBuilder(source_id=source_id)
    builder.observe_rows(
        [("Practitioner", "a", "a" * 64)],
        input_lineage=[dict(source_file_id="file", range_ordinal=0, input_sha256="a" * 64, artifact_sha256="b" * 64)],
    )
    materialization = canonical_materialization_proof(
        builder.complete(),
        UhcCanonicalMaterializationIdentity(
            "c" * 64, "d" * 64, ("e" * 64,), source_id, "semantic-v2", 2, "canonical-v1"
        ),
        UhcCanonicalNpiProof(
            1,
            1,
            "f" * 64,
            (
                dict(
                    source_id=source_id,
                    source_file_id="file",
                    range_ordinal=0,
                    row_count=1,
                    input_sha256="a" * 64,
                    artifact_sha256="b" * 64,
                    layout_sha256="c" * 64,
                ),
            ),
        ),
    )
    proof = bind_uhc_canonical_content_proof(
        materialization, dataset_id="dataset", endpoint_id="endpoint", acquisition_root_run_id="root"
    )
    cohort = build_uhc_flex_official_cohort(
        official_endpoint_id="endpoint",
        official_dataset_id="dataset",
        official_acquisition_root_run_id="root",
        official_dataset_hash=proof["dataset_hash"],
        official_content_proof_sha256=proof["proof_sha256"],
        practitioner_resource_count=1,
        npi_count=1,
    )
    dataset_by_field = dict(
        dataset_id="dataset",
        endpoint_id="endpoint",
        acquisition_root_run_id="root",
        dataset_hash=proof["dataset_hash"],
        status="superseded",
        resource_count=1,
        publication_metadata_json={history.UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY: proof},
    )
    return asdict(cohort), dataset_by_field


def test_cohort_accepts_authentic_old_bound_proof_without_current_alias(cohort_records):
    cohort, dataset = cohort_records
    assert history._cohort_record(cohort, dataset).cohort_id == cohort["cohort_id"]
    dataset["status"] = "published"
    dataset["is_current"] = False
    dataset["publication_metadata_json"] = json.dumps(dataset["publication_metadata_json"])
    assert history._cohort_record(cohort, dataset).official_endpoint_id == "endpoint"


@pytest.mark.parametrize(
    "field,value",
    [("cohort_id", "pdufc_" + "0" * 48), ("authority_id", "other"), ("official_dataset_id", "other"), ("npi_count", 2)],
)
def test_cohort_rejects_noncanonical_identity(cohort_records, field, value):
    cohort, dataset = cohort_records
    cohort[field] = value
    with pytest.raises(ValueError):
        history._cohort_record(cohort, dataset)


@pytest.mark.parametrize(
    "field,value",
    [
        ("endpoint_id", "other"),
        ("acquisition_root_run_id", "other"),
        ("dataset_hash", "0" * 64),
        ("status", "building"),
        ("resource_count", 2),
    ],
)
def test_cohort_rejects_wrong_native_dataset_and_counts(cohort_records, field, value):
    cohort, dataset = cohort_records
    dataset[field] = value
    with pytest.raises(ValueError):
        history._cohort_record(cohort, dataset)


def test_cohort_rejects_changed_original_proof(cohort_records):
    cohort, dataset = cohort_records
    dataset["publication_metadata_json"][history.UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY]["source_id"] = "other"
    with pytest.raises(ValueError, match="proof_invalid"):
        history._cohort_record(cohort, dataset)


@pytest.mark.parametrize("ids", [(), ("a",), ("Z", "a", "9", "A-1")])
def test_result_manifest_bytes_match_existing_canonical_query_contract(ids):
    result = canonical_result(*ids)
    fields = canonical_resource_fields_list(result)
    objects = [
        f'{{"resource_id":{json.dumps(row["resource_id"])},"sha256":{json.dumps(row["payload_sha256"])}}}'
        for row in fields
    ]
    original = (
        '{"contract_id":'
        + json.dumps(UHC_FLEX_PRACTITIONER_QUERY_CONTRACT_ID)
        + ',"outcome":'
        + json.dumps(result.outcome)
        + ',"requested_npi":1234567893,"resources":['
        + ",".join(objects)
        + "]}"
    )
    assert sha256(original.encode()).hexdigest() == result.result_sha256
    assert [row["resource_id"] for row in fields] == sorted(ids)
    history._canonical_batch(payload_rows(*(row["payload_json_text"] for row in fields)))


@pytest.mark.parametrize("identifier", [1, 1.25, True, None, [], {}])
def test_canonical_scalar_id_still_requires_original_string_semantics(identifier):
    document = json.loads(canonical_result("1")._resource_json_rows[0][1])
    document["id"] = identifier
    text = json.dumps(document, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    # Canonical bytes alone cannot distinguish a valid FHIR id from a JSON scalar.
    history._canonical_batch(payload_rows(text))
    with pytest.raises(UHCFlexPractitionerQueryError, match="resource ID"):
        UHCFlexPractitionerQueryResult(1234567893, "matched", "a" * 64, (("1", text),))
    assert "pg_catalog.jsonb_typeof((r.payload_json_text::jsonb)->'id')='string'" in history._SQL_PATH.read_text()


@pytest.mark.parametrize("identifier", ["1", "A-1", ".", "z9", "a" * 64])
def test_original_string_id_forms_remain_canonical_and_accepted(identifier):
    result = canonical_result(identifier)
    assert result.resource_ids == (identifier,)
    history._canonical_batch(payload_rows(result._resource_json_rows[0][1]))


@pytest.mark.parametrize("status,error", [("matched", None), ("unmatched", None), ("error", "content_type_invalid")])
def test_terminal_record_uses_original_unit_separator_protocol(status, error):
    result = canonical_result("a") if status == "matched" else canonical_result()
    digest = None if error else result.result_sha256
    count = 1 if status == "matched" else 0
    claim = UHCFlexPractitionerWorkClaim("pdufpa_" + "a" * 48, "pdufc_" + "b" * 48, 1234567893, 2, "c" * 64)
    raw = "\x1f".join(
        (
            history.UHC_FLEX_PRACTITIONER_TERMINAL_RECORD_CONTRACT_ID,
            "1234567893",
            status,
            digest or "",
            str(count),
            error or "",
        )
    )
    assert (
        terminal_record_sha256(claim, status=status, result_sha256=digest, resource_count=count, error_code=error)
        == sha256(raw.encode()).hexdigest()
    )


@pytest.mark.parametrize(
    "text", ['{ "a":1}', '{"z":1,"a":2}', '{"a":1,"a":1}', '{"a":NaN}', '{"a":"\\u00e9"}', "[] ", "{"]
)
def test_batch_rejects_noncanonical_original_bytes_without_rewriting(text):
    rows = payload_rows(text)
    before = deepcopy(rows)
    with pytest.raises(ValueError, match="payload"):
        history._canonical_batch(rows)
    assert rows == before


def test_batch_codec_runs_once_for_mixed_payloads(monkeypatch):
    rows = payload_rows(
        *(row["payload_json_text"] for row in canonical_resource_fields_list(canonical_result("Z", "a")))
    )
    loads, dumps, calls = json.loads, json.dumps, []

    def read(*args, **kwargs):
        calls.append("loads")
        return loads(*args, **kwargs)

    def write(*args, **kwargs):
        calls.append(("dumps", kwargs))
        return dumps(*args, **kwargs)

    monkeypatch.setattr(history.json, "loads", read)
    monkeypatch.setattr(history.json, "dumps", write)
    history._canonical_batch(rows)
    assert calls == [
        "loads",
        ("dumps", dict(allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True)),
    ]


def test_batch_accepts_original_single_resource_limit_and_bounds_rows():
    text = '{"x":"' + "x" * (history._PAYLOAD_BYTES - 8) + '"}'
    assert len(text.encode()) == history._PAYLOAD_BYTES
    history._canonical_batch(payload_rows(text))
    for rows in (payload_rows(text + " "), payload_rows(*(["{}"] * 17))):
        with pytest.raises(ValueError, match="payload_size"):
            history._canonical_batch(rows)


def test_rendering_includes_only_original_parents_and_captured_detached_members(policy, family):
    raw = member(family)
    selected, detached, loading, initialized = history._scope(policy, family)
    statement = history._statement(policy, detached)
    assert raw["name"] in statement and loading == [raw["acquisition_id"]] and not initialized
    assert len(selected) == len(history._FIXED) + 1
    assert "__" not in statement
    assert f'FROM "sample"."{history._WORK}" r' in statement
    attached = member(family, resource=True, phase="attached", attached=True)
    _, detached, _, _ = history._scope(policy, family)
    assert attached["name"] not in history._statement(policy, detached)


@pytest.mark.parametrize(
    "field,value", [("name", "not_a_captured_candidate"), ("parent_oid", 999), ("phase", "sealed")]
)
def test_scope_refuses_wrong_or_detached_sealed_member(policy, family, field, value):
    item = member(family)
    item[field] = value
    with pytest.raises(ValueError):
        history._scope(policy, family)


@pytest.mark.parametrize("fault", ["missing", "header", "columns", "parent", "acl", "checks", "alias"])
def test_source_reads_require_captured_closed_family(family, fault):
    header = family["source_inputs"]["header"]
    root = family["source_inputs"]["family"][0]
    mutate_by_fault = {
        "missing": lambda: family.pop("source_inputs"),
        "header": lambda: header.update(name="unrelated"),
        "columns": lambda: header.update(columns=[]),
        "parent": lambda: root.update(parents=[999]),
        "acl": lambda: root.update(direct_acl_closed=False),
        "checks": lambda: root.update(native_checks=False),
        "alias": lambda: family["source_inputs"].update(header=header | {"oid": 999}),
    }
    mutate_by_fault[fault]()
    with pytest.raises(ValueError, match="source"):
        history._source_scope(family)


@pytest.mark.asyncio
async def test_locked_input_substitution_refuses_before_any_payload_read(policy, family):
    driver = NativeFixture(family)
    source_row = next(entry for entry in driver.rows if entry["name"] == history._SOURCE_RESOURCE)
    source_row["oid"] += 1
    with pytest.raises(ValueError, match="relation_changed"):
        await history.validate_practitioner_history(driver, policy, family)
    assert not any(sql.startswith("DECLARE") for sql, _ in driver.statements)


@pytest.mark.asyncio
async def test_runner_invokes_genuine_low_volume_contracts(policy, family, cohort_records):
    cohort, dataset = cohort_records
    driver = NativeFixture(family, result={"cohorts": 1})
    driver.cohorts = [dict(cohort=json.dumps(cohort), dataset=json.dumps(dataset))]
    assert (await history.validate_practitioner_history(driver, policy, family))["cohorts"] == 1
    driver.cohorts[0]["dataset"] = json.dumps(dataset | {"endpoint_id": "changed"})
    with pytest.raises(ValueError, match="cohort_dataset_changed"):
        await history.validate_practitioner_history(driver, policy, family)


@pytest.mark.asyncio
async def test_runner_preserves_original_transaction_and_returns_only_aggregate_receipt(policy, family):
    driver = NativeFixture(family)
    before = deepcopy(family)
    receipt = await history.validate_practitioner_history(driver, policy, family)
    assert receipt["protocol"] == 1 and receipt["work_rows"] == receipt["resource_rows"] == 0
    assert receipt["loading_acquisitions"] == 0 and family == before and driver.is_in_transaction()
    statements = [sql for sql, _ in driver.statements]
    assert (
        sum(sql.startswith("DECLARE") for sql in statements) == sum(sql.startswith("CLOSE") for sql in statements) == 1
    )
    assert not any(sql.startswith(("BEGIN", "COMMIT", "ROLLBACK", "SET", "LOCK")) for sql in statements)
    assert any(
        'npi,attempt,resource_id COLLATE pg_catalog."C"' in sql for sql in statements if sql.startswith("DECLARE")
    )
    parameters = next(args for sql, args in driver.statements if sql.startswith("-- Licensed"))
    assert parameters[5:8] == ("retry_exhausted_transport", 8, 50)


@pytest.mark.asyncio
@pytest.mark.parametrize("field", history._INVALID)
async def test_runner_refuses_each_native_semantic_failure_before_payload_cursor(policy, family, field):
    driver = NativeFixture(family, result={field: 1})
    with pytest.raises(ValueError, match="set_invalid"):
        await history.validate_practitioner_history(driver, policy, family)
    assert not any(sql.startswith("DECLARE") for sql, _ in driver.statements)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["admin", "lock", "oid", "count", "boolean", "xid"])
async def test_runner_refuses_actor_lock_native_replacement_and_count_drift(policy, family, fault):
    driver = NativeFixture(family)
    mutate_by_fault = {
        "admin": lambda: setattr(driver, "admin", False),
        "lock": lambda: driver.rows[0].update(locked=False),
        "oid": lambda: setattr(driver, "replaced", True),
        "count": lambda: driver.result.update(resource_rows=1),
        "boolean": lambda: driver.result.update(resource_rows=True),
        "xid": lambda: setattr(driver, "next_xid", "1002"),
    }
    mutate_by_fault[fault]()
    with pytest.raises(ValueError):
        await history.validate_practitioner_history(driver, policy, family)


@pytest.mark.asyncio
async def test_cancellation_closes_portal_and_preserves_original_error(policy, family):
    driver = NativeFixture(family)
    driver.cancel, driver.fail_close = True, True
    with pytest.raises(asyncio.CancelledError):
        await history.validate_practitioner_history(driver, policy, family)
    assert any(sql.startswith("CLOSE") for sql, _ in driver.statements)
    assert driver.is_in_transaction()


def test_sql_preserves_historical_states_attempt_fold_and_no_current_time(policy):
    statement = history._statement(policy, {history._WORK: [], history._RESOURCE: []})
    assert "r.attempt BETWEEN 1 AND w.attempt_count" in statement
    assert "(w.acquisition_id,w.cohort_id,w.npi,w.attempt_count)" in statement
    assert "h.acquisition_id NOT IN (SELECT acquisition_id FROM loading)" in statement
    assert "COALESCE(c.total,0)>0 OR h.status='sealed'" in statement
    assert "c.errors::numeric*$8::numeric<h.expected_npi_count::numeric" in statement
    assert "w.status='error'" in statement and "w.status='leased'" in statement
    assert "w.terminal_at=s.first_created" in statement and "w.terminal_at=w.updated_at" in statement
    assert "WHERE i->>'system'=$9::text" in statement and "r.normalized_npi" in statement
    assert "pg_catalog.mod(" in statement and "public_evidence_npi_valid" not in statement
    compact = "".join(statement.split())
    assert "NOTEXISTS(SELECTFROMsource_npisrWHERE(r.cohort_id,r.normalized_npi)=(m.cohort_id,m.npi))" in compact
    assert "NOTEXISTS(SELECTFROMmembersmWHERE(m.cohort_id,m.npi)=(r.cohort_id,r.normalized_npi))" in compact
    assert "transaction_timestamp" not in statement and "clock_timestamp" not in statement
    assert "::jsonb)::text" not in statement and 'COLLATE pg_catalog."C"' in statement
    for forbidden in ("CREATE FUNCTION", "CREATE TRIGGER", "INSERT INTO", "UPDATE ", "DELETE ", "COPY "):
        assert forbidden not in statement


def test_sql_manifest_and_terminal_fold_match_authentic_predecessor_structure():
    root = Path(__file__).resolve().parents[1]
    predecessor = (
        root / "alembic/versions/20260810060000_provider_directory_uhc_flex_practitioner_acquisition.py"
    ).read_text()
    statement = history._SQL_PATH.read_text()
    for fragment in (
        "npi / 1000",
        "bucket_id / 1000",
        "ORDER BY work.npi",
        "ORDER BY leaf.bucket_id",
        "ORDER BY middle.bucket_id",
    ):
        assert fragment in predecessor
    assert "w.npi/1000" in statement and "bucket_id/1000" in statement
    assert statement.count("pg_catalog.chr(30::integer)") == 3
    assert "'false' || pg_catalog.chr(31::integer) || 'false'" in statement
    assert "payload_sha256=pg_catalog.encode(pg_catalog.sha256(" in statement
    assert "pg_catalog.convert_to(r.payload_json_text::text,'UTF8'::name)" in statement
