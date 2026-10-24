# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected native ledger fixture; complete deployed authority remains separate."""

import json
from dataclasses import asdict
from types import SimpleNamespace
from uuid import uuid4

from db.plan_release_storage import storage_relation, storage_snapshot_index_sql, storage_table_sql
from process.plan_release_preparation_pin_catalog import capture_preparation_pin_heap
from process.plan_release_preparation_pin_contract import (
    PlanReleasePublication,
    PreparationPinAdmission,
    PreparationPinAuthority,
    PreparationPinHeap,
    PreparationPinScope,
    digest,
    preparation_pin_receipt_from_payload,
)
from tests.plan_release_storage_cohort_support import CAPTURED_TIME, prepare_native_cohort
from tests.plan_release_storage_postgres_support import NEW_REVISION, PLAN, RELEASE

SECOND_REVISION = "hpserve_" + "2" * 26
THIRD_REVISION = "hpserve_" + "3" * 26


def document(value):
    return json.loads(value) if type(value) is str else value


async def capture_heap(case, connection, name, *, pin, parent_oid=None):
    async with connection.transaction():
        await connection.execute(f'SET LOCAL ROLE "{case.owner}"')
        return await capture_preparation_pin_heap(
            connection,
            (case.preparation_schema, name),
            storage_schema=case.schema,
            pin=pin,
            parent_oid=parent_oid,
            **case.profile,
        )


async def _expand_inputs(case, admin, revisions):
    for table in ("cohort_binding", "cohort_pin"):
        relation = storage_relation(case.preparation_schema, table)
        bounds = await admin.fetch(
            "SELECT conname FROM pg_constraint WHERE conrelid=$1::regclass AND contype='c' AND position($2 in pg_get_constraintdef(oid))>0",
            relation,
            NEW_REVISION,
        )
        for bound in bounds:
            await admin.execute(f'ALTER TABLE {relation} DROP CONSTRAINT "{bound["conname"]}"')
        for revision in revisions[1:]:
            columns = (
                "serving_revision_id,snapshot_id,reason,created_at"
                if table == "cohort_pin"
                else "serving_revision_id,binding_ordinal,snapshot_id,source_key,plan_id,plan_market_type,role,required,metadata_json,created_at"
            )
            suffix = columns.split(",", 1)[1]
            await admin.execute(
                f"INSERT INTO {relation} SELECT $1,{suffix} FROM {relation} WHERE serving_revision_id=$2",
                revision,
                NEW_REVISION,
            )
        literal_ids = ",".join("'" + revision + "'" for revision in revisions)
        await admin.execute(f"ALTER TABLE {relation} ADD CHECK(serving_revision_id IN ({literal_ids}))")


async def prepare_pin_case(case, *, revisions=(NEW_REVISION,)):
    await prepare_native_cohort(case)
    admin = await case.connect()
    await admin.execute(
        f'UPDATE {case.relation("ptg2_snapshot")} SET manifest=\'{{"serving_index":{{"source_key":"synthetic_source"}}}}\'::jsonb'
    )
    if len(revisions) > 1:
        await _expand_inputs(case, admin, revisions)
    publisher = await case.connect(case.publisher)
    images = [
        await capture_heap(case, publisher, name, pin=is_pin)
        for name, is_pin in (("cohort_binding", False), ("cohort_pin", True))
    ]
    preparation_id = uuid4()
    publications = tuple(sorted((digest([str(preparation_id), revision]), revision) for revision in revisions))
    outcome_by_field = {"publication_revisions": publications, "source_keys": ["synthetic_source"]}
    scope = PreparationPinScope(
        preparation_id,
        digest(["scope", str(preparation_id)]),
        digest(outcome_by_field),
        digest(asdict(images[0])),
        digest([str(preparation_id), "cohort"]),
        publications,
        ("synthetic_source",),
    ).validate()
    tables_by_kind = {
        kind: storage_relation(case.preparation_schema, table)
        for kind, table in (
            ("preparation", "catalog_release_preparation"),
            ("admission", "catalog_release_admission_attempt"),
            ("jobs", "native_engine_job"),
        )
    }
    await _create_native_ledger(case, admin, tables_by_kind, scope, images)
    return SimpleNamespace(
        case=case,
        publisher=publisher,
        scope=scope,
        binding_heap=images[0],
        pin_heap=images[1],
        tables=tables_by_kind,
        authority=native_authority(tables_by_kind),
    )


async def _create_native_ledger(case, admin, tables, scope, images):
    await admin.execute(
        f"CREATE TABLE {tables['preparation']}(preparation_id uuid PRIMARY KEY,scope jsonb NOT NULL,status text NOT NULL,inventory jsonb NOT NULL,terminal_intent text,writers_closed boolean NOT NULL DEFAULT false)"
    )
    await admin.execute(
        f"CREATE TABLE {tables['admission']}(publication_identity text PRIMARY KEY,serving_revision_id text NOT NULL,attempt_identity text NOT NULL,request_digest text NOT NULL,admission_key text NOT NULL,status text NOT NULL,dispatches jsonb NOT NULL,engine_run_id text,retryable boolean NOT NULL DEFAULT false)"
    )
    await admin.execute(
        f"CREATE TABLE {tables['jobs']}(job_id text PRIMARY KEY,admission_key text NOT NULL,run_id text,status text NOT NULL)"
    )
    for relation in tables.values():
        await admin.execute(f'ALTER TABLE {relation} OWNER TO "{case.owner}"')
        await admin.execute(f"REVOKE ALL ON {relation} FROM PUBLIC")
    inventory_by_field = {
        "binding_heap": asdict(images[0]),
        "pin_heap": asdict(images[1]),
        "registered": None,
        "detached": [],
        "cleaned": [],
    }
    await admin.execute(
        f"INSERT INTO {tables['preparation']} VALUES($1,$2::jsonb,'admitting',$3::jsonb,NULL,false)",
        scope.preparation_id,
        json.dumps(scope.payload()),
        json.dumps(inventory_by_field),
    )
    for publication, revision in scope.publication_revisions:
        await admin.execute(
            f"INSERT INTO {tables['admission']} VALUES($1,$2,$3,$4,$5,'reserved','[]'::jsonb,NULL,false)",
            publication,
            revision,
            digest([publication, "attempt"]),
            digest([publication, "request"]),
            "plan-pricing-projection:" + publication,
        )


def _transition_inventory(inventory_by_field, action, observed):
    old = inventory_by_field["registered"]
    observed_payload = document(json.dumps(observed.payload()))
    if action == "retain":
        inventory_by_field["registered"] = observed_payload
    elif action == "exchange":
        if old != observed_payload:
            inventory_by_field["detached"].append(old)
        inventory_by_field["registered"] = observed_payload
    elif action == "retire":
        if old is not None:
            inventory_by_field["detached"].append(old)
        inventory_by_field["registered"] = None
    elif action == "cleanup":
        inventory_by_field["detached"].remove(observed_payload)
        inventory_by_field["cleaned"].append(observed_payload)
    else:
        assert action in {"serving", "publish"}


async def _admissions(connection, tables):
    rows = await connection.fetch(
        f"SELECT a.*,jsonb_array_length(a.dispatches) AS dispatch_count,(SELECT count(*) FROM {tables['jobs']} j WHERE j.admission_key=a.admission_key AND j.status NOT IN ('succeeded','failed','canceled','cancelled','dead_letter')) AS remaining_work FROM {tables['admission']} a ORDER BY publication_identity"
    )
    return tuple(
        PreparationPinAdmission(
            row["publication_identity"],
            row["serving_revision_id"],
            row["attempt_identity"],
            row["request_digest"],
            row["admission_key"],
            row["status"],
            row["dispatch_count"],
            row["engine_run_id"],
            row["remaining_work"],
            row["retryable"],
        )
        for row in rows
    )


def native_authority(tables):
    async def authority(connection, scope, *, action, phase, observed):
        preparation_by_field = await connection.fetchrow(
            f"SELECT * FROM {tables['preparation']} WHERE preparation_id=$1 FOR UPDATE NOWAIT", scope.preparation_id
        )
        assert preparation_by_field is not None and document(preparation_by_field["scope"]) == document(
            json.dumps(scope.payload())
        )
        inventory_by_field = document(preparation_by_field["inventory"])
        if phase == "after":
            _transition_inventory(inventory_by_field, action, observed)
            await connection.execute(
                f"UPDATE {tables['preparation']} SET inventory=$2::jsonb WHERE preparation_id=$1",
                scope.preparation_id,
                json.dumps(inventory_by_field),
            )
            if action == "publish":
                await connection.execute(
                    f"UPDATE {tables['preparation']} SET status='published',writers_closed=true WHERE preparation_id=$1",
                    scope.preparation_id,
                )
                preparation_by_field = {**preparation_by_field, "status": "published", "writers_closed": True}
        receipt = preparation_pin_receipt_from_payload
        return PreparationPinAuthority(
            scope,
            preparation_by_field["status"],
            await _admissions(connection, tables),
            receipt(inventory_by_field["registered"]) if inventory_by_field["registered"] else None,
            tuple(receipt(encoded_receipt) for encoded_receipt in inventory_by_field["detached"]),
            preparation_by_field["terminal_intent"],
            preparation_by_field["writers_closed"],
            PreparationPinHeap(**inventory_by_field["binding_heap"]),
            PreparationPinHeap(**inventory_by_field["pin_heap"]),
            tuple(receipt(encoded_receipt) for encoded_receipt in inventory_by_field["cleaned"]),
            inventory_by_field.get("publication_digest"),
        )

    return authority


async def seal_native_publication(prepared):
    """Seal a genuine no-network outcome against the captured native inventory."""
    publication = PlanReleasePublication(
        NEW_REVISION,
        RELEASE,
        PLAN,
        None,
        "2026-10",
        prepared.binding_heap.row_count,
        prepared.scope.binding_inputs_digest,
        {"binding_payload_digest": prepared.binding_heap.payload_digest},
    ).validate()
    admin = await prepared.case.connect()
    await admin.execute(f"UPDATE {prepared.tables['admission']} SET status='not_required'")
    await admin.execute(
        f"UPDATE {prepared.tables['preparation']} SET status='sealed',inventory=jsonb_set(inventory,'{{publication_digest}}',$1::jsonb)",
        json.dumps(digest(publication.payload())),
    )
    return publication


async def settle(prepared, statuses, *, state="sealed", intent=False):
    admin = await prepared.case.connect()
    for revision, status in statuses.items():
        run_id = "synthetic-run-" + revision
        await admin.execute(
            f"UPDATE {prepared.tables['admission']} SET status=$2,dispatches='[{{}}]'::jsonb,engine_run_id=$3 WHERE serving_revision_id=$1",
            revision,
            status,
            run_id,
        )
        await admin.execute(
            f"INSERT INTO {prepared.tables['jobs']} SELECT $1,admission_key,$2,$3 FROM {prepared.tables['admission']} WHERE serving_revision_id=$4 ON CONFLICT(job_id) DO UPDATE SET status=excluded.status",
            run_id,
            run_id,
            "succeeded" if status == "ready" else "failed",
            revision,
        )
    await admin.execute(
        f"UPDATE {prepared.tables['preparation']} SET status=$1,terminal_intent=$2,writers_closed=$3",
        state,
        digest([str(prepared.scope.preparation_id), "explicit-abandon"]) if intent else None,
        intent or state == "published",
    )


async def replacement_heap(prepared, retained):
    case = prepared.case
    admin = await case.connect()
    relation = storage_relation(case.preparation_schema, "replacement_pin")
    await admin.execute(storage_table_sql(case.preparation_schema, "replacement_pin", pin=True, primary_key=False))
    await admin.execute(
        f"INSERT INTO {relation} SELECT * FROM ONLY {storage_relation(prepared.pin_heap.schema, prepared.pin_heap.name)} WHERE serving_revision_id=ANY($1::text[])",
        list(retained),
    )
    await admin.execute(f"ALTER TABLE {relation} ADD PRIMARY KEY(serving_revision_id,snapshot_id)")
    await admin.execute(
        storage_snapshot_index_sql(case.preparation_schema, "replacement_pin", "replacement_pin_snapshot_idx")
    )
    literals = ",".join("'" + revision + "'" for revision in retained)
    await admin.execute(f"ALTER TABLE {relation} ADD CHECK(serving_revision_id IN ({literals}))")
    await admin.execute(f'ALTER TABLE {relation} OWNER TO "{case.owner}"')
    await admin.execute(f'GRANT SELECT ON {relation} TO "{case.reader}"')
    image = await capture_heap(case, prepared.publisher, "replacement_pin", pin=True)
    await admin.execute(
        f"UPDATE {prepared.tables['preparation']} SET inventory=jsonb_set(inventory,'{{pin_heap}}',$1::jsonb)",
        json.dumps(asdict(image)),
    )
    return image
