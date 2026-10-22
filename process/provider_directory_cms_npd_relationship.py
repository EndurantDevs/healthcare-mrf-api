# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Project exact CMS FHIR references from one retained eight-resource dataset."""

import json

from sqlalchemy import text

# These are explicit FHIR Reference fields, including ones omitted from the
# normalized serving JSON.
FIELDS = {
    "Organization": (("partOf", "Organization", False), ("endpoint", "Endpoint", True)),
    "Location": (
        ("managingOrganization", "Organization", False),
        ("partOf", "Location", False),
        ("endpoint", "Endpoint", True),
    ),
    "Endpoint": (("managingOrganization", "Organization", False),),
    "HealthcareService": (
        ("providedBy", "Organization", False),
        ("location", "Location", True),
        ("endpoint", "Endpoint", True),
        ("coverageArea", "Location", True),
    ),
    "InsurancePlan": (
        ("ownedBy", "Organization", False),
        ("administeredBy", "Organization", False),
        ("network", "Organization", True),
        ("coverageArea", "Location", True),
        ("endpoint", "Endpoint", True),
    ),
    "Practitioner": (),
    "PractitionerRole": (
        ("practitioner", "Practitioner", False),
        ("organization", "Organization", False),
        ("location", "Location", True),
        ("healthcareService", "HealthcareService", True),
        ("network", "Organization", True),
        ("insurancePlan", "InsurancePlan", True),
        ("endpoint", "Endpoint", True),
    ),
    "OrganizationAffiliation": (
        ("organization", "Organization", False),
        ("participatingOrganization", "Organization", False),
        ("network", "Organization", True),
        ("location", "Location", True),
        ("healthcareService", "HealthcareService", True),
        ("endpoint", "Endpoint", True),
        ("insurancePlan", "InsurancePlan", True),
    ),
}
NESTED_FIELDS = (
    ("Practitioner", "qualification", "issuer", "Organization", False),
    ("InsurancePlan", "plan", "network", "Organization", True),
    ("InsurancePlan", "plan", "coverageArea", "Location", True),
    ("InsurancePlan", "coverage", "network", "Organization", True),
)

_TABLE = "provider_directory_cms_npd_relationship"
_RESOURCE = "provider_directory_dataset_resource"
_WITNESS = "provider_directory_cms_npd_resource_witness"
_RECEIPT = "provider_directory_cms_npd_relationship_receipt"
_BINDING = "provider_directory_entity_source_binding"
CONTRACT = "cms-npd-reference-ledger-v1"
_VALUES = ", ".join(
    f"('{kind}', '{field}', '{target}', {'true' if is_array else 'false'})"
    for kind, fields in FIELDS.items()
    for field, target, is_array in fields
)
_NESTED_VALUES = ", ".join(
    f"('{kind}', '{parent}', '{field}', '{target}', {'true' if is_array else 'false'})"
    for kind, parent, field, target, is_array in NESTED_FIELDS
)


def _references(page_sql):
    return f"""WITH specs(resource_type, reference_field, target_type, is_array) AS (VALUES {_VALUES}),
      nested_specs(resource_type, parent_field, child_field, target_type, is_array) AS (VALUES {_NESTED_VALUES}),
      page AS ({page_sql}),
      reference_objects AS (
        SELECT p.resource_type, p.resource_id, p.payload_hash, p.raw_payload_sha256,
               p.raw_payload_json AS payload, s.reference_field, s.target_type,
               0::integer AS parent_ordinal, ref.ordinal::integer AS reference_ordinal,
               ref.value AS reference_object
        FROM page p JOIN specs s USING (resource_type)
        CROSS JOIN LATERAL jsonb_array_elements(
          CASE WHEN s.is_array AND jsonb_typeof(p.raw_payload_json->s.reference_field)='array'
                 THEN p.raw_payload_json->s.reference_field
               WHEN p.raw_payload_json ? s.reference_field
                 THEN jsonb_build_array(p.raw_payload_json->s.reference_field)
               ELSE '[]'::jsonb END
        ) WITH ORDINALITY ref(value, ordinal)
        UNION ALL
        SELECT p.resource_type, p.resource_id, p.payload_hash, p.raw_payload_sha256,
               CASE WHEN parent.value ? 'period' THEN parent.value ELSE p.raw_payload_json END,
               s.parent_field || '.' || s.child_field, s.target_type,
               parent.ordinal::integer, child.ordinal::integer, child.value
        FROM page p JOIN nested_specs s USING (resource_type)
        CROSS JOIN LATERAL jsonb_array_elements(
          CASE WHEN jsonb_typeof(p.raw_payload_json->s.parent_field)='array'
               THEN p.raw_payload_json->s.parent_field ELSE '[]'::jsonb END
        ) WITH ORDINALITY parent(value, ordinal)
        CROSS JOIN LATERAL jsonb_array_elements(
          CASE WHEN s.is_array AND jsonb_typeof(parent.value->s.child_field)='array'
                 THEN parent.value->s.child_field
               WHEN parent.value ? s.child_field THEN jsonb_build_array(parent.value->s.child_field)
               ELSE '[]'::jsonb END
        ) WITH ORDINALITY child(value, ordinal)
      ),
      refs AS (
        SELECT *, CASE WHEN jsonb_typeof(reference_object)='object' THEN reference_object->>'reference'
                       WHEN jsonb_typeof(reference_object)='string' THEN reference_object #>> '{{}}'
                       END AS target_reference
        FROM reference_objects
      )"""


def _family_page_sql(resource, witness):
    """Join every retained row of one family with its exact qualifying witness."""
    return f"""SELECT source.resource_type, source.resource_id, source.payload_hash,
        witness.raw_payload_sha256, witness.raw_payload_json FROM {resource} source
      JOIN {witness} witness ON witness.dataset_id=source.dataset_id
        AND witness.resource_type=source.resource_type AND witness.resource_id=source.resource_id
        AND witness.source_id='cms-npd' AND witness.release_id=:release_id
        AND witness.normalized_payload_hash=source.payload_hash
      WHERE source.dataset_id=:dataset_id AND source.resource_type=:resource_type"""


def _projection_sql(fhir):
    schema = fhir._schema()
    resource = fhir._qt(schema, _RESOURCE)
    witness = fhir._qt(schema, _WITNESS)
    binding = fhir._qt(schema, _BINDING)
    page = _family_page_sql(resource, witness)
    return (
        _references(page)
        + f""",
      local_refs AS (
        SELECT refs.*, CASE WHEN target_reference ~ ('^' || target_type || '/[A-Za-z0-9.-]{{1,64}}$')
          THEN split_part(target_reference, '/', 2) END AS target_resource_id FROM refs
      ),
      selected_bindings AS MATERIALIZED (
        SELECT DISTINCT b.resource_type,b.resource_id,b.organization_id,b.site_id
        FROM local_refs r JOIN {binding} b ON b.source_id='cms-npd'
          AND b.resource_type=r.target_type AND b.resource_id=r.target_resource_id
        WHERE r.target_type IN ('Organization','Location')
      ),
      ambiguous_bindings AS MATERIALIZED (
        SELECT selected.resource_type,selected.resource_id
        FROM selected_bindings selected JOIN {binding} alias
          ON alias.source_id='cms-npd' AND alias.resource_type=selected.resource_type
          AND alias.resource_id<>selected.resource_id
          AND ((selected.resource_type='Organization' AND alias.organization_id=selected.organization_id)
            OR (selected.resource_type='Location' AND alias.site_id=selected.site_id))
        JOIN {resource} other ON other.dataset_id=:dataset_id
          AND other.resource_type=alias.resource_type AND other.resource_id=alias.resource_id
        GROUP BY selected.resource_type,selected.resource_id
      )
      SELECT :dataset_id, 'cms-npd', :release_id, r.resource_type, r.resource_id,
        r.payload_hash, r.raw_payload_sha256,
        r.reference_field, r.parent_ordinal, r.reference_ordinal,
        r.target_type, r.target_reference, r.target_resource_id,
        CASE
          WHEN target.resource_id IS NULL THEN 'unresolved'
          WHEN r.target_type IN ('Organization','Location') AND b.resource_id IS NULL THEN 'unresolved'
          WHEN ambiguous.resource_id IS NOT NULL THEN 'ambiguous'
          ELSE 'resolved' END,
        r.payload->'period'->>'start', r.payload->'period'->>'end'
      FROM local_refs r
      LEFT JOIN {resource} target ON target.dataset_id=:dataset_id
        AND target.resource_type=r.target_type AND target.resource_id=r.target_resource_id
      LEFT JOIN {binding} b ON b.source_id='cms-npd' AND b.resource_type=r.target_type
        AND b.resource_id=r.target_resource_id AND r.target_type IN ('Organization','Location')
      LEFT JOIN ambiguous_bindings ambiguous ON ambiguous.resource_type=r.target_type
        AND ambiguous.resource_id=r.target_resource_id
"""
    )


async def _family_complete(fhir, session, dataset_id, release_id, resource_type):
    """Compare one family's complete indexed projection, including resolved target identity."""
    relationship = fhir._qt(fhir._schema(), _TABLE)
    params_by_field = {"dataset_id": dataset_id, "release_id": release_id, "resource_type": resource_type}
    fields = ",".join(_COLUMNS)
    different = " OR ".join(f"link.{column} IS DISTINCT FROM expected.{column}" for column in _COLUMNS)
    expected, missing = (
        await session.execute(
            text(
                f"WITH expected({fields}) AS ({_projection_sql(fhir)}) "
                f"SELECT count(*),coalesce(bool_or(link.resource_type IS NULL OR {different}),false) "
                f"FROM expected LEFT JOIN {relationship} link ON link.dataset_id=expected.dataset_id "
                "AND link.resource_type=expected.resource_type AND link.resource_id=expected.resource_id "
                "AND link.reference_field=expected.reference_field AND link.parent_ordinal=expected.parent_ordinal "
                "AND link.reference_ordinal=expected.reference_ordinal"
            ),
            params_by_field,
        )
    ).one()
    actual = (
        await session.execute(
            text(f"SELECT count(*) FROM {relationship} WHERE dataset_id=:dataset_id AND resource_type=:resource_type"),
            params_by_field,
        )
    ).scalar_one()
    if int(expected or 0) != int(actual or 0) or missing:
        raise RuntimeError("cms_npd_relationship_projection_incomplete")
    return int(actual or 0)


_COLUMNS = (
    "dataset_id",
    "source_id",
    "release_id",
    "resource_type",
    "resource_id",
    "source_payload_hash",
    "raw_payload_sha256",
    "reference_field",
    "parent_ordinal",
    "reference_ordinal",
    "target_type",
    "target_reference",
    "target_resource_id",
    "resolution_status",
    "period_start",
    "period_end",
)


async def _insert_family(fhir, session, dataset_id, release_id, resource_type):
    """Project one family's references server-side into the protected private heap."""
    from db.plan_release_storage import storage_relation
    from process.provider_directory_cms_identity_candidate import identity_candidate

    publisher, identity, _ = await identity_candidate(session, phases=("sealed",))
    raw = await publisher.connection.fetchval(
        f"SELECT candidate_storage_json FROM {storage_relation(publisher.policy.schema, 'provider_directory_endpoint_dataset')} "
        "WHERE dataset_id=$1",
        identity["dataset_id"],
    )
    binding = json.loads(raw) if isinstance(raw, str) else raw
    table = binding["tables"][_TABLE]["name"]
    if (
        dataset_id != identity["dataset_id"]
        or table == _TABLE
        or fhir._qt(fhir._schema(), _TABLE) != storage_relation(publisher.policy.schema, table)
    ):
        raise RuntimeError("cms_npd_relationship_private_candidate_required")
    inserted = await session.execute(
        text(
            f"INSERT INTO {fhir._qt(fhir._schema(), _TABLE)} ({','.join(_COLUMNS)}) "
            f"SELECT * FROM ({_projection_sql(fhir)}) AS cms_relationship_rows"
        ),
        {"dataset_id": dataset_id, "release_id": release_id, "resource_type": resource_type},
    )
    return int(inserted.rowcount)


async def _project_families(fhir, dataset_id, release_id, *, ctx=None, task=None):
    """Project every family once with one set-based statement each."""
    count = 0
    for resource_type in FIELDS:
        async with fhir.db.session() as session:
            count += await _insert_family(fhir, session, dataset_id, release_id, resource_type)
        if ctx is not None and task is not None:
            await fhir._raise_if_resource_import_cancelled(ctx, task)
    return count


async def assert_complete(fhir, dataset_id, release_id):
    """Require every explicit reference, one indexed family comparison at a time."""
    verified_count = 0
    async with fhir.db.session() as session:
        for resource_type in FIELDS:
            verified_count += await _family_complete(fhir, session, dataset_id, release_id, resource_type)
    actual = await fhir.db.scalar(
        f"SELECT count(*) FROM {fhir._qt(fhir._schema(), _TABLE)} WHERE dataset_id=:dataset_id", dataset_id=dataset_id
    )
    if int(actual or 0) != verified_count:
        raise RuntimeError("cms_npd_relationship_projection_incomplete")
    return verified_count


async def completed_receipt_count(fhir, dataset_id, release_id):
    """Read the indexed seal for a complete immutable relationship projection."""
    receipt = fhir._qt(fhir._schema(), _RECEIPT)
    row = await fhir.db.first(
        f"SELECT relationship_count FROM {receipt} "
        "WHERE dataset_id=:dataset_id AND release_id=:release_id AND projection_contract=:contract",
        dataset_id=dataset_id,
        release_id=release_id,
        contract=CONTRACT,
    )
    return int(row[0]) if row is not None else None


async def _assert_relationship_sealed(fhir, candidate, release_id):
    """Recheck the exact finished private group in the original receipt transaction."""
    from db.plan_release_storage import storage_relation
    from process import provider_directory_generic_source_auxiliary as auxiliary
    from process import provider_directory_generic_source_storage as storage
    from process.provider_directory_generic_source_policy import bound_publisher

    publisher = bound_publisher()
    identity = storage.candidate_identity(fhir, candidate)
    storage._require(
        publisher.policy.schema == fhir._schema()
        and identity["metadata"]["source_release"].get("vector_sha256") == release_id,
        "relationship_identity_changed",
    )
    binding = await storage._locked(publisher.connection, publisher.policy, identity, phases=("prepared",))
    storage._require(binding is not None, "relationship_seal_required")
    await storage._verify_tables(publisher.connection, publisher.policy, binding)
    proof = binding["tables"][storage.RELATIONSHIP]
    identity_stages = binding["auxiliary"]
    storage._require(
        binding["relationship_sha256"] == auxiliary.relationship_seal(identity, proof)
        and identity_stages is not None
        and identity_stages["phase"] == "sealed"
        and identity_stages["sealed_sha256"] == auxiliary._seal_digest(identity, identity_stages["tables"])
        and fhir._qt(fhir._schema(), _TABLE) == storage_relation(publisher.policy.schema, proof["name"]),
        "relationship_seal_changed",
    )


async def _insert_relationship_receipt(fhir, candidate, release_id, count):
    """Append only after native custody and same-transaction indexed seal checks."""
    from process.provider_directory_generic_source_policy import bound_publisher, source_transaction
    from process.provider_directory_source_cms_epoch_control import capture_source_cms_epoch_controls

    async with source_transaction(fhir.db, isolation="repeatable read"):
        publisher = bound_publisher()
        await capture_source_cms_epoch_controls(publisher.connection, publisher.policy)
        await _assert_relationship_sealed(fhir, candidate, release_id)
        if await assert_complete(fhir, candidate.dataset_id, release_id) != count:
            raise RuntimeError("cms_npd_relationship_projection_incomplete")
        receipt = fhir._qt(fhir._schema(), _RECEIPT)
        await fhir.db.status(
            f"INSERT INTO {receipt} (dataset_id, release_id, projection_contract, relationship_count) "
            "VALUES (:dataset_id, :release_id, :contract, :count) ON CONFLICT (dataset_id) DO NOTHING",
            dataset_id=candidate.dataset_id,
            release_id=release_id,
            contract=CONTRACT,
            count=count,
        )
        await _assert_relationship_sealed(fhir, candidate, release_id)
        if await completed_receipt_count(fhir, candidate.dataset_id, release_id) != count:
            raise RuntimeError("cms_npd_relationship_receipt_changed")


async def materialize(fhir, candidate, release_id, ctx=None, task=None):
    """Project the complete reference ledger before source-local publication."""
    sealed_count = await completed_receipt_count(fhir, candidate.dataset_id, release_id)
    if sealed_count is not None:
        if candidate.already_validated or candidate.already_published:
            return
        if await assert_complete(fhir, candidate.dataset_id, release_id) != sealed_count:
            raise RuntimeError("cms_npd_relationship_receipt_changed")
        return
    from process.provider_directory_generic_source_auxiliary import seal_identity_candidate

    await seal_identity_candidate(fhir.db, fhir, candidate)
    relationship = fhir._qt(fhir._schema(), _TABLE)
    if await fhir.db.scalar(
        f"SELECT EXISTS(SELECT FROM {relationship} WHERE dataset_id=:dataset_id)", dataset_id=candidate.dataset_id
    ):
        raise RuntimeError("cms_npd_relationship_unsealed_candidate_reprepare_required")
    count = await _project_families(fhir, candidate.dataset_id, release_id, ctx=ctx, task=task)
    from process.provider_directory_generic_source_auxiliary import seal_relationship_candidate

    await seal_relationship_candidate(fhir.db, fhir, candidate)
    await _insert_relationship_receipt(fhir, candidate, release_id, count)
