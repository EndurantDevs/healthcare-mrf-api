# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Project exact CMS FHIR references from one retained eight-resource dataset."""

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
_BATCH_SIZE = 5_000
_BATCH_BYTES = 8 * 1024 * 1024
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
      page AS MATERIALIZED ({page_sql}),
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


def _insert_sql(fhir):
    schema = fhir._schema()
    resource = fhir._qt(schema, _RESOURCE)
    witness = fhir._qt(schema, _WITNESS)
    binding = fhir._qt(schema, _BINDING)
    relationship = fhir._qt(schema, _TABLE)
    page = f"""SELECT source.resource_type, source.resource_id, source.payload_hash,
        witness.raw_payload_sha256, witness.raw_payload_json FROM {resource} source
      JOIN {witness} witness ON witness.dataset_id=source.dataset_id
        AND witness.resource_type=source.resource_type AND witness.resource_id=source.resource_id
        AND witness.source_id='cms-npd' AND witness.release_id=:release_id
        AND (witness.resource_type, witness.resource_id) > (:after_type, :after_id)
        AND (witness.resource_type, witness.resource_id) <= (:last_type, :last_id)
        AND witness.normalized_payload_hash=source.payload_hash
      WHERE source.dataset_id=:dataset_id
        AND (source.resource_type, source.resource_id) > (:after_type, :after_id)
        AND (source.resource_type, source.resource_id) <= (:last_type, :last_id)"""
    return (
        _references(page)
        + f""",
      local_refs AS (
        SELECT refs.*, CASE WHEN target_reference ~ ('^' || target_type || '/[A-Za-z0-9.-]{{1,64}}$')
          THEN split_part(target_reference, '/', 2) END AS target_resource_id FROM refs
      )
      INSERT INTO {relationship} (
        dataset_id, source_id, release_id, resource_type, resource_id, source_payload_hash, raw_payload_sha256,
        reference_field, parent_ordinal, reference_ordinal, target_type, target_reference, target_resource_id,
        resolution_status, period_start, period_end
      )
      SELECT :dataset_id, 'cms-npd', :release_id, r.resource_type, r.resource_id,
        r.payload_hash, r.raw_payload_sha256,
        r.reference_field, r.parent_ordinal, r.reference_ordinal,
        r.target_type, r.target_reference, r.target_resource_id,
        CASE
          WHEN target.resource_id IS NULL THEN 'unresolved'
          WHEN r.target_type IN ('Organization','Location') AND b.resource_id IS NULL THEN 'unresolved'
          WHEN r.target_type IN ('Organization','Location') AND EXISTS (
            SELECT 1 FROM {binding} alias
            JOIN {resource} other ON other.dataset_id=:dataset_id
              AND other.resource_type=alias.resource_type AND other.resource_id=alias.resource_id
            WHERE alias.source_id='cms-npd' AND alias.resource_type=r.target_type
              AND alias.resource_id<>r.target_resource_id
              AND ((r.target_type='Organization' AND alias.organization_id=b.organization_id)
                OR (r.target_type='Location' AND alias.site_id=b.site_id))
          ) THEN 'ambiguous'
          ELSE 'resolved' END,
        r.payload->'period'->>'start', r.payload->'period'->>'end'
      FROM local_refs r
      LEFT JOIN {resource} target ON target.dataset_id=:dataset_id
        AND target.resource_type=r.target_type AND target.resource_id=r.target_resource_id
      LEFT JOIN {binding} b ON b.source_id='cms-npd' AND b.resource_type=r.target_type
        AND b.resource_id=r.target_resource_id AND r.target_type IN ('Organization','Location')
      ON CONFLICT (dataset_id, resource_type, resource_id, reference_field, parent_ordinal, reference_ordinal)
      DO NOTHING"""
    )


async def _page_complete(fhir, session, dataset_id, release_id, after_type, after_id, last_type, last_id):
    """Check one bounded source page against its exact retained references."""
    schema = fhir._schema()
    resource = fhir._qt(schema, _RESOURCE)
    witness = fhir._qt(schema, _WITNESS)
    relationship = fhir._qt(schema, _TABLE)
    page = f"""SELECT source.resource_type, source.resource_id, source.payload_hash,
        witness.raw_payload_sha256, witness.raw_payload_json FROM {resource} source
      JOIN {witness} witness ON witness.dataset_id=source.dataset_id
        AND witness.resource_type=source.resource_type AND witness.resource_id=source.resource_id
        AND witness.source_id='cms-npd' AND witness.release_id=:release_id
        AND (witness.resource_type, witness.resource_id) > (:after_type, :after_id)
        AND (witness.resource_type, witness.resource_id) <= (:last_type, :last_id)
        AND witness.normalized_payload_hash=source.payload_hash
      WHERE source.dataset_id=:dataset_id
        AND (source.resource_type, source.resource_id) > (:after_type, :after_id)
        AND (source.resource_type, source.resource_id) <= (:last_type, :last_id)"""
    params_by_field = {
        "dataset_id": dataset_id,
        "release_id": release_id,
        "after_type": after_type,
        "after_id": after_id,
        "last_type": last_type,
        "last_id": last_id,
    }
    expected, missing = (
        await session.execute(
            text(
                _references(page)
                + f""" SELECT count(*), coalesce(bool_or(
            link.resource_type IS NULL OR link.source_id<>'cms-npd' OR link.release_id<>:release_id
            OR link.source_payload_hash<>refs.payload_hash
            OR link.raw_payload_sha256<>refs.raw_payload_sha256
            OR link.target_type<>refs.target_type
            OR link.target_reference IS DISTINCT FROM refs.target_reference
            OR link.period_start IS DISTINCT FROM refs.payload->'period'->>'start'
            OR link.period_end IS DISTINCT FROM refs.payload->'period'->>'end'
          ), false)
          FROM refs LEFT JOIN {relationship} link ON link.dataset_id=:dataset_id
            AND link.resource_type=refs.resource_type AND link.resource_id=refs.resource_id
            AND link.reference_field=refs.reference_field
            AND link.parent_ordinal=refs.parent_ordinal
            AND link.reference_ordinal=refs.reference_ordinal"""
            ),
            params_by_field,
        )
    ).one()
    actual = (
        await session.execute(
            text(
                f"SELECT count(*) FROM {relationship} WHERE dataset_id=:dataset_id "
                "AND (resource_type, resource_id) > (:after_type, :after_id) "
                "AND (resource_type, resource_id) <= (:last_type, :last_id)"
            ),
            params_by_field,
        )
    ).scalar_one()
    if int(expected or 0) != int(actual or 0) or missing:
        raise RuntimeError("cms_npd_relationship_projection_incomplete")
    return int(actual or 0)


async def _page_keys(session, resource, witness, dataset_id, after_type, after_id):
    """Choose a bounded key range using the stored witness size as a cap."""
    keys = (
        await session.execute(
            text(
                f"SELECT source.resource_type, source.resource_id, "
                f"coalesce(pg_column_size(witness.raw_payload_json), 0) "
                f"FROM {resource} source LEFT JOIN {witness} witness "
                "ON witness.dataset_id=source.dataset_id "
                "AND witness.resource_type=source.resource_type AND witness.resource_id=source.resource_id "
                "WHERE source.dataset_id=:dataset_id "
                "AND (source.resource_type, source.resource_id) > (:after_type, :after_id) "
                "ORDER BY source.resource_type, source.resource_id LIMIT :batch_size"
            ),
            {
                "dataset_id": dataset_id,
                "after_type": after_type,
                "after_id": after_id,
                "batch_size": _BATCH_SIZE,
            },
        )
    ).all()
    page_bytes = 0
    last_type = last_id = ""
    for resource_type, resource_id, stored_bytes in keys:
        if last_id and page_bytes + int(stored_bytes) > _BATCH_BYTES:
            break
        last_type, last_id = resource_type, resource_id
        page_bytes += int(stored_bytes)
    return last_type, last_id


async def _scan_pages(fhir, dataset_id, release_id, *, write, ctx=None, task=None):
    """Project and verify key pages without materializing the full raw release."""
    schema = fhir._schema()
    resource = fhir._qt(schema, _RESOURCE)
    witness = fhir._qt(schema, _WITNESS)
    relationship = fhir._qt(schema, _TABLE)
    after_type = after_id = ""
    verified_count = 0
    while True:
        async with fhir.db.session() as session:
            last_type, last_id = await _page_keys(session, resource, witness, dataset_id, after_type, after_id)
            if not last_id:
                break
            params_by_field = {
                "dataset_id": dataset_id,
                "release_id": release_id,
                "after_type": after_type,
                "after_id": after_id,
                "last_type": last_type,
                "last_id": last_id,
            }
            if write:
                await session.execute(text(_insert_sql(fhir)), params_by_field)
            verified_count += await _page_complete(
                fhir, session, dataset_id, release_id, after_type, after_id, last_type, last_id
            )
        after_type, after_id = last_type, last_id
        if ctx is not None and task is not None:
            await fhir._raise_if_resource_import_cancelled(ctx, task)
    actual = await fhir.db.scalar(
        f"SELECT count(*) FROM {relationship} WHERE dataset_id=:dataset_id", dataset_id=dataset_id
    )
    if int(actual or 0) != verified_count:
        raise RuntimeError("cms_npd_relationship_projection_incomplete")
    return verified_count


async def assert_complete(fhir, dataset_id, release_id):
    """Require every explicit reference through bounded key pages."""
    return await _scan_pages(fhir, dataset_id, release_id, write=False)


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


async def materialize(fhir, candidate, release_id, ctx=None, task=None):
    """Write bounded, restartable batches before source-local publication."""
    sealed_count = await completed_receipt_count(fhir, candidate.dataset_id, release_id)
    if sealed_count is not None:
        if candidate.already_validated or candidate.already_published:
            return
        if await assert_complete(fhir, candidate.dataset_id, release_id) != sealed_count:
            raise RuntimeError("cms_npd_relationship_receipt_changed")
        return
    count = await _scan_pages(fhir, candidate.dataset_id, release_id, write=True, ctx=ctx, task=task)
    receipt = fhir._qt(fhir._schema(), _RECEIPT)
    await fhir.db.status(
        f"INSERT INTO {receipt} (dataset_id, release_id, projection_contract, relationship_count) "
        "VALUES (:dataset_id, :release_id, :contract, :count) ON CONFLICT (dataset_id) DO NOTHING",
        dataset_id=candidate.dataset_id,
        release_id=release_id,
        contract=CONTRACT,
        count=count,
    )
    if await completed_receipt_count(fhir, candidate.dataset_id, release_id) != count:
        raise RuntimeError("cms_npd_relationship_receipt_changed")
