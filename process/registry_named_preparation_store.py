# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named Catalog, Network and Office admission, validation and publication."""

from process.registry_named_preparation_catalog import locked_preparation, preparation_authority
from process.registry_named_preparation_claims import (
    abandon_preparation,
    claim_preparation,
    renew_preparation,
    request_cancel,
)
from process.registry_named_preparation_lifecycle import admit_preparation, intake_preparation
from process.registry_named_preparation_records import admission_values, intake_values, require, sha256
from process.registry_named_preparation_transactions import preparation_mutation


class NamedPreparationStore:
    """Use one existing lifecycle for closed named registry generations."""

    def __init__(self, connection, roles, *, kind):
        require(kind in {"catalog", "network", "office"}, "named_kind_invalid")
        self.connection, self.authority = connection, None
        self.roles, self.namespace_owner_role, self.application_roles = named_preparation_roles(roles)
        self.named_kind, self.fhir_source, self.alias_spec = kind, None, None
        self.source_authority, self.grants, self.verifier = {}, frozenset(), None

    def admit(self, request):
        """Commit the exact predecessor and its pin before any native dispatch."""
        admission, previous = admission_values(
            self.alias_spec,
            request,
            self.source_authority,
            fhir_source=self.fhir_source,
            named_kind=getattr(self, "named_kind", None),
        )
        return self._admit(admission, previous)

    @preparation_mutation
    async def _admit(self, admission, previous):
        """The detached request cannot change during catalog authorization."""
        await preparation_authority(self)
        return await admit_preparation(self, admission, previous)

    @preparation_mutation
    async def claim(self, preparation_id, admission_sha256, lease_token, *, lease_seconds=30):
        """Claim one bounded incarnation; old tokens remain permanently consumed."""
        await preparation_authority(self)
        return await claim_preparation(self, preparation_id, admission_sha256, lease_token, lease_seconds)

    @preparation_mutation
    async def renew(self, claim, *, lease_seconds=30):
        """Renew only a still-live exact claim, never revive an expired worker."""
        await preparation_authority(self)
        return await renew_preparation(self, claim, lease_seconds)

    def intake(self, claim, request):
        """Bind a verified native handoff and its complete builder-owned inventory."""
        return self._intake(claim, intake_values(request))

    @preparation_mutation
    async def _intake(self, claim, intake):
        """Detach nested native evidence before the shared savepoint performs I/O."""
        await preparation_authority(self)
        return await intake_preparation(self, claim, intake)

    @preparation_mutation
    async def observe_frozen(self, claim):
        """Require the separately completed administrative freeze before observing it."""
        from process.registry_named_preparation_catalog import locked_claim
        from process.registry_named_preparation_lifecycle import _frozen_inventory

        await preparation_authority(self)
        prepared_by_field = await locked_claim(self, claim)
        require(prepared_by_field["state"] in {"frozen", "validated"}, "administrative_freeze_required")
        await _frozen_inventory(self, prepared_by_field)
        return prepared_by_field

    @preparation_mutation
    async def request_cancel(self, preparation_id, admission_sha256):
        """Fence later work without deleting data, releasing pins or inventing a terminal result."""
        await preparation_authority(self)
        return await request_cancel(self, preparation_id, admission_sha256)

    @preparation_mutation
    async def abandon(self, claim, *, reason_sha256):
        """Record cancellation under a live claim; retain every physical claim and pin."""
        await preparation_authority(self)
        return await abandon_preparation(self, claim, reason_sha256)

    @preparation_mutation
    async def read_exact(self, preparation_id, admission_sha256):
        """Reconcile an uncertain result by exact immutable admission identity."""
        await preparation_authority(self)
        return await locked_preparation(self, preparation_id, sha256(admission_sha256))

    @preparation_mutation
    async def read_named(self, candidate_id):
        """Load one exact UUID, retaining its control lock and genuine ready-row agreement."""
        from process.registry_named_preparation_catalog import preparation_record
        from process.registry_named_preparation_records import PREPARATION, local_uuid

        row = await self.connection.fetchrow(
            f"SELECT * FROM {PREPARATION} WHERE preparation_id=$1 FOR UPDATE NOWAIT", local_uuid(candidate_id)
        )
        if row is None:
            return None
        row = preparation_record(row)
        require(
            row["importer_id"] == row["admission"].get("kind") == self.named_kind
            and row["preparation_id"] == row["generation_id"],
            "named_candidate_changed",
        )
        if row["handoff"] is not None:
            document = named_preparation_document(row["handoff"], self.named_kind)
            require(document["candidate_id"] == str(row["preparation_id"]), "named_candidate_changed")
            if row["state"] != "abandoned":
                await require_named_ready(self.connection, document)
        return row

    @preparation_mutation
    async def admit_named(self, request, handoff, *, attempt_id):
        """Commit one complete handoff or claim its existing frozen candidate for resume."""
        from uuid import uuid4

        from process.registry_named_preparation_records import NativePreparationIntake, PreparationClaim, digest

        document = named_preparation_document(handoff, self.named_kind)
        require(
            str(request.preparation_id) == document["candidate_id"] and request.request_sha256 == digest(document),
            "named_candidate_changed",
        )
        await require_named_ready(self.connection, document)
        preparation_row = await self.admit(request)
        if preparation_row["handoff"] is not None:
            require(preparation_row["handoff"] == document, "handoff_changed")
        if preparation_row["state"] in {"handed_off", "published"}:
            return preparation_row, None
        preparation_row = await self.claim(
            str(preparation_row["preparation_id"]), preparation_row["admission_sha256"], str(uuid4())
        )
        claim = PreparationClaim(
            str(preparation_row["preparation_id"]),
            str(preparation_row["claim_token"]),
            preparation_row["claim_attempt"],
        )
        if preparation_row["state"] == "admitted":
            inventory_by_field = {
                "relations": [
                    {"schema_name": table["schema"], "relation_name": table["name"]} for table in document["tables"]
                ]
            }
            await self.intake(claim, NativePreparationIntake(attempt_id, inventory_by_field, document))
            return await self.yield_named_claim(claim), None
        require(preparation_row["state"] in {"frozen", "validated"}, "preparation_state_invalid")
        return preparation_row, claim

    @preparation_mutation
    async def yield_named_claim(self, claim):
        """Expire only this exact completed phase's lease while preserving token history."""
        from process.registry_named_preparation_catalog import locked_claim, preparation_record
        from process.registry_named_preparation_records import PREPARATION

        row = await locked_claim(self, claim)
        return preparation_record(
            await self.connection.fetchrow(
                f"UPDATE {PREPARATION} SET claim_expires_at=clock_timestamp(),updated_at=clock_timestamp() "
                "WHERE preparation_id=$1 AND claim_token=$2 AND claim_attempt=$3 RETURNING *",
                row["preparation_id"],
                row["claim_token"],
                row["claim_attempt"],
            )
        )

    @preparation_mutation
    async def validate_named(self, claim, evidence):
        """Record the actual Core data validator's completed report for this frozen claim."""
        import json

        from process.registry_named_preparation_catalog import locked_claim, preparation_record
        from process.registry_named_preparation_lifecycle import _frozen_inventory
        from process.registry_named_preparation_records import PREPARATION, digest

        preparation_row = await locked_claim(self, claim)
        require(preparation_row["state"] in {"frozen", "validated"}, "freeze_required")
        document = named_preparation_document(preparation_row["handoff"], self.named_kind)
        await require_named_ready(self.connection, document)
        await _frozen_inventory(self, preparation_row)
        report = named_preparation_result(evidence, document)
        validation_by_field = {
            "identity": {"preparation_id": document["candidate_id"], "attempt_id": preparation_row["attempt_id"]},
            "evidence": report,
        }
        if preparation_row["state"] == "validated":
            require(preparation_row["validation"] == validation_by_field, "validation_changed")
            return preparation_row
        await locked_claim(self, claim)
        return preparation_record(
            await self.connection.fetchrow(
                f"UPDATE {PREPARATION} SET state='validated',validation=$2::jsonb,validation_sha256=$3,"
                "updated_at=clock_timestamp() WHERE preparation_id=$1 AND claim_token=$4 AND claim_attempt=$5 "
                "AND NOT cancellation_requested AND claim_expires_at>clock_timestamp() RETURNING *",
                preparation_row["preparation_id"],
                json.dumps(validation_by_field),
                digest(validation_by_field),
                preparation_row["claim_token"],
                preparation_row["claim_attempt"],
            )
        )

    @preparation_mutation
    async def publish_named(self, claim, publication):
        """Record Core's successful pointer CAS in that same original caller transaction."""
        import json

        from process.registry_named_preparation_catalog import locked_claim, preparation_record
        from process.registry_named_preparation_records import PREPARATION, digest

        preparation_row = await locked_claim(self, claim)
        require(preparation_row["state"] == "validated", "validation_required")
        require(
            preparation_row["validation"]["identity"]
            == {"preparation_id": str(preparation_row["preparation_id"]), "attempt_id": preparation_row["attempt_id"]},
            "validation_changed",
        )
        require(digest(preparation_row["validation"]) == preparation_row["validation_sha256"], "validation_changed")
        document = named_preparation_document(preparation_row["handoff"], self.named_kind)
        named_preparation_result(preparation_row["validation"]["evidence"], document)
        await require_named_ready(self.connection, document)
        report = named_preparation_result(publication, document, fence=str(preparation_row["publication_fence"]))
        await locked_claim(self, claim)
        return preparation_record(
            await self.connection.fetchrow(
                f"UPDATE {PREPARATION} SET state='published',publication=$2::jsonb,publication_sha256=$3,"
                "published_at=clock_timestamp(),published_fence_token=NULL,updated_at=clock_timestamp() "
                "WHERE preparation_id=$1 AND claim_token=$4 AND claim_attempt=$5 "
                "AND NOT cancellation_requested AND claim_expires_at>clock_timestamp() RETURNING *",
                preparation_row["preparation_id"],
                json.dumps(report),
                digest(report),
                preparation_row["claim_token"],
                preparation_row["claim_attempt"],
            )
        )


def named_preparation_roles(roles):
    """Use the existing Core role domain without reducing its reader capacity."""
    from process.registry_named_preparation_records import ProtectedPreparation, name

    require(
        type(roles) is dict and set(roles) == {"namespace_owner_role", "publisher_role", "application_roles"},
        "named_admin_config_required",
    )
    owner, publisher = name(roles["namespace_owner_role"]), name(roles["publisher_role"])
    applications = roles["application_roles"]
    require(isinstance(applications, (tuple, list)) and bool(applications), "named_admin_config_required")
    from process.registry_named_preparation_records import json_object

    json_object({"roles": list(applications)})
    applications = tuple(dict.fromkeys(name(role) for role in applications))
    require(
        owner != publisher and owner not in applications and publisher in applications, "named_admin_config_required"
    )
    readers = tuple(role for role in applications if role != publisher)
    return ProtectedPreparation(owner, publisher, readers), owner, applications


async def named_preparation_role_config(kind, configured):
    """Read the kind's existing runtime configuration outside snapshot metadata."""
    if kind == "catalog":
        from process.registry_fhir_catalog_store import named_catalog_preparation_roles as configured_roles
    elif kind == "office":
        from process.registry_ptg_office_preparation import named_office_preparation_roles as configured_roles
    else:
        require(kind == "network", "named_kind_invalid")
        from process.network_membership_publication import named_network_preparation_roles as configured_roles
    return configured_roles(configured)


def named_preparation_document(handoff, kind):
    """Bound the complete portable resume document before any privileged table operation."""
    from process.registry_named_preparation_records import json_object, local_uuid, relation_inventory

    _bounded_named_json(handoff)
    document = json_object(handoff)
    require(
        set(document) == {"kind", "candidate_id", "catalog_schema", "storage_schema", "tables", "snapshot"}
        and document["kind"] == kind
        and kind in {"catalog", "network", "office"},
        "named_handoff_invalid",
    )
    require(document["candidate_id"] == str(local_uuid(document["candidate_id"])), "named_candidate_changed")
    tables = document["tables"]
    require(
        isinstance(tables, list)
        and bool(tables)
        and all(type(table) is dict and set(table) == {"schema", "name"} for table in tables),
        "named_handoff_invalid",
    )
    inventory = relation_inventory(
        {"relations": [{"schema_name": table["schema"], "relation_name": table["name"]} for table in tables]}
    )
    from process.registry_named_preparation_records import name

    name(document["catalog_schema"])
    name(document["storage_schema"])
    require(isinstance(document["snapshot"], dict) and bool(document["snapshot"]), "named_handoff_invalid")
    schemas = {table["schema_name"] for table in inventory["relations"]}
    allowed_schemas = {document["storage_schema"]}
    if kind == "network" and schemas != allowed_schemas:
        import re

        address_source = document["snapshot"].get("address_source")
        auxiliary_schema = address_source.get("schema_name") if isinstance(address_source, dict) else None
        require(
            isinstance(auxiliary_schema, str) and re.fullmatch(r"network_composition_[0-9a-f]{32}", auxiliary_schema),
            "generation_schema_changed",
        )
        allowed_schemas.add(auxiliary_schema)
    require(
        document["storage_schema"] in schemas
        and schemas <= allowed_schemas
        and document["catalog_schema"] not in schemas,
        "generation_schema_changed",
    )
    _bounded_named_json(document)
    return document


def named_preparation_result(result, document, *, fence=None):
    """Keep completed validation/publication data bound to the exact logical candidate."""
    from process.registry_named_preparation_records import json_object

    _bounded_named_json(result)
    report = json_object(result)
    fields = {"kind", "candidate_id", "data"} | ({"fence_token"} if fence is not None else set())
    require(
        set(report) == fields
        and report["kind"] == document["kind"]
        and report["candidate_id"] == document["candidate_id"]
        and isinstance(report["data"], dict)
        and bool(report["data"]),
        "named_result_invalid",
    )
    require(fence is None or report["fence_token"] == fence, "publication_fence_changed")
    _bounded_named_json(report)
    return report


def _bounded_named_json(document):
    import json

    require(isinstance(document, dict), "named_document_invalid")
    pending, count = [(document, 0)], 0
    forbidden_fields = {
        "database_oid",
        "relation_oid",
        "table_oid",
        "schema_oid",
        "owner_oid",
        "role_oid",
        "schema_owner_oid",
        "namespace_owner_oid",
        "relfilenode",
        "layout_sha256",
        "role_graph",
        "callable_custody",
        "storage_custody",
    }
    while pending:
        nested_value, depth = pending.pop()
        count += 1
        require(depth <= 16 and count <= 4096, "named_document_bounds")
        if isinstance(nested_value, dict):
            require(
                all(isinstance(key, str) for key in nested_value) and not forbidden_fields.intersection(nested_value),
                "named_document_invalid",
            )
            pending.extend((nested, depth + 1) for nested in nested_value.values())
        elif isinstance(nested_value, (tuple, list)):
            pending.extend((nested, depth + 1) for nested in nested_value)
        else:
            require(nested_value is None or type(nested_value) in {str, int, float, bool}, "named_document_invalid")
    require(
        len(json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()) <= 262144,
        "named_document_bounds",
    )


async def require_named_ready(connection, document):
    """Read the kind's genuine persisted candidate through its maintained Core loader."""
    if document["kind"] == "catalog":
        from process.registry_fhir_catalog_store import load_named_catalog_preparation as load_ready
    elif document["kind"] == "office":
        from process.registry_ptg_office_preparation import load_named_office_preparation as load_ready
    else:
        from process.network_membership_publication import load_named_network_preparation as load_ready
    ready = await load_ready(connection, document["candidate_id"], control_schema=document["catalog_schema"])
    require(
        ready is not None and named_preparation_document(ready, document["kind"]) == document, "named_ready_changed"
    )
