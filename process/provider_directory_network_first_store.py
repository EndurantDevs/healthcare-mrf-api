# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Candidate-local durable graph frontier and batched native observations."""

import asyncio
import hashlib
import json
from contextlib import asynccontextmanager

from db.plan_release_storage import storage_identifier, storage_relation
from process.native_copy_cleanup import cleanup_interrupted_copy
from process.provider_directory_generic_source_policy import bound_publisher, source_copy_session, source_transaction


def require(condition, code):
    """Reject a graph contract violation before publishing candidate data."""
    if not condition:
        raise ValueError("provider_directory_network_snapshot_" + code)


def encoded(value):
    """Encode logical graph metadata deterministically."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def decoded(value):
    """Read JSON metadata returned by either supported database adapter."""
    return json.loads(value) if isinstance(value, str) else value


class GraphStore:
    def __init__(self, directory, source):
        self.directory, self.source = directory, source
        self.dataset_id = source.get("_endpoint_dataset_id")
        require(isinstance(self.dataset_id, str) and bool(self.dataset_id), "candidate_required")
        self.prefix = "nf_" + hashlib.sha256(self.dataset_id.encode()).hexdigest()[:28]

    @asynccontextmanager
    async def transaction(self):
        """Open one candidate-owned transaction for rows and their work cursor."""
        async with source_transaction(self.directory.db):
            publisher = bound_publisher()
            self.connection = publisher.connection
            self.schema = publisher.policy.schema
            self.policy = publisher.policy
            yield self

    def table(self, suffix):
        """Resolve an exact candidate-local table name."""
        return storage_relation(self.schema, self.prefix + "_" + suffix)

    async def prepare(self, seeds):
        """Create or resume this logical candidate and its unique seed frontier."""
        async with self.transaction():
            definitions_by_suffix = {
                "state": "singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),identity jsonb NOT NULL,pages bigint NOT NULL DEFAULT 0,complete boolean NOT NULL DEFAULT false",
                "work": "kind text NOT NULL,key text NOT NULL,url text,payload jsonb NOT NULL,pages bigint NOT NULL DEFAULT 0,total bigint,done boolean NOT NULL DEFAULT false,PRIMARY KEY(kind,key)",
                "ids": "kind text NOT NULL,key text NOT NULL,resource_id text NOT NULL,PRIMARY KEY(kind,key,resource_id)",
                "members": "resource_id text NOT NULL,network_ref text NOT NULL,PRIMARY KEY(resource_id,network_ref)",
                "resources": "resource_type text NOT NULL,resource_id text NOT NULL,content_digest text NOT NULL,network_refs text[] NOT NULL,institution_ids text[] NOT NULL,PRIMARY KEY(resource_type,resource_id)",
                "missing": "resource_id text PRIMARY KEY",
                "roots": "resource_id text PRIMARY KEY",
                "networkrefs": "reference text PRIMARY KEY",
                "cursors": "kind text NOT NULL,key text NOT NULL,url text NOT NULL,PRIMARY KEY(kind,key,url)",
            }
            for suffix, definition in definitions_by_suffix.items():
                await self.connection.execute(f"CREATE TABLE IF NOT EXISTS {self.table(suffix)} ({definition})")
                roles = set((*self.policy.runtime_roles, *self.policy.control_runtime_roles))
                await self.connection.execute(f"REVOKE ALL ON {self.table(suffix)} FROM PUBLIC")
                for role in sorted(roles):
                    await self.connection.execute(f"REVOKE ALL ON {self.table(suffix)} FROM {storage_identifier(role)}")
            await self.connection.execute(
                f"CREATE INDEX IF NOT EXISTS {self.prefix}_members_network ON {self.table('members')}(network_ref,resource_id)"
            )
            identity_by_field = {
                "dataset_id": self.dataset_id,
                "source_id": self.source["source_id"],
                "api_base": self.source.get("canonical_api_base") or self.source["api_base"],
                "configuration": self.directory._source_metadata(self.source)["provider_directory_network_snapshot"],
            }
            await self.connection.execute(
                f"INSERT INTO {self.table('state')}(identity) VALUES($1::jsonb) ON CONFLICT DO NOTHING",
                encoded(identity_by_field),
            )
            observed = await self.connection.fetchval(
                f"SELECT identity FROM {self.table('state')} WHERE singleton FOR UPDATE"
            )
            require(decoded(observed) == identity_by_field, "candidate_changed")
            await self.add_frontier(seeds)

    async def copy_stage(self, columns, records, definition):
        """One transaction-local bounded COPY heap, then set-based reconciliation."""
        name = self.prefix + "_batch"
        relation = self.table("batch")
        await self.connection.execute(f"CREATE TABLE {relation} ({definition})")
        try:
            status = await self.connection.copy_records_to_table(
                name, schema_name=self.schema, columns=columns, records=records
            )
        except (asyncio.CancelledError, TimeoutError) as interruption:
            await cleanup_interrupted_copy(source_copy_session(), self.connection, interruption)
            raise
        require(status == f"COPY {len(records)}", "copy_incomplete")
        return relation

    async def add_frontier(self, frontier):
        """Insert distinct observed work without resetting existing cursors."""
        require(len(frontier) <= 4096, "frontier_batch_too_large")
        if not frontier:
            return
        work_by_key = {}
        for work in frontier:
            require(set(work) == {"kind", "key", "url", "payload"}, "invalid_work")
            require(
                work["kind"] in {"owner", "roots", "network", "organization"}
                and isinstance(work["key"], str)
                and bool(work["key"]),
                "invalid_work",
            )
            key = work["kind"], work["key"]
            require(key not in work_by_key or work_by_key[key] == work, "conflicting_work")
            work_by_key[key] = work
        stage = await self.copy_stage(
            ("kind", "key", "url", "payload"),
            [
                (work_record["kind"], work_record["key"], work_record["url"], encoded(work_record["payload"]))
                for work_record in work_by_key.values()
            ],
            "kind text,key text,url text,payload jsonb",
        )
        await self.connection.execute(
            f"INSERT INTO {self.table('work')}(kind,key,url,payload) SELECT kind,key,url,payload FROM {stage} ON CONFLICT DO NOTHING"
        )
        await self.connection.execute(f"DROP TABLE {stage}")

    async def pending(self, *, organizations=False, limit=8):
        """Select an initial page round, then the largest known chains."""
        rows = await self.connection.fetch(
            f"SELECT kind,key,url,payload,pages,total FROM {self.table('work')} WHERE NOT done AND (kind='organization')=$1 ORDER BY total IS NOT NULL,total DESC NULLS FIRST,kind,key LIMIT $2",
            organizations,
            limit,
        )
        return [{**dict(row), "payload": decoded(row["payload"])} for row in rows]

    async def pending_affiliations(self, limit=12):
        """Select unique canonical IDs with their complete observed network union."""
        rows = await self.connection.fetch(
            f"SELECT m.resource_id,array_agg(m.network_ref ORDER BY m.network_ref) AS networks FROM {self.table('members')} m LEFT JOIN {self.table('resources')} r ON r.resource_type='OrganizationAffiliation' AND r.resource_id=m.resource_id WHERE r.resource_id IS NULL GROUP BY m.resource_id ORDER BY m.resource_id LIMIT $1",
            limit,
        )
        return {row["resource_id"]: set(row["networks"]) for row in rows}

    def _unique_entries(self, entries):
        require(len(entries) <= 4096, "entry_batch_too_large")
        entries_by_key = {}
        for entry in entries:
            resource = entry["resource"]
            key = resource["resourceType"], resource["id"]
            require(key[0] in {"Organization", "OrganizationAffiliation"}, "invalid_resource")
            require(key not in entries_by_key or entries_by_key[key]["resource"] == resource, "resource_changed")
            entries_by_key[key] = entry
        return entries_by_key

    def _resource_records(self, entries_by_key):
        from urllib.parse import urlsplit

        from process.provider_directory_affiliation_hydration import institution_refs, network_refs

        base = urlsplit(self.source.get("canonical_api_base") or self.source["api_base"])
        resource_records = [
            (
                kind,
                key,
                hashlib.sha256(encoded(entry["resource"]).encode()).hexdigest(),
                sorted(network_refs(entry["resource"], base)) if kind == "OrganizationAffiliation" else [],
                sorted(ref.split("/")[1] for ref in institution_refs(entry["resource"], base))
                if kind == "OrganizationAffiliation"
                else [],
            )
            for (kind, key), entry in entries_by_key.items()
        ]
        return resource_records

    async def _insert_resource_keys(self, resource_records):
        stage = await self.copy_stage(
            ("resource_type", "resource_id", "content_digest", "network_refs", "institution_ids"),
            resource_records,
            "resource_type text,resource_id text,content_digest text,network_refs text[],institution_ids text[]",
        )
        conflicts = await self.connection.fetchval(
            f"SELECT EXISTS(SELECT 1 FROM {stage} b JOIN {self.table('resources')} r USING(resource_type,resource_id) WHERE r.content_digest IS DISTINCT FROM b.content_digest)"
        )
        require(not conflicts, "fresh_candidate_required:resource_changed")
        inserted_keys = await self.connection.fetch(
            f"INSERT INTO {self.table('resources')}(resource_type,resource_id,content_digest,network_refs,institution_ids) SELECT resource_type,resource_id,content_digest,network_refs,institution_ids FROM {stage} ON CONFLICT DO NOTHING RETURNING resource_type,resource_id"
        )
        await self.connection.execute(f"DROP TABLE {stage}")
        return inserted_keys

    async def _persist_entries(self, entries_by_key, inserted_keys, persist):
        rows_by_model = {}
        for resource_key in inserted_keys:
            entry = entries_by_key[resource_key["resource_type"], resource_key["resource_id"]]
            parsed = self.directory.parse_fhir_resource(
                self.source["source_id"],
                entry["resource"],
                resource_url=entry.get("fullUrl"),
                acquisition=self.directory._rest_bundle_acquisition(
                    entry,
                    entry.get("_retrieved_from"),
                    self.source,
                ),
                run_id=self.source.get("_network_snapshot_run_id"),
            )
            require(
                parsed is not None
                and parsed[0] is self.directory.RESOURCE_MODELS_BY_TYPE[resource_key["resource_type"]],
                "parse_failed",
            )
            rows_by_model.setdefault(parsed[0], []).append(parsed[1])
        for model, resources in rows_by_model.items():
            await persist(model, resources)

    async def _retain_network_references(self, entries_by_key):
        from urllib.parse import urlsplit

        from process.provider_directory_affiliation_hydration import network_refs

        base = urlsplit(self.source.get("canonical_api_base") or self.source["api_base"])
        references = sorted(
            {
                reference
                for (kind, _), entry in entries_by_key.items()
                if kind == "OrganizationAffiliation"
                for reference in network_refs(entry["resource"], base)
            }
        )
        if references:
            stage = await self.copy_stage(("reference",), [(ref,) for ref in references], "reference text")
            await self.connection.execute(
                f"INSERT INTO {self.table('networkrefs')} SELECT reference FROM {stage} ON CONFLICT DO NOTHING"
            )
            await self.connection.execute(f"DROP TABLE {stage}")

    async def retain_entries(self, entries, persist):
        """Persist each canonical resource once, retaining only graph keys locally."""
        entries_by_key = self._unique_entries(entries)
        if not entries_by_key:
            return
        inserted_keys = await self._insert_resource_keys(self._resource_records(entries_by_key))
        await self._persist_entries(entries_by_key, inserted_keys, persist)
        await self._retain_network_references(entries_by_key)

    async def _lock_cursors(self, work_items, page_result):
        for work in work_items:
            observed = await self.connection.fetchrow(
                f"SELECT url,pages FROM {self.table('work')} WHERE kind=$1 AND key=$2 AND NOT done FOR UPDATE",
                work["kind"],
                work["key"],
            )
            require(
                observed is not None and observed["url"] == work["url"] and observed["pages"] == work["pages"],
                "cursor_changed",
            )
        for work in work_items:
            if work["url"]:
                await self.connection.execute(
                    f"INSERT INTO {self.table('cursors')}(kind,key,url) VALUES($1,$2,$3)",
                    work["kind"],
                    work["key"],
                    work["url"],
                )
            if page_result["next_url"]:
                repeated = await self.connection.fetchval(
                    f"SELECT EXISTS(SELECT 1 FROM {self.table('cursors')} WHERE kind=$1 AND key=$2 AND url=$3)",
                    work["kind"],
                    work["key"],
                    page_result["next_url"],
                )
                require(not repeated, "repeated_cursor")

    async def _retain_owned_roots(self, page_result):
        roots = [(work["key"],) for work in page_result["frontier"] if work["kind"] == "organization"]
        if roots:
            stage = await self.copy_stage(("resource_id",), roots, "resource_id text")
            await self.connection.execute(
                f"INSERT INTO {self.table('roots')} SELECT DISTINCT resource_id FROM {stage} ON CONFLICT DO NOTHING"
            )
            await self.connection.execute(f"DROP TABLE {stage}")

    async def _record_root_ids(self, work, ids):
        if ids:
            stage = await self.copy_stage(("resource_id",), [(resource_id,) for resource_id in ids], "resource_id text")
            overlap = await self.connection.fetchval(
                f"SELECT EXISTS(SELECT 1 FROM {stage} b JOIN {self.table('ids')} r USING(resource_id) WHERE r.kind=$1 AND r.key=$2)",
                work["kind"],
                work["key"],
            )
            require(not overlap, "repeated_query_identity")
            await self.connection.execute(
                f"INSERT INTO {self.table('ids')}(kind,key,resource_id) SELECT $1,$2,resource_id FROM {stage}",
                work["kind"],
                work["key"],
            )
            await self.connection.execute(f"DROP TABLE {stage}")
        count = await self.connection.fetchval(
            f"SELECT count(*) FROM {self.table('ids')} WHERE kind=$1 AND key=$2", work["kind"], work["key"]
        )
        return count

    async def _record_memberships(self, work, ids, memberships):
        expected_ref = "Organization/" + work["key"]
        require(
            len(memberships) == len(ids)
            and {key for key, ref in memberships} == set(ids)
            and all(ref == expected_ref for _, ref in memberships),
            "invalid_membership",
        )
        if memberships:
            stage = await self.copy_stage(
                ("resource_id", "network_ref"), memberships, "resource_id text,network_ref text"
            )
            overlap = await self.connection.fetchval(
                f"SELECT EXISTS(SELECT 1 FROM {stage} b JOIN {self.table('members')} m USING(resource_id,network_ref))"
            )
            require(not overlap, "repeated_query_identity")
            contradictory = await self.connection.fetchval(
                f"SELECT EXISTS(SELECT 1 FROM {stage} b JOIN {self.table('resources')} r "
                "ON r.resource_type='OrganizationAffiliation' AND r.resource_id=b.resource_id "
                "WHERE NOT b.network_ref=ANY(r.network_refs))"
            )
            # Roll back this page; a corrected vendor filter may resume without changing retained content.
            require(not contradictory, "late_network_conflict")
            await self.connection.execute(
                f"INSERT INTO {self.table('members')}(resource_id,network_ref) SELECT resource_id,network_ref FROM {stage}"
            )
            await self.connection.execute(f"DROP TABLE {stage}")
        count = await self.connection.fetchval(
            f"SELECT count(*) FROM {self.table('members')} WHERE network_ref=$1", expected_ref
        )
        return count

    async def _validate_query(self, work, page_result):
        ids = page_result["ids"]
        total = page_result["total"]
        require(
            type(total) is int and total >= 0 and (work["total"] is None or total == work["total"]), "total_changed"
        )
        require(len(ids) == len(set(ids)), "repeated_query_identity")
        count = (
            await self._record_root_ids(work, ids)
            if work["kind"] == "roots"
            else await self._record_memberships(work, ids, page_result["memberships"])
        )
        require(count <= total and (page_result["next_url"] is not None or count == total), "query_count_mismatch")

    async def _retain_missing(self, page_result):
        stage = await self.copy_stage(
            ("resource_id",), [(key,) for key in page_result["missing_ids"]], "resource_id text"
        )
        await self.connection.execute(
            f"INSERT INTO {self.table('missing')} SELECT DISTINCT resource_id FROM {stage} ON CONFLICT DO NOTHING"
        )
        await self.connection.execute(f"DROP TABLE {stage}")

    async def _advance_work(self, work_items, page_result):
        for work in work_items:
            await self.connection.execute(
                f"UPDATE {self.table('work')} SET url=$3,pages=pages+1,total=COALESCE(total,$4),done=$5 WHERE kind=$1 AND key=$2",
                work["kind"],
                work["key"],
                page_result["next_url"],
                page_result["total"],
                page_result["next_url"] is None,
            )
        await self.connection.execute(f"UPDATE {self.table('state')} SET pages=pages+1 WHERE singleton")

    async def apply(self, work_items, page_result, persist):
        """Commit observations and advance only their checked durable cursors."""
        writable = await self.connection.fetchval(
            f"SELECT NOT complete FROM {self.table('state')} WHERE singleton FOR UPDATE"
        )
        require(writable is True, "candidate_closed")
        require(len(page_result["ids"]) <= 4096 and len(page_result["memberships"]) <= 4096, "response_batch_too_large")
        await self._lock_cursors(work_items, page_result)
        if len(work_items) == 1 and work_items[0]["kind"] == "roots":
            await self._retain_owned_roots(page_result)
        if len(work_items) == 1 and work_items[0]["kind"] in {"roots", "network"}:
            await self._validate_query(work_items[0], page_result)
        if page_result["missing_ids"]:
            await self._retain_missing(page_result)
        await self.retain_entries(page_result["entries"], persist)
        await self.add_frontier(page_result["frontier"])
        await self._advance_work(work_items, page_result)

    async def counts(self):
        """Count the unique canonical resources already persisted."""
        resources = await self.connection.fetch(
            f"SELECT resource_type,count(*) AS count FROM {self.table('resources')} GROUP BY resource_type"
        )
        return {row["resource_type"]: row["count"] for row in resources}

    async def proof(self):
        """Count resolved native organization/participatingOrganization ID union, plus graph closure."""
        counts_by_type = await self.counts()
        work_by_kind = await self.connection.fetch(
            f"SELECT kind,count(*) AS total,count(*) FILTER(WHERE done) AS complete FROM {self.table('work')} GROUP BY kind"
        )
        work_by_kind = {row["kind"]: dict(row) for row in work_by_kind}
        network = work_by_kind.get("network", {"total": 0, "complete": 0})
        roots = await self.connection.fetchval(f"SELECT count(*) FROM {self.table('roots')}")
        missing = await self.connection.fetchval(f"SELECT count(*) FROM {self.table('missing')}")
        unresolved = await self.connection.fetchval(
            f"SELECT count(*) FROM {self.table('networkrefs')} n WHERE NOT EXISTS(SELECT 1 FROM {self.table('work')} w WHERE w.kind='network' AND n.reference='Organization/'||w.key)"
        )
        institutions = await self.connection.fetchval(
            f"SELECT count(DISTINCT reference.resource_id) FROM {self.table('resources')} affiliation "
            "CROSS JOIN LATERAL unnest(affiliation.institution_ids) reference(resource_id) "
            f"JOIN {self.table('resources')} institution ON institution.resource_type='Organization' "
            "AND institution.resource_id=reference.resource_id WHERE affiliation.resource_type='OrganizationAffiliation'"
        )
        return {
            "owned_network_roots": roots,
            "affiliations": counts_by_type.get("OrganizationAffiliation", 0),
            "discovered_networks": network["total"],
            "unresolved_networks": unresolved,
            "membership_queries": network["total"],
            "membership_queries_complete": network["complete"],
            "participating_organizations": institutions,
            "unresolved_organization_references": missing,
        }

    async def finish(self):
        """Require complete work and bidirectional membership reconciliation."""
        require(
            not await self.connection.fetchval(f"SELECT EXISTS(SELECT 1 FROM {self.table('work')} WHERE NOT done)"),
            "work_remaining",
        )
        missing = await self.pending_affiliations(1)
        require(not missing, "affiliations_remaining")
        conflicts = await self.connection.fetchval(
            f"SELECT EXISTS(SELECT 1 FROM {self.table('members')} m JOIN {self.table('resources')} r ON r.resource_type='OrganizationAffiliation' AND r.resource_id=m.resource_id WHERE NOT m.network_ref=ANY(r.network_refs))"
        )
        require(not conflicts, "fresh_candidate_required:late_network_conflict")
        omitted = await self.connection.fetchval(
            f"SELECT EXISTS(SELECT 1 FROM {self.table('resources')} r CROSS JOIN LATERAL unnest(r.network_refs) n(reference) "
            f"JOIN {self.table('work')} w ON w.kind='network' AND w.done AND n.reference='Organization/'||w.key "
            f"WHERE r.resource_type='OrganizationAffiliation' AND NOT EXISTS(SELECT 1 FROM {self.table('members')} m "
            "WHERE m.resource_id=r.resource_id AND m.network_ref=n.reference))"
        )
        require(not omitted, "fresh_candidate_required:canonical_membership_omitted")
        missing_root = await self.connection.fetchval(
            f"SELECT EXISTS(SELECT 1 FROM {self.table('roots')} r WHERE NOT EXISTS("
            f"SELECT 1 FROM {self.table('work')} w WHERE w.kind='network' AND w.key=r.resource_id AND w.done))"
        )
        require(not missing_root, "root_network_unreconciled")
        proof = await self.proof()
        require(
            proof["owned_network_roots"] > 0 and proof["membership_queries"] == proof["membership_queries_complete"],
            "closure_incomplete",
        )
        await self.connection.execute(f"UPDATE {self.table('state')} SET complete=true WHERE singleton")
        return proof


async def cleanup_published(directory, candidate):
    """Remove only this published dataset's retained graph work, after cutover."""
    from process.provider_directory_generic_source_storage import HEADER

    prefix = "nf_" + hashlib.sha256(candidate.dataset_id.encode()).hexdigest()[:28]
    async with source_transaction(directory.db):
        publisher = bound_publisher()
        connection, schema = publisher.connection, publisher.policy.schema
        state = storage_relation(schema, prefix + "_state")
        if await connection.fetchval("SELECT to_regclass($1) IS NOT NULL", state) is not True:
            return
        identity_by_field = decoded(
            await connection.fetchval(f"SELECT identity FROM {state} WHERE singleton FOR UPDATE")
        )
        require(
            identity_by_field["dataset_id"] == candidate.dataset_id
            and identity_by_field["source_id"] in candidate.source_ids,
            "cleanup_candidate_changed",
        )
        published = await connection.fetchval(
            f"SELECT status='published' FROM {storage_relation(schema, HEADER)} WHERE dataset_id=$1",
            candidate.dataset_id,
        )
        require(published is True, "cleanup_publication_required")
        for suffix in ("cursors", "networkrefs", "roots", "missing", "resources", "members", "ids", "work", "state"):
            await connection.execute(f"DROP TABLE {storage_relation(schema, prefix + '_' + suffix)}")
