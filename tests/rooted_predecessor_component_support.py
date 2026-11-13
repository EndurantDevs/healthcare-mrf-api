# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Scripted row transport for component checks; no native publication authority."""

from contextlib import asynccontextmanager

from db.plan_release_storage import storage_relation
from process.provider_directory_dataset_scoped_publication_support import schema_name


class ComponentRuntime:
    def __init__(self):
        self.transactions = []

    @asynccontextmanager
    async def transaction(self, database, **options):
        self.transactions.append(options)
        yield database


class LegacyPages:
    dataset_hash = "805bf8236fbd50e1ce6f076df24ba041cabe265ffbe0cf9d614926576f3654af"

    def __init__(self):
        self.calls = []
        self.layout_calls = []
        self.tables_by_root = {
            root: {"schema": "predecessor_snapshot", "name": name}
            for root, name in (
                ("provider_directory_dataset_resource", "normalized_resources"),
                ("provider_directory_uhc_flex_practitioner_dataset_resource", "resource_provenance"),
                ("provider_directory_uhc_flex_practitioner_resource", "raw_practitioners"),
                ("provider_directory_uhc_flex_practitioner_work", "practitioner_work"),
            )
        }
        self.resources = [
            {"resource_type": "Practitioner", "resource_id": identity, "payload_hash": digest * 64}
            for identity, digest in (("A-1", "a"), ("B.2", "b"), ("c3", "c"))
        ]
        self.provenance = [{**resource, "valid": True} for resource in self.resources]
        self.raw = [
            {
                "npi": 1000000004 + index,
                "resource_id": resource["resource_id"],
                "valid": True,
            }
            for index, resource in enumerate(self.resources)
        ]

    async def first(self, query, **parameters):
        """Read the current dataset's declared source table layout once."""
        self.layout_calls.append((query, parameters))
        header = storage_relation(schema_name(), "provider_directory_endpoint_dataset")
        assert query == f"SELECT candidate_storage_json FROM {header} WHERE dataset_id=:dataset_id"
        assert parameters == {"dataset_id": "edition"}
        return {
            "candidate_storage_json": {
                "identity": {"dataset_id": "edition"},
                "storage": {"catalog_schema": schema_name(), "storage_schema": "predecessor_snapshot"},
                "tables": self.tables_by_root,
            }
        }

    async def all(self, query, **parameters):
        """Page only the stream selected by its declared qualified table name."""
        self.calls.append((query, parameters))
        relations_by_root = {
            root: storage_relation(table["schema"], table["name"]) for root, table in self.tables_by_root.items()
        }
        resource_relation = relations_by_root["provider_directory_dataset_resource"]
        provenance_relation = relations_by_root["provider_directory_uhc_flex_practitioner_dataset_resource"]
        raw_relation = relations_by_root["provider_directory_uhc_flex_practitioner_resource"]
        if query.startswith(f"SELECT resource_type,resource_id,payload_hash FROM {resource_relation} "):
            selected, columns = self.resources, ("resource_type", "resource_id")
        elif f"SELECT * FROM {provenance_relation}" in query:
            selected, columns = self.provenance, ("resource_type", "resource_id")
        elif f"SELECT * FROM {raw_relation} WHERE acquisition_id=:acquisition_id" in query:
            selected, columns = self.raw, ("npi", "resource_id")
        else:
            raise AssertionError("Unexpected predecessor stream query")
        cursor = (
            (parameters.get("npi", parameters.get("kind")), parameters["identity"])
            if "identity" in parameters
            else None
        )
        return [
            resource
            for resource in selected
            if cursor is None or tuple(resource[column] for column in columns) > cursor
        ][: parameters["page_rows"]]
