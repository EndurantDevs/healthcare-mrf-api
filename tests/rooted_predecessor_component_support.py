# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Scripted row transport for component checks; no native publication authority."""

from contextlib import asynccontextmanager


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
        self.resources = [
            {"leaf": 11, "resource_type": "Practitioner", "resource_id": identity, "payload_hash": digest * 64}
            for identity, digest in (("A-1", "a"), ("B.2", "b"), ("c3", "c"))
        ]
        self.provenance = [
            {**resource, "leaf": 12, "raw_leaf": 13, "work_leaf": 14, "valid": True} for resource in self.resources
        ]
        self.raw = [
            {
                "leaf": 13,
                "npi": 1000000004 + index,
                "resource_id": resource["resource_id"],
                "work_leaf": 14,
                "valid": True,
            }
            for index, resource in enumerate(self.resources)
        ]

    async def all(self, query, **parameters):
        self.calls.append((query, parameters))
        if query.startswith("SELECT tableoid"):
            selected, columns = self.resources, ("resource_type", "resource_id")
        elif "AS raw_leaf" in query:
            selected, columns = self.provenance, ("resource_type", "resource_id")
        else:
            selected, columns = self.raw, ("npi", "resource_id")
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
