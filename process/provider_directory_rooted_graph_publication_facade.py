# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Public async facades for rooted combined dataset publication."""

from __future__ import annotations

from typing import Any

from db.connection import db
from process.provider_directory_rooted_graph_publication_contract import (
    ProviderDirectoryRootedGraphDatasetReadiness,
    ProviderDirectoryRootedGraphPublicationResult,
)


async def load_rooted_graph_dataset_readiness(
    dataset_id: str,
    *,
    database: Any = db,
) -> ProviderDirectoryRootedGraphDatasetReadiness | None:
    """Load one exact rooted dataset only after its database readiness proof."""

    from process.provider_directory_rooted_graph_publication_store import (
        load_dataset_readiness,
    )

    return await load_dataset_readiness(dataset_id, database=database)


load_provider_directory_rooted_graph_dataset_readiness = (
    load_rooted_graph_dataset_readiness
)


async def publish_provider_directory_rooted_graph_dataset(
    publication_acquisition_id: str,
    *,
    database: Any = db,
    batch_size: int = 4096,
) -> ProviderDirectoryRootedGraphPublicationResult:
    """Publish one admitted combined dataset with bounded materialization pages."""

    from process.provider_directory_rooted_graph_publication_store import (
        publish_admitted_rooted_graph_dataset,
    )

    return await publish_admitted_rooted_graph_dataset(
        publication_acquisition_id,
        database=database,
        batch_size=batch_size,
    )


__all__ = (
    "load_provider_directory_rooted_graph_dataset_readiness",
    "publish_provider_directory_rooted_graph_dataset",
)
