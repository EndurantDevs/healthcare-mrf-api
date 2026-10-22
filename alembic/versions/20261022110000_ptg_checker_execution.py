# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Restrict the existing PTG trigger checker to its verified writer family."""

import importlib.util
from pathlib import Path

from alembic import op

revision = "20261022110000_ptg_checker_execution"
down_revision = "20261020210000_registry_manual_acceptance_join"
branch_labels = None
depends_on = None


def upgrade():
    """Apply the same transactional closure used by a fresh candidate installation."""
    path = Path(__file__).with_name("20261005110000_ptg_set_validation.py")
    specification = importlib.util.spec_from_file_location("ptg_checker_predecessor", path)
    predecessor = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(predecessor)
    predecessor.op = op
    predecessor._close_checker_execution(predecessor._schema())


def downgrade():
    """Keep authorized writer execution without reopening public capabilities."""
    return None
