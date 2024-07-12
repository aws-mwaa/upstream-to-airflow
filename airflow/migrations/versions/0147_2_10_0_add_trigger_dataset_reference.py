#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import Integer

# revision identifiers, used by Alembic.
revision = "4b4263b7229"
down_revision = "d482b7261ff9"
branch_labels = None
depends_on = None
airflow_version = "2.10.0"


def _create_trigger_dataset_reference_table():
    op.create_table(
        "trigger_dataset_reference",
        sa.Column("trigger_id", Integer, primary_key=True, nullable=False),
        sa.Column("dataset_id", Integer, primary_key=True, nullable=False),
        sa.ForeignKeyConstraint(
            columns=("trigger_id",),
            refcolumns=["trigger.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ("dataset_id",),
            ["dataset.id"],
            ondelete="CASCADE",
        ),
    )


def upgrade():
    """Apply Add Dataset model."""
    _create_trigger_dataset_reference_table()


def downgrade():
    """Unapply Add Dataset model."""
    op.drop_table("trigger_dataset_reference")
