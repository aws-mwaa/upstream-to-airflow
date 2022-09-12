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

"""add executor field to task instance

Revision ID: f1c9f4bf9d3f
Revises: 8e1c784a4fc7
Create Date: 2024-03-20 16:26:21.429092

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f1c9f4bf9d3f"
down_revision = "8e1c784a4fc7"
branch_labels = None
depends_on = None
airflow_version = "2.9.0"

TABLE_NAME = "task_instance"
COLUMN_NAME = "executor"


def upgrade():
    """Add executor column to task instance"""
    with op.batch_alter_table(TABLE_NAME) as batch_op:
        batch_op.add_column(sa.Column(COLUMN_NAME, sa.String(1000), default=None))


def downgrade():
    """Removes executor column from task instance"""
    # op.drop_column(TABLE_NAME, COLUMN_NAME)
    with op.batch_alter_table(TABLE_NAME) as batch_op:
        batch_op.drop_column(COLUMN_NAME)
