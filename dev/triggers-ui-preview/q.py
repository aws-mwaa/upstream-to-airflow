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
"""Print the rows of a SQL query against the Airflow metadata DB (commits writes)."""

from __future__ import annotations

import sys

from sqlalchemy import text

from airflow.settings import Session, configure_orm

configure_orm()
session = Session()
result = session.execute(text(sys.argv[1]))
if result.returns_rows:
    for row in result:
        print(tuple(row))
session.commit()
