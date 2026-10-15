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
"""Changed files of a push in a contributor fork, computed against apache/airflow (AIP-120)."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

from airflow_breeze.global_constants import APACHE_AIRFLOW_GITHUB_REPOSITORY, GithubEvents
from airflow_breeze.utils.github import run_gh_command

# The compare API lists at most this many files; a longer change set is reported as truncated.
COMPARE_FILE_CAP = 300

RELEASE_BRANCH_PREFIX = re.compile(
    r"^(v[0-9]+-[0-9]+-(?:test|stable)"
    r"|providers-[a-z]+-?[a-z]*/v[0-9]+-[0-9]+"
    r"|chart/v[0-9]+-[0-9]+x-(?:test|stable)"
    r"|airflow-ctl/v[0-9]+-[0-9]+-(?:test|stable))"
)


@dataclass(frozen=True)
class ForkChanges:
    files: tuple[str, ...]
    truncated: bool


def is_fork_push(github_event: GithubEvents, github_repository: str) -> bool:
    return github_event == GithubEvents.PUSH and github_repository != APACHE_AIRFLOW_GITHUB_REPOSITORY


def get_compare_base(branch: str) -> str:
    """Compare against main unless the branch is named after a release branch it targets."""
    match = RELEASE_BRANCH_PREFIX.match(branch)
    return match.group(1) if match else "main"


def get_fork_changes(*, fork_owner: str, sha: str, base: str) -> ForkChanges:
    """
    Three-dot compare of the pushed commit against the upstream base branch.

    Three-dot semantics give the same file list a pull request against upstream would show,
    so a fork whose ``main`` is behind upstream does not inflate the change set.
    """
    result = run_gh_command(
        [
            "gh",
            "api",
            f"repos/{APACHE_AIRFLOW_GITHUB_REPOSITORY}/compare/{base}...{fork_owner}:{sha}",
            "--jq",
            "[.files[].filename]",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Could not compare {fork_owner}:{sha} with {APACHE_AIRFLOW_GITHUB_REPOSITORY}:{base}: "
            f"{result.stderr.strip()}"
        )
    files = tuple(json.loads(result.stdout or "[]"))
    return ForkChanges(files=files, truncated=len(files) >= COMPARE_FILE_CAP)
