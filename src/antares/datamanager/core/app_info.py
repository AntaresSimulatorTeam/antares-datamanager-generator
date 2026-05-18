# Copyright (c) 2024, RTE (https://www.rte-france.com)
#
# See AUTHORS.txt
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# SPDX-License-Identifier: MPL-2.0
#
# This file is part of the Antares project.

import subprocess
from datetime import datetime
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field


class AppInfoModel(BaseModel):
    """Application information model"""

    appName: str = Field(..., description="Application name")
    appDescription: str = Field(..., description="Application description")
    appVersion: str = Field(..., description="Application version")
    appBranch: Optional[str] = Field(None, description="Git branch name")
    commitId: Optional[str] = Field(None, description="Git commit ID (SHA)")
    commitTime: Optional[datetime] = Field(None, description="Git commit timestamp")


def _get_git_info() -> tuple[Optional[str], Optional[str], Optional[datetime]]:
    """
    Get Git information (branch, commit ID, commit time).

    Returns:
        Tuple of (branch, commit_id, commit_time) or (None, None, None) if not a git repo
    """
    try:
        # Get the git repository root directory
        git_root = subprocess.check_output(
            ["git", "rev-parse", "--show-toplevel"], stderr=subprocess.DEVNULL, text=True
        ).strip()

        # Verify we're in the right directory (antares-datamanager-generator)
        if "antares-datamanager-generator" not in git_root:
            return None, None, None

        # Get current branch
        try:
            branch = subprocess.check_output(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"], stderr=subprocess.DEVNULL, text=True
            ).strip()
        except subprocess.CalledProcessError:
            branch = None

        # Get latest commit ID
        try:
            commit_id = subprocess.check_output(
                ["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL, text=True
            ).strip()
        except subprocess.CalledProcessError:
            commit_id = None

        # Get latest commit timestamp
        commit_time = None
        try:
            timestamp_str = subprocess.check_output(
                ["git", "log", "-1", "--format=%cI"], stderr=subprocess.DEVNULL, text=True
            ).strip()
            if timestamp_str:
                commit_time = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        except (subprocess.CalledProcessError, ValueError):
            commit_time = None

        return branch, commit_id, commit_time

    except (subprocess.CalledProcessError, FileNotFoundError):
        # Git not available or not in a git repository
        return None, None, None


def get_app_info() -> AppInfoModel:
    """
    Get complete application information.

    Returns:
        AppInfoModel with all application details
    """
    branch, commit_id, commit_time = _get_git_info()

    return AppInfoModel(
        appName="antares-datamanager-generator",
        appDescription="API to launch datamanager study generation",
        appVersion="0.0.1",
        appBranch=branch,
        commitId=commit_id,
        commitTime=commit_time,
    )

