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
import logging
import tomllib
from datetime import datetime
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field


logger = logging.getLogger(__name__)

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
    app_version = _read_version_from_pyproject()

    return AppInfoModel(
        appName="antares-datamanager-generator",
        appDescription="API to launch datamanager study generation",
        appVersion=app_version,
        appBranch=branch,
        commitId=commit_id,
        commitTime=commit_time,
    )
def _read_version_from_pyproject() -> str:
    """
    Read the application version from pyproject.toml.

    Returns:
        Version string from pyproject.toml, or "unknown" if not found
    """
    try:
        # Find pyproject.toml from the package root
        # The package is in src/antares/datamanager/, so we need to go up several levels
        package_dir = Path(__file__).parent.parent.parent.parent  # src/
        project_root = package_dir.parent  # project root
        pyproject_path = project_root / "pyproject.toml"

        if pyproject_path.exists():
            with open(pyproject_path, "rb") as f:
                data = tomllib.load(f)
                version = data.get("project", {}).get("version", "unknown")
                logger.debug(f"Read version from pyproject.toml: {version}")
                return str(version)
        else:
            logger.warning(f"pyproject.toml not found at {pyproject_path}")
            return "unknown"

    except Exception as e:
        logger.warning(f"Failed to read version from pyproject.toml: {e}")
        return "unknown"
