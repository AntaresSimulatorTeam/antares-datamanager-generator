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

import logging
import os
import tomllib
from datetime import datetime
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field

from antares.datamanager.core.git_info import GIT_INFO

logger = logging.getLogger(__name__)


class AppInfoModel(BaseModel):
    """Application information model"""

    appName: str = Field(..., description="Application name")
    appDescription: str = Field(..., description="Application description")
    appVersion: str = Field(..., description="Application version")
    appBranch: Optional[str] = Field(None, description="Git branch name")
    commitId: Optional[str] = Field(None, description="Git commit ID (SHA)")
    commitTime: Optional[datetime] = Field(None, description="Git commit timestamp")


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


def _get_git_info() -> tuple[Optional[str], Optional[str], Optional[datetime]]:
    """
    Get Git information from the auto-generated git_info.py file.

    This file is generated at build time using scripts/generate_git_info.py
    Similar to the React frontend approach in src/gitInfo.ts

    Environment variable GIT_FORCE_REBUILD can force a rebuild:
    - GIT_FORCE_REBUILD=true: Regenerate git info from git commands

    Returns:
        Tuple of (branch, commit_id, commit_time)
    """
    branch = None
    commit_id = None
    commit_time = None

    # Check if we should regenerate git info
    if os.getenv("GIT_FORCE_REBUILD") == "true":
        try:
            import subprocess

            # Get branch from environment or git
            branch = os.getenv("BUILD_VERSION", "")
            if not branch:
                try:
                    branch = subprocess.check_output(
                        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                        stderr=subprocess.DEVNULL,
                        text=True
                    ).strip()
                except (subprocess.CalledProcessError, FileNotFoundError):
                    logger.debug("Failed to get git branch")

            # Get short commit ID
            try:
                commit_id = subprocess.check_output(
                    ["git", "rev-parse", "--short", "HEAD"],
                    stderr=subprocess.DEVNULL,
                    text=True
                ).strip()
            except (subprocess.CalledProcessError, FileNotFoundError):
                logger.debug("Failed to get git commit ID")

            # Get commit time
            try:
                timestamp_str = subprocess.check_output(
                    ["git", "log", "-1", "--format=%cI"],
                    stderr=subprocess.DEVNULL,
                    text=True
                ).strip()
                if timestamp_str:
                    commit_time = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            except (subprocess.CalledProcessError, ValueError):
                logger.debug("Failed to get git commit time")

        except Exception as e:
            logger.debug(f"Failed to rebuild git info: {e}")

    # Read from generated file
    if not branch or not commit_id:
        branch = GIT_INFO.get("branch") or None
        commit_id = GIT_INFO.get("commit") or None

        commit_time_str = GIT_INFO.get("commit_time", "")
        if commit_time_str:
            try:
                commit_time = datetime.fromisoformat(commit_time_str.replace("Z", "+00:00"))
            except ValueError:
                logger.debug(f"Failed to parse commit time: {commit_time_str}")

    # Fall back to environment variables if still not set
    if not branch:
        branch = os.getenv("GIT_BRANCH")

    if not commit_id:
        commit_id = os.getenv("GIT_COMMIT_ID")

    if not commit_time:
        commit_time_env = os.getenv("GIT_COMMIT_TIME")
        if commit_time_env:
            try:
                commit_time = datetime.fromisoformat(commit_time_env.replace("Z", "+00:00"))
            except ValueError:
                logger.debug(f"Invalid GIT_COMMIT_TIME format: {commit_time_env}")

    return branch, commit_id, commit_time



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

