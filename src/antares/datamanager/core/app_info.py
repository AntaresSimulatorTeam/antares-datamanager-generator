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
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field

# Python 3.11+ has tomllib built-in, Python 3.10 needs tomli
if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


logger = logging.getLogger(__name__)

class AppInfoModel(BaseModel):
    """Application information model"""

    appName: str = Field(..., description="Application name")
    appDescription: str = Field(..., description="Application description")
    appVersion: str = Field(..., description="Application version")
    appBranch: Optional[str] = Field(None, description="Git branch name")
    commitId: Optional[str] = Field(None, description="Git commit ID (SHA)")
    commitTime: Optional[datetime] = Field(None, description="Git commit timestamp")


def _read_build_info_from_file() -> tuple[Optional[str], Optional[str], Optional[str], Optional[datetime]]:
    """
    Try to read build information from build-info.json file (located in core package).
    
    Returns:
        Tuple of (version, branch, commit_id, commit_time) or (None, None, None, None) if file not found
    """
    try:
        # Try multiple paths
        possible_paths = [
            Path("/conf/build-info.json"),  # Docker case (copied during build)
            Path(__file__).parent / "build-info.json",  # Local case (same directory as app_info.py)
        ]
        
        for build_info_path in possible_paths:
            if build_info_path.exists():
                logger.debug(f"Reading build info from: {build_info_path}")
                with open(build_info_path, "r") as f:
                    data = json.load(f)
                    version = data.get("appVersion")
                    branch = data.get("appBranch")
                    commit_id = data.get("commitId")
                    commit_time_str = data.get("commitTime")
                    
                    # Parse commit time if present
                    commit_time = None
                    if commit_time_str:
                        try:
                            commit_time = datetime.fromisoformat(commit_time_str.replace("Z", "+00:00"))
                        except ValueError as e:
                            logger.warning(f"Failed to parse commit time '{commit_time_str}': {e}")
                    
                    logger.debug(f"Successfully read build info from {build_info_path}")
                    return version, branch, commit_id, commit_time
    except Exception as e:
        logger.debug(f"Could not read build info from file: {e}")
    
    return None, None, None, None


def get_app_info() -> AppInfoModel:
    """
    Get complete application information from build-info.json and pyproject.toml.
    
    The build-info.json file is created before Docker build with:
    - Git branch, commit ID, and commit time captured at build time
    
    The appVersion is always read from pyproject.toml.

    Returns:
        AppInfoModel with all application details
    """
    # Read from build-info.json (Git info)
    _, branch, commit_id, commit_time = _read_build_info_from_file()
    
    # Always read version from pyproject.toml
    version = _read_version_from_pyproject()

    return AppInfoModel(
        appName="antares-datamanager-generator",
        appDescription="API to launch datamanager study generation",
        appVersion=version,
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
        pyproject_path =Path("/conf/pyproject.toml")

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

