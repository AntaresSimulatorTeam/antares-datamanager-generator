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


from antares.datamanager.core.app_info import get_app_info, _read_version_from_pyproject


def test_get_app_info_returns_required_fields():
    """Test that get_app_info returns all required fields"""
    info = get_app_info()
    expected_version = _read_version_from_pyproject()

    assert info.appName == "antares-datamanager-generator"
    assert info.appDescription == "API to launch datamanager study generation"
    assert info.appVersion == expected_version


def test_get_app_info_optional_git_fields():
    """Test that optional git fields are present (can be None if not in git repo)"""
    info = get_app_info()

    # These fields may be None if git is not available or not in a repo
    # But they should be accessible
    assert hasattr(info, "appBranch")
    assert hasattr(info, "commitId")
    assert hasattr(info, "commitTime")


def test_get_app_info_model_structure():
    """Test that the returned model is properly typed"""
    info = get_app_info()

    # Check types
    assert isinstance(info.appName, str)
    assert isinstance(info.appDescription, str)
    assert isinstance(info.appVersion, str)
    assert info.appBranch is None or isinstance(info.appBranch, str)
    assert info.commitId is None or isinstance(info.commitId, str)
    # commitTime is datetime or None
    assert info.commitTime is None or hasattr(info.commitTime, "isoformat")
