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

import pytest

from fastapi.testclient import TestClient

from antares.datamanager.main import app


@pytest.fixture
def client():
    """Create a test client for the FastAPI application"""
    return TestClient(app)


def test_app_info_endpoint_returns_200(client):
    """Test that /app-info endpoint returns 200 status"""
    response = client.get("/app-info")
    assert response.status_code == 200


def test_app_info_endpoint_returns_json(client):
    """Test that /app-info endpoint returns valid JSON"""
    response = client.get("/app-info")
    data = response.json()

    assert isinstance(data, dict)


def test_app_info_endpoint_has_required_fields(client):
    """Test that /app-info endpoint returns all required fields"""
    response = client.get("/app-info")
    data = response.json()

    required_fields = ["appName", "appDescription", "appVersion", "appBranch", "commitId", "commitTime"]
    for field in required_fields:
        assert field in data, f"Missing field: {field}"


def test_app_info_endpoint_app_name(client):
    """Test that appName is correct"""
    response = client.get("/app-info")
    data = response.json()

    assert data["appName"] == "antares-datamanager-generator"


def test_app_info_endpoint_app_description(client):
    """Test that appDescription is correct"""
    response = client.get("/app-info")
    data = response.json()

    assert data["appDescription"] == "API to launch datamanager study generation"

def test_app_info_endpoint_git_fields_are_optional(client):
    """Test that git fields can be None"""
    response = client.get("/app-info")
    data = response.json()

    # Git fields can be None or actual values
    assert data["appBranch"] is None or isinstance(data["appBranch"], str)
    assert data["commitId"] is None or isinstance(data["commitId"], str)
    assert data["commitTime"] is None or isinstance(data["commitTime"], str)


def test_app_info_endpoint_is_documented(client):
    """Test that /app-info is documented in OpenAPI"""
    response = client.get("/openapi.json")
    openapi_schema = response.json()

    assert "/app-info" in openapi_schema["paths"]
