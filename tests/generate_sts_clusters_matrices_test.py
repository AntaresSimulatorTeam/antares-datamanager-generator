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

import pandas as pd

from antares.craft import AdditionalConstraintOperator, AdditionalConstraintVariable
from antares.datamanager.generator.generate_sts_clusters import generate_sts_clusters


class StorageForTest:
    def __init__(self):
        self.calls = {}
        self.constraints = {}
        self.constraint_terms = {}

    def _record(self, name, df):
        self.calls[name] = df

    def set_storage_inflows(self, df):
        self._record("inflows", df)

    def set_lower_rule_curve(self, df):
        self._record("lower_curve", df)

    def update_pmax_injection(self, df):
        self._record("Pmax_injection", df)

    def set_pmax_withdrawal(self, df):
        self._record("Pmax_soutirage", df)

    def set_upper_rule_curve(self, df):
        self._record("upper_curve", df)

    def create_constraints(self, constraints):
        for constraint in constraints:
            self.constraints[constraint.name] = constraint

    def set_constraint_term(self, constraint_id, matrix):
        self.constraint_terms[constraint_id] = matrix


@pytest.fixture
def storage_for_test():
    return StorageForTest()


class AreaForTest:
    def __init__(self, storage_factory):
        self.created = []
        self.last_storage = None
        self.storage_factory = storage_factory

    def create_st_storage(self, name, properties):
        self.last_storage = self.storage_factory()
        self.created.append((name, properties))
        return self.last_storage


@pytest.fixture
def area():
    return AreaForTest(lambda: StorageForTest())


def test_generate_sts_clusters_basic(area):
    sts_data = {
        "cluster1": {
            "properties": {
                "enabled": True,
                "group": "battery",
                "reservoir_capacity": 100,
            },
            "series": [],
        }
    }

    generate_sts_clusters(area, sts_data)

    assert len(area.created) == 1
    name, props = area.created[0]
    assert name == "cluster1"
    assert props.enabled is True
    assert props.group == "battery"
    assert props.reservoir_capacity == 100


def test_generate_sts_clusters_matrices(tmp_path, monkeypatch, area):
    monkeypatch.setattr(
        "antares.datamanager.generator.generate_sts_clusters.settings",
        type("S", (), {"sts_ts_directory": tmp_path}),
    )

    # Simulate realistic data with 8760 rows and 2 columns
    df = pd.DataFrame({"time": range(8760), "TS1": [float(i) for i in range(8760)]})

    filenames = [
        "inflows.xlsx.uuid1.arrow",
        "lower_curve.xlsx.uuid2.arrow",
        "Pmax_injection.xlsx.uuid3.arrow",
        "Pmax_soutirage.xlsx.uuid4.arrow",
        "upper_curve.xlsx.uuid5.arrow",
        "unknown.xlsx.uuid6.arrow",
    ]

    for name in filenames:
        df.to_feather(tmp_path / name)

    sts_data = {"cluster1": {"properties": {}, "series": filenames}}

    generate_sts_clusters(area, sts_data)

    storage = area.last_storage
    expected_df = df.iloc[:, [1]]

    assert storage.calls["inflows"].shape == (8760, 1)
    assert storage.calls["inflows"].equals(expected_df)
    assert storage.calls["lower_curve"].equals(expected_df)
    assert storage.calls["Pmax_injection"].equals(expected_df)
    assert storage.calls["Pmax_soutirage"].equals(expected_df)
    assert storage.calls["upper_curve"].equals(expected_df)

    # unknown prefix ignored
    assert len(storage.calls) == 5


def test_generate_sts_clusters_supports_nested_series_payload(tmp_path, monkeypatch, area):
    monkeypatch.setattr(
        "antares.datamanager.generator.generate_sts_clusters.settings",
        type("S", (), {"sts_ts_directory": tmp_path}),
    )

    df = pd.DataFrame({"time": range(2), "TS1": [10.0, 20.0]})
    df.to_feather(tmp_path / "inflows.xlsx.uuid.arrow")

    sts_data = {
        "cluster1": {
            "properties": {},
            "series": {"series": ["inflows.xlsx.uuid.arrow"]},
        }
    }

    generate_sts_clusters(area, sts_data)

    storage = area.last_storage
    assert storage.calls["inflows"].equals(df.iloc[:, [1]])


def test_generate_sts_clusters_invalid_series_payload_raises(area):
    sts_data = {
        "cluster1": {
            "properties": {},
            "series": "inflows.xlsx.uuid.arrow",
        }
    }

    with pytest.raises(ValueError) as exc:
        generate_sts_clusters(area, sts_data)

    assert "Invalid STS series payload" in str(exc.value)
    assert "cluster1" in str(exc.value)


def test_generate_sts_clusters_creates_additional_constraints_from_rhs_series(tmp_path, monkeypatch, area):
    monkeypatch.setattr(
        "antares.datamanager.generator.generate_sts_clusters.settings",
        type("S", (), {"sts_ts_directory": tmp_path}),
    )

    rhs_v1g = pd.DataFrame({"time": [0, 1], "TS1": [12.0, 13.0]})
    rhs_v2g = pd.DataFrame({"time": [0, 1], "TS1": [22.0, 23.0]})
    rhs_v1g.to_feather(tmp_path / "daily_min_v1g_fr.csv.uuid.arrow")
    rhs_v2g.to_feather(tmp_path / "daily_min_v2g_fr.csv.uuid.arrow")

    sts_data = {
        "cluster1": {
            "properties": {},
            "series": [],
            "constraintParameters": {
                "daily_min_v1g_fr": {
                    "variable": "injection",
                    "operator": "greater",
                    "enabled": "true",
                    "hours": [[1, 2, 3], [4, 5, 6]],
                },
                "daily_min_v2g_fr": {
                    "variable": "withdrawal",
                    "operator": "less",
                    "enabled": False,
                    "hours": [[7, 8, 9]],
                },
            },
            # Intentionally inverted order to validate name-based matching.
            "stsConstraintsSeriesList": [
                "daily_min_v2g_fr.csv.uuid.arrow",
                "daily_min_v1g_fr.csv.uuid.arrow",
            ],
        }
    }

    generate_sts_clusters(area, sts_data)

    storage = area.last_storage
    assert set(storage.constraints.keys()) == {"daily_min_v1g_fr", "daily_min_v2g_fr"}
    assert storage.constraints["daily_min_v1g_fr"].variable == AdditionalConstraintVariable.INJECTION
    assert storage.constraints["daily_min_v1g_fr"].operator == AdditionalConstraintOperator.GREATER
    assert storage.constraints["daily_min_v1g_fr"].enabled is True
    assert storage.constraints["daily_min_v1g_fr"].occurrences[0].hours == [1, 2, 3]

    assert storage.constraints["daily_min_v2g_fr"].variable == AdditionalConstraintVariable.WITHDRAWAL
    assert storage.constraints["daily_min_v2g_fr"].operator == AdditionalConstraintOperator.LESS
    assert storage.constraints["daily_min_v2g_fr"].enabled is False
    assert storage.constraint_terms["daily_min_v1g_fr"].equals(rhs_v1g.iloc[:, [1]])
    assert storage.constraint_terms["daily_min_v2g_fr"].equals(rhs_v2g.iloc[:, [1]])


def test_generate_sts_clusters_missing_constraint_rhs_file_raises(tmp_path, monkeypatch, area):
    monkeypatch.setattr(
        "antares.datamanager.generator.generate_sts_clusters.settings",
        type("S", (), {"sts_ts_directory": tmp_path}),
    )

    sts_data = {
        "cluster1": {
            "properties": {},
            "series": [],
            "constraintParameters": {
                "daily_min_v1g_fr": {
                    "variable": "injection",
                    "operator": "greater",
                    "enabled": "true",
                    "hours": [[1, 2, 3]],
                }
            },
            "stsConstraintsSeriesList": [],
        }
    }

    with pytest.raises(FileNotFoundError) as exc:
        generate_sts_clusters(area, sts_data)

    assert "No RHS series found" in str(exc.value)


def test_generate_sts_clusters_missing_file_raises(tmp_path, monkeypatch, area):
    monkeypatch.setattr(
        "antares.datamanager.generator.generate_sts_clusters.settings",
        type("S", (), {"sts_ts_directory": tmp_path}),
    )

    sts_data = {
        "cluster1": {
            "properties": {},
            "series": ["inflows.xlsx.uuid1.arrow"],
        }
    }

    with pytest.raises(FileNotFoundError) as exc:
        generate_sts_clusters(area, sts_data)

    assert "STS matrix file not found" in str(exc.value)
    assert "cluster1" in str(exc.value)
