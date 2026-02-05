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

from antares.datamanager.generator.generate_sts_clusters import generate_sts_clusters


class StorageForTest:
    def __init__(self):
        self.calls = {}

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
