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

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from antares.craft import BindingConstraintFrequency, BindingConstraintOperator, Month, TransmissionCapacities
from antares.datamanager.exceptions.exceptions import FlowbasedGenerationError
from antares.datamanager.generator.generate_flowbased import (
    SECOND_MEMBER_FILENAME,
    SUMMER_MODEL_FILENAME,
    WEIGHT_FILENAME,
    WINTER_MODEL_FILENAME,
    FlowbasedFileReader,
    create_flowbased_areas_and_links,
    generate_flowbased_binding_constraints,
)
from antares.datamanager.models.study_data_json_model import StudyData

EXPECTED_HOURS = 8760
HUB_AREAS = ("at", "be", "de", "fr", "nl")
SPLIT_FIELD = "fr_load_normalized"
SPLIT_THRESHOLD = 50.0


def _single_split_pmml(class_low: str, class_high: str) -> str:
    """A one-tree, one-feature forest: votes `class_low` if fr_load_normalized <= 50, else `class_high`."""
    return f"""<?xml version="1.0"?>
<PMML version="4.4.1" xmlns="http://www.dmg.org/PMML-4_4">
 <Header copyright="Copyright (c) 1970 Placeholder" description="Fabricated sample model for flowbased tests">
  <Application name="Fake PMML Generator" version="0.0.0"/>
  <Timestamp>1970-01-01 00:00:00</Timestamp>
 </Header>
 <DataDictionary numberOfFields="2">
  <DataField name="category" optype="categorical" dataType="string">
   <Value value="{class_low}"/>
   <Value value="{class_high}"/>
  </DataField>
  <DataField name="{SPLIT_FIELD}" optype="continuous" dataType="double"/>
 </DataDictionary>
 <MiningModel modelName="sample_forest" algorithmName="randomForest" functionName="classification">
  <MiningSchema>
   <MiningField name="category" usageType="predicted" invalidValueTreatment="returnInvalid"/>
   <MiningField name="{SPLIT_FIELD}" usageType="active" invalidValueTreatment="returnInvalid"/>
  </MiningSchema>
  <Segmentation multipleModelMethod="majorityVote">
   <Segment id="1">
    <True/>
    <TreeModel modelName="sample_forest" functionName="classification" algorithmName="randomForest" splitCharacteristic="binarySplit">
     <MiningSchema>
      <MiningField name="category" usageType="predicted" invalidValueTreatment="asIs"/>
      <MiningField name="{SPLIT_FIELD}" usageType="active" invalidValueTreatment="asIs"/>
     </MiningSchema>
     <Node id="1">
      <True/>
      <Node id="2" score="{class_low}">
       <SimplePredicate field="{SPLIT_FIELD}" operator="lessOrEqual" value="{SPLIT_THRESHOLD}"/>
      </Node>
      <Node id="3" score="{class_high}">
       <SimplePredicate field="{SPLIT_FIELD}" operator="greaterThan" value="{SPLIT_THRESHOLD}"/>
      </Node>
     </Node>
    </TreeModel>
   </Segment>
  </Segmentation>
 </MiningModel>
</PMML>
"""


def _write_two_column_series(path: Path, low_value: float, high_value: float) -> None:
    """Column 0 is always `low_value`, column 1 is always `high_value`, over 8760 rows."""
    df = pd.DataFrame({"0": [low_value] * EXPECTED_HOURS, "1": [high_value] * EXPECTED_HOURS})
    df.to_feather(path)


def _write_res_series(path: Path, value: float) -> None:
    """RES series have a leading date column that read_res_hourly_series drops."""
    df = pd.DataFrame(
        {
            "date": range(EXPECTED_HOURS),
            "0": [value] * EXPECTED_HOURS,
            "1": [value] * EXPECTED_HOURS,
        }
    )
    df.to_feather(path)


@pytest.fixture
def flowbased_fixture(tmp_path: Path) -> dict:
    load_dir = tmp_path / "load"
    res_dir = tmp_path / "res"
    hydro_dir = tmp_path / "hydro"
    flowbased_root = tmp_path / "flowbased"
    trajectory_dir = flowbased_root / "model_2024"
    for directory in (load_dir, res_dir, hydro_dir, trajectory_dir):
        directory.mkdir(parents=True)

    area_loads: dict[str, list[str]] = {}
    area_res: dict[str, dict] = {}
    area_hydro: dict[str, dict] = {}
    for area in HUB_AREAS:
        load_value_low, load_value_high = (10.0, 90.0) if area == "fr" else (1.0, 1.0)
        _write_two_column_series(load_dir / f"{area}_load.arrow", load_value_low, load_value_high)
        area_loads[area] = [f"{area}_load.arrow"]

        _write_res_series(res_dir / f"{area}_wind.arrow", 0.3)
        _write_res_series(res_dir / f"{area}_solar.arrow", 0.4)
        area_res[area] = {
            "wind_cluster": {"properties": {"group": "wind_onshore"}, "series": [f"{area}_wind.arrow"]},
            "solar_cluster": {"properties": {"group": "solar_pv"}, "series": [f"{area}_solar.arrow"]},
        }

        _write_two_column_series(hydro_dir / f"{area}_ror.arrow", 5.0, 5.0)
        area_hydro[area] = {"series": [f"{area}_ror.arrow"]}

    (trajectory_dir / SUMMER_MODEL_FILENAME).write_text(_single_split_pmml("summer1", "summer2"))
    (trajectory_dir / WINTER_MODEL_FILENAME).write_text(_single_split_pmml("winter1", "winter2"))
    (trajectory_dir / WEIGHT_FILENAME).write_text("Name fr.zz_flowbased ch.fr\nFB001 -1.0 1.0\nFB002 0.5 -0.5\n")
    (trajectory_dir / SECOND_MEMBER_FILENAME).write_text(
        "Id_Day Id_Hour Name vect_b\n"
        # Wrong values at other Id_hour values: if the
        # code ever stops filtering on Id_hour == 16, these would fail
        "1 1 FB001 -999.0\n"
        "2 1 FB001 -999.0\n"
        "3 1 FB001 -999.0\n"
        "4 1 FB001 -999.0\n"
        "1 16 FB001 10.0\n"
        "2 16 FB001 20.0\n"
        "3 16 FB001 30.0\n"
        "4 16 FB001 40.0\n"
        "1 16 FB002 100.0\n"
        "2 16 FB002 200.0\n"
        "3 16 FB002 300.0\n"
        "4 16 FB002 400.0\n"
    )

    study_data = StudyData(
        name="test-study",
        area_loads=area_loads,
        area_res=area_res,
        area_hydro=area_hydro,
        first_month=Month.JANUARY,
        nb_years=5,
    )
    flowbased_data = {
        "recalculate_ts": True,
        "ts_path": "flowbased/model_2024",  # real payloads carry this "flowbased/" prefix, must be stripped
        "type_days": [
            {"clustering": "winter1", "id_type_day": 1, "class_day": "winterWd"},
            {"clustering": "winter2", "id_type_day": 2, "class_day": "winterWd"},
            {"clustering": "summer1", "id_type_day": 3, "class_day": "summerWd"},
            {"clustering": "summer2", "id_type_day": 4, "class_day": "summerWd"},
        ],
    }

    return {
        "study_data": study_data,
        "flowbased_data": flowbased_data,
        "load_dir": load_dir,
        "res_dir": res_dir,
        "hydro_dir": hydro_dir,
        "flowbased_root": flowbased_root,
    }


@patch("antares.datamanager.generator.generate_flowbased.settings")
def test_generate_flowbased_binding_constraints_builds_expected_rhs(mock_settings, flowbased_fixture):
    mock_settings.load_output_directory = flowbased_fixture["load_dir"]
    mock_settings.res_ts_directory = flowbased_fixture["res_dir"]
    mock_settings.hydro_ts_directory = flowbased_fixture["hydro_dir"]
    mock_settings.flowbased_directory = flowbased_fixture["flowbased_root"]

    study = MagicMock()
    used_files: set[Path] = set()

    generate_flowbased_binding_constraints(
        study, flowbased_fixture["flowbased_data"], flowbased_fixture["study_data"], used_files
    )

    assert study.create_binding_constraint.call_count == 2
    calls_by_name = {call.kwargs["name"]: call.kwargs for call in study.create_binding_constraint.call_args_list}
    assert set(calls_by_name) == {"FB001", "FB002"}

    fb001_kwargs = calls_by_name["FB001"]
    assert fb001_kwargs["properties"].time_step == BindingConstraintFrequency.HOURLY
    assert fb001_kwargs["properties"].operator == BindingConstraintOperator.LESS
    assert fb001_kwargs["properties"].group == "flowbased_fb2"

    terms_by_link = {(t.data.area1, t.data.area2): t.weight for t in fb001_kwargs["terms"]}
    assert terms_by_link == {("fr", "zz_flowbased"): -1.0, ("ch", "fr"): 1.0}

    rhs = fb001_kwargs["less_term_matrix"]
    # Winter hour (hour 0, day 1): column 0 -> idDayType 1 -> vect_b 10, column 1 -> idDayType 2 -> vect_b 20
    assert rhs.iloc[0, 0] == 10.0
    assert rhs.iloc[0, 1] == 20.0
    # Summer hour (hour 3000, day ~126): column 0 -> idDayType 3 -> vect_b 30, column 1 -> idDayType 4 -> vect_b 40
    assert rhs.iloc[3000, 0] == 30.0
    assert rhs.iloc[3000, 1] == 40.0
    # Winter hour again (hour 8759, day 365)
    assert rhs.iloc[8759, 0] == 10.0
    assert rhs.iloc[8759, 1] == 20.0

    fb002_rhs = calls_by_name["FB002"]["less_term_matrix"]
    assert fb002_rhs.iloc[0, 0] == 100.0
    assert fb002_rhs.iloc[3000, 1] == 400.0


@patch("antares.datamanager.generator.generate_flowbased.settings")
def test_generate_flowbased_binding_constraints_wires_scenario_builder(mock_settings, flowbased_fixture):
    mock_settings.load_output_directory = flowbased_fixture["load_dir"]
    mock_settings.res_ts_directory = flowbased_fixture["res_dir"]
    mock_settings.hydro_ts_directory = flowbased_fixture["hydro_dir"]
    mock_settings.flowbased_directory = flowbased_fixture["flowbased_root"]

    study = MagicMock()
    used_files: set[Path] = set()

    generate_flowbased_binding_constraints(
        study, flowbased_fixture["flowbased_data"], flowbased_fixture["study_data"], used_files
    )

    group_matrix = study.get_scenario_builder.return_value.binding_constraint.get_group.return_value
    study.get_scenario_builder.return_value.binding_constraint.get_group.assert_called_once_with("flowbased_fb2")
    # nb_years=5, n_columns=2 -> [0 % 2, 1 % 2, 2 % 2, 3 % 2, 4 % 2]
    group_matrix.set_new_scenario.assert_called_once_with([0, 1, 0, 1, 0])
    study.set_scenario_builder.assert_called_once_with(study.get_scenario_builder.return_value)


@patch("antares.datamanager.generator.generate_flowbased.settings")
def test_generate_flowbased_binding_constraints_tracks_used_files(mock_settings, flowbased_fixture):
    mock_settings.load_output_directory = flowbased_fixture["load_dir"]
    mock_settings.res_ts_directory = flowbased_fixture["res_dir"]
    mock_settings.hydro_ts_directory = flowbased_fixture["hydro_dir"]
    mock_settings.flowbased_directory = flowbased_fixture["flowbased_root"]

    study = MagicMock()
    used_files: set[Path] = set()

    generate_flowbased_binding_constraints(
        study, flowbased_fixture["flowbased_data"], flowbased_fixture["study_data"], used_files
    )

    trajectory_dir = flowbased_fixture["flowbased_root"] / "model_2024"
    assert trajectory_dir / SUMMER_MODEL_FILENAME in used_files
    assert trajectory_dir / WINTER_MODEL_FILENAME in used_files
    assert trajectory_dir / WEIGHT_FILENAME in used_files
    assert trajectory_dir / SECOND_MEMBER_FILENAME in used_files
    assert flowbased_fixture["load_dir"] / "fr_load.arrow" in used_files


@patch("antares.datamanager.generator.generate_flowbased.settings")
def test_generate_flowbased_binding_constraints_raises_when_ts_path_missing(mock_settings, flowbased_fixture):
    mock_settings.load_output_directory = flowbased_fixture["load_dir"]
    mock_settings.res_ts_directory = flowbased_fixture["res_dir"]
    mock_settings.hydro_ts_directory = flowbased_fixture["hydro_dir"]
    mock_settings.flowbased_directory = flowbased_fixture["flowbased_root"]

    flowbased_data = dict(flowbased_fixture["flowbased_data"])
    flowbased_data.pop("ts_path")

    with pytest.raises(FlowbasedGenerationError):
        generate_flowbased_binding_constraints(MagicMock(), flowbased_data, flowbased_fixture["study_data"], set())


# --- create_flowbased_areas_and_links ---

ALEGRO_CAPACITY_MW = {
    "winter_HP_direct_MW": 1000,
    "summer_HP_direct_MW": 1000,
    "winter_HP_indirect_MW": 1000,
    "summer_HP_indirect_MW": 1000,
    "winter_HC_direct_MW": 1000,
    "winter_HC_indirect_MW": 1000,
    "summer_HC_direct_MW": 1000,
    "summer_HC_indirect_MW": 1000,
}


def _hub_link_entries() -> list[dict]:
    return [
        {"transmission_capacities": "INFINITE", "name": f"{area} - zz_flowbased"}
        for area in ("at", "be", "de", "fr", "nl")
    ]


def _alegro_link_entries() -> list[dict]:
    return [
        {**ALEGRO_CAPACITY_MW, "name": name, "transmission_capacities": "ENABLED"}
        for name in ("alegro1 - alegro2", "alegro2 - alegro3", "alegro3 - alegro1")
    ]


def _structural_flowbased_data() -> dict:
    return {
        "virtual_nodes": ["alegro1", "alegro2", "alegro3", "model_description_fb", "zz_flowbased"],
        "links": _hub_link_entries() + _alegro_link_entries(),
    }


def test_create_flowbased_areas_and_links_creates_virtual_areas():
    study = MagicMock()

    create_flowbased_areas_and_links(study, _structural_flowbased_data(), set())

    created_areas = {call.kwargs["area_name"] for call in study.create_area.call_args_list}
    assert created_areas == {"alegro1", "alegro2", "alegro3", "model_description_fb", "zz_flowbased"}


def test_create_flowbased_areas_and_links_creates_hub_links_as_infinite():
    study = MagicMock()

    create_flowbased_areas_and_links(study, _structural_flowbased_data(), set())

    hub_calls = {
        call.kwargs["area_from"]: call.kwargs["properties"]
        for call in study.create_link.call_args_list
        if call.kwargs.get("area_to") == "zz_flowbased"
    }
    assert set(hub_calls) == {"at", "be", "de", "fr", "nl"}
    for properties in hub_calls.values():
        assert properties.transmission_capacities == TransmissionCapacities.INFINITE


def test_create_flowbased_areas_and_links_transmission_capacities_is_case_insensitive():
    study = MagicMock()
    flowbased_data = _structural_flowbased_data()
    flowbased_data["links"] = [
        {"transmission_capacities": "infinite" if area == "fr" else "INFINITE", "name": f"{area} - zz_flowbased"}
        for area in ("at", "be", "de", "fr", "nl")
    ] + _alegro_link_entries()

    create_flowbased_areas_and_links(study, flowbased_data, set())

    fr_call = next(call for call in study.create_link.call_args_list if call.kwargs.get("area_from") == "fr")
    assert fr_call.kwargs["properties"].transmission_capacities == TransmissionCapacities.INFINITE


def test_create_flowbased_areas_and_links_creates_alegro_links_with_capacity_matrices():
    study = MagicMock()

    create_flowbased_areas_and_links(study, _structural_flowbased_data(), set())

    alegro_pairs = {
        (call.kwargs["area_from"], call.kwargs["area_to"])
        for call in study.create_link.call_args_list
        if call.kwargs.get("area_to") != "zz_flowbased"
    }
    assert alegro_pairs == {("alegro1", "alegro2"), ("alegro2", "alegro3"), ("alegro3", "alegro1")}
    assert study.create_link.return_value.set_capacity_direct.call_count == 3
    assert study.create_link.return_value.set_capacity_indirect.call_count == 3


def test_create_flowbased_areas_and_links_allows_alegro_links_with_different_capacities():
    study = MagicMock()
    flowbased_data = _structural_flowbased_data()
    flowbased_data["links"][-1] = {**flowbased_data["links"][-1], "winter_HP_direct_MW": 500}

    create_flowbased_areas_and_links(study, flowbased_data, set())

    assert study.create_link.call_count == len(flowbased_data["links"])


def test_create_flowbased_areas_and_links_raises_on_unknown_transmission_capacities_value():
    study = MagicMock()
    flowbased_data = _structural_flowbased_data()
    for entry in flowbased_data["links"]:
        if entry["name"] == "fr - zz_flowbased":
            entry["transmission_capacities"] = "NOT_A_REAL_VALUE"

    with pytest.raises(FlowbasedGenerationError):
        create_flowbased_areas_and_links(study, flowbased_data, set())


def test_create_flowbased_areas_and_links_raises_on_malformed_link_name():
    study = MagicMock()
    flowbased_data = _structural_flowbased_data()
    flowbased_data["links"].append({"transmission_capacities": "INFINITE", "name": "not_a_pair"})

    with pytest.raises(FlowbasedGenerationError):
        create_flowbased_areas_and_links(study, flowbased_data, set())


# --- FlowbasedFileReader ---

SAMPLE_WEIGHT_FILE = """Name fr.zz_flowbased ch.fr alegro1.alegro2
FB001 -1.0 1.0 0.0
FB002 0.5 -0.5 0.2
"""

SAMPLE_SECOND_MEMBER_FILE = """Id_Day Id_Hour Name vect_b
1 0 FB001 100.0
1 1 FB001 100.0
2 0 FB001 250.5
1 0 FB002 42.0
"""


@pytest.fixture
def sample_weight_path(tmp_path: Path) -> Path:
    weight_path = tmp_path / "weight.txt"
    weight_path.write_text(SAMPLE_WEIGHT_FILE)
    return weight_path


@pytest.fixture
def sample_second_member_path(tmp_path: Path) -> Path:
    second_member_path = tmp_path / "second_member.txt"
    second_member_path.write_text(SAMPLE_SECOND_MEMBER_FILE)
    return second_member_path


def test_should_read_weight_file_indexed_by_constraint_name(sample_weight_path):
    weight_df = FlowbasedFileReader.read_weight_file(sample_weight_path)

    assert list(weight_df.index) == ["FB001", "FB002"]
    assert list(weight_df.columns) == ["fr.zz_flowbased", "ch.fr", "alegro1.alegro2"]
    assert weight_df.loc["FB001", "fr.zz_flowbased"] == -1.0
    assert weight_df.loc["FB002", "ch.fr"] == -0.5


def test_should_raise_flowbased_generation_error_when_weight_file_missing_name_column(tmp_path):
    weight_path = tmp_path / "weight.txt"
    weight_path.write_text("fr.zz_flowbased ch.fr\n-1.0 1.0\n")

    with pytest.raises(FlowbasedGenerationError):
        FlowbasedFileReader.read_weight_file(weight_path)


def test_should_raise_flowbased_generation_error_when_weight_file_is_missing():
    with pytest.raises(FlowbasedGenerationError):
        FlowbasedFileReader.read_weight_file(Path("/nonexistent/weight.txt"))


def test_should_read_second_member_file_with_lowercase_columns(sample_second_member_path):
    second_member_df = FlowbasedFileReader.read_second_member_file(sample_second_member_path)

    assert list(second_member_df.columns) == ["id_day", "id_hour", "name", "vect_b"]
    assert len(second_member_df) == 4
    fb001_day1 = second_member_df[(second_member_df["name"] == "FB001") & (second_member_df["id_day"] == 1)]
    assert set(fb001_day1["vect_b"]) == {100.0}


def test_should_raise_flowbased_generation_error_when_second_member_file_missing_column(tmp_path):
    second_member_path = tmp_path / "second_member.txt"
    second_member_path.write_text("Id_Day Id_Hour Name\n1 0 FB001\n")

    with pytest.raises(FlowbasedGenerationError):
        FlowbasedFileReader.read_second_member_file(second_member_path)


def test_should_raise_flowbased_generation_error_when_second_member_file_is_missing():
    with pytest.raises(FlowbasedGenerationError):
        FlowbasedFileReader.read_second_member_file(Path("/nonexistent/second_member.txt"))
