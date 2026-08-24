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
from antares.craft.model.area import Area
from antares.craft.model.renewable import RenewableCluster
from antares.craft import (
    BindingConstraintFrequency,
    BindingConstraintOperator,
    ClusterData,
    ConstraintTerm,
    LinkData,
    Month,
    ThermalClusterProperties,
    TransmissionCapacities,
)
from antares.datamanager.exceptions.exceptions import FlowbasedGenerationError
from antares.datamanager.generator.generate_flowbased import (
    BINDING_CONSTRAINT_HOURLY_ROWS,
    SECOND_MEMBER_FILENAME,
    SUMMER_MODEL_FILENAME,
    TS_FILENAME,
    WEIGHT_FILENAME,
    WINTER_MODEL_FILENAME,
    FlowbasedFileReader,
    _pad_to_binding_constraint_hourly_rows,
    _read_combined_res_series,
    _zscore_pooled,
    compute_id_day_types,
    create_flowbased_areas_and_links,
    create_restriction_ahc,
    generate_flowbased_binding_constraints,
)
from antares.datamanager.models.study_data_json_model import StudyData
from antares.datamanager.utils.random_forest_reader import load_forest_model

EXPECTED_HOURS = 8760
HUB_AREAS = ("at", "be", "de", "fr", "nl")
SPLIT_FIELD = "fr_load_normalized"
# Pooled z-score of two constants centers on 0 - the fake model's split threshold is the
# z-scored midpoint, not the raw midpoint (see _single_split_pmml docstring).
SPLIT_THRESHOLD = 0.0


def _single_split_pmml(class_low: str, class_high: str) -> str:
    """A one-tree, one-feature forest: votes `class_low` if fr_load_normalized <= 0 (z-scored), else `class_high`."""
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


def _two_column_series(low_value: float, high_value: float) -> pd.DataFrame:
    """Column 0 is always `low_value`, column 1 is always `high_value`, over 8760 rows."""
    return pd.DataFrame({0: [low_value] * EXPECTED_HOURS, 1: [high_value] * EXPECTED_HOURS})


def _make_renewable_cluster(group: str, factor_df: pd.DataFrame, enabled_capacity: float) -> MagicMock:
    cluster = MagicMock(spec=RenewableCluster)
    cluster.get_timeseries.return_value = factor_df
    cluster.properties = MagicMock(group=group, enabled_capacity=enabled_capacity)
    return cluster


def _make_area(area_id: str, load_df: pd.DataFrame, renewables: list[MagicMock], ror_df: pd.DataFrame) -> MagicMock:
    area = MagicMock(spec=Area)
    area.id = area_id
    area.get_load_matrix.return_value = load_df
    area.get_renewables.return_value = {f"cluster{i}": cluster for i, cluster in enumerate(renewables)}
    area.hydro.get_ror_series.return_value = ror_df
    return area


@pytest.fixture
def flowbased_fixture(tmp_path: Path) -> dict:
    flowbased_root = tmp_path / "flowbased"
    trajectory_dir = flowbased_root / "model_2024"
    trajectory_dir.mkdir(parents=True)

    areas: dict[str, MagicMock] = {}
    for area in HUB_AREAS:
        load_low, load_high = (10.0, 90.0) if area == "fr" else (1.0, 1.5)
        load_df = _two_column_series(load_low, load_high)
        wind_cluster = _make_renewable_cluster("Wind Onshore", _two_column_series(0.3, 0.35), enabled_capacity=100.0)
        solar_cluster = _make_renewable_cluster("Solar PV", _two_column_series(0.4, 0.45), enabled_capacity=100.0)
        ror_df = _two_column_series(5.0, 6.0)
        areas[area] = _make_area(area, load_df, [wind_cluster, solar_cluster], ror_df)

    study = MagicMock()
    study.get_areas.return_value = areas

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
    (trajectory_dir / TS_FILENAME).write_text("Date 1 2\n1 3 4\n2 3 4\n")

    study_data = StudyData(name="test-study", first_month=Month.JANUARY, nb_years=5)
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
        "study": study,
        "study_data": study_data,
        "flowbased_data": flowbased_data,
        "flowbased_root": flowbased_root,
    }


@patch("antares.datamanager.generator.generate_flowbased.settings")
def test_generate_flowbased_binding_constraints_builds_expected_rhs(mock_settings, flowbased_fixture):
    mock_settings.flowbased_directory = flowbased_fixture["flowbased_root"]

    study = flowbased_fixture["study"]
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
    # Winter day (hour 0, day 1): column 0 -> idDayType 1 -> vect_b 10, column 1 -> idDayType 2 -> vect_b 20
    assert rhs.iloc[0, 0] == 10.0
    assert rhs.iloc[0, 1] == 20.0
    # Summer day (hour 3000, day ~126): column 0 -> idDayType 3 -> vect_b 30, column 1 -> idDayType 4 -> vect_b 40
    assert rhs.iloc[3000, 0] == 30.0
    assert rhs.iloc[3000, 1] == 40.0
    # Winter day again (hour 8759, day 365)
    assert rhs.iloc[8759, 0] == 10.0
    assert rhs.iloc[8759, 1] == 20.0
    # BC matrix is always 8784 rows hourly, extra rows are 0
    assert rhs.shape == (BINDING_CONSTRAINT_HOURLY_ROWS, 2)
    assert (rhs.iloc[EXPECTED_HOURS:BINDING_CONSTRAINT_HOURLY_ROWS] == 0.0).all(axis=None)

    fb002_rhs = calls_by_name["FB002"]["less_term_matrix"]
    assert fb002_rhs.iloc[0, 0] == 100.0
    assert fb002_rhs.iloc[3000, 1] == 400.0
    assert fb002_rhs.shape == (BINDING_CONSTRAINT_HOURLY_ROWS, 2)


@patch("antares.datamanager.generator.generate_flowbased.settings")
def test_generate_flowbased_read_binding_constraints_builds_expected_rhs(mock_settings, flowbased_fixture):
    mock_settings.flowbased_directory = flowbased_fixture["flowbased_root"]

    study = MagicMock()
    used_files: set[Path] = set()

    flowbasedData = {
        "recalculate_ts": False,
        "ts_path": "flowbased/model_2024",
    }

    generate_flowbased_binding_constraints(study, flowbasedData, flowbased_fixture["study_data"], used_files)

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
    assert rhs.iloc[0, 0] == 30.0
    assert rhs.iloc[0, 1] == 40.0
    assert rhs.iloc[1, 0] == 30.0
    assert rhs.iloc[1, 1] == 40.0

    fb002_rhs = calls_by_name["FB002"]["less_term_matrix"]
    assert fb002_rhs.iloc[0, 0] == 300.0
    assert fb002_rhs.iloc[1, 1] == 400.0


@patch("antares.datamanager.generator.generate_flowbased.settings")
def test_generate_flowbased_binding_constraints_wires_scenario_builder(mock_settings, flowbased_fixture):
    mock_settings.flowbased_directory = flowbased_fixture["flowbased_root"]

    study = flowbased_fixture["study"]
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
    mock_settings.flowbased_directory = flowbased_fixture["flowbased_root"]

    study = flowbased_fixture["study"]
    used_files: set[Path] = set()

    generate_flowbased_binding_constraints(
        study, flowbased_fixture["flowbased_data"], flowbased_fixture["study_data"], used_files
    )

    trajectory_dir = flowbased_fixture["flowbased_root"] / "model_2024"
    assert trajectory_dir / SUMMER_MODEL_FILENAME in used_files
    assert trajectory_dir / WINTER_MODEL_FILENAME in used_files
    assert trajectory_dir / WEIGHT_FILENAME in used_files
    assert trajectory_dir / SECOND_MEMBER_FILENAME in used_files


@patch("antares.datamanager.generator.generate_flowbased.settings")
def test_generate_flowbased_binding_constraints_raises_when_ts_path_missing(mock_settings, flowbased_fixture):
    mock_settings.flowbased_directory = flowbased_fixture["flowbased_root"]

    flowbased_data = dict(flowbased_fixture["flowbased_data"])
    flowbased_data.pop("ts_path")

    with pytest.raises(FlowbasedGenerationError):
        generate_flowbased_binding_constraints(
            flowbased_fixture["study"], flowbased_data, flowbased_fixture["study_data"], set()
        )


# --- _read_combined_res_series ---


def test_read_combined_res_series_weights_by_enabled_capacity():
    onshore = _make_renewable_cluster("Wind Onshore", _two_column_series(0.5, 0.6), enabled_capacity=100.0)
    offshore = _make_renewable_cluster("Wind Offshore", _two_column_series(0.2, 0.3), enabled_capacity=50.0)
    area = _make_area("fr", _two_column_series(1.0, 1.0), [onshore, offshore], _two_column_series(1.0, 1.0))

    combined = _read_combined_res_series(area, {"Wind Onshore", "Wind Offshore"})

    # 0.5*100 + 0.2*50 = 60.0 ; 0.6*100 + 0.3*50 = 75.0
    assert combined.iloc[0, 0] == 60.0
    assert combined.iloc[0, 1] == 75.0


def test_read_combined_res_series_disabled_cluster_contributes_zero():
    onshore = _make_renewable_cluster("Wind Onshore", _two_column_series(0.5, 0.6), enabled_capacity=100.0)
    disabled_offshore = _make_renewable_cluster("Wind Offshore", _two_column_series(0.9, 0.9), enabled_capacity=0.0)
    area = _make_area("fr", _two_column_series(1.0, 1.0), [onshore, disabled_offshore], _two_column_series(1.0, 1.0))

    combined = _read_combined_res_series(area, {"Wind Onshore", "Wind Offshore"})

    assert combined.iloc[0, 0] == 50.0
    assert combined.iloc[0, 1] == 60.0


def test_read_combined_res_series_raises_when_no_matching_cluster():
    area = _make_area("fr", _two_column_series(1.0, 1.0), [], _two_column_series(1.0, 1.0))

    with pytest.raises(FlowbasedGenerationError):
        _read_combined_res_series(area, {"Wind Onshore", "Wind Offshore"})


# --- _zscore_pooled / compute_id_day_types ---


def test_zscore_pooled_matches_manual_computation():
    hourly = pd.DataFrame({0: [1.0, 2.0, 3.0], 1: [4.0, 5.0, 6.0]})

    result = _zscore_pooled(hourly)

    values = hourly.to_numpy().ravel()
    expected = (hourly - values.mean()) / values.std(ddof=1)
    pd.testing.assert_frame_equal(result, expected)


def test_zscore_pooled_raises_on_constant_series():
    hourly = pd.DataFrame({0: [5.0, 5.0], 1: [5.0, 5.0]})

    with pytest.raises(FlowbasedGenerationError):
        _zscore_pooled(hourly)


def test_compute_id_day_types_predicts_independently_per_hour(tmp_path):
    """
    The model must be called once per hour, not once per day with the result broadcast.
    """
    pmml_path = tmp_path / "model.pmml"
    pmml_path.write_text(_single_split_pmml("low", "high"))
    model = load_forest_model(pmml_path)

    fr_load_values = ([10.0] * 12 + [90.0] * 12) * 365
    filler_values = [1.0, 1.5] * (EXPECTED_HOURS // 2)
    hub_features = {
        area: {
            variable: pd.DataFrame({0: fr_load_values if (area == "fr" and variable == "load") else filler_values})
            for variable in ("load", "wind", "solar", "h_ror")
        }
        for area in HUB_AREAS
    }
    type_days = [
        {"clustering": "low", "id_type_day": 1, "class_day": "winterWd"},
        {"clustering": "high", "id_type_day": 2, "class_day": "winterWd"},
    ]

    id_day_types = compute_id_day_types(model, model, hub_features, type_days, Month.JANUARY)

    assert id_day_types.iloc[0, 0] != id_day_types.iloc[12, 0]
    assert id_day_types.shape == (EXPECTED_HOURS, 1)


def test_pad_to_binding_constraint_hourly_rows_appends_zeros():
    matrix = pd.DataFrame({0: [1.0] * EXPECTED_HOURS, 1: [2.0] * EXPECTED_HOURS})

    padded = _pad_to_binding_constraint_hourly_rows(matrix)

    assert padded.shape == (BINDING_CONSTRAINT_HOURLY_ROWS, 2)
    # Original rows are preserved unchanged
    assert (padded.iloc[0:EXPECTED_HOURS, 0] == 1.0).all()
    assert (padded.iloc[0:EXPECTED_HOURS, 1] == 2.0).all()
    # Extra rows (the "366th day") are zero
    assert (padded.iloc[EXPECTED_HOURS:BINDING_CONSTRAINT_HOURLY_ROWS] == 0.0).all(axis=None)


def test_pad_to_binding_constraint_hourly_rows_raises_on_unexpected_row_count():
    matrix = pd.DataFrame({0: [1.0] * 100})

    with pytest.raises(FlowbasedGenerationError):
        _pad_to_binding_constraint_hourly_rows(matrix)


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

    create_flowbased_areas_and_links(study, _structural_flowbased_data())

    created_areas = {call.kwargs["area_name"] for call in study.create_area.call_args_list}
    assert created_areas == {"alegro1", "alegro2", "alegro3", "model_description_fb", "zz_flowbased"}


def test_create_flowbased_areas_and_links_creates_hub_links_as_infinite():
    study = MagicMock()

    create_flowbased_areas_and_links(study, _structural_flowbased_data())

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

    create_flowbased_areas_and_links(study, flowbased_data)

    fr_call = next(call for call in study.create_link.call_args_list if call.kwargs.get("area_from") == "fr")
    assert fr_call.kwargs["properties"].transmission_capacities == TransmissionCapacities.INFINITE


def test_create_flowbased_areas_and_links_creates_alegro_links_with_capacity_matrices():
    study = MagicMock()

    create_flowbased_areas_and_links(study, _structural_flowbased_data())

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

    create_flowbased_areas_and_links(study, flowbased_data)

    assert study.create_link.call_count == len(flowbased_data["links"])


def test_create_flowbased_areas_and_links_raises_on_unknown_transmission_capacities_value():
    study = MagicMock()
    flowbased_data = _structural_flowbased_data()
    for entry in flowbased_data["links"]:
        if entry["name"] == "fr - zz_flowbased":
            entry["transmission_capacities"] = "NOT_A_REAL_VALUE"

    with pytest.raises(FlowbasedGenerationError):
        create_flowbased_areas_and_links(study, flowbased_data)


def test_create_flowbased_areas_and_links_raises_on_malformed_link_name():
    study = MagicMock()
    flowbased_data = _structural_flowbased_data()
    flowbased_data["links"].append({"transmission_capacities": "INFINITE", "name": "not_a_pair"})

    with pytest.raises(FlowbasedGenerationError):
        create_flowbased_areas_and_links(study, flowbased_data)


# --- Restriction AHC ---


def test_create_restriction_ahc_creates_thermal_cluster_and_binding_constraint():
    study = MagicMock()
    model_description_fb_area = MagicMock()
    study.get_areas.return_value = {"model_description_fb": model_description_fb_area}

    create_restriction_ahc(study)

    # 1. Vérification de la création du cluster thermique
    model_description_fb_area.create_thermal_cluster.assert_called_once()
    cluster_kwargs = model_description_fb_area.create_thermal_cluster.call_args.kwargs
    assert cluster_kwargs["cluster_name"] == "restriction_ahc"
    cluster_props = cluster_kwargs["properties"]
    assert cluster_props.nominal_capacity == 10000.0
    assert cluster_props.unit_count == 1
    assert cluster_props.enabled is True

    # 2. Vérification de la création de la contrainte couplante
    study.create_binding_constraint.assert_called_once()
    constraint_kwargs = study.create_binding_constraint.call_args.kwargs
    assert constraint_kwargs["name"] == "restriction_ahc"

    properties = constraint_kwargs["properties"]
    assert properties.enabled is True
    assert properties.time_step == BindingConstraintFrequency.HOURLY
    assert properties.operator == BindingConstraintOperator.LESS

    terms = constraint_kwargs["terms"]
    assert len(terms) == 4

    # 1 * (ch%fr)
    term_ch_fr = next(t for t in terms if isinstance(t.data, LinkData) and t.data.area1 == "ch" and t.data.area2 == "fr")
    assert term_ch_fr.weight == 1.0

    # -1 * (fr%itn)
    term_fr_itn = next(
        t for t in terms if isinstance(t.data, LinkData) and t.data.area1 == "fr" and t.data.area2 == "itn"
    )
    assert term_fr_itn.weight == -1.0

    # -1 * (fr%zz_flowbased)
    term_fr_zz = next(
        t for t in terms if isinstance(t.data, LinkData) and t.data.area1 == "fr" and t.data.area2 == "zz_flowbased"
    )
    assert term_fr_zz.weight == -1.0

    # -1 * (model_description_fb.restriction_ahc)
    term_cluster = next(t for t in terms if isinstance(t.data, ClusterData))
    assert term_cluster.data.area == "model_description_fb"
    assert term_cluster.data.cluster == "restriction_ahc"
    assert term_cluster.weight == -1.0

    # 3. Matrice second membre (RHS) : 0 sur 8760 heures
    less_term_matrix = constraint_kwargs["less_term_matrix"]
    assert isinstance(less_term_matrix, pd.DataFrame)
    assert less_term_matrix.shape == (EXPECTED_HOURS, 1)
    assert (less_term_matrix == 0).all().all()


def test_create_restriction_ahc_with_custom_limitation_mw():
    study = MagicMock()
    model_description_fb_area = MagicMock()
    study.get_areas.return_value = {"model_description_fb": model_description_fb_area}

    create_restriction_ahc(study, limitation_mw=5000.0)

    model_description_fb_area.create_thermal_cluster.assert_called_once()
    cluster_kwargs = model_description_fb_area.create_thermal_cluster.call_args.kwargs
    assert cluster_kwargs["properties"].nominal_capacity == 5000.0


def test_create_flowbased_areas_and_links_creates_restriction_ahc_when_model_description_fb_present():
    study = MagicMock()
    model_description_fb_area = MagicMock()
    study.get_areas.return_value = {"model_description_fb": model_description_fb_area}
    flowbased_data = _structural_flowbased_data()
    assert "model_description_fb" in flowbased_data["virtual_nodes"]

    create_flowbased_areas_and_links(study, flowbased_data, set())

    model_description_fb_area.create_thermal_cluster.assert_called_once()
    assert model_description_fb_area.create_thermal_cluster.call_args.kwargs["cluster_name"] == "restriction_ahc"
    binding_constraint_names = [call.kwargs["name"] for call in study.create_binding_constraint.call_args_list]
    assert "restriction_ahc" in binding_constraint_names


def test_create_flowbased_areas_and_links_does_not_create_restriction_ahc_when_model_description_fb_absent():
    study = MagicMock()
    model_description_fb_area = MagicMock()
    study.get_areas.return_value = {"model_description_fb": model_description_fb_area}
    flowbased_data = _structural_flowbased_data()
    flowbased_data["virtual_nodes"] = ["alegro1", "alegro2", "alegro3", "zz_flowbased"]

    create_flowbased_areas_and_links(study, flowbased_data, set())

    model_description_fb_area.create_thermal_cluster.assert_not_called()
    binding_constraint_names = [call.kwargs["name"] for call in study.create_binding_constraint.call_args_list]
    assert "restriction_ahc" not in binding_constraint_names


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

SAMPLE_TS_FILE = """"Date" "1" "2" "3"
1 3 3 3
2 4 4 4
3 1 1 1
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


@pytest.fixture
def sample_ts_path(tmp_path: Path) -> Path:
    ts_path = tmp_path / "ts.txt"
    ts_path.write_text(SAMPLE_TS_FILE)
    return ts_path


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


def test_should_read_ts_file(sample_ts_path):
    ts_df = FlowbasedFileReader.read_ts_file(sample_ts_path)

    assert list(ts_df.columns) == [0, 1, 2]

    assert ts_df[0].tolist() == [3, 4, 1]
    assert ts_df[1].tolist() == [3, 4, 1]
    assert ts_df[2].tolist() == [3, 4, 1]


def test_should_raise_flowbased_generation_error_when_ts_file_is_missing():
    with pytest.raises(FlowbasedGenerationError):
        FlowbasedFileReader.read_ts_file(Path("/nonexistent/ts.txt"))
