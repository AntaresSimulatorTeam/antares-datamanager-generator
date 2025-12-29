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

import json

from pathlib import Path
from typing import Any

import pandas as pd

from antares.craft import ThermalClusterProperties
from antares.craft.api_conf.api_conf import APIconf
from antares.craft.model.area import AreaUi
from antares.craft.model.study import Study, create_study_api
from antares.datamanager.env_variables import EnvVariableType
from antares.datamanager.exceptions.exceptions import APIGenerationError, AreaGenerationError, LinkGenerationError
from antares.datamanager.generator.study_adapters import StudyFactory
from antares.datamanager.generator.generate_link_capacity_data import generate_link_capacity_df
from antares.datamanager.generator.generate_thermal_matrices_data import (
    create_modulation_matrix,
    create_prepro_data_matrix,
)
from antares.datamanager.utils.areaUi import generate_random_color, generate_random_coordinate
from antares.datamanager.utils.resolve_directory import resolve_directory


def generate_study(study_id: str, factory: StudyFactory) -> dict[str, str]:
    study_name, areas, links, area_loads, area_thermals, random_gen_settings = read_study_data_from_json(study_id)
    study = factory.create_study(study_name) # can specify version

    add_areas_to_study(study, areas, area_loads, area_thermals)
    add_links_to_study(study, links)
    if area_thermals and random_gen_settings[0] is True:
        print(f"Generating timeseries for {random_gen_settings[1]} years")
        study.generate_thermal_timeseries(random_gen_settings[1])

    return {
        "message": f"Study {study_name} successfully generated",
        "study_id": study.service.study_id,
        "study_path": str(study.path) if study.path else ""
    }


def read_study_data_from_json(
    study_id: str,
) -> tuple[str, list[str], dict[str, dict[str, int]], dict[str, list[str]], dict[str, Any], tuple[bool, int]]:
    json_dir = resolve_directory("PEGASE_STUDY_JSON_OUTPUT_DIRECTORY")
    joined_path = json_dir / f"{study_id}.json"

    print(f"Path to JSON with data for generation : {joined_path}")

    try:
        with open(joined_path, "r", encoding="utf-8") as file:
            data = json.load(file)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"File does not exist: {joined_path}") from e

    study_name = list(data.keys())[0]
    study_data = data.get(study_name, {})
    areas_dict = study_data.get("areas", {})
    links_dict = study_data.get("links", {})
    random_gen_settings = study_data.get("enable_random_ts", True), study_data.get("nb_years", 1)

    area_names = list(areas_dict.keys())
    area_loads = {}
    area_thermals = {}
    for area, area_data in areas_dict.items():
        # Loads
        loads = area_data.get("loads", [])

        if isinstance(loads, list):
            area_loads[area] = loads
        else:
            area_loads[area] = []

        # Thermals
        thermals_dict = area_data.get("thermals", {})
        if thermals_dict:
            area_thermals[area] = thermals_dict

    return study_name, area_names, links_dict, area_loads, area_thermals, random_gen_settings


def generator_load_directory() -> Path:
    env_vars = EnvVariableType()
    path_to_nas = env_vars.get_env_variable("NAS_PATH")
    path_to_load_directory = env_vars.get_env_variable("PEGASE_LOAD_OUTPUT_DIRECTORY")
    return Path(path_to_nas) / Path(path_to_load_directory)

def add_areas_to_study(
    study: Study, areas: list[str], area_loads: dict[str, list[str]], area_thermals: dict[str, Any]
) -> None:
    path_to_load_directory = generator_load_directory()
    print(areas)
    for area in areas:
        x, y = generate_random_coordinate()
        color_rgb = generate_random_color()
        area_ui = AreaUi(x=x, y=y, color_rgb=color_rgb)
        loads = area_loads.get(area, [])
        thermals = area_thermals.get(area, {})

        try:
            area_obj = study.create_area(area_name=area, ui=area_ui)
            for load_file in loads:
                load_path = Path(path_to_load_directory) / load_file
                df = pd.read_feather(load_path)
                area_obj.set_load(df)

            # Thermals
            for cluster_name, values in thermals.items():
                print(f"Creating thermal cluster: {cluster_name}")
                cluster_properties = ThermalClusterProperties(**values.get("properties", {}))
                # If cluster_properties doesn't expose attributes (e.g., patched as dict in tests),
                if not hasattr(cluster_properties, "unit_count"):
                    area_obj.create_thermal_cluster(cluster_name, cluster_properties)
                    continue

                cluster_data = values.get("data", {})
                unit_count = cluster_properties.unit_count
                prepro_matrix = create_prepro_data_matrix(cluster_data, unit_count)

                cluster_modulation = values.get("modulation", {})
                modulation_matrix = create_modulation_matrix(cluster_modulation)

                thermal_cluster = area_obj.create_thermal_cluster(cluster_name, cluster_properties)
                thermal_cluster.set_prepro_data(prepro_matrix)
                thermal_cluster.set_prepro_modulation(modulation_matrix)

            print(f"Successfully created area for {area}")
        except APIGenerationError as e:
            raise AreaGenerationError(area, e.message) from e


def add_links_to_study(study: Study, links: dict[str, dict[str, int]]) -> None:
    for key, link_data in links.items():
        area_from, area_to = key.split("/")
        df_capacity_direct = generate_link_capacity_df(link_data, "direct")
        df_capacity_indirect = generate_link_capacity_df(link_data, "indirect")

        try:
            link = study.create_link(area_from=area_from, area_to=area_to)
            link.set_capacity_direct(df_capacity_direct)
            link.set_capacity_indirect(df_capacity_indirect)
            print(f"Called create_link for: {area_from} and {area_to}")
        except APIGenerationError as e:
            raise LinkGenerationError(area_from, area_to, f"Link from {area_from} to {area_to} not created") from e
