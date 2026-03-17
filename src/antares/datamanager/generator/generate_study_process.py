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
import os
import shutil

from pathlib import Path

import pandas as pd

from antares.craft import (
    APIconf,
    BindingConstraintFrequency,
    BindingConstraintOperator,
    BindingConstraintProperties,
    ClusterData,
    ConstraintTerm,
    GeneralParametersUpdate,
    LinkPropertiesUpdate,
    Month,
    StudySettingsUpdate,
)
from antares.craft.model.area import AreaProperties, AreaUi
from antares.craft.model.study import Study, import_study_api
from antares.datamanager.core.settings import GenerationMode, settings
from antares.datamanager.exceptions.exceptions import APIGenerationError, AreaGenerationError, LinkGenerationError
from antares.datamanager.generator.generate_dsr_clusters import generate_dsr_clusters
from antares.datamanager.generator.generate_link_matrices import generate_link_capacity_df, generate_link_parameters_df
from antares.datamanager.generator.generate_sts_clusters import generate_sts_clusters
from antares.datamanager.generator.generate_thermal_clusters import generate_thermal_clusters
from antares.datamanager.generator.study_adapters import StudyFactory
from antares.datamanager.logs.logging_setup import configure_ecs_logger, get_logger
from antares.datamanager.models.study_data_json_model import StudyData
from antares.datamanager.utils.area_ui_utils import generate_random_color, generate_random_coordinate

# Configurer le logger au démarrage du module (ou appeler configure_ecs_logger() dans le main)
configure_ecs_logger()
logger = get_logger(__name__)


def generate_study(study_id: str, factory: StudyFactory) -> dict[str, str]:
    study_data = read_study_data_from_json(study_id)
    study = factory.create_study(study_data.name)
    study_settings = StudySettingsUpdate(
        general_parameters=GeneralParametersUpdate(
            first_month_in_year=study_data.first_month, nb_years=study_data.nb_years
        )
    )
    study.update_settings(study_settings)

    add_areas_to_study(study, study_data)
    add_links_to_study(study, study_data.links)
    if study_data.area_thermals and study_data.enable_random_ts:
        logger.info(f"Generating timeseries for {study_data.nb_years} years")
        study.generate_thermal_timeseries(settings.nb_years)

    if settings.generation_mode == GenerationMode.LOCAL:
        _package_and_upload_local_study(study_data.name)

    return {
        "message": f"Study {study_data.name} successfully generated",
        "study_id": study_id,
        "study_path": str(study.path) if study.path else "",
    }


def read_study_data_from_json(study_id: str) -> StudyData:
    json_dir = settings.study_json_directory
    joined_path = json_dir / f"{study_id}.json"

    logger.info(f"Path to JSON with data for generation : {joined_path}")

    try:
        with open(joined_path, "r", encoding="utf-8") as file:
            data = json.load(file)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"File does not exist: {joined_path}") from e

    study_name = list(data.keys())[0]
    raw_study_data = data.get(study_name, {})

    first_month_val = raw_study_data.get("first_month")
    if first_month_val:
        first_month = Month(first_month_val)
    else:
        first_month = settings.study_setting_first_month

    study_data = StudyData(
        name=study_name,
        areas=raw_study_data.get("areas", {}),
        links=raw_study_data.get("links", {}),
        enable_random_ts=raw_study_data.get("enable_random_ts", True),
        nb_years=raw_study_data.get("nb_years", settings.nb_years),
        first_month=first_month,
    )

    for area, area_info in study_data.areas.items():
        # Loads
        loads = area_info.get("loads", [])
        study_data.area_loads[area] = loads if isinstance(loads, list) else []

        # Thermals
        thermals = area_info.get("thermals", {})
        if thermals:
            study_data.area_thermals[area] = thermals

        # STS
        sts = area_info.get("sts", {})
        if sts:
            study_data.area_sts[area] = sts

        # DSR
        sts = area_info.get("dsr", {})
        if sts:
            study_data.area_dsr[area] = sts

    return study_data


def generator_load_directory() -> Path:
    return settings.load_output_directory


def add_areas_to_study(study: Study, study_data: StudyData) -> None:
    path_to_load_directory = generator_load_directory()
    logger.info(list(study_data.areas.keys()))
    for area_name, area_def in study_data.areas.items():
        # UI from JSON if provided, otherwise random
        ui_json = area_def.get("ui") if isinstance(area_def, dict) else None
        area_ui = None
        if isinstance(ui_json, dict):
            try:
                area_ui = AreaUi(**ui_json)
            except Exception:
                area_ui = None
        if area_ui is None:
            x, y = generate_random_coordinate()
            color_rgb = generate_random_color()
            area_ui = AreaUi(x=x, y=y, color_rgb=color_rgb)
        properties_json = area_def.get("properties") if isinstance(area_def, dict) else None
        area_properties = None
        if isinstance(properties_json, dict):
            has_ecu = "energy_cost_unsupplied" in properties_json
            has_ecs = "energy_cost_spilled" in properties_json

            if has_ecu or has_ecs:
                area_properties = AreaProperties(
                    energy_cost_unsupplied=properties_json.get("energy_cost_unsupplied", 0.0),
                    energy_cost_spilled=properties_json.get("energy_cost_spilled", 0.0),
                )

        loads = study_data.area_loads.get(area_name, [])
        thermals = study_data.area_thermals.get(area_name, {})
        sts = study_data.area_sts.get(area_name, {})
        dsr = study_data.area_dsr.get(area_name, {})

        try:
            area_obj = study.create_area(area_name=area_name, properties=area_properties, ui=area_ui)
            for load_file in loads:
                load_path = Path(path_to_load_directory) / load_file
                df = pd.read_feather(load_path)
                area_obj.set_load(df)

            generate_thermal_clusters(area_obj, thermals, first_month=study_data.first_month)
            generate_sts_clusters(area_obj, sts)
            df_dsr_constraints = generate_dsr_clusters(area_obj, dsr, first_month=study_data.first_month)
            if not df_dsr_constraints.empty:
                logger.info(f"DSR constraints generated for {area_name}: {df_dsr_constraints.columns.tolist()}")
                for column in df_dsr_constraints.columns:
                    if column.startswith("FR_"):
                        # FR Case
                        bc_name = f"{column}_stock"
                        cluster_name = column
                        area_id = "fr"
                    else:
                        # Non-FR Case
                        # Column name is expected to be {area_name}_DSR
                        actual_area_name = column.split("_")[0]
                        bc_name = f"DSR_{actual_area_name}_stock"
                        cluster_name = f"{actual_area_name.lower()}_dsr 0"
                        area_id = actual_area_name.lower()

                    properties = BindingConstraintProperties(
                        enabled=True,
                        time_step=BindingConstraintFrequency.DAILY,
                        operator=BindingConstraintOperator.LESS,
                    )
                    terms = [
                        ConstraintTerm(
                            data=ClusterData(area=area_id, cluster=cluster_name),
                            weight=1,
                            offset=0,
                        )
                    ]

                    # The matrix should be a single column DataFrame for the binding constraint
                    less_term_matrix = df_dsr_constraints[[column]]
                    logger.debug(f"Generated less term matrix for {bc_name}: {less_term_matrix.shape}")

                    study.create_binding_constraint(
                        name=bc_name,
                        properties=properties,
                        terms=terms,
                        less_term_matrix=less_term_matrix,
                    )

                    logger.info(f"Created binding constraint {bc_name} for area {area_name}")

            logger.info(f"Successfully created area for {area_name}")
        except APIGenerationError as e:
            raise AreaGenerationError(area_name, e.message) from e


def add_links_to_study(study: Study, links: dict[str, dict[str, int]]) -> None:
    for key, link_data in links.items():
        area_from, area_to = key.split("/")
        df_capacity_direct = generate_link_capacity_df(link_data, "direct")
        df_capacity_indirect = generate_link_capacity_df(link_data, "indirect")

        try:
            link = study.create_link(area_from=area_from, area_to=area_to)
            link.set_capacity_direct(df_capacity_direct)
            link.set_capacity_indirect(df_capacity_indirect)
            if link_data["hurdleCost"] is not None:
                df_parameters = generate_link_parameters_df(link_data["hurdleCost"])
                link.update_properties(LinkPropertiesUpdate(hurdles_cost=True))
                link.set_parameters(df_parameters)
            logger.info(f"Called create_link for: {area_from} and {area_to}")
        except APIGenerationError as e:
            raise LinkGenerationError(area_from, area_to, f"Link from {area_from} to {area_to} not created") from e


def _package_and_upload_local_study(study_id_name: str) -> None:
    try:
        logger.info("Starting compression and upload of local study...")

        study_path = settings.nas_path / study_id_name
        if not study_path.exists():
            logger.info(f"Study directory not found at {study_path}")
            return

        # archive
        zip_base_name = str(study_path)
        archive_path = shutil.make_archive(zip_base_name, "zip", root_dir=study_path)
        logger.info(f"Study compressed to: {archive_path}")

        api_conf = APIconf(api_host=settings.api_host, token=settings.api_token, verify=settings.verify_ssl)
        # upload
        import_study_api(api_conf, Path(archive_path))
        logger.info("Study uploaded to Antares Web.")

        os.remove(archive_path)
        shutil.rmtree(study_path)
    except Exception as e:
        raise APIGenerationError(f"Failed to archive or upload local study {study_id_name}: {str(e)}") from e
