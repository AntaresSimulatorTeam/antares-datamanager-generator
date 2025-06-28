import json

import pandas as pd
from pathlib import Path
from typing import Dict

from antares.craft.api_conf.api_conf import APIconf
from antares.craft.model.area import AreaUi
from antares.craft.model.study import Study, create_study_api
from antares.datamanager.APIGeneratorConfig.config import APIGeneratorConfig
from antares.datamanager.env_variables import EnvVariableType
from antares.datamanager.exceptions.exceptions import APIGenerationError, AreaGenerationError, LinkGenerationError
from antares.datamanager.generator.generate_link_capacity_data import generate_link_capacity_df
from antares.datamanager.utils.areaUi import generate_random_color, generate_random_coordinate


def generate_study(study_id: str) -> dict[str, str]:
    study_name, areas, links, area_loads = load_study_data(study_id)
    study = create_study(study_name)

    add_areas_to_study(study, areas, area_loads)
    add_links_to_study(study, links)

    return {"message": f"Study {study_name} successfully generated"}


def load_study_data(study_id: str) -> tuple[str, list[str], Dict[str, Dict[str, int]], Dict[str, list[str]]]:
    path_to_load_directory = generator_load_directory()
    joined_path = Path(path_to_load_directory) / f"{study_id}.json"
    print(f"Chemin du fichier JSON utilisé : {joined_path}")

    with open(joined_path, "r", encoding="utf-8") as file:
        data = json.load(file)

    study_name = list(data.keys())[0]
    areas_dict = data.get(study_name, {}).get("areas", {})
    links_dict = data.get(study_name, {}).get("links", {})

    area_names = list(areas_dict.keys())
    area_loads = {}
    for area, area_data in areas_dict.items():
        loads = area_data.get("loads", [])
        # Si "loads" est une chaîne (ex: "No LOAD files for this area"), on retourne une liste vide
        if isinstance(loads, list):
            area_loads[area] = loads
        else:
            area_loads[area] = []

    return study_name, area_names, links_dict, area_loads


def generator_load_directory():
    env_vars = EnvVariableType()
    path_to_nas = env_vars.get_env_variable("NAS_PATH")
    path_to_load_directory = env_vars.get_env_variable("PEGASE_LOAD_OUTPUT_DIRECTORY")
    return Path(path_to_nas) / Path(path_to_load_directory)


def create_study(study_name: str) -> Study:
    generator_config = APIGeneratorConfig()
    api_config = APIconf(generator_config.host, generator_config.token, generator_config.verify_ssl)
    return create_study_api(study_name, "8.8", api_config)


def add_areas_to_study(study: Study, areas: list[str], area_loads: dict[str, list[str]]) -> None:
    path_to_load_directory = generator_load_directory()
    for area in areas:
        x, y = generate_random_coordinate()
        color_rgb = generate_random_color()
        area_ui = AreaUi(x=x, y=y, color_rgb=color_rgb)
        loads = area_loads.get(area, [])

        try:
            area_obj = study.create_area(area_name=area, ui=area_ui)
            for load_file in loads:
                load_path = Path(path_to_load_directory) / load_file
                df = pd.read_feather(load_path)
                area_obj.set_load(df)
            print(f"Successfully created area for {area} with loads: {loads}")
        except APIGenerationError as e:
            raise AreaGenerationError(area, e.message) from e


def add_links_to_study(study: Study, links: Dict[str, Dict[str, int]]) -> None:
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
