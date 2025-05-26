import json

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
    study_name, areas, links = load_study_data(study_id)
    study = create_study(study_name)

    add_areas_to_study(study, areas)
    add_links_to_study(study, links)

    return {"message": f"Study {study_name} successfully generated"}


def load_study_data(study_id: str) -> tuple[str, list[str], Dict[str, Dict[str, int]]]:
    env_vars = EnvVariableType()
    path_to_nas_directory = Path(env_vars.get_env_variable("NAS_PATH"))
    path_to_json_directory = Path(env_vars.get_env_variable("PEGASE_LOAD_OUTPUT_DIRECTORY"))
    json_path = path_to_nas_directory / path_to_json_directory / f"{study_id}.json"

    with open(json_path, "r", encoding="utf-8") as file:
        data = json.load(file)

    study_name = list(data.keys())[0]
    areas_dict = data.get(study_name, {}).get("areas", {})
    links_dict = data.get(study_name, {}).get("links", {})

    area_names = list(areas_dict.keys())

    return study_name, area_names, links_dict


def create_study(study_name: str) -> Study:
    generator_config = APIGeneratorConfig()
    api_config = APIconf(generator_config.host, generator_config.token, generator_config.verify_ssl)
    return create_study_api(study_name, "8.8", api_config)


def add_areas_to_study(study: Study, areas: list[str]) -> None:
    for area in areas:
        x, y = generate_random_coordinate()
        color_rgb = generate_random_color()
        area_ui = AreaUi(x=x, y=y, color_rgb=color_rgb)

        try:
            study.create_area(area_name=area, ui=area_ui)
            print(f"Successfully created area for {area}")
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
