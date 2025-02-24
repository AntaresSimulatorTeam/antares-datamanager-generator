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

import pandas as pd

from antares import craft
from antares.craft import APIconf
from antares.craft.model.area import AreaUi

from datamanager.APIGeneratorConfig.config import APIGeneratorConfig

from datamanager.env_variables import EnvVariableType
from datamanager.exceptions.exceptions import APIGenerationError, AreaGenerationError, LinkGenerationError
from datamanager.utils.areaUi import generate_random_color, generate_random_coordinate


def generate_study(study_id: str) -> dict[str, str]:
    env_vars = EnvVariableType()
    path_to_json = Path(env_vars.get_env_variable("NAS_PATH"))
    nas_path = path_to_json / f"{study_id}.json"

    with open(nas_path, "r", encoding="utf-8") as file:
        data = json.load(file)

    study_name = list(data.keys())[0]

    areas_dict = data.get(study_name, {}).get("areas", {})

    areas_df = pd.DataFrame.from_dict(areas_dict, orient="index")

    areas_codes = areas_df.index.tolist()

    links_dict = data.get(study_name, {}).get("links", {})

    link_keys = list(links_dict.keys())

    generator_config = APIGeneratorConfig()
    api_config = APIconf(generator_config.host, generator_config.token, generator_config.verify_ssl)
    study = craft.create_study_api(study_name, "8.8", api_config)

    for area in areas_codes:
        x, y = generate_random_coordinate()
        color_rgb = generate_random_color()
        area_ui = AreaUi(x=x, y=y, color_rgb=color_rgb)
        try:
            study.create_area(area_name=area, ui=area_ui)
            print(f"Successfully created area for {area}")
        except APIGenerationError as e:
            raise AreaGenerationError(area, e.message) from e

    for key in link_keys:
        area_from, area_to = key.split("/")

        try:
            study.create_link(area_from=area_from, area_to=area_to)
            print(f"Called create_link for: {area_from} and {area_to}")
        except APIGenerationError as e:
            raise LinkGenerationError(area_from, area_to, f"Link from {area_from} to {area_to} not created") from e

    return {"message": f"Study {study_name} successfully generated"}
