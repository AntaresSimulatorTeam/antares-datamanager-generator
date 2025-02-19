import pandas as pd
from pathlib import Path

from antares.craft.model.area import AreaUi

from antares import craft
from antares.craft.api_conf.api_conf import APIconf

from antares.utils.areaUi import generate_random_coordinate, generate_random_color

base_dir = Path(__file__).resolve().parent.parent.parent.parent  # Two levels up (from src to project root)

nas_path = base_dir / "mnt" /"nas"/ "generatorJsons" / "1.json"

print(f"Checking path: {nas_path}")


if nas_path.exists():
    print("✅ File exists")
else:
    print("❌ File not found")

import json

with open(nas_path, "r", encoding="utf-8") as file:
    data = json.load(file)

study_name = list(data.keys())[0]

areas_dict = data.get(study_name, {}).get('areas', {})


areas_df = pd.DataFrame.from_dict(areas_dict, orient="index")

areas_codes = areas_df.index.tolist()


links_dict = data.get(study_name, {}).get('links', {})

link_keys = list(links_dict.keys())
print(link_keys)

token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ7XCJpZFwiOiAxMTIsIFwidHlwZVwiOiBcImJvdHNcIiwgXCJpbXBlcnNvbmF0b3JcIjogMiwgXCJncm91cHNcIjogW3tcImlkXCI6IFwiNDE5OTgzMWYtMGU5Ny00MjkwLTkxYjItZjlhY2Y5ZWY3MzM0XCIsIFwibmFtZVwiOiBcIkZvcm1hdGlvblwiLCBcInJvbGVcIjogMzB9LCB7XCJpZFwiOiBcInRlc3RcIiwgXCJuYW1lXCI6IFwidGVzdFwiLCBcInJvbGVcIjogNDB9XX0iLCJpYXQiOjE3MTIzMjYwODUsIm5iZiI6MTcxMjMyNjA4NSwianRpIjoiMWZkZmM5ODktMGIwMy00Yjk3LWFlZDEtODgwMjkyMzU4NDliIiwiZXhwIjo4MDcxMzY2MDg1LCJ0eXBlIjoiYWNjZXNzIiwiZnJlc2giOmZhbHNlfQ.S9Snc1QRfWqQ0kxqcHk_vys75T_pkQYpdgsfDmYIwQU"
host="https://antares-web-recette.rte-france.com"
api_config = APIconf(host,token,False)
study=craft.create_study_api(study_name,"8.8", api_config)


for area in areas_codes:

    x, y = generate_random_coordinate()
    color_rgb = generate_random_color()
    area_ui = AreaUi(x=x, y=y, color_rgb=color_rgb)
    try:
        study.create_area(area_name=area,ui=area_ui)
        print(f"Successfully created area for {area}")
    except Exception as e:
        print(f"Failed to create area for {area}: {str(e)}")



for key in link_keys:
    area_from, area_to = key.split('/')
    study.create_link(area_from=area_from, area_to=area_to)
    print(f"Called create_link for: {area_from} and {area_to}")