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

import uvicorn

from antares.generator.readJsonForGeneration import generate_study
from fastapi import FastAPI

app = FastAPI(
    title="antares-datamanager-generator", description="API to launch antares study generation", version="0.0.1"
)


@app.post("/generate_study/")
def create_study(study_id: str) -> dict[str, str]:
    return generate_study(study_id)


if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8094, reload=True)
