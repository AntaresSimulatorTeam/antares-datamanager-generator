# Antares Datamanager Generator

[![License: MPL 2.0](https://img.shields.io/badge/License-MPL%202.0-brightgreen.svg)](https://opensource.org/licenses/MPL-2.0)

A FastAPI-based web service for generating Antares studies with areas, links, and load data.

## Overview

Antares Datamanager Generator is a tool designed to automate the creation of Antares studies. It provides a simple API endpoint that takes a study ID, loads the corresponding configuration from a JSON file, and generates a complete study with:

- Areas with random coordinates and colors
- Links between areas with capacity data
- Load data for areas from feather files

The generator is designed to work with the Antares simulation platform and simplifies the process of creating new studies for simulation.

## Installation

### Requirements

- Python 3.10 or higher
- Access to an Antares API instance

### Basic Installation

```bash
pip install antares-datamanager-generator
```

### Development Installation

For development purposes, install the development requirements:

```bash
pip install -r requirements-dev.txt
```

## Configuration

The generator requires the following environment variables:

- `NAS_PATH`: Path to the NAS storage
- `PEGASE_LOAD_OUTPUT_DIRECTORY`: Directory containing load data files

## Usage

### Generating a Study

To generate a study, make a POST request to the `/generate_study/` endpoint with a study ID:

```bash
curl -X POST "http://localhost:8094/generate_study/?study_id=my_study_id"
```

The study ID should correspond to a JSON file in the configured load directory with the following structure:

```json
{
  "study_name": {
    "areas": {
      "area1": {
        "loads": ["load_file1.feather", "load_file2.feather"]
      },
      "area2": {
        "loads": []
      }
    },
    "links": {
      "area1/area2": {
        "direct": 1000,
        "indirect": 1000
      }
    }
  }
}
```

## Features

- FastAPI-based web service
- Automatic generation of areas with random coordinates and colors
- Creation of links between areas with configurable capacities
- Loading of area load data from feather files
- Integration with Antares API for study creation

## Development

### Linting and Formatting

To reformat your code, use this command line:

```bash
ruff check src/ tests/ --fix && ruff format src/ tests/
```

### Type Checking

To typecheck your code, use this command line:

```bash
mypy
```

### Testing with Tox

To use [tox](https://tox.wiki/) to run unit tests in multiple python versions at the same time as linting and formatting with ruff and typing with mypy:

1. As the dev requirements include [uv](https://docs.astral.sh/uv/) and `tox-uv` there is no need to install python versions, `uv` will do this for you.
2. Use `tox -p` to run the environments in parallel to save time, this will create virtual environment with the necessary python versions the first time you run tox.

## License

This project is licensed under the Mozilla Public License 2.0 - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Authors

See [AUTHORS.txt](AUTHORS.txt) for a list of contributors.
