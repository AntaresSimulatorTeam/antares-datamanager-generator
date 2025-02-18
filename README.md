# antares-datamanager-generator

### install dev requirements

Install dev requirements with `pip install -r requirements-dev.txt`

### linting and formatting

To reformat your code, use this command line: `ruff check src/ tests/ --fix && ruff format src/ tests/`

### typechecking

To typecheck your code, use this command line: `mypy`

### tox
To use [tox](https://tox.wiki/) to run unit tests in multiple python versions at the same time as linting and formatting
with ruff and typing with mypy:  
1) As the dev requirements include [uv](https://docs.astral.sh/uv/) and `tox-uv` there is no need to install python 
versions, `uv` will do this for you.  
2) Use `tox -p` to run the environments in parallel to save time, this will create virtual environment with the 
necessary python versions the first time you run tox.