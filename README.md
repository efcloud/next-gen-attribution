## Next Generation Attribution

<hr>

*The NGA team's attribution codebase.*

<br>

[![Project Status: Active](https://www.repostatus.org/badges/latest/active.svg)](https://www.repostatus.org/#active)

`next-gen-attribution` is a package developed by the NGA team in close collaboration with EF Digital and Tours that contains:

- Preprocessor classes
- Attribution classes
- Workflow scripts
- Visualization

© Education First 2022

## Documentation

The official documentation is hosted on this repository's [wiki](), along with a long-term roadmap. A short-term todo list for this project is posted on our [kanban board](https://trello.com/b/r3LKElQD/tours-working-group).


## Installation
Set up a Python 3.11.0 virtual environment, then make the following local invocations from the terminal:

```
pip install -e .

pre-commit install

pre-commit autoupdate
```

## Unit tests

After installation, make the following local invocation from the terminal:
```
pytest
```

## Quick Start

```
python workflows/attribution/data/preprocessing.py --businessUnit=tours --workflowMode=dev
```
