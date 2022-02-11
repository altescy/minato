PWD        := $(shell pwd)
MODULE     := minato
POETRY     := poetry
PYTHON     := $(POETRY) run python
PYTEST     := $(POETRY) run pytest
PYSEN      := $(POETRY) run pysen
PYTHONPATH := $(PWD)

.PHONY: all
all: format lint test

.PHONY: test
test:
	PYTHONPATH=$(PYTHONPATH) $(PYTEST)

.PHONY: lint
lint:
	PYTHONPATH=$(PYTHONPATH) $(PYSEN) run lint

.PHONY: format
format:
	PYTHONPATH=$(PYTHONPATH) $(PYSEN) run format

.PHONY: clean
clean: clean-pyc clean-build

.PHONY: clean-pyc
clean-pyc:
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

.PHONY: clean-build
clean-build:
	rm -rf build/
	rm -rf dist/
	rm -rf $(MODULE).egg-info/
	rm -rf pip-wheel-metadata/
