APPNAME=$(shell grep -m 1 name pyproject.toml|cut -f2 -d'"')
TARGETS=build clean create-envdependencies deploy editable_install install test uninstall
VERSION=$(shell grep version pyproject.toml|cut -f2 -d'"')
ENV_NAME=${APPNAME}
CONDA_ACTIVATE=source $$(conda info --base)/etc/profile.d/conda.sh ; conda activate ; conda activate

.PHONY: ${TARGETS}

all:
	@echo "Try one of: ${TARGETS}"

build_conda_lock_files:
	conda-lock -k explicit -f base_environment.yml --conda mamba -p 'linux-64' -p 'osx-arm64'

build: clean dependencies
	@[ -z "${CONDA_DEFAULT_ENV}" ] && $(CONDA_ACTIVATE) ${ENV_NAME} || true; \
	poetry build

clean:
	find . -name '*.pyc' -delete
	find . -type d -name '__pycache__' -exec rm -rf {} +
	rm -rf dist build

# Target to create the conda environment if it doesn't exist
create-env:
	conda env list | grep "^${ENV_NAME} " >/dev/null 2>&1 || conda env create -f base_environment.yml

dependencies:
	@[ -z "${CONDA_DEFAULT_ENV}" ] && $(CONDA_ACTIVATE) ${ENV_NAME} || true; \
	poetry install --without dev --no-root

dependencies_dev:
	@[ -z "${CONDA_DEFAULT_ENV}" ] && $(CONDA_ACTIVATE) ${ENV_NAME} || true; \
	poetry install --only dev --no-root

deploy:
	@[ -z "${CONDA_DEFAULT_ENV}" ] && $(CONDA_ACTIVATE) ${ENV_NAME} || true; \
	poetry install

editable_install: build
	@[ -z "${CONDA_DEFAULT_ENV}" ] && $(CONDA_ACTIVATE) ${ENV_NAME} || true; \
	pip install --editable .

install: build
	@[ -z "${CONDA_DEFAULT_ENV}" ] && $(CONDA_ACTIVATE) ${ENV_NAME} || true; \
	pip install dist/*.whl

pre-commit: dependencies_dev
	@[ -z "${CONDA_DEFAULT_ENV}" ] && $(CONDA_ACTIVATE) ${ENV_NAME} || true; \
	if [ ! -f .git/hooks/pre-commit ]; then pre-commit install; fi; \
	pre-commit run --all-files

tag:
	git tag v${VERSION}

test:
	@echo "Testing"
	@[ -z "${CONDA_DEFAULT_ENV}" ] && $(CONDA_ACTIVATE) ${ENV_NAME} || true; \
	pytest --cov=src/gwasstudio/ tests

uninstall:
	@[ -z "${CONDA_DEFAULT_ENV}" ] && $(CONDA_ACTIVATE) ${ENV_NAME} || true; \
	pip uninstall -y ${APPNAME}
