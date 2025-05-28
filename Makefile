APPNAME=$(shell grep -m 1 name pyproject.toml|cut -f2 -d'"')
TARGETS=build clean create-env dependencies deploy editable_install install test uninstall
VERSION=$(shell grep version pyproject.toml|cut -f2 -d'"')
ENV_NAME=${APPNAME}
CONDA_ACTIVATE=source $$(conda info --base)/etc/profile.d/conda.sh ; conda activate ; conda activate

.PHONY: ${TARGETS}

all:
	@echo "Try one of: ${TARGETS}"

build_conda_lock_files:
	conda-lock -k explicit -f base_environment.yml --conda mamba -p 'linux-64' -p 'osx-arm64'

build: clean dependencies
	@if [ -z "${CONDA_DEFAULT_ENV}" ] || [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	poetry build

clean:
	find . -name '*.pyc' -delete
	find . -type d -name '__pycache__' -exec rm -rf {} +
	rm -rf dist build

# Target to create the conda environment if it doesn't exist
create-env:
	@if conda env list | grep "^${ENV_NAME} " >/dev/null 2>&1; then \
 		echo "Environment ${ENV_NAME} already exists."; \
 	else \
 		conda env create --file base_environment.yml; \
 	fi

dependencies:
	@if [ -z "${CONDA_DEFAULT_ENV}" ] || [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	poetry install --without dev --no-root

dependencies_dev:
	@if [ -z "${CONDA_DEFAULT_ENV}" ] || [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	poetry install --only dev --no-root

deploy:
	@if [ -z "${CONDA_DEFAULT_ENV}" ] || [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	poetry install

editable_install: build
	@if [ -z "${CONDA_DEFAULT_ENV}" ] || [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	pip install --editable .

install: build
	@if [ -z "${CONDA_DEFAULT_ENV}" ] || [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	pip install dist/*.whl

pre-commit: dependencies_dev
	@if [ -z "${CONDA_DEFAULT_ENV}" ] || [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	if [ ! -f .git/hooks/pre-commit ]; then pre-commit install; fi; \
	pre-commit run --all-files

tag:
	git tag v${VERSION}

test:
	@echo "Testing"
	@if [ -z "${CONDA_DEFAULT_ENV}" ] || [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	pytest --cov=src/gwasstudio/ tests

uninstall:
	@if [ -z "${CONDA_DEFAULT_ENV}" ] || [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	pip uninstall -y ${APPNAME}
