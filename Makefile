APPNAME=$(shell grep -m 1 name pyproject.toml|cut -f2 -d'"')
TARGETS=build clean create-env dependencies deploy editable_install install test uninstall
VERSION=$(shell grep -m 1 version pyproject.toml|cut -f2 -d'"')
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

bump-version:
	git-cliff --bumped-version > version.txt
	python bump-version.py
	git-cliff --bump > docs/changelog.md

clean:
	find . -name '*.pyc' -delete
	find . -type d -name '__pycache__' -exec rm -rf {} +
	rm -rf dist build site

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

dependencies_extras:
	@if [ -z "${CONDA_DEFAULT_ENV}" ] || [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	poetry install --without dev --no-root --extras plot

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

unit_test:
	@echo "Unit Testing"
	@if [ -z "${CONDA_DEFAULT_ENV}" ] || [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	pytest --cov=src/gwasstudio/ tests

functional_test_00:
	@echo "Functional test 00"
	@if [ -z "${CONDA_DEFAULT_ENV}" ] || [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	cd scripts && ./test_00.sh

functional_test_01:
	@echo "Functional test 01"
	@if [ -z "${CONDA_DEFAULT_ENV}" ] || [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	cd scripts && ./test_01.sh

functional_test_02:
	@echo "Functional test 02"
	@if [ -z "${CONDA_DEFAULT_ENV}" ] || [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	cd scripts && ./test_02.sh

test: unit_test functional_test_00
	@echo "End-to-End tests"

test-integration: test-integration-setup test-integration-exec #test-integration-stop## Run integration tests

test-integration-setup: ## Start Docker services for integration tests
	docker compose -f dev/docker-compose-integration.yml up -d
	sleep 5

test-integration-exec: ## Run integration tests (excluding provision)
	@echo "Integration test 03"
	@if [ -z "${CONDA_DEFAULT_ENV}" ] || [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	cd scripts && ./test_03.sh

test-integration-stop:
	docker compose -f dev/docker-compose-integration.yml rm -f -s -v minio
	docker compose -f dev/docker-compose-integration.yml rm -f -s -v mongodb
	docker compose -f dev/docker-compose-integration.yml rm -f -s -v vault
	docker compose -f dev/docker-compose-integration.yml rm -f -s -v vault-init
	docker network rm -f internal_net
	docker volume rm dev_minio_data dev_mongodb_data dev_vault_data



uninstall:
	@if [ -z "${CONDA_DEFAULT_ENV}" ] || [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	pip uninstall -y ${APPNAME}
