APPNAME=$(shell grep -m 1 name pyproject.toml|cut -f2 -d'"')
TARGETS=build clean create-env create-env-versioned dependencies deploy editable_install install test uninstall test-dependencies
VERSION=$(shell grep -m 1 version pyproject.toml|cut -f2 -d'"')
ENV_NAME=${APPNAME}
ENV_PATH=/exchange/healthds/software/envs/${APPNAME}_${VERSION}
CONDA_ACTIVATE=source $$(conda info --base)/etc/profile.d/conda.sh ; conda activate ; conda activate

.PHONY: ${TARGETS}

# Target to install MongoDB dependencies for testing
test-dependencies:
	@if [ "${CONDA_PREFIX}" != "${ENV_PATH}" ] && [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	if ! command -v mongod &> /dev/null; then \
	    echo "Installing MongoDB dependencies for testing..."; \
	    mamba install -y mongodb mongo-tools; \
	else \
	    echo "MongoDB dependencies already installed"; \
	fi

all:
	@echo "Try one of: ${TARGETS}"

build_conda_lock_files:
	conda-lock -k explicit -f base_environment.yml --conda mamba -p 'linux-64' -p 'osx-arm64'

build: clean dependencies
	@if [ "${CONDA_PREFIX}" != "${ENV_PATH}" ] && [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	poetry build

bump-version:
	@if [ "${CONDA_PREFIX}" != "${ENV_PATH}" ] && [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	git-cliff --bumped-version | sed 's/^v//' > version.txt
	python bump-version.py
	git-cliff --bump > docs/changelog.md

clean:
	find . -name '*.pyc' -delete
	find . -type d -name '__pycache__' -exec rm -rf {} +
	rm -rf dist build site

# Target to create the versioned conda environment in the shared software directory
create-env-versioned:
	@if [ -d "${ENV_PATH}" ]; then \
 		echo "Environment already exists at ${ENV_PATH}"; \
 	else \
 		mkdir -p /exchange/healthds/software/envs; \
 		conda env create --prefix ${ENV_PATH} --file base_environment.yml; \
 	fi

# Target to create the conda environment if it doesn't exist
create-env:
	@if conda env list | grep "^${ENV_NAME} " >/dev/null 2>&1; then \
 		echo "Environment ${ENV_NAME} already exists."; \
 	else \
 		conda env create --file base_environment.yml; \
 	fi

dependencies:
	@if [ "${CONDA_PREFIX}" != "${ENV_PATH}" ] && [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	poetry install --without dev --no-root

dependencies_dev:
	@if [ "${CONDA_PREFIX}" != "${ENV_PATH}" ] && [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	poetry install --only dev --no-root

dependencies_extras:
	@if [ "${CONDA_PREFIX}" != "${ENV_PATH}" ] && [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	poetry install --without dev --no-root --extras plot

deploy:
	@if [ "${CONDA_PREFIX}" != "${ENV_PATH}" ] && [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	poetry install

editable_install: build
	@if [ "${CONDA_PREFIX}" != "${ENV_PATH}" ] && [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	pip install --editable .

install: build
	@if [ "${CONDA_PREFIX}" != "${ENV_PATH}" ] && [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	pip install dist/*.whl

pre-commit: dependencies_dev
	@if [ "${CONDA_PREFIX}" != "${ENV_PATH}" ] && [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	if [ ! -f .git/hooks/pre-commit ]; then pre-commit install; fi; \
	pre-commit run --all-files

tag:
	git tag v${VERSION}

unit_test:
	@echo "Unit Testing"
	@if [ "${CONDA_PREFIX}" != "${ENV_PATH}" ] && [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	pytest --cov=src/gwasstudio/ tests

test_ingest_metadata: test-dependencies
	@echo "Running ingest + query integration tests..."
	@if [ "${CONDA_PREFIX}" != "${ENV_PATH}" ] && [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	bash tests/integration/test_ingest_metadata.sh

test_full_export: test-dependencies
	@echo "Running full pipeline integration tests..."
	@if [ "${CONDA_PREFIX}" != "${ENV_PATH}" ] && [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	bash tests/integration/test_full_export.sh

test_ingest_with_recalc: test-dependencies
	@echo "Running ingest with -log10p recalculation tests..."
	@if [ "${CONDA_PREFIX}" != "${ENV_PATH}" ] && [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	bash tests/integration/test_ingest_with_recalc.sh

test_export_flags: test-dependencies
	@echo "Running --fail-on-empty / --region-buffer integration tests..."
	@echo "Date: $$(date)"
	@if [ "${CONDA_PREFIX}" != "${ENV_PATH}" ] && [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	bash tests/integration/test_export_flags.sh

test_trait_ontology_ids: test-dependencies
	@echo "Running trait_ontology_ids integration tests..."
	@if [ "${CONDA_PREFIX}" != "${ENV_PATH}" ] && [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	bash tests/integration/test_trait_ontology_ids.sh

test-integration-pytest: test-dependencies
	@echo "Running integration tests via pytest..."
	@if [ "${CONDA_PREFIX}" != "${ENV_PATH}" ] && [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	pytest tests/integration/ -v --tb=short

test: unit_test test_ingest_metadata test_trait_ontology_ids
	@echo "End-to-End tests"

test-integration: test-integration-setup test-integration-exec #test-integration-stop## Run integration tests

test-integration-setup: ## Start Docker services for integration tests
	docker compose -f dev/docker-compose-integration.yml up -d
	sleep 5

test-integration-exec: ## Run integration tests (excluding provision)
	@echo "Integration test 03"
	@if [ "${CONDA_PREFIX}" != "${ENV_PATH}" ] && [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
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
	@if [ "${CONDA_PREFIX}" != "${ENV_PATH}" ] && [ "${CONDA_DEFAULT_ENV}" != "${ENV_NAME}" ]; then \
        echo "Activating conda environment: ${ENV_NAME}"; \
		$(CONDA_ACTIVATE) ${ENV_NAME}; \
	fi; \
	pip uninstall -y ${APPNAME}
