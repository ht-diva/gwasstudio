APPNAME=$(shell grep name pyproject.toml|cut -f2 -d'"')
TARGETS=build clean dependencies deploy install test uninstall
VERSION=$(shell grep version pyproject.toml|cut -f2 -d'"')
.ONESHELL:
# Need to specify bash in order for conda activate to work.
SHELL=/bin/bash
# https://stackoverflow.com/questions/53382383/makefile-cant-use-conda-activate
CONDA_ACTIVATE=source $$(conda info --base)/etc/profile.d/conda.sh ; conda activate ; conda activate


all:
	@echo "Try one of: ${TARGETS}"

# https://stackoverflow.com/questions/70851048/does-it-make-sense-to-use-conda-poetry
bootstrap:
	conda create -p /tmp/bootstrap -c conda-forge mamba conda-lock poetry='1.*'
	$(CONDA_ACTIVATE) /tmp/bootstrap
	/tmp/bootstrap/bin/conda-lock -k explicit --conda mamba
	poetry init --python=~3.10  # version spec should match the one from environment.yml
	poetry add --lock tiledb-py=0.32.2
	poetry add --lock conda-lock
	conda deactivate
	rm -rf /tmp/bootstrap

build_conda_lock_files:
	conda-lock -k explicit --conda mamba -p 'linux-64' -p 'osx-64' -p 'osx-arm64'

build: clean dependencies
	poetry build

clean:
	find . -name '*.pyc' -delete
	find . -type d -name '__pycache__' -exec rm -rf {} +
	rm -rf dist build

dependencies:
	poetry install --without dev --no-root

dependencies_dev:
	poetry install --only dev --no-root

deploy:
	poetry install

install: build
	pip install dist/*.whl

m1_env:
	conda create -n ${APPNAME} --file conda-osx-arm64.lock
	$(CONDA_ACTIVATE) ${APPNAME}
	find $(CONDA_PREFIX)/lib/python*/site-packages/ \
     	-maxdepth 2 -name direct_url.json \
     	-exec rm -f {} +

pre-commit: dependencies_dev
	if [ ! -f .git/hooks/pre-commit ]; then pre-commit install; fi
	pre-commit run --all-files

tag:
	git tag v${VERSION}

test:
	@echo "Testing"
	python3 -m unittest discover -s tests

uninstall:
	pip uninstall -y ${APPNAME}
