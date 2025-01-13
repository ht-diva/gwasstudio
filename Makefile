APPNAME=$(shell grep -m 1 name pyproject.toml|cut -f2 -d'"')
TARGETS=build clean dependencies deploy editable_install install test uninstall
VERSION=$(shell grep version pyproject.toml|cut -f2 -d'"')


all:
	@echo "Try one of: ${TARGETS}"

build_conda_lock_files:
	conda-lock -k explicit -f environment.yml --conda mamba -p 'linux-64' -p 'osx-arm64'

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

editable_install:
	pip install --editable .

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
	pytest --cov=src/gwasstudio/ tests

uninstall:
	pip uninstall -y ${APPNAME}
