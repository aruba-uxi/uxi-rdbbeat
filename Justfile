# Show this message and exit.
help:
	@just --list

# Clean temporary files from repo folder
clean:
	rm -rf build dist wheels *.egg-info
	rm -rf */build */dist
	find . -path '*/__pycache__/*' -delete
	find . -type d -name '__pycache__' -empty -delete
	rm -rf '.mypy_cache'
	rm -rf '.pytest_cache'
	rm -rf '.coverage'

# Install pip-tools
install-pip-tools:
	pip install --quiet pip-tools

# Setup dev requirements
setup-dev:
	pip install -r dev-requirements.in
	pip install -e .

# Auto-format the code
fmt:
	isort .
	flynt --quiet .
	black .

# Run all lints
lint:
	flake8 .
	flynt --dry-run --fail-on-change --quiet .
	isort --diff --check .
	black --diff --check .
	mypy .
	yamllint .

# Run tests
test +ARGS='':
	coverage run -m pytest {{ARGS}}

# Create coverage report
coverage:
	coverage xml
	coverage report

# Server version bump using bumpversion (major, minor, patch)
bumpversion +ARGS='':
	bumpversion --config-file .bumpversion.cfg {{ARGS}}
