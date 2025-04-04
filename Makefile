.PHONY: install-build build clean install

# Install build dependencies
install-build:
	pip install -q build twine

# Build the package
build:
	python -m build

# Clean up build artifacts
clean:
	rm -rf build dist *.egg-info

# Install the built package locally
install:
	pip install dist/*.whl
