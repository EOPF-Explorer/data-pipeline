.PHONY: help setup lint format typecheck pre-commit build push clean

IMAGE_NAME := w9mllyot.c1.de1.container-registry.ovh.net/eopf-sentinel-zarr-explorer/data-pipeline
TAG := v0

help:  ## Show this help message
	@echo "🚀 EOPF GeoZarr Data Pipeline (Slim Branch)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

setup:  ## Install dependencies and pre-commit hooks
	@echo "📦 Installing dependencies..."
	uv sync --all-groups
	@echo "🔧 Installing pre-commit hooks..."
	uv run pre-commit install

lint:  ## Check code style with ruff
	@echo "🔍 Linting with ruff..."
	uv run ruff check .

format:  ## Auto-format code with ruff
	@echo "✨ Formatting with ruff..."
	uv run ruff format .

typecheck:  ## Type check with mypy
	@echo "🔍 Type checking with mypy..."
	uv run mypy scripts/

pre-commit:  ## Run all pre-commit hooks
	@echo "🔧 Running pre-commit hooks..."
	uv run pre-commit run --all-files

build:  ## Build Docker image
	@echo "Building $(IMAGE_NAME):$(TAG) ..."
	docker build --platform linux/amd64 \
		-f docker/Dockerfile \
		-t $(IMAGE_NAME):$(TAG) \
		-t $(IMAGE_NAME):latest \
		.

push:  ## Push Docker image to registry
	@echo "Pushing $(IMAGE_NAME):$(TAG) ..."
	docker push $(IMAGE_NAME):$(TAG)
	docker push $(IMAGE_NAME):latest

clean:  ## Clean generated files and caches
	@echo "Cleaning generated files..."
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name '*.pyc' -delete 2>/dev/null || true
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage
	@echo "✓ Clean complete"
