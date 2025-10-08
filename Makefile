.PHONY: help test test-cov lint format typecheck check build push publish deploy clean pre-commit

IMAGE_NAME := ghcr.io/eopf-explorer/data-pipeline
TAG := v0

help:  ## Show this help message
	@echo "🚀 EOPF GeoZarr Data Pipeline"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

setup:  ## Install dependencies and pre-commit hooks
	@echo "📦 Installing dependencies..."
	uv sync --all-extras
	@echo "🔧 Installing pre-commit hooks..."
	uv run pre-commit install

test:  ## Run tests with pytest
	@echo "🧪 Running tests..."
	uv run pytest -v

test-cov:  ## Run tests with coverage report
	@echo "🧪 Running tests with coverage..."
	uv run pytest --cov=scripts --cov-report=html --cov-report=term
	@echo "📊 Coverage report: htmlcov/index.html"

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

check: lint typecheck test  ## Run all checks (lint + typecheck + test)
	@echo "✅ All checks passed!"

build:  ## Build Docker image
	@echo "Building $(IMAGE_NAME):$(TAG) ..."
	docker build --platform linux/amd64 \
		-f docker/Dockerfile \
		-t $(IMAGE_NAME):$(TAG) \
		-t $(IMAGE_NAME):latest \
		.

push:
	@echo "Pushing $(IMAGE_NAME):$(TAG) ..."
	docker push $(IMAGE_NAME):$(TAG)
	docker push $(IMAGE_NAME):latest

publish: build push
	@echo "Published $(IMAGE_NAME):$(TAG)"

deploy:
	kubectl apply -f workflows/template.yaml
	kubectl apply -f workflows/eventsource.yaml
	kubectl apply -f workflows/sensor.yaml

clean:
	@echo "Cleaning generated files..."
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name '*.pyc' -delete 2>/dev/null || true
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage
	@echo "✓ Clean complete"
