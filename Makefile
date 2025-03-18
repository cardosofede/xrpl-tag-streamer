.PHONY: setup install stream history query stats clean docker-build docker-up docker-down

# Setup environment
setup:
	./setup.sh

# Install dependencies
install:
	pip install -e .

# Install development dependencies
dev-install:
	pip install -e ".[dev]"

# Run streamer
stream:
	python -m src.main stream

# Run historical processor
history:
	python -m src.main history

# Query transactions
query:
	python -m src.main query

# Show statistics
stats:
	python -m src.main stats

# Clean build artifacts
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +

# Format code
format:
	black src/
	isort src/

# Run tests
test:
	pytest 

# Docker commands
docker-build:
	docker build -t xrpl-tag-streamer:latest .

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down 