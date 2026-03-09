.PHONY: help install validate deploy-dev deploy-prod logs-dev logs-prod logs-dev-follow logs-prod-follow status-dev status-prod list destroy-dev destroy-prod test local local-reload local-query refresh-token setup-postgis build-import resume-index clean info dev prod

# Default target
.DEFAULT_GOAL := help

# Colors
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[1;33m
NC := \033[0m # No Color

help: ## Show this help message
	@echo ""
	@echo "$(BLUE)Nominatim Geocoding API - Databricks Deployment$(NC)"
	@echo ""
	@echo "$(GREEN)Available commands:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""

refresh-token: ## Refresh Databricks database OAuth token
	@echo "$(BLUE)Refreshing database OAuth token...$(NC)"
	python scripts/00_refresh_environment.py
	@echo "$(GREEN)✓ Token refreshed$(NC)"

setup-postgis: ## Ensure PostGIS/hstore extensions exist
	@echo "$(BLUE)Setting up PostGIS extensions...$(NC)"
	python scripts/01_setup_postgis.py
	@echo "$(GREEN)✓ PostGIS setup complete$(NC)"

build-import: ## Build/rebuild Nominatim DB from OSM files (set OSM_FILES="...")
	@if [ -z "$(OSM_FILES)" ]; then \
		echo "$(YELLOW)Usage: make build-import OSM_FILES=\"osm_data/monaco-latest.osm.pbf [more files]\" [THREADS=8] [CACHE_MB=8000]$(NC)"; \
		exit 1; \
	fi
	@echo "$(BLUE)Starting Nominatim import...$(NC)"
	@if [ -n "$(THREADS)" ] && [ -n "$(CACHE_MB)" ]; then \
		bash scripts/03_build_nominatim_server.sh $(OSM_FILES) --threads $(THREADS) --cache-mb $(CACHE_MB); \
	elif [ -n "$(THREADS)" ]; then \
		bash scripts/03_build_nominatim_server.sh $(OSM_FILES) --threads $(THREADS); \
	elif [ -n "$(CACHE_MB)" ]; then \
		bash scripts/03_build_nominatim_server.sh $(OSM_FILES) --cache-mb $(CACHE_MB); \
	else \
		bash scripts/03_build_nominatim_server.sh $(OSM_FILES); \
	fi

resume-index: ## Resume failed indexing (optional THREADS=4)
	@echo "$(BLUE)Resuming failed indexing...$(NC)"
	@if [ -n "$(THREADS)" ]; then \
		bash scripts/04_resume_failed_indexing.sh --threads $(THREADS); \
	else \
		bash scripts/04_resume_failed_indexing.sh; \
	fi

install: ## Install Python dependencies
	@echo "$(BLUE)Installing dependencies...$(NC)"
	pip install -r requirements.txt
	@echo "$(GREEN)✓ Dependencies installed$(NC)"

validate: ## Validate Databricks bundle configuration
	@echo "$(BLUE)Validating bundle...$(NC)"
	databricks bundle validate
	@echo "$(GREEN)✓ Bundle is valid$(NC)"

deploy-dev: validate ## Deploy to development environment
	@echo "$(BLUE)Deploying to dev...$(NC)"
	databricks bundle deploy -t dev
	@echo "$(GREEN)✓ Deployed to dev$(NC)"
	@echo ""
	@echo "View logs: make logs-dev"
	@echo "View status: make status-dev"

deploy-prod: validate ## Deploy to production environment
	@echo "$(YELLOW)⚠ Deploying to PRODUCTION$(NC)"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		databricks bundle deploy -t prod; \
		echo "$(GREEN)✓ Deployed to production$(NC)"; \
	else \
		echo "Cancelled"; \
	fi

logs-dev: ## View logs from dev environment
	@databricks apps logs nominatim-geocoding-api-dev --tail 100

logs-prod: ## View logs from production environment
	@databricks apps logs nominatim-geocoding-api-prod --tail 100

logs-dev-follow: ## Follow logs from dev environment
	@databricks apps logs nominatim-geocoding-api-dev --follow

logs-prod-follow: ## Follow logs from production environment
	@databricks apps logs nominatim-geocoding-api-prod --follow

status-dev: ## Show dev app status
	@databricks apps get nominatim-geocoding-api-dev

status-prod: ## Show production app status
	@databricks apps get nominatim-geocoding-api-prod

list: ## List all deployed apps
	@databricks apps list | grep nominatim || echo "No nominatim apps found"

destroy-dev: ## Destroy dev deployment
	@echo "$(YELLOW)⚠ This will destroy the dev deployment$(NC)"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		databricks bundle destroy -t dev; \
		echo "$(GREEN)✓ Dev deployment destroyed$(NC)"; \
	else \
		echo "Cancelled"; \
	fi

destroy-prod: ## Destroy production deployment
	@echo "$(YELLOW)⚠ This will destroy the PRODUCTION deployment$(NC)"
	@read -p "Type 'destroy-prod' to confirm: " confirm; \
	if [ "$$confirm" = "destroy-prod" ]; then \
		databricks bundle destroy -t prod; \
		echo "$(GREEN)✓ Production deployment destroyed$(NC)"; \
	else \
		echo "Cancelled"; \
	fi

test: ## Run local tests (if available)
	@if [ -f pytest.ini ] || [ -d tests ]; then \
		pytest tests/ -v; \
	else \
		echo "$(YELLOW)No tests configured$(NC)"; \
	fi

local: ## Run app locally
	@echo "$(BLUE)Starting local server...$(NC)"
	@echo "Make sure environment variables are set!"
	@echo ""
	python app.py

local-reload: ## Run app locally with auto-reload
	@echo "$(BLUE)Starting local server with auto-reload...$(NC)"
	@echo "Make sure environment variables are set!"
	@echo ""
	uvicorn app:app --reload --host 0.0.0.0 --port 8000

local-query: ## Run a quick local /search query (set Q=...)
	@QVAL="$(if $(Q),$(Q),Tehran)"; \
	echo "$(BLUE)Querying local API for '$$QVAL'...$(NC)"; \
	curl -s "http://localhost:8000/search?q=$$QVAL&limit=3" | jq .

health-dev: ## Check health of dev deployment
	@echo "$(BLUE)Checking dev health...$(NC)"
	@curl -s $$(databricks apps get nominatim-geocoding-api-dev --format json | jq -r .url)/health | jq

health-prod: ## Check health of production deployment
	@echo "$(BLUE)Checking production health...$(NC)"
	@curl -s $$(databricks apps get nominatim-geocoding-api-prod --format json | jq -r .url)/health | jq

clean: ## Clean Python cache files
	@echo "$(BLUE)Cleaning cache files...$(NC)"
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name "*.pyo" -delete 2>/dev/null || true
	@echo "$(GREEN)✓ Cache cleaned$(NC)"

info: ## Show deployment information
	@echo ""
	@echo "$(BLUE)╔════════════════════════════════════════╗$(NC)"
	@echo "$(BLUE)║  Nominatim Geocoding API - Info       ║$(NC)"
	@echo "$(BLUE)╚════════════════════════════════════════╝$(NC)"
	@echo ""
	@echo "$(GREEN)Bundle:$(NC)       nominatim-geocoding-api"
	@echo "$(GREEN)Environments:$(NC) dev, prod"
	@echo "$(GREEN)Port:$(NC)         8080 (Databricks), 8000 (local)"
	@echo ""
	@echo "$(GREEN)Quick Commands:$(NC)"
	@echo "  make deploy-dev       - Deploy to dev"
	@echo "  make logs-dev         - View dev logs"
	@echo "  make status-dev       - Check dev status"
	@echo "  make build-import     - Build Nominatim database"
	@echo "  make resume-index     - Resume failed indexing"
	@echo "  make local            - Run locally"
	@echo ""
	@echo "For more commands: make help"
	@echo ""

# Development workflow
dev: validate deploy-dev logs-dev-follow ## Full dev workflow: validate, deploy, and follow logs

# Production workflow
prod: validate deploy-prod ## Full prod workflow: validate and deploy
