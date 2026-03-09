#!/bin/bash
set -Eeuo pipefail

# Databricks Asset Bundle Deployment Script
# This script simplifies deploying the Nominatim Geocoding API

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored messages
print_info() {
    echo -e "${BLUE}ℹ ${1}${NC}"
}

print_success() {
    echo -e "${GREEN}✓ ${1}${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ ${1}${NC}"
}

print_error() {
    echo -e "${RED}✗ ${1}${NC}"
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Parse command line arguments
TARGET="${1:-dev}"
COMMAND="${2:-deploy}"

if [[ ! "$TARGET" =~ ^(dev|prod)$ ]]; then
    print_error "Invalid target. Use 'dev' or 'prod'"
    echo "Usage: ./deploy.sh [dev|prod] [deploy|validate|destroy|logs|status|summary]"
    exit 1
fi

echo ""
echo "╔════════════════════════════════════════════════════╗"
echo "║   Nominatim Geocoding API - Databricks Deployment ║"
echo "╚════════════════════════════════════════════════════╝"
echo ""

# Check prerequisites
print_info "Checking prerequisites..."

if ! command_exists databricks; then
    print_error "Databricks CLI not found!"
    echo "Install it with: pip install databricks-cli"
    exit 1
fi

# Check Databricks CLI version
CLI_VERSION=$(databricks --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)
print_success "Databricks CLI version: $CLI_VERSION"

# Verify authentication
print_info "Verifying Databricks authentication..."
if databricks auth describe &>/dev/null; then
    print_success "Authentication verified"
else
    print_error "Authentication failed!"
    echo "Run: databricks configure"
    exit 1
fi

# Execute command
echo ""
print_info "Target environment: $TARGET"
echo ""

case "$COMMAND" in
    validate)
        print_info "Validating bundle configuration..."
        databricks bundle validate -t "$TARGET"
        print_success "Bundle configuration is valid!"
        ;;

    deploy)
        print_info "Validating bundle..."
        if databricks bundle validate -t "$TARGET"; then
            print_success "Validation passed"
        else
            print_error "Validation failed"
            exit 1
        fi

        echo ""
        print_info "Deploying to $TARGET environment..."
        databricks bundle deploy -t "$TARGET"

        echo ""
        print_success "Deployment complete!"
        echo ""
        print_info "App name: nominatim-geocoding-api-$TARGET"
        print_info "View logs: databricks apps logs nominatim-geocoding-api-$TARGET"
        print_info "View status: databricks apps get nominatim-geocoding-api-$TARGET"
        echo ""
        ;;

    destroy)
        print_warning "This will destroy the $TARGET deployment!"
        read -p "Are you sure? (yes/no): " -r
        echo
        if [[ $REPLY =~ ^[Yy]es$ ]]; then
            print_info "Destroying $TARGET deployment..."
            databricks bundle destroy -t "$TARGET"
            print_success "Deployment destroyed"
        else
            print_info "Cancelled"
        fi
        ;;

    logs)
        print_info "Fetching logs for nominatim-geocoding-api-$TARGET..."
        echo ""
        databricks apps logs "nominatim-geocoding-api-$TARGET" --tail 100
        ;;

    status)
        print_info "Fetching status for nominatim-geocoding-api-$TARGET..."
        echo ""
        databricks apps get "nominatim-geocoding-api-$TARGET"
        ;;

    summary)
        print_info "Fetching deployment summary..."
        echo ""
        databricks bundle summary -t "$TARGET"
        ;;

    *)
        print_error "Unknown command: $COMMAND"
        echo "Available commands: validate, deploy, destroy, logs, status, summary"
        exit 1
        ;;
esac

echo ""
print_success "Done!"
