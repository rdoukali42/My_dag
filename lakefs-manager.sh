#!/bin/bash

# LakeFS Docker Setup Script
# This script sets up and manages LakeFS with PostgreSQL using Docker Compose

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/dagster_pipeline/.env"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.lakefs.yaml"

# Function to detect docker compose command
get_compose_cmd() {
    if command -v docker-compose >/dev/null 2>&1; then
        echo "docker-compose"
    elif docker compose version >/dev/null 2>&1; then
        echo "docker compose"
    else
        print_error "Neither 'docker-compose' nor 'docker compose' found. Please install Docker Compose."
        exit 1
    fi
}

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if Docker is running
check_docker() {
    if ! docker info >/dev/null 2>&1; then
        print_error "Docker is not running. Please start Docker and try again."
        exit 1
    fi
}

# Function to check if .env file exists
check_env_file() {
    if [[ ! -f "$ENV_FILE" ]]; then
        print_error ".env file not found at $ENV_FILE"
        exit 1
    fi
}

# Function to start LakeFS
start_lakefs() {
    print_status "Starting LakeFS with PostgreSQL..."
    
    check_docker
    check_env_file
    
    # Get the correct compose command
    COMPOSE_CMD=$(get_compose_cmd)
    
    # Load environment variables
    set -a  # automatically export all variables
    source "$ENV_FILE"
    set +a  # stop automatically exporting
    
    # Start services
    $COMPOSE_CMD -f "$COMPOSE_FILE" up -d
    
    print_status "LakeFS services started successfully!"
    print_status "PostgreSQL: localhost:5433"
    print_status "LakeFS UI: $LAKEFS2_HOST"
    print_warning "Please wait a few seconds for services to fully initialize."
    
    # Wait for LakeFS to be ready
    print_status "Waiting for LakeFS to be ready..."
    for i in {1..30}; do
        if curl -s "$LAKEFS2_HOST/api/v1/healthcheck" >/dev/null 2>&1; then
            print_status "LakeFS is ready!"
            break
        fi
        sleep 2
        echo -n "."
    done
    echo ""
    
    print_status "Setup complete! You can now:"
    echo "1. Access LakeFS UI at: $LAKEFS2_HOST"
    echo "2. Create your initial admin user"
    echo "3. Create the repository: $LAKEFS2_REPOSITORY"
}

# Function to stop LakeFS
stop_lakefs() {
    print_status "Stopping LakeFS services..."
    COMPOSE_CMD=$(get_compose_cmd)
    $COMPOSE_CMD -f "$COMPOSE_FILE" down
    print_status "LakeFS services stopped."
}

# Function to restart LakeFS
restart_lakefs() {
    print_status "Restarting LakeFS services..."
    stop_lakefs
    start_lakefs
}

# Function to view logs
logs_lakefs() {
    COMPOSE_CMD=$(get_compose_cmd)
    $COMPOSE_CMD -f "$COMPOSE_FILE" logs -f
}

# Function to show status
status_lakefs() {
    print_status "LakeFS services status:"
    COMPOSE_CMD=$(get_compose_cmd)
    $COMPOSE_CMD -f "$COMPOSE_FILE" ps
}

# Function to clean up (remove volumes)
cleanup_lakefs() {
    print_warning "This will remove all LakeFS data and volumes!"
    read -p "Are you sure? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        print_status "Cleaning up LakeFS data..."
        COMPOSE_CMD=$(get_compose_cmd)
        $COMPOSE_CMD -f "$COMPOSE_FILE" down -v
        print_status "Cleanup complete."
    else
        print_status "Cleanup cancelled."
    fi
}

# Function to show help
show_help() {
    echo "LakeFS Docker Management Script"
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  start     Start LakeFS services"
    echo "  stop      Stop LakeFS services"
    echo "  restart   Restart LakeFS services"
    echo "  logs      Show LakeFS logs (follow mode)"
    echo "  status    Show services status"
    echo "  cleanup   Stop services and remove all data"
    echo "  help      Show this help message"
    echo ""
    echo "Configuration is loaded from: $ENV_FILE"
}

# Main script logic
case "${1:-}" in
    start)
        start_lakefs
        ;;
    stop)
        stop_lakefs
        ;;
    restart)
        restart_lakefs
        ;;
    logs)
        logs_lakefs
        ;;
    status)
        status_lakefs
        ;;
    cleanup)
        cleanup_lakefs
        ;;
    help|--help|-h)
        show_help
        ;;
    "")
        print_warning "No command specified. Use 'help' to see available commands."
        show_help
        ;;
    *)
        print_error "Unknown command: $1"
        show_help
        exit 1
        ;;
esac