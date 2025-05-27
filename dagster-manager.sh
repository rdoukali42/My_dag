#!/bin/bash

# Dagster Manager Script
# Usage: ./dagster-manager.sh [dev|prod|setup|cleanup|help]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"

# Environment file
ENV_FILE="$PROJECT_DIR/.env"

# Function to print colored output
print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Function to check if environment file exists
check_env() {
    if [[ ! -f "$ENV_FILE" ]]; then
        print_error "Environment file not found: $ENV_FILE"
        print_info "Please create a .env file with your LakeFS and MLflow configurations"
        exit 1
    fi
    print_success "Environment file found: $ENV_FILE"
}

# Function to check if we're in the right directory
check_directory() {
    if [[ ! -f "$PROJECT_DIR/pyproject.toml" ]] || [[ ! -d "$PROJECT_DIR/dagster_pipeline" ]]; then
        print_error "Not in a Dagster project directory"
        print_info "Please run this script from the dagster_pipeline project root"
        exit 1
    fi
    print_success "Dagster project directory confirmed"
}

# Function to load environment variables
load_env() {
    if [[ -f "$ENV_FILE" ]]; then
        print_info "Loading environment variables from $ENV_FILE"
        set -a
        source "$ENV_FILE"
        set +a
    fi
}

# Function to start Dagster in development mode
start_dev() {
    print_info "Starting Dagster in development mode..."
    print_info "Web UI will be available at: http://localhost:3000"
    print_warning "Press Ctrl+C to stop the server"
    
    cd "$PROJECT_DIR"
    exec dagster dev --port 3000
}

# Function to start Dagster in production mode
start_prod() {
    print_info "Starting Dagster in production mode..."
    print_warning "Make sure you have proper production configurations"
    
    cd "$PROJECT_DIR"
    
    # Start Dagster daemon in background
    print_info "Starting Dagster daemon..."
    dagster-daemon run &
    DAEMON_PID=$!
    
    # Start Dagster webserver
    print_info "Starting Dagster webserver on port 3000..."
    print_info "Web UI will be available at: http://localhost:3000"
    
    # Trap to cleanup on exit
    trap "print_info 'Stopping Dagster services...'; kill $DAEMON_PID 2>/dev/null || true; exit 0" INT TERM
    
    exec dagster-webserver --host 0.0.0.0 --port 3000
}

# Function to execute setup_lakefs_job
run_setup() {
    print_info "Executing LakeFS setup job..."
    print_info "This will create branches: Processed_data, Models_deployed"
    print_info "This will create folders: data/, new_data/, new_merge_data/"
    
    cd "$PROJECT_DIR"
    
    print_info "Starting job execution..."
    dagster job execute \
        --module-name dagster_pipeline.repository \
        --job setup_lakefs_job
    
    if [[ $? -eq 0 ]]; then
        print_success "LakeFS setup job completed successfully!"
    else
        print_error "LakeFS setup job failed!"
        exit 1
    fi
}

# Function to execute cleanup_lakefs_job
run_cleanup() {
    print_warning "⚠️  WARNING: This will clean your LakeFS repository!"
    print_warning "This will:"
    print_warning "  - Delete ALL branches except 'main'"
    print_warning "  - Delete ALL files from the main branch"
    print_warning "  - This operation CANNOT be undone!"
    
    echo
    read -p "Are you sure you want to continue? (yes/no): " -r
    echo
    
    if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
        print_info "Cleanup cancelled by user"
        exit 0
    fi
    
    print_info "Executing LakeFS cleanup job..."
    
    cd "$PROJECT_DIR"
    
    print_info "Starting cleanup job execution..."
    dagster job execute \
        --module-name dagster_pipeline.repository \
        --job cleanup_lakefs_job
    
    if [[ $? -eq 0 ]]; then
        print_success "LakeFS cleanup job completed successfully!"
    else
        print_error "LakeFS cleanup job failed!"
        exit 1
    fi
}

# Function to show help
show_help() {
    echo
    echo "Dagster Manager Script"
    echo "====================="
    echo
    echo "Usage: $0 [COMMAND]"
    echo
    echo "Commands:"
    echo "  dev        Start Dagster in development mode (with hot reload)"
    echo "  prod       Start Dagster in production mode (daemon + webserver)"
    echo "  setup      Execute LakeFS setup job (create branches and folders)"
    echo "  cleanup    Execute LakeFS cleanup job (WARNING: destructive operation)"
    echo "  help       Show this help message"
    echo
    echo "Examples:"
    echo "  $0 dev                 # Start development server"
    echo "  $0 prod                # Start production server"
    echo "  $0 setup               # Setup LakeFS repository structure"
    echo "  $0 cleanup             # Clean LakeFS repository (with confirmation)"
    echo
    echo "Environment:"
    echo "  Requires .env file with LakeFS and MLflow configurations"
    echo "  Must be run from the dagster_pipeline project root directory"
    echo
    echo "Ports:"
    echo "  Development/Production UI: http://localhost:3000"
    echo
}

# Function to check dependencies
check_dependencies() {
    local missing_deps=()
    
    if ! command -v dagster &> /dev/null; then
        missing_deps+=("dagster")
    fi
    
    if ! command -v dagster-daemon &> /dev/null; then
        missing_deps+=("dagster-daemon")
    fi
    
    if ! command -v dagster-webserver &> /dev/null; then
        missing_deps+=("dagster-webserver")
    fi
    
    if [[ ${#missing_deps[@]} -ne 0 ]]; then
        print_error "Missing dependencies: ${missing_deps[*]}"
        print_info "Please install Dagster: pip install dagster dagster-webserver"
        exit 1
    fi
}

# Main script logic
main() {
    local command="${1:-help}"
    
    print_info "Dagster Manager - Starting with command: $command"
    
    # Always check dependencies and directory
    check_dependencies
    check_directory
    
    case "$command" in
        "dev")
            check_env
            load_env
            start_dev
            ;;
        "prod")
            check_env
            load_env
            start_prod
            ;;
        "setup")
            check_env
            load_env
            run_setup
            ;;
        "cleanup")
            check_env
            load_env
            run_cleanup
            ;;
        "help"|"-h"|"--help")
            show_help
            ;;
        *)
            print_error "Unknown command: $command"
            echo
            show_help
            exit 1
            ;;
    esac
}

# Run main function with all arguments
main "$@"
