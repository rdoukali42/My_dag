#!/bin/bash

# Simple Dagster Runner
# Quick commands for common Dagster operations

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Get script directory
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

case "$1" in
    "dev")
        echo -e "${BLUE}🚀 Starting Dagster Development Server...${NC}"
        echo -e "${GREEN}➜ http://localhost:3000${NC}"
        cd "$DIR" && dagster dev --port 3000
        ;;
    "prod")
        echo -e "${BLUE}🚀 Starting Dagster Production Mode...${NC}"
        echo -e "${GREEN}➜ http://localhost:3000${NC}"
        cd "$DIR"
        dagster-daemon run &
        dagster-webserver --host 0.0.0.0 --port 3000
        ;;
    "setup")
        echo -e "${BLUE}🔧 Running LakeFS Setup Job...${NC}"
        cd "$DIR" && dagster job execute -m dagster_pipeline.repository -j setup_lakefs_job
        ;;
    "cleanup")
        echo -e "${YELLOW}⚠️  WARNING: This will clean your LakeFS repository!${NC}"
        read -p "Continue? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            echo -e "${RED}🧹 Running LakeFS Cleanup Job...${NC}"
            cd "$DIR" && dagster job execute -m dagster_pipeline.repository -j cleanup_lakefs_job
        else
            echo -e "${GREEN}Cancelled${NC}"
        fi
        ;;
    *)
        echo "Dagster Quick Runner"
        echo "==================="
        echo
        echo "Usage: $0 [command]"
        echo
        echo "Commands:"
        echo "  dev      - Start development server (hot reload)"
        echo "  prod     - Start production server (daemon + webserver)"
        echo "  setup    - Run LakeFS setup job"
        echo "  cleanup  - Run LakeFS cleanup job (with confirmation)"
        echo
        echo "Examples:"
        echo "  $0 dev"
        echo "  $0 setup"
        ;;
esac
