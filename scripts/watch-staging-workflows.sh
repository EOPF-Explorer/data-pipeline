#!/bin/bash
# Helper script to monitor devseed-staging workflows
# Usage: ./watch-staging-workflows.sh [workflow-name]

set -e

NAMESPACE="devseed-staging"

if [ $# -eq 0 ]; then
    echo "📋 Listing all workflows in $NAMESPACE..."
    argo list -n "$NAMESPACE"
    echo ""
    echo "💡 Usage:"
    echo "  $0                    # List all workflows"
    echo "  $0 <workflow-name>    # Watch specific workflow"
    echo "  $0 logs <workflow>    # View workflow logs"
    echo "  $0 get <workflow>     # Get workflow details"
elif [ "$1" = "logs" ]; then
    shift
    argo logs "$@" -n "$NAMESPACE"
elif [ "$1" = "get" ]; then
    shift
    argo get "$@" -n "$NAMESPACE"
else
    echo "🔍 Watching workflow: $1"
    argo watch "$1" -n "$NAMESPACE"
fi
