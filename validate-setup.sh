#!/bin/bash
# Validate data-pipeline setup
# Run this after following GETTING_STARTED.md to verify everything works

set -euo pipefail

# Error trap for better debugging
trap 'echo "❌ Validation failed at line $LINENO with exit code $?"' ERR

NAMESPACE="${NAMESPACE:-devseed}"
PASS=0
FAIL=0

echo "=========================================="
echo "🔍 Data Pipeline Setup Validation"
echo "=========================================="
echo ""

# Function to check and report
check() {
    local name="$1"
    local command="$2"

    echo -n "  Checking $name... "
    if eval "$command" &>/dev/null; then
        echo "✅"
        ((PASS++))
        return 0
    else
        echo "❌"
        ((FAIL++))
        return 1
    fi
}

# 1. kubectl access
echo "📋 Step 1: kubectl Configuration"
check "kubectl installed" "command -v kubectl"
check "KUBECONFIG set" "test -n \"\${KUBECONFIG:-}\""
check "cluster access" "kubectl get nodes"
check "namespace exists" "kubectl get namespace $NAMESPACE"
echo ""

# 2. Infrastructure deployed
echo "📋 Step 2: Pipeline Infrastructure"
check "RBAC (ServiceAccount)" "kubectl get serviceaccount operate-workflow-sa -n $NAMESPACE"
check "RBAC (Role)" "kubectl get role operate-workflow-creator -n $NAMESPACE"
check "RBAC (RoleBinding)" "kubectl get rolebinding operate-workflow-creator-binding -n $NAMESPACE"
check "EventSource" "kubectl get eventsource rabbitmq-geozarr -n $NAMESPACE"
check "Sensor" "kubectl get sensor geozarr-sensor -n $NAMESPACE"
check "WorkflowTemplate" "kubectl get workflowtemplate geozarr-pipeline -n $NAMESPACE"
echo ""

# 3. Core services (from platform-deploy)
echo "📋 Step 3: Core Services"
check "RabbitMQ deployed" "kubectl get pods -n core -l app.kubernetes.io/name=rabbitmq | grep -q Running"
check "RabbitMQ secret exists" "kubectl get secret rabbitmq-password -n core"
check "Argo Workflows deployed" "kubectl get pods -n core -l app.kubernetes.io/name=argo-workflows-server | grep -q Running"
check "STAC API reachable" "curl -sf https://api.explorer.eopf.copernicus.eu/stac/ -o /dev/null"
check "Raster API reachable" "curl -sf https://api.explorer.eopf.copernicus.eu/raster/ -o /dev/null"
echo ""

# 4. Python environment
echo "📋 Step 4: Python Environment"
check "Python 3.11+" "python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)'"

if command -v uv &>/dev/null; then
    check "uv installed" "command -v uv"
    check "dependencies synced" "test -f .venv/bin/python"
else
    check "pip installed" "command -v pip"
    check "pika installed" "python3 -c 'import pika'"
    check "click installed" "python3 -c 'import click'"
fi
echo ""

# 5. Sensor status (check if it's receiving messages)
echo "📋 Step 5: Event Processing"
SENSOR_POD=$(kubectl get pods -n $NAMESPACE -l sensor-name=geozarr-sensor -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
if [ -n "$SENSOR_POD" ]; then
    check "Sensor pod running" "kubectl get pod $SENSOR_POD -n $NAMESPACE | grep -q Running"

    # Check if sensor has logged any activity (not critical)
    if kubectl logs -n $NAMESPACE $SENSOR_POD --tail=10 2>/dev/null | grep -q "sensor"; then
        echo "  Sensor logs present... ✅"
        ((PASS++))
    else
        echo "  Sensor logs empty (no jobs yet)... ⚠️  (not an error)"
    fi
else
    echo "  Sensor pod not found... ❌"
    ((FAIL++))
fi
echo ""

# Summary
echo "=========================================="
echo "📊 Validation Summary"
echo "=========================================="
echo "✅ Passed: $PASS"
echo "❌ Failed: $FAIL"
echo ""

if [ $FAIL -eq 0 ]; then
    echo "🎉 Setup complete! You're ready to submit jobs."
    echo ""
    echo "Next steps:"
    echo "  1. Port-forward RabbitMQ:"
    echo "     kubectl port-forward -n core svc/rabbitmq 5672:5672 &"
    echo ""
    echo "  2. Get RabbitMQ password and submit:"
    echo "     export AMQP_URL=\"amqp://user:\$(kubectl get secret rabbitmq-password -n core -o jsonpath='{.data.rabbitmq-password}' | base64 -d)@localhost:5672/\""
    echo "     uv run python examples/submit.py \\"
    echo "       --stac-url \"https://stac.core.eopf.eodc.eu/collections/sentinel-2-l2a/items/S2B_...\" \\"
    echo "       --collection \"sentinel-2-l2a-dp-test\""
    echo ""
    echo "  3. Monitor:"
    echo "     kubectl get workflows -n devseed -w"
    echo ""
    exit 0
else
    echo "❌ Setup incomplete. Please fix the failed checks above."
    echo ""
    echo "Common fixes:"
    echo "  - Missing infrastructure: kubectl apply -f workflows/rbac.yaml -n $NAMESPACE"
    echo "  - No cluster access: Check KUBECONFIG points to valid file"
    echo "  - Platform services down: Check platform-deploy status"
    echo ""
    echo "See GETTING_STARTED.md for detailed setup instructions."
    echo ""
    exit 1
fi
