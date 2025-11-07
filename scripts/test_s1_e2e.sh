#!/bin/bash
# Test S1 GRD end-to-end pipeline in devseed-staging namespace
#
# This script:
# 1. Applies the workflow template
# 2. Publishes an S1 test payload via AMQP
# 3. Waits for workflow completion
# 4. Shows logs and verifies STAC item was created

set -euo pipefail

# Set kubeconfig
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
export KUBECONFIG="${KUBECONFIG:-$PROJECT_ROOT/.work/kubeconfig}"

if [ ! -f "$KUBECONFIG" ]; then
  echo "❌ Kubeconfig not found at: $KUBECONFIG"
  echo "Please set KUBECONFIG environment variable or create .work/kubeconfig"
  exit 1
fi

NAMESPACE="${NAMESPACE:-devseed-staging}"
PAYLOAD_FILE="${PAYLOAD_FILE:-workflows/examples/payload-s1.json}"
TIMEOUT="${TIMEOUT:-600}"  # 10 minutes

echo "=========================================="
echo "S1 GRD Pipeline E2E Test"
echo "=========================================="
echo "Kubeconfig: $KUBECONFIG"
echo "Namespace: $NAMESPACE"
echo "Payload: $PAYLOAD_FILE"
echo "Timeout: ${TIMEOUT}s"
echo ""

# Step 1: Apply workflow template
echo "📝 Applying workflow template..."
kubectl -n "$NAMESPACE" apply -f workflows/template.yaml
echo "✅ Template applied"
echo ""

# Step 2: Publish AMQP message
echo "📤 Publishing test payload..."
kubectl -n "$NAMESPACE" delete job amqp-publish-once --ignore-not-found=true
kubectl -n "$NAMESPACE" delete configmap amqp-payload --ignore-not-found=true
kubectl -n "$NAMESPACE" create configmap amqp-payload --from-file=body.json="$PAYLOAD_FILE"
kubectl -n "$NAMESPACE" apply -f workflows/amqp-publish-once.yaml
echo "⏳ Waiting for publish job..."
kubectl -n "$NAMESPACE" wait --for=condition=complete --timeout=120s job/amqp-publish-once
echo "✅ Payload published"
echo ""

# Step 3: Get latest workflow
echo "🔍 Finding triggered workflow..."
sleep 3  # Give sensor time to create workflow
WORKFLOW=$(kubectl -n "$NAMESPACE" get wf --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1:].metadata.name}' 2>/dev/null || true)
if [ -z "$WORKFLOW" ]; then
  echo "❌ No workflow found!"
  exit 1
fi
echo "✅ Workflow: $WORKFLOW"
echo ""

# Step 4: Wait for completion
echo "⏳ Waiting for workflow completion (timeout: ${TIMEOUT}s)..."
START_TIME=$(date +%s)
while true; do
  PHASE=$(kubectl -n "$NAMESPACE" get wf "$WORKFLOW" -o jsonpath='{.status.phase}' 2>/dev/null || echo "Unknown")
  ELAPSED=$(($(date +%s) - START_TIME))

  echo "  [${ELAPSED}s] Phase: $PHASE"

  case "$PHASE" in
    Succeeded)
      echo "✅ Workflow succeeded!"
      break
      ;;
    Failed|Error)
      echo "❌ Workflow failed!"
      break
      ;;
    Unknown)
      echo "❌ Workflow disappeared!"
      exit 1
      ;;
  esac

  if [ $ELAPSED -ge $TIMEOUT ]; then
    echo "⏰ Timeout reached!"
    break
  fi

  sleep 5
done
echo ""

# Step 5: Show workflow details
echo "=========================================="
echo "Workflow Details"
echo "=========================================="
kubectl -n "$NAMESPACE" get wf "$WORKFLOW" -o jsonpath='
Name: {.metadata.name}
Status: {.status.phase}
Started: {.status.startedAt}
Finished: {.status.finishedAt}
Duration: {.status.estimatedDuration}

Parameters:
  source_url: {.spec.arguments.parameters[?(@.name=="source_url")].value}
  item_id: {.spec.arguments.parameters[?(@.name=="item_id")].value}
  collection: {.spec.arguments.parameters[?(@.name=="register_collection")].value}
'
echo ""
echo ""

# Step 6: Show pod logs
echo "=========================================="
echo "Pod Logs"
echo "=========================================="
PODS=$(kubectl -n "$NAMESPACE" get pods -l workflows.argoproj.io/workflow="$WORKFLOW" -o name 2>/dev/null || true)
if [ -z "$PODS" ]; then
  echo "⚠️  No pods found"
else
  for POD in $PODS; do
    POD_NAME=$(basename "$POD")
    TEMPLATE=$(kubectl -n "$NAMESPACE" get pod "$POD_NAME" -o jsonpath='{.metadata.labels.workflows\.argoproj\.io/template}' 2>/dev/null || echo "unknown")
    echo ""
    echo "--- $POD_NAME ($TEMPLATE) ---"
    kubectl -n "$NAMESPACE" logs "$POD_NAME" --tail=100 -c main 2>/dev/null || echo "No logs available"
  done
fi
echo ""

# Step 7: Verify STAC item
echo "=========================================="
echo "STAC Item Verification"
echo "=========================================="
ITEM_ID=$(kubectl -n "$NAMESPACE" get wf "$WORKFLOW" -o jsonpath='{.spec.arguments.parameters[?(@.name=="item_id")].value}')
COLLECTION=$(kubectl -n "$NAMESPACE" get wf "$WORKFLOW" -o jsonpath='{.spec.arguments.parameters[?(@.name=="register_collection")].value}')
STAC_URL="https://api.explorer.eopf.copernicus.eu/stac/collections/$COLLECTION/items/$ITEM_ID"

echo "Checking: $STAC_URL"
ITEM_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$STAC_URL")
if [ "$ITEM_STATUS" = "200" ]; then
  echo "✅ STAC item exists!"
  echo ""
  curl -s "$STAC_URL" | jq '{
    id: .id,
    collection: .collection,
    geometry: .geometry.type,
    assets: [.assets | keys[]],
    links: [.links[] | select(.rel=="xyz" or .rel=="viewer" or .rel=="tilejson") | {rel, href}]
  }'
else
  echo "❌ STAC item not found (HTTP $ITEM_STATUS)"
fi
echo ""

echo "=========================================="
echo "Test Summary"
echo "=========================================="
echo "Workflow: $WORKFLOW"
echo "Status: $PHASE"
echo "STAC Item: $ITEM_STATUS"
echo ""
if [ "$PHASE" = "Succeeded" ] && [ "$ITEM_STATUS" = "200" ]; then
  echo "🎉 END-TO-END TEST PASSED!"
  exit 0
else
  echo "❌ END-TO-END TEST FAILED"
  exit 1
fi
