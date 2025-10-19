# STAC Registration Errors

Troubleshooting guide for STAC catalog registration issues.

## 409 Conflict

**Symptom:** `409 Conflict - Item already exists`

**Causes:**
- Re-running conversion for same item ID
- Duplicate workflow triggered by AMQP retry
- Item exists in catalog from previous run

**Resolution:**
1. Check if item exists: `curl $STAC_API/collections/$COLLECTION/items/$ITEM_ID`
2. Delete existing item: `curl -X DELETE $STAC_API/collections/$COLLECTION/items/$ITEM_ID`
3. Or update workflow to use `PUT` instead of `POST` for idempotency
4. Add `--replace` flag to registration script

## 401 Unauthorized

**Symptom:** `401 Unauthorized` or `Authentication required`

**Causes:**
- Missing or expired API token
- Secret not mounted in workflow pod
- Wrong STAC API endpoint (auth required but not configured)

**Resolution:**
1. Verify secret exists: `kubectl get secret stac-api-credentials -n devseed`
2. Check secret mounted: `kubectl describe pod $POD_NAME -n devseed | grep Mounts`
3. Test credentials: `curl -H "Authorization: Bearer $TOKEN" $STAC_API/collections`
4. Refresh token if expired

## 500 Server Error

**Symptom:** `500 Internal Server Error` from pgSTAC

**Causes:**
- PostgreSQL database connection failure
- Invalid STAC item schema (missing required fields)
- pgSTAC extension validation error

**Resolution:**
1. Check pgSTAC pod status: `kubectl get pods -n core -l app=pgstac`
2. View pgSTAC logs: `kubectl logs -n core -l app=pgstac --tail=100`
3. Validate STAC item locally: `pystac item validate $ITEM_JSON`
4. Check PostgreSQL connection: `kubectl exec -it $PGSTAC_POD -n core -- psql -c "SELECT version()"`

## 400 Bad Request

**Symptom:** `400 Bad Request - Invalid item`

**Causes:**
- Missing required STAC fields (geometry, bbox, properties)
- Invalid GeoJSON geometry
- Projection extension missing CRS info
- Asset href not accessible

**Resolution:**
1. Validate item structure: `pystac item validate $ITEM_JSON`
2. Check geometry: Must be valid GeoJSON (lon/lat order)
3. Verify projection:ext:code exists (e.g., `EPSG:32629`)
4. Test asset URL: `curl -I $ASSET_HREF`

## Network Timeout

**Symptom:** `Connection timeout`, `Read timed out`

**Causes:**
- STAC API pod not ready
- Network policy blocks traffic
- High API load (too many concurrent requests)

**Resolution:**
1. Check STAC API health: `curl $STAC_API/`
2. Verify network policies: `kubectl get networkpolicies -n core`
3. Check API pod: `kubectl get pods -n core -l app=stac-api`
4. Add retry logic with exponential backoff

## Augmentation Failures

**Symptom:** Item registered but viewer links missing

**Causes:**
- `augment_stac_item.py` failed after registration
- TiTiler API unavailable
- CRS not supported by TiTiler (rare)

**Resolution:**
1. Check augmentation logs in workflow pod
2. Verify TiTiler API: `curl $RASTER_API/healthz`
3. Re-run augmentation standalone: `python scripts/augment_stac_item.py --item-id $ITEM_ID`
4. Check TileMatrixSet created: Item should have `xyz` and `tilejson` links
