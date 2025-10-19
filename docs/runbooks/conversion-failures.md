# Conversion Failures

Troubleshooting guide for GeoZarr conversion issues.

## S3 Timeout

**Symptom:** `botocore.exceptions.ReadTimeoutError` or `Connection timeout`

**Causes:**
- Network instability between K8s cluster and S3
- Large dataset transfer (>10 GB)
- S3 bucket throttling

**Resolution:**
1. Check S3 endpoint connectivity: `curl -I $AWS_ENDPOINT_URL`
2. Verify bucket permissions: `aws s3 ls s3://$BUCKET --endpoint-url $AWS_ENDPOINT_URL`
3. Increase retry config in conversion script
4. Split large conversions into band subsets

## Out of Memory

**Symptom:** `MemoryError`, `Killed`, or pod eviction

**Causes:**
- Insufficient memory limits (< 6Gi for S2, < 8Gi for S1)
- Large chunk sizes loaded into memory
- Dask worker memory leak

**Resolution:**
1. Increase workflow memory limits in `workflows/template.yaml`
2. Check Dask worker memory: Add `DASK_DISTRIBUTED__WORKER__MEMORY__TARGET=0.8`
3. Reduce chunk size in conversion parameters
4. Monitor with `kubectl top pod -n devseed`

## Invalid Input

**Symptom:** `ValueError: Source Zarr not found` or `KeyError: 'measurements'`

**Causes:**
- Source STAC item missing Zarr asset
- Incorrect group path (e.g., `/measurements/reflectance` vs `/measurements`)
- Source Zarr corrupted or incomplete

**Resolution:**
1. Verify source STAC item: `curl $STAC_API/collections/$COLLECTION/items/$ITEM_ID`
2. Check Zarr structure: `zarrita info $SOURCE_URL`
3. Validate groups parameter matches source hierarchy
4. Re-trigger upstream Zarr generation if corrupted

## Dask Worker Crashes

**Symptom:** `KilledWorker`, `CommClosedError`, or workflow hangs

**Causes:**
- Worker OOM (exceeds pod limits)
- Network partition between workers
- Corrupted intermediate data

**Resolution:**
1. Check worker logs: `kubectl logs -n devseed -l app=dask-worker`
2. Reduce worker count or increase memory per worker
3. Enable Dask dashboard: Port-forward 8787, check task graph
4. Restart with clean Dask cluster

## Permission Denied

**Symptom:** `AccessDenied`, `403 Forbidden`

**Causes:**
- Invalid S3 credentials
- Bucket policy restricts access
- Wrong S3 endpoint URL

**Resolution:**
1. Verify secret exists: `kubectl get secret geozarr-s3-credentials -n devseed`
2. Test credentials: `aws s3 ls s3://$BUCKET --endpoint-url $AWS_ENDPOINT_URL`
3. Check bucket policy allows PutObject/GetObject
4. Confirm endpoint matches bucket region

## Disk Space

**Symptom:** `No space left on device`, pod in `Evicted` state

**Causes:**
- Insufficient ephemeral storage for intermediate files
- Zarr consolidation writes large metadata
- Multiple failed runs leave artifacts

**Resolution:**
1. Increase ephemeral-storage request in workflow pod spec
2. Clean up failed workflow artifacts: `kubectl delete wf -n devseed --field-selector status.phase=Failed`
3. Monitor node disk: `kubectl describe nodes | grep ephemeral-storage`
4. Use S3 for intermediate data instead of local disk
