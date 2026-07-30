#!/usr/bin/env bash
# Runtime contract for the pipeline image.
#
# Argo (EOPF-Explorer/platform-deploy) invokes this image three ways — `command: [python, ...]`,
# `command: [sh]` script templates, and `command: [bash, -c]` cron steps — and no template sets
# securityContext.runAsUser, so the image's own USER decides the runtime uid. Each check below is
# one of those assumptions; `--entrypoint` mirrors how Argo overrides the entrypoint.
#
# Usage: docker/smoke-test.sh <image>
set -uo pipefail

IMAGE="${1:?usage: docker/smoke-test.sh <image>}"
FAILED=0

check() { # check <name> <expected-exit> <entrypoint> <args...>
    local name="$1" want="$2" entry="$3"
    shift 3
    local out rc
    out="$(docker run --rm --entrypoint "$entry" "$IMAGE" "$@" 2>&1)"
    rc=$?
    if [ "$rc" -eq "$want" ]; then
        printf 'PASS  %-38s %s\n' "$name" "$(printf '%s' "$out" | tr '\n' ' ' | cut -c1-90)"
    else
        FAILED=$((FAILED + 1))
        printf 'FAIL  %-38s (exit %s, wanted %s)\n%s\n' "$name" "$rc" "$want" "$out"
    fi
}

echo "== smoke test: $IMAGE =="

# 1. The full runtime import closure must load — this is what `command: [python, ...]` steps need.
check "python imports runtime closure" 0 python -c '
import rasterio, pyproj, zarr, numcodecs, google_crc32c, eopf_geozarr, s3fs, pystac_client, xarray
print("py", __import__("sys").version.split()[0], "| GDAL", rasterio.__gdal_version__,
      "| PROJ", pyproj.proj_version_str, "| zarr", zarr.__version__,
      "| crc32c", google_crc32c.implementation)'

# 2. GDAL Python bindings are deliberately absent: nothing in the closure imports osgeo, and no
#    script shells out to a gdal CLI. This check fails loudly if that assumption ever changes.
check "osgeo absent (expected failure)" 1 python -c 'import osgeo'

# 3. Both shells, because prod uses both.
check "bash present" 0 bash -c 'echo bash ok'
check "sh present" 0 sh -c 'echo sh ok'

# 4. Non-root at uid 1000 — changing this changes ownership expectations on every mounted volume.
check "runs as appuser uid 1000" 0 sh -c \
    'set -e; [ "$(id -u)" = 1000 ] || { echo "uid=$(id -u)"; exit 9; }; id -un'

# 5. Writable paths the scripts rely on (query_stac.py writes /tmp/items.json).
check "/tmp and \$HOME writable" 0 sh -c \
    'set -e; touch /tmp/.smoke; touch "${HOME:-/home/appuser}/.smoke"; echo "HOME=${HOME:-unset}"'

# 6. `python` must resolve to the venv interpreter via PATH, not the system one.
check "python resolves to venv" 0 sh -c \
    'set -e; p="$(command -v python)"; [ "$p" = /app/.venv/bin/python ] || { echo "got $p"; exit 9; }; echo "$p"'

# 7. Entrypoints Argo actually calls.
for s in convert_v1_s2.py register_v1.py query_stac.py change_storage_tier.py cleanup_expired_items.py; do
    check "scripts/$s --help" 0 python "/app/scripts/$s" --help
done

# 8. Test helpers must not ship in the runtime image.
check "no test_*.py under /app/scripts" 0 sh -c \
    'n="$(find /app/scripts -name "test_*.py" | wc -l)"; [ "$n" -eq 0 ] || { find /app/scripts -name "test_*.py"; exit 9; }; echo "none"'

echo
if [ "$FAILED" -eq 0 ]; then
    echo "SMOKE TEST PASSED: $IMAGE"
else
    echo "SMOKE TEST FAILED: $FAILED check(s) — $IMAGE"
fi
exit $((FAILED > 0))
