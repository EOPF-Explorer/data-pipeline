# Schematic: S1 RTC input-data caching — current vs planned

Supports `s1_input_data_caching.md` (spec) and `../plans/s1_input_data_caching.md` (plan).
Mermaid renders on GitHub / Obsidian; ASCII equivalents live in the spec/plan and the session notes.

## Current pipeline (per-tile fan-out · each tile re-downloads from CDSE)

```mermaid
flowchart TB
  CDSE[("CDSE — Copernicus<br/>~6 MB/s/file, throttled")]

  subgraph CRON["CronWorkflow eopf-explorer-s1rtc · every 4h · SUSPENDED"]
    DISC["discover · trigger_cdse.py<br/>query CDSE per tile + dedup vs STAC"]
    FAN{{"withParam fan-out · parallelism 6<br/>1 chain per product (= per TILE)"}}
    DISC --> FAN
  end

  subgraph TILE["Child WF eopf-explorer-s1tiling — PER TILE (× ~163)"]
    direction TB
    DEM["ensure-dem → /data/dem<br/>(GLO-30, anon AWS — not the bottleneck)"]
    S1P["s1processor (OTB)<br/>S1Processor → eodag DOWNLOAD"]
    RAW[/"/data/data_raw<br/>per-workflow RWO PVC (ephemeral per tile)"/]
    OUT["orthorectify + γ0 RTC → /data/data_out"]
    UP["upload-geotiffs · s3fs → S3"]
    DEM --> S1P --> RAW --> OUT --> UP
  end

  FAN -->|per product| TILE
  S1P -. "⚠ downloads S1A+S1D + ALL overlapping frames, NO shared cache" .-> CDSE
  UP --> INGEST["submit-ingest → GeoZarr cube → append S3 → register STAC"]

  classDef bad fill:#fdd,stroke:#c00;
  class S1P bad;
```

**Redundancy:** one frame F (250×170 km) overlaps ~12 MGRS tiles ⇒ 12 independent workflows ⇒ F is
downloaded ~12× from CDSE ⇒ runs are download-bound (~35 min/run observed).

## Planned pipeline (P0 platform filter + P1 in-region S3 frame cache · download-once)

```mermaid
flowchart TB
  CDSE[("CDSE — frame F fetched ONCE<br/>by the first tile only (MISS)")]
  CACHE[("IN-REGION S3 FRAME CACHE<br/>OVH same region · ~320 MB/s @ concurrency")]

  subgraph TILE["Child WF eopf-explorer-s1tiling — PER TILE [ RWO PVC /data ]"]
    direction TB
    DEM["ensure-dem → /data/dem"]
    PRE["▶ NEW pre-step · cache_frames.py<br/>pull cache HITS → untar SAFE into /data/data_raw"]
    S1P["s1processor (OTB) · download:True<br/>HITS skipped (disk scan) · MISSES → CDSE"]
    POST["▶ NEW post-step<br/>upload freshly-downloaded SAFEs (misses) → cache"]
    OUT["orthorectify + γ0 RTC → /data/data_out"]
    UP["upload-geotiffs · s3fs → S3 (downstream UNCHANGED)"]
    DEM --> PRE --> S1P --> OUT --> UP
    S1P --> POST
  end

  CACHE -->|"HIT: in-region pull (parallel, fast)"| PRE
  POST -->|"populate-on-miss"| CACHE
  S1P -. "MISS only (once per frame)" .-> CDSE

  P0["P0 (ships first, standalone): eodag patch SEARCH-TIME platform filter<br/>→ pull only requested platform ⇒ no redundant S1D (4 products → 2)"]
  P0 -.-> S1P

  classDef good fill:#dfd,stroke:#090;
  classDef new fill:#ddf,stroke:#33c;
  class CACHE good;
  class PRE,POST,P0 new;
```

**Fleet effect:** frame F overlaps ~12 tiles → tile A (first) MISS→download-once→push to cache; tiles
B…L cache-HIT→in-region pull→local block (OTB at ~897 MB/s). CDSE egress for F: **12× → 1×** ⇒
compute-bound. OTB never reads through S3-FUSE (spike: 31 MB/s, wrong for random reads) — only fast
local block. DEM caching + RWX/Substrate-(ii) are explicitly out of scope for this plan.
