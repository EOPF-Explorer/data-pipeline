# Data Pipeline: Gaps Analysis & Priority Roadmap

**Date:** 2025-10-18
**Context:** Against Devseed SOW and production deployment requirements
**Status:** Pipeline operational, but production-readiness gaps identified

---

## Executive Summary

The data-pipeline successfully achieves core SOW Task 1 deliverables (GeoZarr conversion, STAC integration, Argo orchestration). However, **critical production gaps** exist in operations, monitoring, testing, and documentation that risk operational stability and maintainability.

**Severity Scale:** 🔴 Critical | 🟡 Important | 🟢 Nice-to-have

---

## 1. CRITICAL GAPS (Block Production) 🔴

### 1.1 Missing Alerting & On-Call Runbooks
**Impact:** No way to detect/respond to failures in production
**Evidence:**
- Prometheus metrics exist but NO alerts configured
- No PagerDuty/Slack integration
- No runbooks for common failure modes
- Silent failures possible (see OBSERVABILITY.md SLOs but no actual alerts)

**Action Required:**
```yaml
# Missing: platform-deploy/core/monitoring/alerts/data-pipeline-alerts.yaml
- alert: GeozarrWorkflowFailureRate
  expr: rate(geozarr_conversion_failures[5m]) > 0.1
  for: 5m
  annotations:
    runbook_url: https://docs.../runbook-conversion-failures
```

**Priority:** P0 - Deploy before production launch
**Effort:** 2-3 days (alerts + 3-4 runbooks)

---

### 1.2 No Disaster Recovery / Data Loss Prevention
**Impact:** Catastrophic data loss possible, no rollback capability
**Evidence:**
- No S3 versioning mentioned in platform-deploy configs
- No backup strategy for STAC catalog (pgSTAC database)
- No way to reprocess failed items after >1 hour (workflow retention unclear)
- No "undo" for incorrect STAC registrations

**Missing:**
- S3 bucket lifecycle policies
- STAC database backup automation
- Failed workflow replay mechanism
- Item deletion/correction procedures

**Priority:** P0 - Required for production
**Effort:** 1 week (infra + procedures + testing)

---

### 1.3 CI/CD Pipeline Incomplete
**Impact:** Cannot safely deploy updates, high regression risk
**Current State:**
- ✅ Unit tests exist (8 files for 9 scripts)
- ✅ GitHub Actions test workflow
- ❌ **No integration tests in CI** (test_pipeline_e2e.py not run automatically)
- ❌ **No Docker build on merge to main** (build.yml only triggers on tags or manual)
- ❌ **No staging deployment automation** (manual kubectl apply required)
- ❌ **No rollback strategy** if bad image deployed

**Gaps:**
```yaml
# .github/workflows/test.yml missing:
- name: Integration tests
  run: pytest tests/integration/ --s3-bucket=test-bucket
```

**Priority:** P0 - Before next release
**Effort:** 3-4 days

---

### 1.4 Production Secrets Management Immature
**Impact:** Security vulnerability, credential leaks possible
**Evidence:**
- `.work/` directory contains plaintext tokens (argo.token, kubeconfig)
- PAT tokens required for GHCR (added manually, no rotation policy)
- S3 credentials in K8s secrets but no documented rotation procedure
- RabbitMQ password exposed in multiple places

**Required:**
- Vault integration or sealed-secrets for all credentials
- Documented rotation procedures (90-day cycle)
- Remove plaintext secrets from git history audit
- Principle of least privilege review

**Priority:** P0 - Security audit required
**Effort:** 1 week

---

## 2. IMPORTANT GAPS (Operational Risk) 🟡

### 2.1 Insufficient Test Coverage
**Current:** 8 unit test files, unclear integration coverage
**Missing:**
- **No S1 GRD conversion tests** (only S2 mentioned in docs)
- **No failure injection tests** (what happens when STAC API is down?)
- **No load/stress tests** (how many concurrent workflows can cluster handle?)
- **No data validation tests** (GeoZarr spec compliance automated testing)
- Integration tests exist but not run in CI

**Metric:** Test coverage report shows ~60-70% (estimated from htmlcov), should be >85% for production

**Priority:** P1 - 2-week sprint
**Effort:** 1-2 weeks

---

### 2.2 Operational Documentation Gaps
**Strong:** ARCHITECTURE.md, OBSERVABILITY.md, README.md
**Weak:**
- **No troubleshooting decision tree** (see README "Common issues" - only 3 items)
- **No capacity planning guide** (how many workflows per node? When to scale?)
- **No cost analysis** (S3 egress, compute costs per item)
- **No performance tuning guide** (when to adjust chunk size, memory limits)
- **No data retention policy** (how long keep GeoZarr outputs?)

**Priority:** P1 - Before handoff to ops team
**Effort:** 1 week (collaborative documentation)

---

### 2.3 No Batch Processing / Backfill Strategy
**Impact:** Cannot efficiently reprocess collections
**Evidence:**
- Workflow templates designed for single-item processing
- No "batch convert all S2 r60m from date range" capability
- Manual kubectl create per item
- No progress tracking for large batch jobs

**Use Case:** SOW requires processing "petabytes of Sentinel imagery" - current approach doesn't scale

**Priority:** P1 - Required for initial bulk load
**Effort:** 1-2 weeks (design + implementation)

---

### 2.4 Limited Observability Beyond Metrics
**Current:** Prometheus metrics, Argo UI logs
**Missing:**
- **No distributed tracing** (workflow spans through convert → validate → register → augment)
- **No structured logging** (logs are print statements, hard to parse)
- **No log aggregation** (Loki/ELK stack not mentioned in platform-deploy)
- **No cost tracking** (S3 costs per collection, compute costs per workflow)

**Priority:** P1 - Improves debugging significantly
**Effort:** 1 week (structured logging + log shipper)

---

### 2.5 Workflow Template Maintenance Complexity
**Issue:** Multiple YAML files with duplicate configuration
**Evidence:**
```
workflows/
  template.yaml           # Main template (6 resource blocks)
  run-s1-test.yaml       # Test workflow
  run-benchmark-test.yaml
  rbac.yaml
  rbac-staging.yaml      # Duplicate RBAC configs
```

**Impact:**
- Resource limit changes must be applied to 6+ places
- High risk of configuration drift between staging/prod
- No schema validation (easy to create invalid YAML)

**Solution:**
- Kustomize overlays (staging/prod inherit from base)
- Helm chart for parameterized deployments
- CI validation of workflow templates

**Priority:** P1 - Tech debt cleanup
**Effort:** 3-4 days

---

## 3. SOW ALIGNMENT GAPS 🟡

### 3.1 Task 1 Gaps (Pipeline & Transformation)
**Delivered:** ✅ S1 GRD + S2 L2A support, Argo + Dask, STAC integration, CI/CD, docs
**Missing from SOW:**
- ❌ "Optimal chunking and compression strategies identified through testing"
  - No documented benchmarking methodology
  - No A/B test results comparing chunk sizes
  - No justification for chosen compression codec (see data-model README)
- ❌ "Automated CI/CD on OVH cloud infrastructure"
  - CI runs on GitHub Actions (not OVH)
  - No automated deployment to OVH cluster
  - Manual kubectl apply still required

**Priority:** P1 - SOW compliance
**Effort:** 1 week (benchmarking + documentation)

---

### 3.2 Task 2 Gaps (Demonstration & Validation)
**Delivered:** ✅ Jupyter notebooks (3), benchmarking tools, validation framework
**Missing from SOW:**
- ❌ "Performance comparison between original EOPF and GeoZarr formats"
  - benchmark_geozarr.py exists but no published results
  - No white paper or report showing improvements
  - No before/after metrics in production
- ❌ "Working prototype ready for V0 demonstration (September 29)"
  - Delivered late (based on commit history in docs)
  - No V0 demo recording or materials archived

**Priority:** P2 - Deliverable completion
**Effort:** 2-3 days (run benchmarks + write report)

---

### 3.3 Community Engagement Gaps
**SOW Requirements:**
- "Engage with the GeoZarr community to contribute to technical specifications"
- "Participate in standardization working groups"
- "Take on development of the necessary enhancements in existing python libraries"

**Evidence of Gaps:**
- No public PRs to zarr-python, xarray, xpublish (checked GitHub histories)
- No mention of GeoZarr spec contributions in data-model CHANGELOG
- No conference talks, blog posts, or public demos referenced

**Priority:** P2 - SOW requirement
**Effort:** Ongoing quarterly activity

---

## 4. NICE-TO-HAVE IMPROVEMENTS 🟢

### 4.1 Developer Experience
- Pre-commit hooks slow (all checks run even on small changes)
- No local testing environment (Kind/Minikube setup)
- No VS Code devcontainer config
- Notebooks require manual port-forwarding

**Priority:** P3
**Effort:** 2-3 days

---

### 4.2 Performance Optimizations
- Dask cluster configuration not documented (how many workers?)
- No caching layer for frequently accessed source items
- No partial conversion restart (if convert fails at 90%, restart from scratch)
- S3 multipart upload not mentioned (affects large files)

**Priority:** P3
**Effort:** 1-2 weeks

---

### 4.3 Additional Monitoring
- Cost per workflow (compute + storage)
- Quality metrics (GeoZarr compliance score distribution)
- User engagement (tile request patterns from Titiler)
- Data freshness (time from Sentinel acquisition to GeoZarr availability)

**Priority:** P3
**Effort:** 1 week

---

## 5. ARCHITECTURAL CONCERNS

### 5.1 Tight Coupling: data-pipeline ↔ data-model
**Issue:** data-pipeline depends on `eopf-geozarr` CLI from data-model, but:
- No versioning contract (what if CLI changes?)
- Docker image includes both repos (update one = rebuild both)
- No compatibility matrix documented

**Recommendation:**
- Semantic versioning for eopf-geozarr CLI
- Separate Docker images (data-model base, data-pipeline extends)
- Integration tests across version combinations

**Priority:** P2
**Effort:** 3-4 days

---

### 5.2 Single Point of Failure: STAC API
**Issue:** Entire pipeline blocks if STAC API unavailable
**Evidence:**
- No retry with exponential backoff in register_stac.py
- No circuit breaker pattern
- No fallback to queue registration requests

**Recommendation:**
- Implement tenacity retry decorator (exists but not used everywhere)
- Add dead-letter queue for failed registrations
- STAC API health check before workflow submission

**Priority:** P2
**Effort:** 2-3 days

---

### 5.3 Resource Limits May Be Insufficient
**Current:** 6Gi memory request / 10Gi limit for conversion
**Concern:** S2 r10m full tile may exceed limits for large datasets
**Evidence:** Template.yaml shows fixed limits, but no autoscaling mentioned

**Recommendation:**
- Document memory requirements per collection/resolution
- Add resource request scaling based on input size
- Configure HPA for workflow pods (if supported by Argo)

**Priority:** P2
**Effort:** 2-3 days (profiling + documentation)

---

## 6. PRIORITY ROADMAP

### Sprint 1 (Week 1-2): Production Readiness 🔴
**Goal:** Zero critical blockers for production launch

1. **Alerting & Runbooks** (P0, 2-3 days)
   - Create 5 alerts (failure rate, latency, capacity)
   - Write 3 runbooks (common failures, capacity issues, STAC sync)
   - Deploy to Prometheus

2. **Disaster Recovery** (P0, 5 days)
   - Enable S3 versioning + lifecycle policies
   - Document STAC backup procedure
   - Implement failed workflow replay script
   - Test restore procedures

3. **CI/CD Completion** (P0, 3-4 days)
   - Add integration tests to CI
   - Auto-build Docker on merge to main
   - Document rollback procedure
   - Add deployment smoke tests

**Deliverable:** Production launch checklist 100% complete

---

### Sprint 2 (Week 3-4): Operational Stability 🟡

1. **Test Coverage** (P1, 1 week)
   - Add S1 GRD integration tests
   - Add failure injection tests
   - Add load tests (10 concurrent workflows)
   - Reach 85% code coverage

2. **Operational Docs** (P1, 1 week)
   - Troubleshooting decision tree
   - Capacity planning guide
   - Performance tuning guide
   - Cost analysis template

**Deliverable:** Ops team can manage pipeline independently

---

### Sprint 3 (Week 5-6): Scale & Performance 🟡

1. **Batch Processing** (P1, 1-2 weeks)
   - Design batch workflow template
   - Implement progress tracking
   - Add date-range query support
   - Test bulk conversion (1000 items)

2. **Observability Enhancement** (P1, 1 week)
   - Add structured logging (JSON format)
   - Deploy log aggregation (Loki recommended)
   - Implement distributed tracing (optional)

**Deliverable:** Can process historical data at scale

---

### Sprint 4 (Week 7-8): SOW Compliance & Cleanup 🟡

1. **Chunking/Compression Benchmarks** (P1, 1 week)
   - Run systematic benchmarks
   - Document results
   - Publish recommendations

2. **Workflow Template Refactor** (P1, 3-4 days)
   - Convert to Kustomize overlays
   - Add schema validation
   - Document template customization

3. **SOW Deliverable Documentation** (P2, 2-3 days)
   - Write performance comparison report
   - Archive V0 demo materials
   - Update SOW checklist

**Deliverable:** Clean, maintainable codebase + SOW evidence

---

## 7. RISK ASSESSMENT

| Risk | Probability | Impact | Mitigation |
|------|------------|--------|------------|
| Production incident without alerting | High | Critical | Sprint 1: Deploy alerts immediately |
| Data loss from failed conversion | Medium | Critical | Sprint 1: Backup + replay mechanisms |
| Cannot scale to petabyte processing | High | High | Sprint 3: Batch processing design |
| Ops team cannot debug issues | High | High | Sprint 2: Operational documentation |
| Configuration drift staging/prod | Medium | Medium | Sprint 4: Kustomize refactor |
| STAC API downtime blocks pipeline | Medium | High | Sprint 2: Add circuit breaker |
| Insufficient test coverage causes regressions | High | Medium | Sprint 2: Increase test coverage |

---

## 8. METRICS FOR SUCCESS

**Production Readiness Score** (Current: 60% → Target: 95%)
- ✅ Core functionality: 100%
- 🟡 Alerting: 20% (metrics exist, alerts missing)
- 🟡 Testing: 70% (unit tests good, integration incomplete)
- 🟡 Documentation: 75% (architecture good, operations weak)
- ❌ Disaster recovery: 10% (no procedures)
- 🟡 CI/CD: 60% (tests run, deployment manual)

**Operational Excellence Score** (Target by End of Sprint 4: 90%)
- Incident MTTR < 30 minutes
- Deployment frequency: weekly
- Change failure rate < 5%
- Test coverage > 85%
- Documentation freshness < 30 days stale

---

## 9. RECOMMENDATIONS

### Immediate Actions (This Week)
1. Create PR for alerting configuration (2 days, assigned to devops)
2. Document S3 backup procedure (1 day, assigned to architect)
3. Add integration tests to CI (1 day, assigned to developer)

### Technical Debt Cleanup (Next Quarter)
1. Refactor workflow templates to Kustomize
2. Separate data-pipeline and data-model Docker images
3. Implement structured logging across all scripts

### Long-term Improvements (6 months)
1. Build web UI for workflow submission (replace kubectl)
2. Add workflow scheduling (cron-like triggers)
3. Integrate with data quality monitoring system

---

## 10. CONCLUSION

The data-pipeline is **functionally complete** but **operationally immature**. Core technical requirements from the SOW are met, but production deployment risks are significant without:

1. **Alerting & incident response procedures** (P0)
2. **Disaster recovery mechanisms** (P0)
3. **Automated deployment pipeline** (P0)
4. **Comprehensive operational documentation** (P1)

**Recommended Path Forward:**
- **Immediate:** 2-week production readiness sprint (Sprint 1 above)
- **Short-term:** 4-week operational stability phase (Sprints 2-3)
- **Long-term:** Continuous improvement + SOW compliance (Sprint 4+)

**Resource Requirements:**
- 1 senior engineer (full-time, 8 weeks)
- 1 devops engineer (part-time, 4 weeks)
- 1 technical writer (part-time, 2 weeks)

**Estimated Timeline to Production-Ready:** 6-8 weeks

---

**Document Version:** 1.0
**Last Updated:** 2025-10-18
**Review Cycle:** Every 2 weeks during roadmap execution
