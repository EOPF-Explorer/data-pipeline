<!-- DRAFT design-decision comment for coordination#183. Post manually with:
     gh issue comment 183 --repo EOPF-Explorer/coordination --body-file this-file
     (strip this HTML comment first). Body was already edited live 2026-07-09. -->

Recording where I think the thread has converged, so shout if you disagree.

The plan is to go expiry-driven rather than age-driven: we stamp an `expires` property on each item at registration and backfill it onto existing items, and the cleanup job only deletes items where `expires < now`. The nice property is that any item without an `expires` is simply undeletable, which is exactly what keeps our manually-ingested demo data (the 2021 scenes) safe.

For the retention period I'd propose 6 months (183 days) — it's a one-constant change if we'd rather go with 3 or 12, so easy to revisit.

For protecting demo data I want two layers: an explicit exclude list as the primary safeguard, plus a review of the `created − datetime` gap as a secondary check. I'd rather set that gap threshold from a dry-run histogram than guess a number, because a naive heuristic would wrongly skip our bulk-converted historical data. For the actual deletion we'll reuse the method from #89.

One thing this mechanism doesn't cover: orphan S3 objects that have no STAC item at all (left over from collection deletes before #89), since everything here keys off STAC items. I'd suggest we track that as a separate follow-up.

cc @j08lue @emmanuelmathot
