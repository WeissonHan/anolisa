// SPDX-License-Identifier: Apache-2.0
//! In-process counters surfaced via `GET /v1/metrics` (Prometheus text
//! exposition). v0.1 ships a small, fixed set; richer histograms wait
//! for an opinion-graded collector in Phase 2.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct Metrics {
    pub requests_total: AtomicU64,
    pub instances_created: AtomicU64,
    pub instances_destroyed: AtomicU64,
    pub instances_resets: AtomicU64,
    pub instances_paused: AtomicU64,
    pub instances_resumed: AtomicU64,
    pub snapshots_created: AtomicU64,
    pub snapshots_failed: AtomicU64,
    pub snapshots_deleted: AtomicU64,
    pub pool_hits: AtomicU64,
    pub pool_misses: AtomicU64,
    pub policy_eval_failures: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inc(&self, counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Render every counter into the Prometheus text exposition format.
    pub fn render(&self) -> String {
        let mut out = String::new();
        let series = [
            (
                "blaze_requests_total",
                "Total HTTP requests served by the blaze daemon",
                self.requests_total.load(Ordering::Relaxed),
            ),
            (
                "blaze_instances_created_total",
                "Total sandbox instances created",
                self.instances_created.load(Ordering::Relaxed),
            ),
            (
                "blaze_instances_destroyed_total",
                "Total sandbox instances destroyed",
                self.instances_destroyed.load(Ordering::Relaxed),
            ),
            (
                "blaze_instances_resets_total",
                "Total sandbox instances reset",
                self.instances_resets.load(Ordering::Relaxed),
            ),
            (
                "blaze_instances_paused_total",
                "Total sandbox instances paused",
                self.instances_paused.load(Ordering::Relaxed),
            ),
            (
                "blaze_instances_resumed_total",
                "Total sandbox instances resumed",
                self.instances_resumed.load(Ordering::Relaxed),
            ),
            (
                "blaze_snapshots_created_total",
                "Total checkpoint payloads written",
                self.snapshots_created.load(Ordering::Relaxed),
            ),
            (
                "blaze_snapshots_failed_total",
                "Total checkpoint attempts that failed",
                self.snapshots_failed.load(Ordering::Relaxed),
            ),
            (
                "blaze_snapshots_deleted_total",
                "Total checkpoint payloads deleted",
                self.snapshots_deleted.load(Ordering::Relaxed),
            ),
            (
                "blaze_pool_hits_total",
                "Warm pool hits (instance reused)",
                self.pool_hits.load(Ordering::Relaxed),
            ),
            (
                "blaze_pool_misses_total",
                "Warm pool misses (cold boot)",
                self.pool_misses.load(Ordering::Relaxed),
            ),
            (
                "blaze_policy_eval_failures_total",
                "Number of failed policy evaluations",
                self.policy_eval_failures.load(Ordering::Relaxed),
            ),
        ];
        for (name, help, value) in series {
            use std::fmt::Write;
            let _ = writeln!(&mut out, "# HELP {name} {help}");
            let _ = writeln!(&mut out, "# TYPE {name} counter");
            let _ = writeln!(&mut out, "{name} {value}");
        }
        out
    }
}
