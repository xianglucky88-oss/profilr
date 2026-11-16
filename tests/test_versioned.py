import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from profilr.baseline import (
    BaselineError,
    BaselineNotFoundError,
    BaselineStore,
    ComparisonNotFoundError,
    VersionNotFoundError,
)
from profilr.drift import DEFAULT_SPEC, compare_profiles
from profilr.pipeline import (
    compare_versions,
    get_comparison_result,
    reevaluate,
    rollback_baseline,
    save_baseline,
)
from profilr.profile import profile_rows


def _profile(rows):
    return profile_rows(rows)


class VersionedPipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name) / "profilr.db")
        self.store = BaselineStore(self.db)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def _two_versions(self, table="orders"):
        save_baseline(self.store, table, _profile([{"amount": "1"}, {"amount": "2"}]), "batch-1")
        save_baseline(self.store, table, _profile([{"amount": "1"}, {"amount": ""}]), "batch-2")

    # -- idempotency / retries -------------------------------------------
    def test_same_input_retry_is_deduplicated(self):
        profile = _profile([{"a": "1"}])
        first = save_baseline(self.store, "t", profile, "input-1")
        second = save_baseline(self.store, "t", profile, "input-1")
        self.assertTrue(first["created"])
        self.assertFalse(second["created"])
        self.assertEqual(first["version"], second["version"])
        self.assertEqual(len(self.store.versions("t")), 1)

    def test_distinct_inputs_allocate_increasing_versions(self):
        save_baseline(self.store, "t", _profile([{"a": "1"}]), "input-1")
        save_baseline(self.store, "t", _profile([{"a": "2"}]), "input-2")
        self.assertEqual([v["version"] for v in self.store.versions("t")], [1, 2])

    # -- concurrency ------------------------------------------------------
    def test_concurrent_saves_allocate_unique_versions(self):
        barrier = threading.Barrier(8)
        errors = []

        def worker(i):
            try:
                barrier.wait(timeout=10)
                save_baseline(self.store, "t", _profile([{"a": str(i)}]), f"input-{i}")
            except Exception as exc:  # pragma: no cover - failure path
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        self.assertEqual(errors, [])
        versions = sorted(v["version"] for v in self.store.versions("t"))
        self.assertEqual(versions, list(range(1, 9)))
        self.assertEqual(self.store.head("t")["version"], 8)

    # -- rollback ----------------------------------------------------------
    def test_rollback_moves_head_and_is_audited(self):
        self._two_versions()
        result = rollback_baseline(self.store, "orders", 1)
        self.assertTrue(result["changed"])
        self.assertEqual(self.store.head("orders")["version"], 1)
        self.assertEqual(self.store.load("orders")["row_count"], 2)
        events = [e["event"] for e in self.store.events("orders")]
        self.assertIn("rollback", events)
        # versions are immutable: both still listed
        self.assertEqual([v["version"] for v in self.store.versions("orders")], [1, 2])

    def test_rollback_to_missing_version_raises(self):
        self._two_versions()
        with self.assertRaises(VersionNotFoundError):
            rollback_baseline(self.store, "orders", 99)

    # -- missing baselines -------------------------------------------------
    def test_compare_without_baseline_raises(self):
        with self.assertRaises(BaselineNotFoundError):
            compare_versions(self.store, "nope")

    def test_compare_with_missing_version_raises(self):
        save_baseline(self.store, "t", _profile([{"a": "1"}]), "input-1")
        with self.assertRaises(VersionNotFoundError):
            compare_versions(self.store, "t", from_version=1, to_version=9)

    def test_compare_same_version_raises(self):
        self._two_versions()
        with self.assertRaises(BaselineError):
            compare_versions(self.store, "orders", 1, 1)

    # -- schema events vs data drift ---------------------------------------
    def test_schema_events_are_classified_separately(self):
        save_baseline(self.store, "t", _profile([{"a": "1", "b": "x"}]), "v1")
        save_baseline(self.store, "t", _profile([{"a": "hello", "c": "y"}]), "v2")
        result = compare_versions(self.store, "t", 1, 2)
        events = {(e["field"], e["event"]) for e in result["schema_events"]}
        self.assertEqual(events, {("a", "type_changed"), ("b", "field_removed"), ("c", "field_added")})
        schema_alerts = [a for a in result["alerts"] if a["category"] == "schema_event"]
        self.assertEqual(len(schema_alerts), 3)
        self.assertTrue(all(a["reason"] for a in schema_alerts))

    def test_mixed_metrics_and_alert_reasons(self):
        baseline_rows = [{"amount": str(v)} for v in range(1, 21)]
        current_rows = [{"amount": str(v * 100)} for v in range(1, 16)] + [{"amount": ""} for _ in range(5)]
        save_baseline(self.store, "t", _profile(baseline_rows), "v1")
        save_baseline(self.store, "t", _profile(current_rows), "v2")
        result = compare_versions(self.store, "t", 1, 2)
        metrics = result["field_metrics"][0]["metrics"]
        self.assertIn("psi", metrics)
        self.assertIn("missing_rate", metrics)
        fired = {(a["metric"], a["category"]) for a in result["alerts"]}
        self.assertIn(("missing_rate", "data_drift"), fired)
        self.assertIn(("psi", "data_drift"), fired)
        for alert in result["alerts"]:
            self.assertIn("阈值", alert["reason"])
            self.assertEqual(alert["field"], "amount")

    def test_missing_field_does_not_crash_drift(self):
        save_baseline(self.store, "t", _profile([{"a": "1", "b": "2"}]), "v1")
        save_baseline(self.store, "t", _profile([{"a": "1"}]), "v2")
        result = compare_versions(self.store, "t", 1, 2)
        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["schema_events"][0]["event"], "field_removed")

    # -- reproducibility -----------------------------------------------------
    def test_comparison_is_reproducible_and_reused(self):
        self._two_versions()
        first = compare_versions(self.store, "orders", 1, 2)
        second = compare_versions(self.store, "orders", 1, 2)
        self.assertFalse(first["reused"])
        self.assertTrue(second["reused"])
        self.assertEqual(first["comparison_id"], second["comparison_id"])
        self.assertEqual(first["alerts"], second["alerts"])
        self.assertEqual(first["compared_versions"], {"baseline": 1, "current": 2})

    def test_default_compare_uses_head_and_predecessor(self):
        self._two_versions()
        result = compare_versions(self.store, "orders")
        self.assertEqual(result["compared_versions"], {"baseline": 1, "current": 2})

    # -- threshold re-interpretation -----------------------------------------
    def test_reevaluate_with_relaxed_thresholds_clears_alerts(self):
        self._two_versions()
        original = compare_versions(self.store, "orders", 1, 2)
        self.assertGreater(original["summary"]["alert_count"], 0)
        relaxed = reevaluate(self.store, "orders", original["comparison_id"], {
            "missing_rate_threshold": 10.0,
            "psi_threshold": 10.0,
            "categorical_psi_threshold": 10.0,
        })
        self.assertEqual(relaxed["alerts"], [])
        self.assertEqual(relaxed["parent_id"], original["comparison_id"])
        self.assertNotEqual(relaxed["comparison_id"], original["comparison_id"])
        # raw metrics are preserved across re-interpretation
        self.assertEqual(relaxed["field_metrics"], original["field_metrics"])

    def test_reevaluate_requires_completed_comparison(self):
        with self.assertRaises(ComparisonNotFoundError):
            reevaluate(self.store, "orders", "cmp-missing", {})

    # -- interrupted computations ---------------------------------------------
    def test_interrupted_comparison_is_recovered_and_recomputed(self):
        self._two_versions()
        spec_id = self.store.ensure_spec(dict(DEFAULT_SPEC))
        cid = BaselineStore.comparison_id("orders", 1, 2, spec_id)
        self.store.start_comparison(cid, "orders", 1, 2, spec_id)  # simulate crash mid-run
        self.store.close()
        self.store = BaselineStore(self.db)  # reopen triggers recovery sweep
        self.assertEqual(self.store.get_comparison(cid)["status"], "interrupted")
        result = compare_versions(self.store, "orders", 1, 2)
        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["comparison_id"], cid)

    # -- result lookup ---------------------------------------------------------
    def test_get_comparison_result_explains_versions(self):
        self._two_versions()
        comparison = compare_versions(self.store, "orders", 1, 2)
        fetched = get_comparison_result(self.store, comparison["comparison_id"])
        self.assertEqual(fetched["compared_versions"], {"baseline": 1, "current": 2})
        self.assertEqual(fetched["spec"], comparison["spec"])

    # -- legacy migration -------------------------------------------------------
    def test_legacy_baselines_table_is_migrated(self):
        self.store.close()
        legacy_db = str(Path(self.tmp.name) / "legacy.db")
        conn = sqlite3.connect(legacy_db)
        conn.execute("CREATE TABLE baselines (table_name TEXT, version INTEGER, payload TEXT, PRIMARY KEY(table_name, version))")
        conn.execute("INSERT INTO baselines VALUES ('orders', 1, ?)", (json.dumps({"row_count": 3, "fields": {}}),))
        conn.commit()
        conn.close()
        store = BaselineStore(legacy_db)
        try:
            self.assertEqual(store.head("orders")["version"], 1)
            self.assertEqual(store.load("orders")["row_count"], 3)
            self.assertIn("legacy_migrated", [e["event"] for e in store.events("orders")])
        finally:
            store.close()


class PureDriftTests(unittest.TestCase):
    def test_compare_profiles_separates_schema_and_drift(self):
        before = _profile([{"n": "1", "s": "a"}, {"n": "2", "s": "b"}])
        after = _profile([{"n": "1"}, {"n": ""}])
        result = compare_profiles(before, after)
        self.assertEqual([e["event"] for e in result["schema_events"]], ["field_removed"])
        drift_metrics = result["field_metrics"][0]["metrics"]
        self.assertAlmostEqual(drift_metrics["missing_rate"]["delta"], 0.5)


if __name__ == "__main__":
    unittest.main()
