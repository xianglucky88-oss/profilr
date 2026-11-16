import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from http.client import RemoteDisconnected
from pathlib import Path

from profilr.alerting import (
    STATUS_ACTIVE,
    STATUS_NEW,
    STATUS_RECOVERED,
    STATUS_SUPPRESSED,
    AlertStore,
    alert_id_for,
)
from profilr.api import serve
from profilr.baseline import BaselineStore
from profilr.notify import merge_alerts, run_alert_pipeline


def _obs(field="amount", metric="missing_rate", delta=0.4):
    return {"field": field, "metric": metric, "delta": delta}


class _Stores:
    def __init__(self, tmpdir):
        self.path = str(Path(tmpdir) / "test.db")
        self.baselines = BaselineStore(self.path)
        self.alerts = AlertStore(self.path)


class AlertLifecycleTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.stores = _Stores(self._tmp.name)
        self.alerts = self.stores.alerts

    # ------------------------------------------------------------- identity

    def test_identity_is_stable_across_versions_and_thresholds(self):
        aid1 = alert_id_for("orders", "amount", "missing_rate")
        aid2 = alert_id_for("orders", "amount", "missing_rate")
        self.assertEqual(aid1, aid2)
        self.assertNotEqual(aid1, alert_id_for("orders", "amount", "mean"))
        self.assertNotEqual(aid1, alert_id_for("payments", "amount", "missing_rate"))

        self.alerts.ingest_observations(
            "orders", [_obs()], baseline_version=1, observed_version=2,
            threshold=0.1, batch_id="b2",
        )
        # Different threshold and newer baseline: same identity, same row.
        self.alerts.ingest_observations(
            "orders", [_obs(delta=0.5)], baseline_version=3, observed_version=4,
            threshold=0.2, batch_id="b4",
        )
        rows = self.alerts.query_alerts(table_name="orders", limit=10)["items"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["alert_id"], aid1)
        self.assertEqual(rows[0]["threshold"], 0.2)
        self.assertEqual(rows[0]["baseline_version"], 3)

    # ------------------------------------------------------------ lifecycle

    def test_new_active_recovered_reopen_states(self):
        s1 = self.alerts.ingest_observations(
            "orders", [_obs()], baseline_version=1, observed_version=2,
            batch_id="b2",
        )
        self.assertEqual(s1["opened"], 1)
        row = self.alerts.query_alerts(limit=1)["items"][0]
        self.assertEqual(row["status"], STATUS_NEW)
        self.assertEqual(row["occurrence_count"], 1)
        self.assertEqual(row["reopen_count"], 0)
        self.assertIsNone(row["recovered_at"])

        self.alerts.ingest_observations(
            "orders", [_obs(delta=0.45)], baseline_version=1, observed_version=3,
            batch_id="b3",
        )
        row = self.alerts.query_alerts(limit=1)["items"][0]
        self.assertEqual(row["status"], STATUS_ACTIVE)
        self.assertEqual(row["occurrence_count"], 2)
        self.assertEqual(row["latest_delta"], 0.45)

        # Quiet observation recovers; below-threshold pairs are required.
        self.alerts.ingest_observations(
            "orders", [_obs(delta=0.01)], baseline_version=1, observed_version=4,
            batch_id="b4",
        )
        row = self.alerts.query_alerts(limit=1)["items"][0]
        self.assertEqual(row["status"], STATUS_RECOVERED)
        self.assertIsNotNone(row["recovered_at"])

        # Same identity drifting again reopens the same row as active, not new.
        self.alerts.ingest_observations(
            "orders", [_obs(delta=0.6)], baseline_version=2, observed_version=5,
            batch_id="b5",
        )
        rows = self.alerts.query_alerts(table_name="orders", limit=10)["items"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], STATUS_ACTIVE)
        self.assertEqual(rows[0]["reopen_count"], 1)

    def test_below_threshold_never_creates_an_alert(self):
        summary = self.alerts.ingest_observations(
            "orders", [_obs(delta=0.02)], threshold=0.1, batch_id="b1",
        )
        self.assertEqual((summary["opened"], summary["recovered"]), (0, 0))
        self.assertEqual(
            self.alerts.query_alerts(table_name="orders", limit=10)["items"], []
        )

    def test_acknowledgment_does_not_change_status(self):
        self.alerts.ingest_observations(
            "orders", [_obs()], batch_id="b2",
        )
        aid = alert_id_for("orders", "amount", "missing_rate")
        updated = self.alerts.acknowledge(aid)
        self.assertEqual(updated["status"], STATUS_NEW)
        self.assertIsNotNone(updated["acknowledged_at"])
        types = [e["event_type"] for e in self.alerts.list_events(aid)]
        self.assertIn("acknowledged", types)

    # ----------------------------------------------------------- suppression

    def test_suppression_window_mutes_but_tracks_repeated_drift(self):
        now = "2026-11-16T10:00:00+00:00"
        self.alerts.ingest_observations(
            "orders", [_obs()], batch_id="b2", now=now,
        )
        aid = alert_id_for("orders", "amount", "missing_rate")
        self.alerts.suppress(aid, "2026-11-16T12:00:00+00:00", now=now)

        hit = self.alerts.ingest_observations(
            "orders", [_obs(delta=0.5)], batch_id="b3",
            now="2026-11-16T11:00:00+00:00",
        )
        self.assertEqual(hit["suppressed_hits"], 1)
        row = self.alerts.get_alert(aid)
        self.assertEqual(row["status"], STATUS_SUPPRESSED)
        self.assertTrue(row["suppression_active"])
        # Repeated drift inside the window still refreshes deltas and counts.
        self.assertEqual(row["latest_delta"], 0.5)
        self.assertEqual(row["occurrence_count"], 2)

        # After the window expires, the same drift reactivates the alert.
        self.alerts.ingest_observations(
            "orders", [_obs(delta=0.5)], batch_id="b4",
            now="2026-11-16T12:30:00+00:00",
        )
        row = self.alerts.get_alert(aid)
        self.assertEqual(row["status"], STATUS_ACTIVE)
        self.assertIsNone(row["suppress_until"])
        self.assertFalse(row["suppression_active"])

    def test_threshold_change_does_not_reopen_suppressed_alert(self):
        now = "2026-11-16T10:00:00+00:00"
        self.alerts.ingest_observations(
            "orders", [_obs(delta=0.3)], threshold=0.2, batch_id="b2", now=now,
        )
        aid = alert_id_for("orders", "amount", "missing_rate")
        self.alerts.suppress(aid, "2026-11-16T18:00:00+00:00", now=now)

        # Even a much stricter threshold (0.05) keeps the alert suppressed
        # until the window ends; threshold edits never force a reopen.
        hit = self.alerts.ingest_observations(
            "orders", [_obs(delta=0.3)], threshold=0.05, batch_id="b3",
            now="2026-11-16T11:00:00+00:00",
        )
        self.assertEqual(hit["suppressed_hits"], 1)
        self.assertEqual(self.alerts.get_alert(aid)["status"], STATUS_SUPPRESSED)
        self.assertEqual(self.alerts.get_alert(aid)["threshold"], 0.05)

    def test_suppress_rejects_past_window(self):
        self.alerts.ingest_observations("orders", [_obs()], batch_id="b2")
        aid = alert_id_for("orders", "amount", "missing_rate")
        with self.assertRaises(ValueError):
            self.alerts.suppress(aid, "2000-01-01T00:00:00+00:00")

    def test_suppressed_alert_recovers_on_quiet_batch(self):
        now = "2026-11-16T10:00:00+00:00"
        self.alerts.ingest_observations(
            "orders", [_obs()], batch_id="b2", now=now,
        )
        aid = alert_id_for("orders", "amount", "missing_rate")
        self.alerts.suppress(aid, "2026-11-16T18:00:00+00:00", now=now)
        summary = self.alerts.ingest_observations(
            "orders", [_obs(delta=0.0)], batch_id="b3",
            now="2026-11-16T11:00:00+00:00",
        )
        self.assertEqual(summary["recovered"], 1)
        self.assertEqual(self.alerts.get_alert(aid)["status"], STATUS_RECOVERED)

    # ----------------------------------------------------------- idempotency

    def test_duplicate_batch_is_idempotent(self):
        first = self.alerts.ingest_observations(
            "orders", [_obs()], batch_id="same",
        )
        second = self.alerts.ingest_observations(
            "orders", [_obs()], batch_id="same",
        )
        self.assertFalse(first["duplicate"])
        self.assertTrue(second["duplicate"])
        rows = self.alerts.query_alerts(table_name="orders", limit=10)["items"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["occurrence_count"], 1)
        events = self.alerts.list_events(rows[0]["alert_id"])
        self.assertEqual(len(events), 1)

    def test_auto_batch_id_is_deterministic_from_contents(self):
        a = self.alerts.ingest_observations(
            "orders", [_obs(delta=0.4)], observed_version=2,
        )
        b = self.alerts.ingest_observations(
            "orders", [_obs(delta=0.4)], observed_version=2,
        )
        self.assertEqual(a["batch_id"], b["batch_id"])
        self.assertTrue(b["duplicate"])

    def test_concurrent_writers_create_one_active_row(self):
        import concurrent.futures

        def write(_):
            store = AlertStore(self.stores.path)
            return store.ingest_observations(
                "orders", [_obs()],
                baseline_version=1, observed_version=2, batch_id=None,
            )

        # Same contents => identical derived batch_id: N-1 replays, one insert.
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(write, range(8)))
        inserts = [r for r in results if not r["duplicate"]]
        self.assertEqual(len(inserts), 1)
        rows = self.alerts.query_alerts(table_name="orders", limit=10)["items"]
        self.assertEqual(len(rows), 1)

    def test_concurrent_distinct_batches_update_single_row(self):
        import concurrent.futures

        def write(idx):
            store = AlertStore(self.stores.path)
            return store.ingest_observations(
                "orders", [_obs(delta=0.3 + idx * 0.01)],
                observed_version=idx, batch_id=f"b{idx}",
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(write, range(8)))
        self.assertTrue(all(not r["duplicate"] for r in results))
        rows = self.alerts.query_alerts(table_name="orders", limit=10)["items"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["occurrence_count"], 8)

    # ------------------------------------------------------------- filtering

    def _seed_many(self):
        for i in range(6):
            self.alerts.ingest_observations(
                "orders",
                [
                    {"field": f"f{i % 3}", "metric": "missing_rate", "delta": 0.5},
                ],
                observed_version=i, batch_id=f"b{i}",
                now=f"2026-11-{10 + i:02d}T08:00:00+00:00",
            )
        self.alerts.ingest_observations(
            "payments", [_obs(field="p", delta=0.5)], batch_id="p1",
        )

    def test_filters_field_metric_status_and_time_range(self):
        self._seed_many()
        by_field = self.alerts.query_alerts(
            table_name="orders", field="f0", limit=10
        )
        self.assertEqual({i["field"] for i in by_field["items"]}, {"f0"})

        by_metric = self.alerts.query_alerts(
            table_name="orders", metric="missing_rate", limit=10
        )
        self.assertEqual(len(by_metric["items"]), 3)

        by_status = self.alerts.query_alerts(
            status=[STATUS_NEW, STATUS_ACTIVE], limit=10
        )
        self.assertTrue(
            all(i["status"] in (STATUS_NEW, STATUS_ACTIVE) for i in by_status["items"])
        )

        # first_seen_at pins each identity to its opening batch day.
        in_window = self.alerts.query_alerts(
            table_name="orders",
            since="2026-11-11T00:00:00+00:00",
            until="2026-11-12T23:59:59+00:00",
            time_field="first_seen_at",
            limit=10,
        )
        self.assertEqual(
            {i["field"] for i in in_window["items"]}, {"f1", "f2"}
        )

    def test_pagination_is_stable(self):
        self._seed_many()
        page1 = self.alerts.query_alerts(table_name="orders", limit=2, offset=0)
        page2 = self.alerts.query_alerts(table_name="orders", limit=2, offset=2)
        self.assertEqual(page1["pagination"]["count"], 3)
        self.assertTrue(page1["pagination"]["has_more"])
        ids1 = [i["alert_id"] for i in page1["items"]]
        ids2 = [i["alert_id"] for i in page2["items"]]
        self.assertFalse(set(ids1) & set(ids2))
        last = self.alerts.query_alerts(table_name="orders", limit=2, offset=2)
        self.assertFalse(last["pagination"]["has_more"])

    def test_invalid_query_arguments(self):
        with self.assertRaises(ValueError):
            self.alerts.query_alerts(limit=0)
        with self.assertRaises(ValueError):
            self.alerts.query_alerts(limit=999)
        with self.assertRaises(ValueError):
            self.alerts.query_alerts(offset=-1)
        with self.assertRaises(ValueError):
            self.alerts.query_alerts(status="bogus")
        with self.assertRaises(ValueError):
            self.alerts.query_alerts(time_field="nope")

    # ---------------------------------------------------- baseline consistency

    def test_baseline_stale_and_missing_flags(self):
        v1 = self.stores.baselines.save("orders", {"row_count": 10})
        self.alerts.ingest_observations(
            "orders", [_obs()], baseline_version=v1, observed_version=2,
            batch_id="b2",
        )
        row = self.alerts.query_alerts(table_name="orders", limit=1)["items"][0]
        self.assertFalse(row["baseline_stale"])
        self.assertFalse(row["baseline_missing"])
        self.assertEqual(row["latest_baseline_version"], 1)

        self.stores.baselines.save("orders", {"row_count": 12})
        row = self.alerts.query_alerts(table_name="orders", limit=1)["items"][0]
        self.assertTrue(row["baseline_stale"])
        self.assertFalse(row["baseline_missing"])
        self.assertEqual(row["latest_baseline_version"], 2)

        # Simulate retention deleting the baseline version referenced by alert.
        self.stores.baselines.conn.execute(
            "DELETE FROM baselines WHERE table_name='orders' AND version=1"
        )
        self.stores.baselines.conn.commit()
        row = self.alerts.query_alerts(table_name="orders", limit=1)["items"][0]
        self.assertTrue(row["baseline_missing"])

    # ------------------------------------------------------------- pipeline

    def test_run_alert_pipeline_keeps_legacy_merged_view(self):
        result = run_alert_pipeline(
            self.alerts, "orders",
            [_obs(delta=0.4), _obs(delta=0.02),
             {"field": "amount", "metric": "mean", "delta": 0.9}],
            baseline_version=1, observed_version=2, threshold=0.1,
            batch_id="b2",
        )
        self.assertEqual(len(result["merged"]), 2)
        self.assertEqual(result["summary"]["opened"], 2)
        self.assertEqual(len(result["alerts"]), 2)

    def test_legacy_merge_alerts_unchanged(self):
        merged = merge_alerts(
            [{"field": "a", "metric": "m", "delta": 0.5},
             {"field": "a", "metric": "m", "delta": 0.2},
             {"field": "b", "metric": "m", "delta": 0.01}],
            threshold=0.1,
        )
        self.assertEqual(merged, [{"field": "a", "metric": "m", "delta": 0.5}])


class ApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        cls.db_path = str(Path(cls._tmp.name) / "api.db")
        cls.server_thread = threading.Thread(
            target=serve, args=(cls.db_path, 9876), daemon=True
        )
        cls.server_thread.start()
        import time
        for _ in range(50):
            try:
                urllib.request.urlopen("http://127.0.0.1:9876/health", timeout=1)
                break
            except (urllib.error.URLError, RemoteDisconnected):
                time.sleep(0.1)

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def setUp(self):
        self.baselines = BaselineStore(self.db_path)
        self.alerts = AlertStore(self.db_path)
        self.base = "http://127.0.0.1:9876"

    @staticmethod
    def _get(path):
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:9876{path}") as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    @staticmethod
    def _post(path, body):
        request = urllib.request.Request(
            f"http://127.0.0.1:9876{path}",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def test_health_and_tables(self):
        status, body = self._get("/health")
        self.assertEqual(status, 200)
        self.baselines.save("orders", {"row_count": 1})
        status, body = self._get("/tables")
        self.assertIn("orders", body["tables"])

    def test_baseline_endpoint_kept_compatible(self):
        self.baselines.save("widgets_t", {"row_count": 7})
        status, body = self._get("/baseline/widgets_t")
        self.assertEqual(status, 200)
        self.assertEqual(body["baseline"]["row_count"], 7)
        status, body = self._get("/baseline/widgets_t?version=1")
        self.assertEqual(status, 200)
        status, body = self._get("/baseline/ghost_t")
        self.assertEqual(status, 404)
        self.assertEqual(body["status"], 404)

    def test_unknown_table_returns_404_but_empty_alerts_returns_200(self):
        self.baselines.save("quiet_t", {"row_count": 1})
        status, body = self._get("/alerts?table=quiet_t")
        self.assertEqual(status, 200)
        self.assertEqual(body["items"], [])
        self.assertEqual(body["pagination"]["count"], 0)

        status, body = self._get("/alerts?table=unknown_table")
        self.assertEqual(status, 404)
        self.assertIn("not found", body["error"])

    def test_alert_query_filters_pagination_and_actions(self):
        for i in range(3):
            self.alerts.ingest_observations(
                "lifecycle_t",
                [{"field": f"col{i}", "metric": "missing_rate", "delta": 0.5}],
                batch_id=f"lc-{i}",
            )
        status, body = self._get("/alerts?table=lifecycle_t&limit=2&offset=0")
        self.assertEqual(status, 200)
        self.assertEqual(len(body["items"]), 2)
        self.assertEqual(body["pagination"]["count"], 3)
        self.assertTrue(body["pagination"]["has_more"])

        status, body = self._get("/alerts?table=lifecycle_t&field=col1")
        self.assertEqual(len(body["items"]), 1)

        status, body = self._get("/alerts?table=lifecycle_t&status=new,active")
        self.assertEqual(status, 200)
        self.assertEqual(body["pagination"]["count"], 3)

        status, body = self._get("/alerts?table=lifecycle_t&status=nope")
        self.assertEqual(status, 400)
        status, body = self._get("/alerts?table=lifecycle_t&limit=abc")
        self.assertEqual(status, 400)

        _, page = self._get("/alerts?table=lifecycle_t&field=col0")
        aid = page["items"][0]["alert_id"]

        status, body = self._get(f"/alerts/{aid}")
        self.assertEqual(status, 200)
        self.assertEqual(body["field"], "col0")

        status, body = self._post(f"/alerts/{aid}/acknowledge", {})
        self.assertEqual(status, 200)
        self.assertIsNotNone(body["acknowledged_at"])

        until = (
            datetime.now(timezone.utc) + timedelta(hours=2)
        ).isoformat(timespec="seconds")
        status, body = self._post(f"/alerts/{aid}/suppress", {"until": until})
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], STATUS_SUPPRESSED)

        status, body = self._get(f"/alerts/{aid}/events")
        self.assertEqual(status, 200)
        self.assertTrue(
            {e["event_type"] for e in body["events"]} >= {"opened", "acknowledged", "suppressed"}
        )

        status, body = self._get("/alerts/deadbeefdeadbeef")
        self.assertEqual(status, 404)
        status, body = self._post("/alerts/deadbeefdeadbeef/acknowledge", {})
        self.assertEqual(status, 404)

        status, body = self._post(
            f"/alerts/{aid}/suppress",
            {"until": "2000-01-01T00:00:00+00:00"},
        )
        self.assertEqual(status, 400)


if __name__ == "__main__":
    unittest.main()
