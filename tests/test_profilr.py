import unittest
from pathlib import Path

from profilr.distribution import Reservoir, histogram, histogram_edges, quantile
from profilr.drift import absolute_drift, psi
from profilr.notify import merge_alerts
from profilr.profile import ProfileConfig, profile_csv, profile_rows
from profilr.validate import check_profile

DATA = Path(__file__).resolve().parents[1] / "data"


class ProfilrTests(unittest.TestCase):
    def test_profile_counts_missing(self):
        result = profile_rows([{"amount": "1"}, {"amount": ""}])
        self.assertEqual(result["row_count"], 2)
        self.assertEqual(result["fields"]["amount"]["missing"], 1)

    def test_distribution(self):
        self.assertEqual(quantile([1, 2, 3, 4], 0.5), 2.5)

    def test_drift_and_alert_merge(self):
        before = profile_rows([{"x": "1"}, {"x": "2"}])
        after = profile_rows([{"x": "1"}, {"x": ""}])
        alerts = merge_alerts(absolute_drift(before, after), threshold=0.1)
        self.assertEqual(len(alerts), 1)

    def test_psi_is_non_negative(self):
        self.assertGreaterEqual(psi([1, 2, 3], [1, 2, 4]), 0)


class DistributionTests(unittest.TestCase):
    def test_quantile_empty_is_none(self):
        self.assertIsNone(quantile([], 0.5))

    def test_quantile_boundaries(self):
        self.assertEqual(quantile([3, 1, 2], 0.0), 1)
        self.assertEqual(quantile([3, 1, 2], 1.0), 3)

    def test_histogram_out_of_range_is_counted(self):
        edges = histogram_edges(0.0, 1.0, bins=2)
        result = histogram([-1.0, 0.25, 0.75, 2.0], edges)
        self.assertEqual(result["counts"], [1, 1])
        self.assertEqual(result["underflow"], 1)
        self.assertEqual(result["overflow"], 1)

    def test_histogram_boundary_goes_right_last_bin_closed(self):
        edges = [0.0, 0.5, 1.0]
        result = histogram([0.5, 1.0], edges)
        self.assertEqual(result["counts"], [0, 2])

    def test_constant_column_edges_stay_increasing(self):
        edges = histogram_edges(2.0, 2.0, bins=4)
        self.assertTrue(all(right > left for left, right in zip(edges, edges[1:])))
        self.assertEqual(histogram([2.0, 2.0], edges)["counts"], [0, 0, 2, 0])

    def test_reservoir_is_bounded_and_deterministic(self):
        first, second = Reservoir(5, seed=1), Reservoir(5, seed=1)
        for value in range(100):
            first.add(value)
            second.add(value)
        self.assertEqual(len(first.values), 5)
        self.assertTrue(first.approximate)
        self.assertEqual(first.values, second.values)


class TypeAwareProfileTests(unittest.TestCase):
    def test_mixed_column_degrades_to_number_with_invalid(self):
        result = profile_rows([{"a": "1"}, {"a": "2"}, {"a": "abc"}])
        field = result["fields"]["a"]
        self.assertEqual(field["type"], "number")
        self.assertEqual(field["status"], "degraded")
        self.assertEqual(field["invalid"], 1)
        self.assertEqual(field["summary"]["count"], 2)
        self.assertEqual(check_profile(result), [])

    def test_mixed_column_tie_degrades_to_string(self):
        result = profile_rows([{"a": "1"}, {"a": "x"}])
        field = result["fields"]["a"]
        self.assertEqual(field["type"], "string")
        self.assertEqual(field["status"], "degraded")
        self.assertEqual(field["summary"]["numeric_like"], 1)

    def test_non_finite_numbers_are_invalid_not_missing(self):
        rows = [{"a": v} for v in ("1", "2", "3", "1e309", "inf", "nan")]
        field = profile_rows(rows)["fields"]["a"]
        self.assertEqual(field["type"], "number")
        self.assertEqual(field["status"], "degraded")
        self.assertEqual(field["missing"], 1)   # "nan" 是缺失标记
        self.assertEqual(field["invalid"], 2)   # "1e309"/"inf" 是非法数字
        self.assertEqual(field["summary"]["max"], 3.0)

    def test_all_missing_column(self):
        result = profile_rows([{"a": ""}, {"a": "N/A"}])
        field = result["fields"]["a"]
        self.assertEqual(field["type"], "unknown")
        self.assertEqual(field["status"], "all_missing")
        self.assertIsNone(field["summary"])
        self.assertEqual(check_profile(result), [])

    def test_empty_rows_with_header_columns(self):
        result = profile_rows([], columns=["a", "b"])
        self.assertEqual(result["status"], "empty")
        self.assertEqual(result["fields"]["a"]["status"], "empty")
        self.assertIsNone(result["fields"]["a"]["missing_rate"])
        self.assertEqual(check_profile(result), [])

    def test_schema_changes_with_reference(self):
        baseline = profile_rows([{"a": "1", "b": "x"}])
        current = profile_rows([{"a": "2", "c": "y"}], reference=baseline)
        self.assertEqual(current["schema"]["dropped"], ["b"])
        self.assertEqual(current["schema"]["added"], ["c"])
        self.assertEqual(current["fields"]["b"]["status"], "dropped")
        self.assertIsNone(current["fields"]["b"]["missing_rate"])
        self.assertEqual(check_profile(current), [])

    def test_type_change_is_reported(self):
        baseline = profile_rows([{"a": "1"}])
        current = profile_rows([{"a": "text"}], reference=baseline)
        self.assertEqual(current["schema"]["type_changed"],
                         [{"field": "a", "from": "number", "to": "string"}])

    def test_histogram_edges_reused_from_reference(self):
        baseline = profile_rows([{"a": "1"}, {"a": "3"}])
        current = profile_rows([{"a": "10"}], reference=baseline)
        hist = current["fields"]["a"]["summary"]["histogram"]
        self.assertEqual(hist["edges_source"], "baseline")
        self.assertEqual(hist["edges"], baseline["fields"]["a"]["summary"]["histogram"]["edges"])
        self.assertEqual(hist["overflow"], 1)
        self.assertEqual(check_profile(current), [])

    def test_string_summary_top_and_numeric_like(self):
        rows = [{"s": "apple"}, {"s": "apple"}, {"s": "42"}, {"s": "banana"}]
        field = profile_rows(rows)["fields"]["s"]
        self.assertEqual(field["status"], "degraded")
        self.assertEqual(field["summary"]["distinct"], 3)
        self.assertEqual(field["summary"]["top"][0], {"value": "apple", "count": 2})
        self.assertEqual(field["summary"]["numeric_like"], 1)

    def test_distinct_cap_marks_lower_bound(self):
        config = ProfileConfig(max_distinct=2)
        rows = [{"s": f"v{i}"} for i in range(5)]
        summary = profile_rows(rows, config=config)["fields"]["s"]["summary"]
        self.assertEqual(summary["distinct"], 2)
        self.assertFalse(summary["distinct_exact"])

    def test_reservoir_bounds_memory_and_stays_deterministic(self):
        config = ProfileConfig(max_numeric_values=10)
        rows = [{"n": str(i)} for i in range(1000)]
        first = profile_rows(rows, config=config)["fields"]["n"]["summary"]
        second = profile_rows(rows, config=config)["fields"]["n"]["summary"]
        self.assertTrue(first["approximate"])
        self.assertEqual(first["sample_count"], 10)
        self.assertEqual(first["count"], 1000)
        self.assertEqual(first["min"], 0.0)
        self.assertEqual(first["max"], 999.0)
        self.assertEqual(first, second)

    def test_mixed_fixture_is_consistent(self):
        profile = profile_csv(DATA / "mixed.csv")
        self.assertEqual(check_profile(profile), [])
        amount = profile["fields"]["amount"]
        self.assertEqual(amount["status"], "degraded")
        self.assertEqual(amount["invalid"], 1)
        self.assertEqual(amount["summary"]["max"], 1000000.5)

    def test_empty_fixture_reports_header_columns(self):
        profile = profile_csv(DATA / "empty.csv")
        self.assertEqual(profile["status"], "empty")
        self.assertEqual(sorted(profile["fields"]), ["amount", "country", "id"])
        self.assertEqual(check_profile(profile), [])

    def test_drift_flags_dropped_column(self):
        before = profile_rows([{"a": "1", "b": "2"}])
        after = profile_rows([{"a": "1"}], reference=before)
        alerts = merge_alerts(absolute_drift(before, after), threshold=0.1)
        self.assertEqual(alerts, [{"field": "b", "metric": "schema", "delta": 1.0}])


if __name__ == "__main__":
    unittest.main()
