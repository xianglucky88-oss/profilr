import unittest

from profilr.distribution import quantile
from profilr.drift import absolute_drift, psi
from profilr.notify import merge_alerts
from profilr.profile import profile_rows


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


if __name__ == "__main__":
    unittest.main()
