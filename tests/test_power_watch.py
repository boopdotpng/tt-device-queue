import unittest

from power_watch import summarize_samples


class PowerWatchTest(unittest.TestCase):
  def test_summarize_samples_reports_avg_min_max(self):
    summary = summarize_samples([118.0, 121.5, 123.0], 3.0, 150.0)

    self.assertIn("Board power over 3.0s:", summary)
    self.assertIn("limit=150.0W", summary)
    self.assertIn("avg=120.8W", summary)
    self.assertIn("min=118.0W", summary)
    self.assertIn("max=123.0W", summary)
    self.assertIn("samples=3", summary)


if __name__ == "__main__":
  unittest.main()
