import unittest
from pathlib import Path
from unittest.mock import patch

from queue_client import QueueClientError, run_tt_smi_snapshot


class PowerSnapshotTest(unittest.TestCase):
  @patch("queue_client.subprocess.run")
  def test_run_tt_smi_snapshot_uses_snapshot_option(self, mock_run):
    mock_run.return_value.returncode = 0
    mock_run.return_value.stdout = "Device snapshot\n  TDP 120 W\n"
    mock_run.return_value.stderr = ""

    output = run_tt_smi_snapshot(Path("/tmp/blackhole-py/tt-smi.py"), device=1)

    self.assertEqual(output, "Device snapshot\n  TDP 120 W")
    mock_run.assert_called_once_with(
      ["/tmp/blackhole-py/tt-smi.py", "--snapshot", "1"],
      cwd="/tmp/blackhole-py",
      capture_output=True,
      text=True,
    )

  @patch("queue_client.subprocess.run")
  def test_run_tt_smi_snapshot_reports_errors(self, mock_run):
    mock_run.return_value.returncode = 1
    mock_run.return_value.stdout = ""
    mock_run.return_value.stderr = "no Blackhole PCIe devices found"

    with self.assertRaisesRegex(QueueClientError, "no Blackhole PCIe devices found"):
      run_tt_smi_snapshot(Path("/tmp/blackhole-py/tt-smi.py"))


if __name__ == "__main__":
  unittest.main()
