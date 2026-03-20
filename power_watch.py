#!/usr/bin/env python3
"""Sample Blackhole board power for a short fixed window."""

from __future__ import annotations

import statistics
import sys
import time

SAMPLE_DURATION_SEC = 3.0
SAMPLE_INTERVAL_SEC = 0.2


def collect_board_power_samples(
  duration_sec: float = SAMPLE_DURATION_SEC,
  interval_sec: float = SAMPLE_INTERVAL_SEC,
) -> tuple[list[float], float | None]:
  from pyluwen import detect_chips

  chips = detect_chips(local_only=True)
  bh = None
  for chip in chips:
    bh = chip.as_bh()
    if bh:
      break

  if bh is None:
    raise RuntimeError("No Blackhole chip found")

  telemetry = bh.get_telemetry()
  board_limit = telemetry.board_power_limit
  samples = [float(telemetry.input_power)]
  deadline = time.monotonic() + duration_sec

  while True:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
      break
    time.sleep(min(interval_sec, remaining))
    telemetry = bh.get_telemetry()
    samples.append(float(telemetry.input_power))

  return samples, float(board_limit) if board_limit is not None else None


def summarize_samples(samples: list[float], duration_sec: float, board_limit: float | None = None) -> str:
  if not samples:
    raise ValueError("No power samples collected")

  avg_w = statistics.fmean(samples)
  min_w = min(samples)
  max_w = max(samples)
  limit_text = f" limit={board_limit:.1f}W" if board_limit is not None else ""
  return (
    f"Board power over {duration_sec:.1f}s:{limit_text} "
    f"avg={avg_w:.1f}W min={min_w:.1f}W max={max_w:.1f}W samples={len(samples)}"
  )


def main() -> int:
  try:
    samples, board_limit = collect_board_power_samples()
    print(summarize_samples(samples, SAMPLE_DURATION_SEC, board_limit))
    return 0
  except Exception as exc:
    print(f"power_watch failed: {exc}", file=sys.stderr)
    return 1


if __name__ == "__main__":
  raise SystemExit(main())
