from __future__ import annotations

import inspect
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import mcp_server


class McpSurfaceTest(unittest.IsolatedAsyncioTestCase):
    def test_compatibility_signatures(self) -> None:
        self.assertEqual(list(inspect.signature(mcp_server.queue).parameters), ["cmd", "cwd", "repeat"])
        self.assertEqual(
            list(inspect.signature(mcp_server.queue_python).parameters),
            ["script", "cwd", "repeat", "python", "args"],
        )
        for name in ("queue", "queue_python", "job", "logs", "result", "status", "kill", "reset"):
            self.assertTrue(hasattr(mcp_server, name), name)
        for name in ("cancel", "last_breakage"):
            self.assertFalse(hasattr(mcp_server, name), name)

    async def test_queue_payload_preserves_client_fairness_shape(self) -> None:
        response = {
            "job_id": "a" * 32, "output_file": "/tmp/output", "position": 0,
            "estimated_wait_sec": 0, "estimated_run_sec": 10, "timeout": 3600,
        }
        with patch("mcp_server._post", new=AsyncMock(return_value=response)) as post:
            result = json.loads(await mcp_server.queue("python x.py", "/repo", 2))
        self.assertEqual(result["job_id"], "a" * 32)
        post.assert_awaited_once_with("/queue", {
            "cmd": "python x.py", "cwd": "/repo", "repeat": 2,
            "mode": "run", "client_id": mcp_server.CLIENT_ID,
        })

    async def test_result_reads_bounded_logs_after_async_wait(self) -> None:
        metadata = {
            "status": "done", "exit_code": 0, "elapsed": 1.2,
            "output_file": "/tmp/output", "timed_out": False,
        }
        with (
            patch("mcp_server._wait_for_job", new=AsyncMock(return_value=metadata)),
            patch("mcp_server._read_result_logs", new=AsyncMock(return_value=("hello", True))),
        ):
            result = await mcp_server.result("job1")
        self.assertIn("Status: OK", result)
        self.assertIn("hello", result)
        self.assertIn("Output truncated", result)

    async def test_queue_python_removes_script_when_submission_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with (
                patch("mcp_server.SCRIPT_DIR", Path(directory)),
                patch("mcp_server._post", new=AsyncMock(side_effect=RuntimeError("no queue"))),
            ):
                with self.assertRaises(RuntimeError):
                    await mcp_server.queue_python("print('x')")
            self.assertEqual(list(Path(directory).glob("*.py")), [])


if __name__ == "__main__":
    unittest.main()
