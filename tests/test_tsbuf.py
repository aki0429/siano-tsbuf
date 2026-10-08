"""Linux ラッパのバイト透過性と終了契約を合成データだけで検証する。"""

import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from synthetic_child import payload

ROOT = Path(__file__).resolve().parent.parent
BINARY = Path(os.environ.get('TSBUF_BINARY', ROOT / 'build/siano-tsbuf')).resolve()
CHILD = Path(__file__).with_name('synthetic_child.py')


class BufferTests(unittest.TestCase):
    def run_buffer(self, *child_args: str, options: tuple[str, ...] = ()) -> subprocess.CompletedProcess:
        # 各試験に有限 timeout を設け、旧版の無限停止も通常の失敗として観測する。
        env = os.environ.copy()
        env.pop('SIANO_TSBUF_MB', None)
        env.pop('SIANO_TSBUF_LOG', None)
        return subprocess.run(
            [str(BINARY), '-l', '-', *options, '--', sys.executable, str(CHILD), *child_args],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=8,
            env=env,
            check=False,
        )

    def test_stalled_consumer_does_not_stall_child_within_capacity(self) -> None:
        with tempfile.TemporaryDirectory(dir=ROOT / 'build') as directory:
            marker = Path(directory) / 'done'
            process = subprocess.Popen(
                [str(BINARY), '-b', '4', '-l', '-', '--', sys.executable, str(CHILD),
                 '--packets', '12000', '--tail', '17', '--done', str(marker)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            try:
                deadline = time.monotonic() + 3
                while not marker.exists() and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertTrue(marker.exists(), 'child could not finish while stdout was unread')
                stdout, stderr = process.communicate(timeout=5)
                self.assertEqual(process.returncode, 0, stderr)
                self.assertEqual(stdout, payload(12000, 17))
                self.assertIn(b'dropped_bytes=0', stderr)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate(timeout=3)

    def test_help_and_version(self) -> None:
        for option in ('--help', '--version'):
            with self.subTest(option=option):
                result = subprocess.run([str(BINARY), option], capture_output=True, timeout=3, check=False)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout, b'')
                self.assertIn(b'siano-tsbuf', result.stderr)
        result = subprocess.run([str(BINARY), '--version'], capture_output=True, timeout=3, check=False)
        self.assertIn(b'0.1.0-beta.1', result.stderr)

    def test_invalid_capacity_is_rejected(self) -> None:
        values = ('0', '3', '257', '-1', '+4', '4.0', '4x', ' 4', '', '9999999999999999999999')
        for value in values:
            with self.subTest(value=value):
                result = self.run_buffer('--packets', '0', options=('-b', value))
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertEqual(result.stdout, b'')

    def test_environment_capacity_is_validated(self) -> None:
        for value in ('0', '257', 'junk'):
            env = os.environ.copy()
            env['SIANO_TSBUF_MB'] = value
            result = subprocess.run(
                [str(BINARY), '-l', '-', '--', sys.executable, str(CHILD), '--packets', '0'],
                capture_output=True, timeout=3, env=env, check=False,
            )
            self.assertEqual(result.returncode, 2, result.stderr)
            self.assertEqual(result.stdout, b'')

    def test_default_and_boundary_capacities_are_logged_exactly(self) -> None:
        for options, capacity in (((), 32 << 20), (('-b', '4'), 4 << 20), (('-b', '256'), 256 << 20)):
            result = self.run_buffer('--packets', '1', options=options)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn(f'capacity_bytes={capacity}'.encode(), result.stderr)
            self.assertIn(b'peak_bytes=', result.stderr)
            self.assertIn(b'dropped_bytes=0', result.stderr)

    def test_partial_packet_is_preserved(self) -> None:
        result = self.run_buffer('--packets', '1000', '--tail', '137', '--chunks', '1,187,189,4093')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(result.stdout), len(payload(1000, 137)))
        self.assertEqual(result.stdout, payload(1000, 137))

    def test_aligned_packets_are_exact(self) -> None:
        result = self.run_buffer('--packets', '4096', '--chunks', '131,4096,8193')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, payload(4096, 0))

    def test_child_exit_code_passes_through(self) -> None:
        result = self.run_buffer('--packets', '32', '--exit-code', '23')
        self.assertEqual(result.returncode, 23, result.stderr)
        self.assertEqual(result.stdout, payload(32, 0))

    def start_hanging(self, directory: str) -> tuple[subprocess.Popen, int]:
        # 子 PID をファイル経由で受け取り、ラッパ終了後に子が残っていないかを検査する。
        pidfile = Path(directory) / 'child.pid'
        process = subprocess.Popen(
            [str(BINARY), '-l', '-', '--', sys.executable, str(CHILD), '--packets', '4', '--hang',
             '--pidfile', str(pidfile)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 3
        while not pidfile.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertTrue(pidfile.exists())
        time.sleep(0.1)
        return process, int(pidfile.read_text())

    def assert_gone(self, pid: int) -> None:
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return
            # zombie は親 (init) の回収待ちなので終了扱い
            try:
                if Path(f'/proc/{pid}/stat').read_text().split()[2] == 'Z':
                    return
            except FileNotFoundError:
                return
            time.sleep(0.05)
        os.kill(pid, 9)
        self.fail(f'child {pid} survived its wrapper (orphan holding the tuner)')

    def test_sigkill_of_wrapper_kills_child(self) -> None:
        with tempfile.TemporaryDirectory(dir=ROOT / 'build') as directory:
            process, pid = self.start_hanging(directory)
            process.kill()
            process.wait(timeout=3)
            self.assert_gone(pid)

    def test_sigterm_of_wrapper_stops_child_and_returns_143(self) -> None:
        with tempfile.TemporaryDirectory(dir=ROOT / 'build') as directory:
            process, pid = self.start_hanging(directory)
            process.terminate()
            self.assertEqual(process.wait(timeout=5), 143)
            self.assert_gone(pid)

    def test_missing_executable_fails_nonzero(self) -> None:
        result = subprocess.run([str(BINARY), '-l', '-', '--', '/nonexistent/tuner-command'],
                                capture_output=True, timeout=5, check=False)
        self.assertEqual(result.returncode, 127, result.stderr)
        self.assertEqual(result.stdout, b'')


if __name__ == '__main__':
    unittest.main()
