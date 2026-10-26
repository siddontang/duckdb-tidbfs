"""A test relay must not inject transport faults into healthy read bursts."""
from pathlib import Path
import socket
import sys
import threading
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from fault_proxy import FaultProxy


class FaultProxyTests(unittest.TestCase):
    def test_connection_burst_survives_paused_accept(self):
        proxy = FaultProxy('http://127.0.0.1:9', objects=True)
        paused, release = threading.Event(), threading.Event()
        original = proxy.server._handle_request_noblock

        def accept():
            paused.set()
            release.wait(timeout=10)
            original()

        # Gate the accept loop, so the burst deterministically exercises the
        # listening queue instead of depending on Python thread scheduling.
        proxy.server._handle_request_noblock = accept
        connections = []
        address = proxy.server.server_address
        try:
            connections.append(socket.create_connection(address, timeout=1))
            self.assertTrue(paused.wait(timeout=3), 'accept loop did not reach gate')
            for _ in range(31):
                connections.append(socket.create_connection(address, timeout=1))
        finally:
            release.set()
            for connection in connections:
                connection.close()
            proxy.close()


if __name__ == '__main__':
    unittest.main()
