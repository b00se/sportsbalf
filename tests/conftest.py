"""Shared pytest fixtures."""

from __future__ import annotations

import os
import socket

import pytest


@pytest.fixture(autouse=True)
def block_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """Block outbound sockets in tests unless explicitly enabled."""

    if os.getenv("ALLOW_NETWORK_TESTS") == "1":
        return

    def _blocked(*args, **kwargs):
        raise RuntimeError("Network access is disabled during tests")

    original_socket = socket.socket

    class GuardedSocket(original_socket):
        def connect(self, *args, **kwargs):
            _blocked(*args, **kwargs)

        def connect_ex(self, *args, **kwargs):
            _blocked(*args, **kwargs)

    monkeypatch.setattr(socket, "socket", GuardedSocket)
    monkeypatch.setattr(socket, "create_connection", _blocked)
