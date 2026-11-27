"""Unit tests for MCPBaseServer connection state machine and lifecycle."""

import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import Optional, cast

import pytest
from mcp import ClientSession
from vision_agents.core.mcp import MCPBaseServer, MCPConnectionState


class _FakeSession:
    """Minimal stand-in for ClientSession used to test lifecycle state."""


class _GatedMCPServer(MCPBaseServer):
    """Concrete MCPBaseServer with explicit async gates for deterministic testing."""

    def __init__(
        self,
        session_timeout: float = 300.0,
        gate_open: bool = False,
        gate_close: bool = False,
        open_error: Optional[Exception] = None,
    ):
        super().__init__(session_timeout=session_timeout)
        self.gate_open = gate_open
        self.gate_close = gate_close
        self.open_error = open_error
        self.open_started = asyncio.Event()
        self.allow_open = asyncio.Event()
        self.close_started = asyncio.Event()
        self.allow_close = asyncio.Event()
        self.open_count = 0
        self.close_count = 0

    @property
    def _supervisor_name(self) -> str:
        return "mcp-supervisor:gated-test"

    @property
    def _server_description(self) -> str:
        return "gated test MCP server"

    @contextlib.asynccontextmanager
    async def _open_session(self) -> AsyncIterator[ClientSession]:
        self.open_count += 1
        self.open_started.set()
        if self.gate_open:
            await self.allow_open.wait()
        if self.open_error is not None:
            raise self.open_error
        try:
            yield cast(ClientSession, _FakeSession())
        finally:
            self.close_started.set()
            if self.gate_close:
                await self.allow_close.wait()
            self.close_count += 1


@pytest.fixture
def gated_open_server() -> _GatedMCPServer:
    return _GatedMCPServer(gate_open=True)


@pytest.fixture
def gated_close_server() -> _GatedMCPServer:
    return _GatedMCPServer(gate_close=True)


class TestMCPBaseServer:
    async def test_initial_state_is_disconnected(self):
        server = _GatedMCPServer()

        assert server.connection_state == MCPConnectionState.DISCONNECTED
        assert not server.is_connected

    async def test_concurrent_connect_spawns_single_session_and_keeps_connection_alive(
        self, gated_open_server: _GatedMCPServer
    ):
        server = gated_open_server
        t1 = asyncio.create_task(server.connect())
        await asyncio.wait_for(server.open_started.wait(), timeout=2.0)

        assert server.connection_state == MCPConnectionState.CONNECTING
        assert not server.is_connected

        t2 = asyncio.create_task(server.connect())
        await asyncio.sleep(0)
        assert server.open_count == 1

        server.allow_open.set()
        await asyncio.wait_for(asyncio.gather(t1, t2), timeout=2.0)

        try:
            assert server.open_count == 1
            assert server.close_count == 0
            assert server.connection_state == MCPConnectionState.CONNECTED
            assert server.is_connected
            assert server._session is not None
        finally:
            await server.disconnect()

        assert server.connection_state == MCPConnectionState.DISCONNECTED
        assert server.close_count == 1

    async def test_cancelling_joining_connect_does_not_abort_in_flight_connect(
        self, gated_open_server: _GatedMCPServer
    ):
        server = gated_open_server
        t1 = asyncio.create_task(server.connect())
        await asyncio.wait_for(server.open_started.wait(), timeout=2.0)

        t2 = asyncio.create_task(server.connect())
        await asyncio.sleep(0)
        t2.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t2

        assert server.connection_state == MCPConnectionState.CONNECTING
        server.allow_open.set()
        await asyncio.wait_for(t1, timeout=2.0)

        try:
            assert server.connection_state == MCPConnectionState.CONNECTED
            assert server.open_count == 1
        finally:
            await server.disconnect()

    async def test_connect_failure_propagates_to_concurrent_callers_and_resets_state(
        self, gated_open_server: _GatedMCPServer
    ):
        server = gated_open_server
        server.open_error = RuntimeError("connection refused")

        t1 = asyncio.create_task(server.connect())
        await asyncio.wait_for(server.open_started.wait(), timeout=2.0)
        t2 = asyncio.create_task(server.connect())
        await asyncio.sleep(0)

        server.allow_open.set()
        results = await asyncio.wait_for(
            asyncio.gather(t1, t2, return_exceptions=True), timeout=2.0
        )

        assert all(
            isinstance(r, RuntimeError) and str(r) == "connection refused"
            for r in results
        )
        assert server.open_count == 1
        assert server.connection_state == MCPConnectionState.DISCONNECTED
        assert not server.is_connected

        server.open_error = None
        server.gate_open = False
        await server.connect()
        try:
            assert server.connection_state == MCPConnectionState.CONNECTED
            assert server.open_count == 2
        finally:
            await server.disconnect()

    async def test_connect_during_disconnect_waits_for_teardown_then_reconnects(
        self, gated_close_server: _GatedMCPServer
    ):
        server = gated_close_server
        await server.connect()
        assert server.connection_state == MCPConnectionState.CONNECTED

        disconnect_task = asyncio.create_task(server.disconnect())
        await asyncio.wait_for(server.close_started.wait(), timeout=2.0)
        assert server.connection_state == MCPConnectionState.DISCONNECTING

        reconnect_task = asyncio.create_task(server.connect())
        await asyncio.sleep(0)
        assert not reconnect_task.done()
        assert server.open_count == 1

        server.gate_close = False
        server.allow_close.set()
        await asyncio.wait_for(
            asyncio.gather(disconnect_task, reconnect_task), timeout=2.0
        )

        try:
            assert server.connection_state == MCPConnectionState.CONNECTED
            assert server.open_count == 2
            assert server.close_count == 1
        finally:
            await server.disconnect()

    async def test_disconnect_during_connect_aborts_cleanly_to_disconnected(
        self, gated_open_server: _GatedMCPServer
    ):
        server = gated_open_server
        connect_task = asyncio.create_task(server.connect())
        await asyncio.wait_for(server.open_started.wait(), timeout=2.0)
        assert server.connection_state == MCPConnectionState.CONNECTING

        await asyncio.wait_for(server.disconnect(), timeout=2.0)

        with pytest.raises(asyncio.CancelledError):
            await connect_task

        assert server.connection_state == MCPConnectionState.DISCONNECTED
        assert not server.is_connected

    async def test_concurrent_disconnect_waits_for_single_teardown(
        self, gated_close_server: _GatedMCPServer
    ):
        server = gated_close_server
        await server.connect()

        d1 = asyncio.create_task(server.disconnect())
        await asyncio.wait_for(server.close_started.wait(), timeout=2.0)
        d2 = asyncio.create_task(server.disconnect())
        await asyncio.sleep(0)
        assert not d2.done()

        server.allow_close.set()
        await asyncio.wait_for(asyncio.gather(d1, d2), timeout=2.0)

        assert server.close_count == 1
        assert server.connection_state == MCPConnectionState.DISCONNECTED

    async def test_idle_timeout_disconnects_cleanly_without_cancelling_monitor_task(
        self,
    ):
        server = _GatedMCPServer(session_timeout=0.05)
        await server.connect()
        timeout_task = server._timeout_task
        supervisor_task = server._supervisor_task
        assert timeout_task is not None
        assert supervisor_task is not None

        await asyncio.wait_for(timeout_task, timeout=2.0)

        assert not timeout_task.cancelled()
        assert timeout_task.cancelling() == 0
        assert supervisor_task.done()
        assert server.close_count == 1
        assert server.connection_state == MCPConnectionState.DISCONNECTED
        assert not server.is_connected
