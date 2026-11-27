import asyncio
import sys
from pathlib import Path

import pytest
from mcp import types
from vision_agents.core.mcp import MCPConnectionState, MCPServerLocal

ECHO_SERVER = Path(__file__).with_name("echo_mcp_server.py")


@pytest.fixture
def server() -> MCPServerLocal:
    return MCPServerLocal(command=f"{sys.executable} {ECHO_SERVER}")


class TestMCPServerLocal:
    async def test_a_tool_on_the_server_can_be_called(self, server: MCPServerLocal):
        async with server:
            tools = await server.list_tools()
            result = await server.call_tool("echo", {"text": "hello"})

        assert [tool.name for tool in tools] == ["echo"]
        assert isinstance(result.content[0], types.TextContent)
        assert result.content[0].text == "hello"
        assert not server.is_connected

    async def test_disconnecting_from_another_task_leaves_the_caller_running(
        self, server: MCPServerLocal
    ):
        # An agent closes on a task of its own, not the one that connected. The stdio
        # transport's cancel scopes must still be exited where they were entered, or the
        # cancellation lands on whoever connected.
        await server.connect()

        await asyncio.create_task(server.disconnect())
        await asyncio.sleep(0.1)

        assert not server.is_connected

    async def test_a_disconnected_server_can_connect_again(
        self, server: MCPServerLocal
    ):
        await server.connect()
        await server.disconnect()

        async with server:
            result = await server.call_tool("echo", {"text": "again"})

        assert isinstance(result.content[0], types.TextContent)
        assert result.content[0].text == "again"

    async def test_concurrent_connect_shares_single_connection(
        self, server: MCPServerLocal
    ):
        await asyncio.wait_for(
            asyncio.gather(server.connect(), server.connect()),
            timeout=5.0,
        )
        try:
            assert server.is_connected
            assert server.connection_state == MCPConnectionState.CONNECTED
            result = await server.call_tool("echo", {"text": "concurrent"})
            assert isinstance(result.content[0], types.TextContent)
            assert result.content[0].text == "concurrent"
        finally:
            await server.disconnect()

    async def test_concurrent_tool_calls_auto_connect_cleanly(
        self, server: MCPServerLocal
    ):
        r1, r2 = await asyncio.wait_for(
            asyncio.gather(
                server.call_tool("echo", {"text": "one"}),
                server.call_tool("echo", {"text": "two"}),
            ),
            timeout=5.0,
        )
        try:
            assert isinstance(r1.content[0], types.TextContent)
            assert isinstance(r2.content[0], types.TextContent)
            assert r1.content[0].text == "one"
            assert r2.content[0].text == "two"
            assert server.is_connected
        finally:
            await server.disconnect()

    async def test_idle_timeout_disconnects_cleanly_without_self_cancelling(self):
        server = MCPServerLocal(
            command=f"{sys.executable} {ECHO_SERVER}",
            session_timeout=0.05,
        )
        await server.connect()
        timeout_task = server._timeout_task
        supervisor_task = server._supervisor_task
        assert timeout_task is not None
        assert supervisor_task is not None

        # Wait for the idle timeout monitor to finish disconnecting (deadline, not sleep).
        await asyncio.wait_for(timeout_task, timeout=5.0)

        assert not timeout_task.cancelled()
        assert timeout_task.cancelling() == 0
        assert supervisor_task.done()
        assert not server.is_connected
        assert server.connection_state == MCPConnectionState.DISCONNECTED

        # Server must be reusable after an idle-timeout disconnect.
        async with server:
            result = await server.call_tool("echo", {"text": "after-timeout"})
        assert isinstance(result.content[0], types.TextContent)
        assert result.content[0].text == "after-timeout"
