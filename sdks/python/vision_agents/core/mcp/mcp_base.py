"""Base class for MCP server connections."""

import asyncio
import logging
from abc import ABC, abstractmethod
from contextlib import AbstractAsyncContextManager
from enum import Enum
from typing import Any, Dict, List, Optional

from mcp import ClientSession, types
from mcp.types import AnyUrl

from ..utils.utils import cancel_and_wait


class MCPConnectionState(Enum):
    """Lifecycle states of an MCP server connection."""

    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    DISCONNECTING = "disconnecting"


class MCPBaseServer(ABC):
    """Base class for MCP server connections."""

    def __init__(self, session_timeout: float = 300.0):
        """Initialize the base MCP server.

        Args:
            session_timeout: How long an established MCP session can sit idle with no tool calls, no traffic (in seconds)
        """
        self.session_timeout = session_timeout
        self.logger = logging.getLogger(__name__)
        self._state: MCPConnectionState = MCPConnectionState.DISCONNECTED
        self._session: Optional[ClientSession] = None
        self._last_activity: Optional[float] = None
        self._timeout_task: Optional[asyncio.Task[None]] = None
        self._supervisor_task: Optional[asyncio.Task[None]] = None
        self._setup: Optional[asyncio.Future[None]] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._teardown_done: Optional[asyncio.Event] = None

    async def connect(self) -> None:
        """Connect to the MCP server.

        Concurrent callers join the single in-flight connection attempt instead
        of spawning duplicate supervisors.
        """
        while True:
            if self._state == MCPConnectionState.CONNECTED:
                self.logger.warning("Already connected to MCP server")
                return

            if self._state == MCPConnectionState.CONNECTING:
                in_flight_setup = self._setup
                if in_flight_setup is not None:
                    await asyncio.shield(in_flight_setup)
                    return
                continue

            if self._state == MCPConnectionState.DISCONNECTING:
                teardown_done = self._teardown_done
                if teardown_done is not None:
                    await teardown_done.wait()
                continue

            self._state = MCPConnectionState.CONNECTING
            self._log_connecting()
            setup: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            stop_event = asyncio.Event()
            self._setup = setup
            self._stop_event = stop_event
            self._supervisor_task = asyncio.create_task(
                self._supervise_session(setup, stop_event),
                name=self._supervisor_name,
            )
            try:
                await setup
            except (Exception, asyncio.CancelledError):
                # Setup failed, or connect() itself was cancelled. Either way,
                # tear the supervisor down and let its cleanup finish before
                # propagating. Anything else (SystemExit, KeyboardInterrupt) is
                # left to propagate immediately without our cleanup detour.
                await self._teardown_supervisor()
                raise
            return

    async def disconnect(self) -> None:
        """Disconnect from the MCP server."""
        if self._state == MCPConnectionState.DISCONNECTED:
            return
        if self._state == MCPConnectionState.DISCONNECTING:
            await self._teardown_supervisor()
            return
        self.logger.info(f"Disconnecting from {self._server_description}")
        await self._teardown_supervisor()
        self.logger.info(f"Disconnected from {self._server_description}")

    @property
    def connection_state(self) -> MCPConnectionState:
        """Return the current connection lifecycle state."""
        return self._state

    @property
    def is_connected(self) -> bool:
        """Check if the server is connected."""
        return self._state == MCPConnectionState.CONNECTED

    async def list_tools(self) -> List[types.Tool]:
        """List available tools from the MCP server with auto-reconnect."""
        return await self._call_with_retry("list_tools", self._list_tools_impl)

    async def call_tool(
        self, name: str, arguments: Dict[str, Any]
    ) -> types.CallToolResult:
        """Call a tool on the MCP server with auto-reconnect."""
        return await self._call_with_retry(
            "call_tool", self._call_tool_impl, name, arguments
        )

    async def list_resources(self) -> List[types.Resource]:
        """List available resources from the MCP server with auto-reconnect."""
        return await self._call_with_retry("list_resources", self._list_resources_impl)

    async def read_resource(self, uri: str) -> types.ReadResourceResult:
        """Read a resource from the MCP server with auto-reconnect."""
        return await self._call_with_retry(
            "read_resource", self._read_resource_impl, uri
        )

    async def list_prompts(self) -> List[types.Prompt]:
        """List available prompts from the MCP server with auto-reconnect."""
        return await self._call_with_retry("list_prompts", self._list_prompts_impl)

    async def get_prompt(
        self, name: str, arguments: Dict[str, Any]
    ) -> types.GetPromptResult:
        """Get a prompt from the MCP server with auto-reconnect."""
        return await self._call_with_retry(
            "get_prompt", self._get_prompt_impl, name, arguments
        )

    @property
    @abstractmethod
    def _supervisor_name(self) -> str:
        """Name for the session supervisor task."""

    @property
    @abstractmethod
    def _server_description(self) -> str:
        """Short description of this server for lifecycle logs."""

    @abstractmethod
    def _open_session(self) -> AbstractAsyncContextManager[ClientSession]:
        """Open the underlying transport and yield an initialized ClientSession."""

    def _log_connecting(self) -> None:
        """Log a message when starting a connection attempt."""
        self.logger.info(f"Connecting to {self._server_description}")

    def _log_connection_success(self) -> None:
        """Log a message once the session is established."""
        self.logger.info(f"Successfully connected to {self._server_description}")

    async def _teardown_supervisor(self) -> None:
        """Signal the supervisor to stop and await its exit."""
        if self._state == MCPConnectionState.DISCONNECTING:
            teardown_done = self._teardown_done
            if teardown_done is not None:
                await teardown_done.wait()
            return

        supervisor_task = self._supervisor_task
        stop_event = self._stop_event
        if supervisor_task is None:
            self._state = MCPConnectionState.DISCONNECTED
            self._setup = None
            self._stop_event = None
            self._session = None
            return

        self._state = MCPConnectionState.DISCONNECTING
        teardown_done = asyncio.Event()
        self._teardown_done = teardown_done
        try:
            await self._stop_timeout_monitor()
            if stop_event is not None:
                stop_event.set()
            await cancel_and_wait(supervisor_task)
        finally:
            self._supervisor_task = None
            self._setup = None
            self._stop_event = None
            self._session = None
            self._state = MCPConnectionState.DISCONNECTED
            self._teardown_done = None
            teardown_done.set()

    async def _supervise_session(
        self,
        setup: asyncio.Future[None],
        stop_event: asyncio.Event,
    ) -> None:
        """Hold the MCP session open until ``stop_event`` is set."""
        try:
            async with self._open_session() as session:
                self._session = session
                await self._update_activity()
                if stop_event.is_set():
                    return
                self._state = MCPConnectionState.CONNECTED
                await self._start_timeout_monitor()
                self._log_connection_success()
                if not setup.done():
                    setup.set_result(None)
                await stop_event.wait()
        except Exception as e:
            if not setup.done():
                setup.set_exception(e)
                self.logger.exception(
                    f"Failed to connect to {self._server_description}"
                )
            else:
                self.logger.warning(
                    "MCP session supervisor exited with error", exc_info=True
                )
        finally:
            # Make sure connect() never hangs even if we were cancelled mid-setup.
            if not setup.done():
                setup.cancel()
            try:
                await self._stop_timeout_monitor()
            except Exception:
                self.logger.warning("Error stopping timeout monitor", exc_info=True)
            if self._supervisor_task is asyncio.current_task():
                self._session = None
                if self._state != MCPConnectionState.DISCONNECTING:
                    self._supervisor_task = None
                    self._setup = None
                    self._stop_event = None
                    self._state = MCPConnectionState.DISCONNECTED

    async def _update_activity(self) -> None:
        """Update the last activity timestamp."""
        self._last_activity = asyncio.get_running_loop().time()

    async def _start_timeout_monitor(self) -> None:
        """Start monitoring for session timeout."""
        await self._stop_timeout_monitor()
        self._timeout_task = asyncio.create_task(self._timeout_monitor())

    async def _timeout_monitor(self) -> None:
        """Monitor for session timeout."""
        check_interval = min(10.0, max(0.01, self.session_timeout / 2))
        while self._state == MCPConnectionState.CONNECTED:
            await asyncio.sleep(check_interval)
            if (
                self._last_activity is not None
                and self._state == MCPConnectionState.CONNECTED
            ):
                idle_time = asyncio.get_running_loop().time() - self._last_activity
                if idle_time > self.session_timeout:
                    self.logger.warning(
                        f"Session timeout after {idle_time:.1f}s of inactivity"
                    )
                    await self.disconnect()
                    break

    async def _stop_timeout_monitor(self) -> None:
        """Stop the timeout monitor without cancelling the current task."""
        task = self._timeout_task
        self._timeout_task = None
        if task is not None and task is not asyncio.current_task():
            await cancel_and_wait(task)

    async def _ensure_connected(self) -> None:
        """Ensure the server is connected, reconnecting if necessary."""
        if self._state != MCPConnectionState.CONNECTED:
            self.logger.info("Reconnecting to MCP server...")
            await self.connect()

    async def _call_with_retry(
        self, operation_name: str, operation_func, *args, **kwargs
    ):
        """Call an MCP operation with auto-reconnect on failure."""
        max_retries = 2
        for attempt in range(max_retries + 1):
            try:
                await self._ensure_connected()
                await self._update_activity()
                return await operation_func(*args, **kwargs)
            except Exception as e:
                if attempt < max_retries:
                    self.logger.warning(
                        f"{operation_name} failed (attempt {attempt + 1}/{max_retries + 1}): {e}. Reconnecting..."
                    )
                    await self.disconnect()
                    await asyncio.sleep(1)  # Brief delay before retry
                else:
                    self.logger.error(
                        f"{operation_name} failed after {max_retries + 1} attempts: {e}"
                    )
                    raise

    async def _list_tools_impl(self) -> List[types.Tool]:
        """Internal implementation of list_tools without retry logic."""
        if not self._session or not self.is_connected:
            raise RuntimeError("Not connected to MCP server")

        await self._update_activity()
        response = await self._session.list_tools()
        return response.tools

    async def _call_tool_impl(
        self, name: str, arguments: Dict[str, Any]
    ) -> types.CallToolResult:
        """Internal implementation of call_tool without retry logic."""
        if not self._session or not self.is_connected:
            raise RuntimeError("Not connected to MCP server")

        await self._update_activity()
        return await self._session.call_tool(name, arguments)

    async def _list_resources_impl(self) -> List[types.Resource]:
        """Internal implementation of list_resources without retry logic."""
        if not self._session or not self.is_connected:
            raise RuntimeError("Not connected to MCP server")

        await self._update_activity()
        response = await self._session.list_resources()
        return response.resources

    async def _read_resource_impl(self, uri: str) -> types.ReadResourceResult:
        """Internal implementation of read_resource without retry logic."""
        if not self._session or not self.is_connected:
            raise RuntimeError("Not connected to MCP server")

        await self._update_activity()
        return await self._session.read_resource(AnyUrl(uri))

    async def _list_prompts_impl(self) -> List[types.Prompt]:
        """Internal implementation of list_prompts without retry logic."""
        if not self._session or not self.is_connected:
            raise RuntimeError("Not connected to MCP server")

        await self._update_activity()
        response = await self._session.list_prompts()
        return response.prompts

    async def _get_prompt_impl(
        self, name: str, arguments: Dict[str, Any]
    ) -> types.GetPromptResult:
        """Internal implementation of get_prompt without retry logic."""
        if not self._session or not self.is_connected:
            raise RuntimeError("Not connected to MCP server")

        await self._update_activity()
        return await self._session.get_prompt(name, arguments)

    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()
