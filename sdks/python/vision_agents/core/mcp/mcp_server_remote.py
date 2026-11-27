"""Remote MCP server connection using HTTP Streamable transport."""

import contextlib
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Callable, Dict, Optional
from urllib.parse import urlparse

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from .mcp_base import MCPBaseServer


@contextlib.asynccontextmanager
async def _open_mcp_session(url: str, headers: Dict[str, str], timeout_seconds: float):
    """Open the streamable-HTTP transport plus a ``ClientSession`` on top of it
    and yield the ready session together with its session-id callback."""
    async with streamablehttp_client(
        url,
        headers=headers,
        timeout=timedelta(seconds=timeout_seconds),
    ) as (read, write, get_session_id_cb):
        async with ClientSession(read, write) as session:
            await session.initialize()
            yield session, get_session_id_cb


class MCPServerRemote(MCPBaseServer):
    """Remote MCP server connection using HTTP Streamable transport."""

    def __init__(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        timeout: float = 30.0,
        session_timeout: float = 300.0,
    ):
        """Initialize the remote MCP server connection.

        Args:
            url: URL of the MCP server (e.g., "http://localhost:8001/mcp")
            headers: Optional HTTP headers to include in requests
            timeout: Connection timeout in seconds
            session_timeout: How long an established MCP session can sit idle with no tool calls, no traffic (in seconds)
        """
        super().__init__(session_timeout)
        self.url = url
        self.headers = headers or {}
        self.timeout = timeout
        self._get_session_id_cb: Optional[Callable[[], Optional[str]]] = None

        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError(f"Invalid URL: {url}")

    @property
    def _supervisor_name(self) -> str:
        return f"mcp-supervisor:{self.url}"

    @property
    def _server_description(self) -> str:
        return "remote MCP server"

    def _log_connecting(self) -> None:
        self.logger.info(f"Connecting to remote MCP server at {self.url}")

    def _log_connection_success(self) -> None:
        """Log a connection-success line, including the MCP session id if known."""
        msg = f"Successfully connected to remote MCP server at {self.url}"
        if self._get_session_id_cb is not None:
            try:
                msg += f" (session: {self._get_session_id_cb()})"
            except Exception as e:
                msg += f" (session ID unavailable: {e})"
                self.logger.debug("Session ID lookup failed", exc_info=True)
        self.logger.info(msg)

    @contextlib.asynccontextmanager
    async def _open_session(self) -> AsyncIterator[ClientSession]:
        async with _open_mcp_session(self.url, self.headers, self.timeout) as (
            session,
            get_session_id_cb,
        ):
            self._get_session_id_cb = get_session_id_cb
            try:
                yield session
            finally:
                self._get_session_id_cb = None

    def __repr__(self) -> str:
        """String representation of the remote MCP server."""
        return f"MCPServerRemote(url='{self.url}', connected={self.is_connected})"
