"""Local MCP server connection using stdio transport."""

import contextlib
from collections.abc import AsyncIterator
from typing import Dict, Optional

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from .mcp_base import MCPBaseServer


class MCPServerLocal(MCPBaseServer):
    """Local MCP server connection using stdio transport."""

    def __init__(
        self,
        command: str,
        env: Optional[Dict[str, str]] = None,
        session_timeout: float = 300.0,
    ):
        """Initialize the local MCP server connection.

        Args:
            command: Command to run the MCP server (e.g., "python", "node", etc.)
            env: Optional environment variables to pass to the server process
            session_timeout: How long an established MCP session can sit idle with no tool calls, no traffic (in seconds)
        """
        super().__init__(session_timeout)
        self.command = command
        self.env = env or {}

        self._parse_command()

    @property
    def _supervisor_name(self) -> str:
        return f"mcp-supervisor:{self._executable}"

    @property
    def _server_description(self) -> str:
        return "local MCP server"

    def _parse_command(self) -> None:
        """Parse the command string into executable and arguments."""
        parts = self.command.split()
        if not parts:
            raise ValueError("Command cannot be empty")

        self._executable = parts[0]
        self._args = parts[1:] if len(parts) > 1 else []

    def _log_connecting(self) -> None:
        self.logger.info(f"Connecting to local MCP server: {self.command}")

    def _log_connection_success(self) -> None:
        self.logger.info(f"Successfully connected to local MCP server: {self.command}")

    @contextlib.asynccontextmanager
    async def _open_session(self) -> AsyncIterator[ClientSession]:
        """Open the stdio transport and yield an initialized ClientSession.

        The stdio transport is built on anyio cancel scopes, which must be entered and
        exited in the same task, so the supervisor task owns the whole session.
        """
        params = StdioServerParameters(
            command=self._executable, args=self._args, env=self.env
        )
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                yield session

    def __repr__(self) -> str:
        """String representation of the local MCP server."""
        return (
            f"MCPServerLocal(command='{self.command}', connected={self.is_connected})"
        )
