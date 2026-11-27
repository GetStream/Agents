import asyncio
from typing import cast

import pytest
from aws_sdk_bedrock_runtime.client import BedrockRuntimeClient
from dotenv import load_dotenv
from vision_agents.core.agents.agent_types import AgentOptions
from vision_agents.core.edge.types import Participant
from vision_agents.core.instructions import Instructions
from vision_agents.core.llm.realtime import (
    RealtimeAgentSpeechEnded,
    RealtimeAgentSpeechStarted,
    RealtimeAudioOutput,
    RealtimeConnectionState,
    RealtimeUserSpeechEnded,
    RealtimeUserSpeechStarted,
)
from vision_agents.plugins.aws import Realtime

load_dotenv()


class _FakeInputStream:
    def __init__(
        self, fail_on_call: int | None = None, error: Exception | None = None
    ) -> None:
        self._fail_on_call = fail_on_call
        self._error = error
        self.calls = 0

    async def send(self, event: object) -> None:
        self.calls += 1
        if self._fail_on_call is not None and self.calls == self._fail_on_call:
            assert self._error is not None
            raise self._error


class _FakeOutputStream:
    def __init__(self) -> None:
        self._gate = asyncio.Event()

    async def receive(self) -> None:
        await self._gate.wait()
        raise StopAsyncIteration

    def close(self) -> None:
        self._gate.set()


class _FakeBidiStream:
    def __init__(
        self, fail_on_send: int | None = None, error: Exception | None = None
    ) -> None:
        self.input_stream = _FakeInputStream(fail_on_call=fail_on_send, error=error)
        self._output = _FakeOutputStream()

    async def await_output(self) -> tuple[None, _FakeOutputStream]:
        return None, self._output


class _FakeBedrockClient:
    def __init__(self, streams: list[_FakeBidiStream]) -> None:
        self._streams = list(streams)
        self.created_streams: list[_FakeBidiStream] = []

    async def invoke_model_with_bidirectional_stream(
        self, operation_input: object
    ) -> _FakeBidiStream:
        stream = self._streams.pop(0)
        self.created_streams.append(stream)
        return stream


@pytest.mark.skip_blockbuster
class TestAWSRealtime:
    """Unit tests for AWS Realtime connection lifecycle and failure cleanup."""

    async def test_connect_without_instructions_cleans_up_resources(self) -> None:
        rt = Realtime(region_name="us-east-1")
        stream = _FakeBidiStream()
        rt.client = cast(BedrockRuntimeClient, _FakeBedrockClient([stream]))

        with pytest.raises(ValueError, match="system instructions"):
            await rt.connect()

        assert rt.connected is False
        assert rt.connection_state is RealtimeConnectionState.DISCONNECTED
        assert rt.connection is None
        assert rt._handle_event_task is None
        await rt.close()
        assert rt.connection_state is RealtimeConnectionState.CLOSED

    async def test_connect_failure_mid_handshake_cleans_up_stream_and_task(
        self,
    ) -> None:
        rt = Realtime(region_name="us-east-1")
        rt.set_instructions(Instructions("Be helpful."))
        stream = _FakeBidiStream(
            fail_on_send=3, error=RuntimeError("system prompt send failed")
        )
        rt.client = cast(BedrockRuntimeClient, _FakeBedrockClient([stream]))

        with pytest.raises(RuntimeError, match="system prompt send failed"):
            await rt.connect()

        assert rt.connected is False
        assert rt.connection_state is RealtimeConnectionState.DISCONNECTED
        assert rt.connection is None
        assert rt._handle_event_task is None
        await rt.close()
        assert rt.connection_state is RealtimeConnectionState.CLOSED

    async def test_reconnect_failure_during_system_instructions_clears_stale_handles(
        self,
    ) -> None:
        rt = Realtime(region_name="us-east-1")
        rt.set_instructions(Instructions("Be helpful."))
        stream1 = _FakeBidiStream()
        stream2 = _FakeBidiStream(
            fail_on_send=3, error=RuntimeError("system prompt failed on reconnect")
        )
        rt.client = cast(BedrockRuntimeClient, _FakeBedrockClient([stream1, stream2]))

        await rt.connect()
        assert rt.connected is True
        assert rt.connection_state is RealtimeConnectionState.CONNECTED

        with pytest.raises(RuntimeError, match="system prompt failed on reconnect"):
            await rt.reconnect()

        assert rt.connected is False
        assert rt.connection_state is RealtimeConnectionState.DISCONNECTED
        assert rt.connection is None
        assert rt._handle_event_task is None
        await rt.close()
        assert rt.connection_state is RealtimeConnectionState.CLOSED

    async def test_reconnect_failure_before_swap_cleans_up_all_handles(self) -> None:
        rt = Realtime(region_name="us-east-1")
        rt.set_instructions(Instructions("Be helpful."))
        stream1 = _FakeBidiStream()
        stream2 = _FakeBidiStream(
            fail_on_send=1, error=RuntimeError("sessionStart failed before swap")
        )
        rt.client = cast(BedrockRuntimeClient, _FakeBedrockClient([stream1, stream2]))

        await rt.connect()
        assert rt.connected is True
        assert rt.connection_state is RealtimeConnectionState.CONNECTED

        with pytest.raises(RuntimeError, match="sessionStart failed before swap"):
            await rt.reconnect()

        assert rt.connected is False
        assert rt.connection_state is RealtimeConnectionState.DISCONNECTED
        assert rt.connection is None
        assert rt._handle_event_task is None
        await rt.close()
        assert rt.connection_state is RealtimeConnectionState.CLOSED

    async def test_close_does_not_deadlock_when_pending_tool_triggers_reconnect(
        self,
    ) -> None:
        rt = Realtime(region_name="us-east-1")
        rt.set_instructions(Instructions("Be helpful."))
        stream1 = _FakeBidiStream()
        rt.client = cast(BedrockRuntimeClient, _FakeBedrockClient([stream1]))

        await rt.connect()

        tool_started = asyncio.Event()
        proceed_tool = asyncio.Event()

        async def _pending_tool() -> None:
            tool_started.set()
            await proceed_tool.wait()
            await rt.reconnect()

        rt._run_tool_in_background(_pending_tool())
        await asyncio.wait_for(tool_started.wait(), timeout=2.0)

        close_task = asyncio.create_task(rt.close())
        await asyncio.sleep(0)
        proceed_tool.set()
        stream1._output.close()

        await asyncio.wait_for(close_task, timeout=2.0)
        assert rt.connection_state is RealtimeConnectionState.CLOSED
        assert rt.connection is None
        assert rt._handle_event_task is None


@pytest.mark.integration
@pytest.mark.skip_blockbuster
class TestAWSRealtimeIntegration:
    """End-to-end tests against AWS Bedrock Realtime."""

    @pytest.fixture
    async def realtime(self, tmp_path):
        rt = Realtime(region_name="us-east-1")
        rt.options = AgentOptions(model_dir=tmp_path.as_posix())
        await rt.warmup()
        rt.set_instructions(
            Instructions("you're a kind assistant, always be friendly please.")
        )
        try:
            await rt.connect()
            yield rt
        finally:
            await rt.close()

    @pytest.fixture
    async def realtime_with_tools(self, tmp_path):
        """Realtime with get_test_data registered, then connected."""
        rt = Realtime(region_name="us-east-1")
        rt.options = AgentOptions(model_dir=tmp_path.as_posix())
        await rt.warmup()
        rt.set_instructions(
            Instructions("you're a kind assistant, always be friendly please.")
        )

        @rt.register_function(
            name="get_test_data", description="Get test data for a given key"
        )
        async def get_test_data(key: str) -> dict:
            """Get test data.

            Args:
                key: The key to look up

            Returns:
                Test data for the key
            """
            test_data = {
                "weather": {"value": "sunny", "temp": 72},
                "time": {"value": "12:00 PM"},
            }
            return test_data.get(key, {"error": "Key not found"})

        try:
            await rt.connect()
            yield rt
        finally:
            await rt.close()

    async def test_simple_response_flow(self, realtime):
        # unlike other realtime LLMs, AWS doesn't reply if you only send text
        realtime.set_instructions(
            Instructions("whenever you reply mention a fun fact about The Netherlands")
        )
        async for _ in realtime.simple_response(
            "Hello, can you hear me? Please respond with a short greeting."
        ):
            pass
        await asyncio.sleep(10.0)

    async def test_audio_first(self, realtime, mia_audio_16khz):
        """Test sending real audio data and verify connection remains stable"""
        async for _ in realtime.simple_response(
            "Listen to the following story, what is Mia looking for?"
        ):
            pass
        await asyncio.sleep(10.0)
        await realtime.simple_audio_response(
            mia_audio_16khz, Participant(original=None, user_id="u", id="u")
        )

        items = await realtime.output.collect(10)
        audio = [i for i in items if isinstance(i, RealtimeAudioOutput)]
        user_started = [i for i in items if isinstance(i, RealtimeUserSpeechStarted)]
        user_ended = [i for i in items if isinstance(i, RealtimeUserSpeechEnded)]
        agent_started = [i for i in items if isinstance(i, RealtimeAgentSpeechStarted)]
        agent_ended = [i for i in items if isinstance(i, RealtimeAgentSpeechEnded)]
        assert len(audio) > 0
        assert len(user_started) >= 1
        assert len(user_ended) >= 1
        assert len(agent_started) >= 1
        assert len(agent_ended) >= 1

    async def test_connection_lifecycle(self, realtime):
        """Connection established by fixture; verify simple_response and clean close."""
        assert realtime.connected is True

        async for _ in realtime.simple_response("Test message"):
            pass
        await asyncio.sleep(2.0)

        await realtime.close()
        assert realtime.connected is False

    async def test_function_calling(self, realtime_with_tools, mia_audio_16khz):
        """Function calling produces audio output after the model invokes the tool."""
        # AWS Nova requires at least one audio content block in the prompt;
        # send a short clip before the text instruction.
        await realtime_with_tools.simple_audio_response(
            mia_audio_16khz, Participant(original=None, user_id="u", id="u")
        )
        async for _ in realtime_with_tools.simple_response(
            "Please use the get_test_data function to get data for the weather key and tell me what you find."
        ):
            pass
        await asyncio.sleep(15.0)

        items = realtime_with_tools.output.peek()
        audio = [i for i in items if isinstance(i, RealtimeAudioOutput)]
        agent_started = [i for i in items if isinstance(i, RealtimeAgentSpeechStarted)]
        agent_ended = [i for i in items if isinstance(i, RealtimeAgentSpeechEnded)]
        assert len(audio) > 0
        assert len(agent_started) >= 1
        assert len(agent_ended) >= 1
