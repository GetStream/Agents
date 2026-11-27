import asyncio
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
import websockets
from dotenv import load_dotenv
from google.genai.errors import APIError
from google.genai.types import (
    Behavior,
    Blob,
    Content,
    FunctionCall,
    InteractionStatus,
    LiveServerContent,
    LiveServerMessage,
    LiveServerSessionResumptionUpdate,
    LiveServerToolCall,
    Part,
    ThinkingLevel,
    Transcription,
    TurnCoverage,
)
from vision_agents.core.edge.types import Participant
from vision_agents.core.llm.realtime import (
    RealtimeAgentSpeechEnded,
    RealtimeAgentSpeechStarted,
    RealtimeAgentTranscript,
    RealtimeAudioOutput,
    RealtimeAudioOutputDone,
    RealtimeConnectionState,
    RealtimeUserSpeechEnded,
    RealtimeUserSpeechStarted,
    RealtimeUserTranscript,
)
from vision_agents.core.utils.audio_input_pacer import AudioInputPacer
from vision_agents.core.utils.audio_input_direct import AudioInputDirect
from vision_agents.plugins.gemini import (
    LIVE_EXTENDED_THINKING_MODEL,
    GeminiModelUnavailableError,
    Realtime,
)
from vision_agents.plugins.gemini.gemini_realtime import (
    DEFAULT_MODEL,
    LIVE_TRANSLATE_MODEL,
    GeminiRealtime,
)
from websockets.frames import Close

# Load environment variables
load_dotenv()


def _make_session(messages: list[LiveServerMessage]) -> AsyncMock:
    """Create a mock async session that yields the given messages."""
    session = AsyncMock()

    async def _receive():
        for msg in messages:
            yield msg

    session.receive = _receive
    return session


class _CaptureSession:
    """Session that records tool responses without mocking."""

    def __init__(self) -> None:
        self.function_responses: list[Any] = []

    async def send_tool_response(self, function_responses: list[Any]) -> None:
        self.function_responses = list(function_responses)


def _fake_client() -> Any:
    return object()


class _FailingLiveConnect:
    """Async context manager that fails on enter, like a rejected Live handshake."""

    def __init__(self, error: APIError) -> None:
        self._error = error

    async def __aenter__(self) -> None:
        raise self._error

    async def __aexit__(self, *exc_info: object) -> bool:
        return False


def _client_rejecting_model(detail: str) -> Any:
    """Build a client whose live.connect fails with an APIError carrying ``detail``."""
    error = APIError(1008, detail, None)

    def connect(model: str, config: Any) -> _FailingLiveConnect:
        return _FailingLiveConnect(error)

    return SimpleNamespace(aio=SimpleNamespace(live=SimpleNamespace(connect=connect)))


def _make_realtime(**kwargs: Any) -> GeminiRealtime:
    """Create a GeminiRealtime instance without connecting."""
    kwargs.setdefault("client", _fake_client())
    return GeminiRealtime(**kwargs)


def _audio_model_turn() -> LiveServerMessage:
    return LiveServerMessage(
        server_content=LiveServerContent(
            model_turn=Content(
                parts=[Part(inline_data=Blob(data=b"\x00" * 100))],
            ),
        ),
    )


class TestGeminiRealtimeInputPacing:
    def test_default_model_and_live_translate_pacing_policy(self):
        default_rt = GeminiRealtime(client=_fake_client())
        translate_rt = GeminiRealtime(
            model=LIVE_TRANSLATE_MODEL,
            client=_fake_client(),
        )
        translate_opt_out = GeminiRealtime(
            model=LIVE_TRANSLATE_MODEL,
            client=_fake_client(),
            input_audio_pacing=None,
        )

        assert DEFAULT_MODEL == "gemini-3.8-live"
        assert LIVE_EXTENDED_THINKING_MODEL == "gemini-3.8-live-extended-thinking"
        assert LIVE_TRANSLATE_MODEL == "gemini-3.5-live-translate-preview"
        assert type(default_rt._audio_input_processor) is AudioInputDirect
        assert isinstance(translate_rt._audio_input_processor, AudioInputPacer)
        assert translate_rt._audio_input_processor.config.silence_when_empty
        assert translate_rt._audio_input_processor.config.startup_buffer_ms == 500
        assert type(translate_opt_out._audio_input_processor) is AudioInputDirect


class TestGeminiRealtimeProcessEvents:
    async def test_input_transcription(self):
        rt = _make_realtime()
        rt._current_participant = Participant(original=None, user_id="u", id="u")
        msg = LiveServerMessage(
            server_content=LiveServerContent(
                input_transcription=Transcription(text="hello"),
            ),
        )
        rt._real_session = _make_session([msg])

        await rt._process_events()

        items = rt.output.peek()
        assert any(isinstance(i, RealtimeUserSpeechStarted) for i in items)
        transcripts = [i for i in items if isinstance(i, RealtimeUserTranscript)]
        assert len(transcripts) == 1
        assert transcripts[0].text == "hello"

    async def test_output_transcription(self):
        rt = _make_realtime()
        msg = LiveServerMessage(
            server_content=LiveServerContent(
                output_transcription=Transcription(text="world"),
            ),
        )
        rt._real_session = _make_session([msg])

        await rt._process_events()

        items = rt.output.peek()
        assert len(items) == 1
        assert isinstance(items[0], RealtimeAgentTranscript)
        assert items[0].text == "world"

    async def test_model_turn_audio(self):
        rt = _make_realtime()
        rt._real_session = _make_session([_audio_model_turn()])

        await rt._process_events()

        items = rt.output.peek()
        assert any(isinstance(i, RealtimeAgentSpeechStarted) for i in items)
        assert any(isinstance(i, RealtimeAudioOutput) for i in items)

    async def test_model_turn_function_call(self):
        rt = _make_realtime()
        msg = LiveServerMessage(
            server_content=LiveServerContent(
                model_turn=Content(
                    parts=[
                        Part(
                            function_call=FunctionCall(
                                id="call_1", name="get_weather", args={"city": "NYC"}
                            )
                        )
                    ],
                ),
            ),
        )
        rt._real_session = _make_session([msg])
        rt._handle_function_call = AsyncMock()

        await rt._process_events()

        rt._handle_function_call.assert_called_once()
        call_arg = rt._handle_function_call.call_args[0][0]
        assert call_arg.name == "get_weather"

    async def test_turn_complete(self):
        rt = _make_realtime()
        msg = LiveServerMessage(
            server_content=LiveServerContent(
                turn_complete=True,
            ),
        )
        rt._real_session = _make_session([msg])

        await rt._process_events()

        items = rt.output.peek()
        assert len(items) == 1
        assert isinstance(items[0], RealtimeAudioOutputDone)

    @pytest.mark.parametrize(
        ("turn_complete", "status", "expect_agent_ended", "expect_audio_done"),
        [
            (True, InteractionStatus.IN_PROGRESS, False, False),
            (True, InteractionStatus.IDLE, True, True),
            (True, InteractionStatus.REQUIRES_ACTION, True, True),
            (False, InteractionStatus.IDLE, True, True),
        ],
    )
    async def test_interaction_status_after_audio(
        self,
        turn_complete: bool,
        status: InteractionStatus,
        expect_agent_ended: bool,
        expect_audio_done: bool,
    ):
        rt = _make_realtime()
        rt._real_session = _make_session(
            [
                _audio_model_turn(),
                LiveServerMessage(
                    server_content=LiveServerContent(
                        turn_complete=turn_complete or None,
                        interaction_status=status,
                    )
                ),
            ]
        )

        await rt._process_events()

        items = rt.output.peek()
        assert (
            any(isinstance(i, RealtimeAudioOutputDone) for i in items)
            is expect_audio_done
        )
        ended = any(isinstance(i, RealtimeAgentSpeechEnded) for i in items)
        assert ended is expect_agent_ended
        assert rt._agent_audio_started is not expect_agent_ended

    async def test_tool_call(self):
        rt = _make_realtime()
        msg = LiveServerMessage(
            tool_call=LiveServerToolCall(
                function_calls=[
                    FunctionCall(id="tc_1", name="search", args={"q": "test"})
                ],
            ),
        )
        rt._real_session = _make_session([msg])
        rt._handle_tool_calls = AsyncMock()

        await rt._process_events()

        rt._handle_tool_calls.assert_called_once_with(msg.tool_call)

    async def test_model_turn_with_turn_complete(self):
        """A single message can have both model_turn and turn_complete."""
        rt = _make_realtime()
        msg = LiveServerMessage(
            server_content=LiveServerContent(
                model_turn=Content(
                    parts=[Part(text="done")],
                ),
                turn_complete=True,
            ),
        )
        rt._real_session = _make_session([msg])

        emitted: list[object] = []
        rt.events.send = lambda e: emitted.append(e)

        await rt._process_events()

        assert any(isinstance(i, RealtimeAudioOutputDone) for i in rt.output.peek())

    async def test_part_with_text_and_audio(self):
        """A single Part with both text and inline_data must emit both events."""
        rt = _make_realtime()
        audio_bytes = b"\x00" * 100
        msg = LiveServerMessage(
            server_content=LiveServerContent(
                model_turn=Content(
                    parts=[Part(text="hello", inline_data=Blob(data=audio_bytes))],
                ),
            ),
        )
        rt._real_session = _make_session([msg])

        emitted: list[object] = []
        rt.events.send = lambda e: emitted.append(e)

        await rt._process_events()

        assert any(isinstance(i, RealtimeAudioOutput) for i in rt.output.peek())

    async def test_transcription_with_audio_same_message(self):
        """Audio in model_turn must not be skipped when transcription is also present."""
        rt = _make_realtime()
        audio_bytes = b"\x00" * 100
        msg = LiveServerMessage(
            server_content=LiveServerContent(
                output_transcription=Transcription(text="hi"),
                model_turn=Content(
                    parts=[Part(inline_data=Blob(data=audio_bytes))],
                ),
            ),
        )
        rt._real_session = _make_session([msg])

        await rt._process_events()

        types = [type(i) for i in rt.output.peek()]
        assert RealtimeAgentTranscript in types
        assert RealtimeAudioOutput in types

    async def test_thought_only_parts_not_logged_as_unrecognized(self, caplog):
        """model_turn with only thought parts should still be handled."""
        rt = _make_realtime()
        msg = LiveServerMessage(
            server_content=LiveServerContent(
                model_turn=Content(
                    parts=[Part(text="thinking...", thought=True)],
                ),
            ),
        )
        rt._real_session = _make_session([msg])

        with caplog.at_level("DEBUG"):
            await rt._process_events()

        assert "Unrecognized" not in caplog.text

    async def test_empty_parts_not_logged_as_unrecognized(self, caplog):
        """model_turn with empty parts should still be handled."""
        rt = _make_realtime()
        msg = LiveServerMessage(
            server_content=LiveServerContent(
                model_turn=Content(parts=[]),
            ),
        )
        rt._real_session = _make_session([msg])

        with caplog.at_level("DEBUG"):
            await rt._process_events()

        assert "Unrecognized" not in caplog.text

    async def test_unrecognized_message_logged(self, caplog):
        """A message with no recognized fields should be logged."""
        rt = _make_realtime()
        msg = LiveServerMessage()
        rt._real_session = _make_session([msg])

        with caplog.at_level("DEBUG"):
            await rt._process_events()

        assert "Unrecognized" in caplog.text

    async def test_session_resumption_update(self):
        """Session resumption handle is stored when present on a response."""
        rt = _make_realtime()
        msg = LiveServerMessage(
            server_content=LiveServerContent(
                model_turn=Content(parts=[Part(text="hi")]),
            ),
            session_resumption_update=LiveServerSessionResumptionUpdate(
                resumable=True,
                new_handle="resume-token-123",
            ),
        )
        rt._real_session = _make_session([msg])

        await rt._process_events()

        assert rt._session_resumption_id == "resume-token-123"

    async def test_input_and_output_transcription_same_message(self):
        """Both input and output transcription in same message are both handled."""
        rt = _make_realtime()
        rt._current_participant = Participant(original=None, user_id="u", id="u")
        msg = LiveServerMessage(
            server_content=LiveServerContent(
                input_transcription=Transcription(text="user said"),
                output_transcription=Transcription(text="model said"),
            ),
        )
        rt._real_session = _make_session([msg])

        await rt._process_events()

        types = [type(i) for i in rt.output.peek()]
        assert RealtimeUserTranscript in types
        assert RealtimeAgentTranscript in types

    async def test_empty_transcription_text_not_emitted(self):
        """Transcription with empty text should not emit events."""
        rt = _make_realtime()
        msg = LiveServerMessage(
            server_content=LiveServerContent(
                input_transcription=Transcription(text=""),
                output_transcription=Transcription(text=""),
            ),
        )
        rt._real_session = _make_session([msg])

        await rt._process_events()

        assert len(rt.output.peek()) == 0

    async def test_function_call_does_not_block_event_loop(self):
        """_process_events must not block while a function call executes."""
        rt = _make_realtime()
        msg = LiveServerMessage(
            server_content=LiveServerContent(
                model_turn=Content(
                    parts=[
                        Part(
                            function_call=FunctionCall(
                                id="call_1", name="slow_tool", args={}
                            )
                        )
                    ],
                ),
            ),
        )
        rt._real_session = _make_session([msg])

        async def slow_handle(function_call: FunctionCall, timeout: float = 30.0):
            await asyncio.sleep(10)

        rt._handle_function_call = slow_handle

        try:
            finished = await asyncio.wait_for(rt._process_events(), timeout=2.0)
            assert finished is False
        finally:
            await rt.close()

    async def test_interrupted_emits_user_speech_ended_before_clearing_participant(
        self,
    ):
        rt = _make_realtime()
        participant = Participant(original=None, user_id="u1", id="u1")
        rt._current_participant = participant
        rt._real_session = _make_session(
            [
                LiveServerMessage(
                    server_content=LiveServerContent(
                        input_transcription=Transcription(text="wait a second"),
                    )
                ),
                LiveServerMessage(server_content=LiveServerContent(interrupted=True)),
            ]
        )

        await rt._process_events()

        items = rt.output.peek()
        user_ended = [i for i in items if isinstance(i, RealtimeUserSpeechEnded)]
        assert len(user_ended) == 1
        assert user_ended[0].participant == participant
        assert rt._user_audio_started is False
        assert rt._current_participant is None


class TestGeminiRealtimeProcessingLoop:
    async def test_clean_websocket_close_marks_disconnected(self):
        rt = _make_realtime()
        rt.connected = True

        async def _raise_closed_ok():
            raise websockets.ConnectionClosedOK(Close(1000, "normal"), None)

        rt._process_events = _raise_closed_ok

        await rt._processing_loop()

        assert rt.connected is False

    async def test_non_reconnectable_close_marks_disconnected(self):
        rt = _make_realtime()
        rt.connected = True

        async def _raise_closed_error():
            raise websockets.ConnectionClosedError(Close(1002, "protocol error"), None)

        rt._process_events = _raise_closed_error

        await rt._processing_loop()

        assert rt.connected is False

    async def test_reconnectable_close_triggers_reconnect(self):
        rt = _make_realtime()
        reconnected = asyncio.Event()

        async def _raise_reconnectable():
            if not reconnected.is_set():
                reconnected.set()
                raise websockets.ConnectionClosedError(Close(1011, "timeout"), None)
            await asyncio.sleep(10)

        rt._process_events = _raise_reconnectable
        rt._establish_session = AsyncMock()

        task = asyncio.create_task(rt._processing_loop())
        await asyncio.wait_for(reconnected.wait(), timeout=2.0)
        task.cancel()
        await task

        rt._establish_session.assert_called_once()

    async def test_consecutive_errors_stop_loop(self):
        rt = _make_realtime()
        rt.connected = True

        async def _always_fail():
            raise ConnectionError("network down")

        rt._process_events = _always_fail

        await rt._processing_loop()

        assert rt.connected is False

    async def test_api_error_with_reconnectable_code_triggers_reconnect(self):
        rt = _make_realtime()
        reconnected = asyncio.Event()

        async def _raise_api_error():
            if not reconnected.is_set():
                reconnected.set()
                raise APIError(1011, None, None)
            await asyncio.sleep(10)

        rt._process_events = _raise_api_error
        rt._establish_session = AsyncMock()

        task = asyncio.create_task(rt._processing_loop())
        await asyncio.wait_for(reconnected.wait(), timeout=2.0)
        task.cancel()
        await task

        rt._establish_session.assert_called_once()

    async def test_api_error_non_reconnectable_stops_loop(self):
        rt = _make_realtime()
        rt.connected = True

        async def _raise_api_error():
            raise APIError(1002, None, None)

        rt._process_events = _raise_api_error

        await rt._processing_loop()

        assert rt.connected is False

    async def test_unavailable_model_raises_actionable_error(self):
        detail = (
            "models/gemini-3.8-live is not found for API version v1alpha, "
            "or is not supported for bidiGenerateContent."
        )
        rt = _make_realtime(client=_client_rejecting_model(detail))

        with pytest.raises(GeminiModelUnavailableError) as exc_info:
            await rt._establish_session()

        assert str(exc_info.value) == (
            "The requested Gemini Live model is not available. Check the model "
            "name and that your API key's project can use it."
        )

    async def test_other_api_errors_are_not_wrapped(self):
        rt = _make_realtime(client=_client_rejecting_model("quota exceeded"))

        with pytest.raises(APIError):
            await rt._establish_session()

    async def test_reconnect_closes_prior_session_and_clears_connected_flag(self):
        reconnect_err = websockets.ConnectionClosedError(Close(1011, "timeout"), None)
        recovered_gate = asyncio.Event()
        observed_connected_during_reconnect: list[bool] = []
        prior_closed_before_next_open: list[bool] = []

        class _FakeSession:
            def __init__(self, raise_err: Exception | None, hold: bool) -> None:
                self._raise_err = raise_err
                self._hold = hold
                self.closed_event = asyncio.Event()

            async def receive(self):
                if self._raise_err is not None:
                    raise self._raise_err
                yield LiveServerMessage(
                    server_content=LiveServerContent(
                        output_transcription=Transcription(text="recovered"),
                    )
                )
                recovered_gate.set()
                if self._hold:
                    await self.closed_event.wait()

            async def send_realtime_input(self, **kwargs: object) -> None:
                if self._raise_err is not None:
                    raise self._raise_err

        class _SessionCtx:
            def __init__(self, session: _FakeSession, on_enter) -> None:
                self.session = session
                self._on_enter = on_enter
                self.exited = False

            async def __aenter__(self) -> _FakeSession:
                await self._on_enter()
                return self.session

            async def __aexit__(self, *exc_info: object) -> None:
                self.exited = True
                self.session.closed_event.set()

        contexts: list[_SessionCtx] = []
        s1 = _FakeSession(raise_err=reconnect_err, hold=False)
        s2 = _FakeSession(raise_err=None, hold=True)

        async def _noop() -> None:
            return None

        async def _on_second_enter() -> None:
            observed_connected_during_reconnect.append(rt.connected)
            prior_closed_before_next_open.append(contexts[0].exited)

        contexts.append(_SessionCtx(s1, _noop))
        contexts.append(_SessionCtx(s2, _on_second_enter))
        call_index = 0

        def _connect(model: str, config: object) -> _SessionCtx:
            nonlocal call_index
            ctx = contexts[call_index]
            call_index += 1
            return ctx

        client = SimpleNamespace(
            aio=SimpleNamespace(live=SimpleNamespace(connect=_connect))
        )
        rt = _make_realtime(client=client)
        try:
            await rt.connect()
            await asyncio.wait_for(recovered_gate.wait(), timeout=2.0)
            assert call_index == 2
            assert observed_connected_during_reconnect == [False]
            assert prior_closed_before_next_open == [True]
            assert rt.connected is True
        finally:
            await rt.close()

        assert contexts[1].exited is True
        assert rt.connected is False

    async def test_concurrent_simple_response_and_loop_reconnect_serialized(self):
        reconnect_err = websockets.ConnectionClosedError(Close(1011, "timeout"), None)
        connect_calls = 0

        class _FakeSession:
            def __init__(self, fail: bool) -> None:
                self._fail = fail
                self.closed_event = asyncio.Event()

            async def receive(self):
                if self._fail:
                    raise reconnect_err
                await self.closed_event.wait()
                if False:
                    yield LiveServerMessage()

            async def send_realtime_input(self, **kwargs: object) -> None:
                if self._fail:
                    raise reconnect_err

        class _SessionCtx:
            def __init__(self, session: _FakeSession, delay: bool) -> None:
                self.session = session
                self._delay = delay
                self.exited = False

            async def __aenter__(self) -> _FakeSession:
                if self._delay:
                    await asyncio.sleep(0.05)
                return self.session

            async def __aexit__(self, *exc_info: object) -> None:
                self.exited = True
                self.session.closed_event.set()

        created_ctxs: list[_SessionCtx] = []

        def _connect(model: str, config: object) -> _SessionCtx:
            nonlocal connect_calls
            connect_calls += 1
            ctx = _SessionCtx(
                _FakeSession(fail=(connect_calls == 1)),
                delay=(connect_calls > 1),
            )
            created_ctxs.append(ctx)
            return ctx

        client = SimpleNamespace(
            aio=SimpleNamespace(live=SimpleNamespace(connect=_connect))
        )
        rt = _make_realtime(client=client)
        try:
            await rt.connect()
            async for _ in rt.simple_response("hello"):
                pass
            await asyncio.sleep(0.1)
            assert connect_calls == 2
            assert created_ctxs[0].exited is True
            assert rt.connected is True
        finally:
            await rt.close()

        assert created_ctxs[-1].exited is True

    async def test_close_closes_exit_stack_even_when_processing_task_failed(self):
        exited = False

        class _DummySession:
            async def receive(self):
                await asyncio.sleep(10)
                if False:
                    yield LiveServerMessage()

        class _DummyCtx:
            async def __aenter__(self) -> _DummySession:
                return _DummySession()

            async def __aexit__(self, *exc_info: object) -> None:
                nonlocal exited
                exited = True

        client = SimpleNamespace(
            aio=SimpleNamespace(
                live=SimpleNamespace(connect=lambda model, config: _DummyCtx())
            )
        )
        rt = _make_realtime(client=client)
        await rt.connect()

        if rt._processing_task is not None:
            rt._processing_task.cancel()
            try:
                await rt._processing_task
            except asyncio.CancelledError:
                pass

        async def _boom() -> None:
            raise RuntimeError("processing task crashed")

        rt._processing_task = asyncio.create_task(_boom())
        await asyncio.sleep(0)

        await rt.close()

        assert exited is True
        assert rt._real_session is None
        assert rt.connected is False
        assert rt.connection_state is RealtimeConnectionState.CLOSED


class TestGeminiRealtimeFunctionCalling:
    """Unit tests for Gemini Realtime tool conversion and config wiring."""

    async def test_convert_tools_to_provider_format(self):
        """Test tool conversion to Gemini Live format."""
        # Create a minimal instance just for testing the conversion method
        realtime = _make_realtime()

        # Test tools
        tools = [
            {
                "name": "get_weather",
                "description": "Get weather information",
                "parameters_schema": {
                    "type": "object",
                    "properties": {
                        "location": {"type": "string", "description": "City name"}
                    },
                    "required": ["location"],
                },
            },
            {
                "name": "calculate",
                "description": "Perform calculations",
                "parameters_schema": {
                    "type": "object",
                    "properties": {
                        "expression": {
                            "type": "string",
                            "description": "Math expression",
                        }
                    },
                    "required": ["expression"],
                },
            },
        ]

        result = realtime._convert_tools_to_provider_format(tools)

        assert len(result) == 1
        assert "function_declarations" in result[0]
        assert len(result[0]["function_declarations"]) == 2

        # Check first tool
        tool1 = result[0]["function_declarations"][0]
        assert tool1["name"] == "get_weather"
        assert tool1["description"] == "Get weather information"
        assert "location" in tool1["parameters"]["properties"]
        assert tool1["behavior"] == Behavior.NON_BLOCKING

        # Check second tool
        tool2 = result[0]["function_declarations"][1]
        assert tool2["name"] == "calculate"
        assert tool2["description"] == "Perform calculations"
        assert "expression" in tool2["parameters"]["properties"]

    async def test_get_config_with_tools(self):
        """Test that tools are added to the config."""
        # Create a minimal instance for testing config creation
        realtime = _make_realtime()

        # Register a test function
        @realtime.register_function(description="Test function")
        async def test_func(param: str) -> str:
            return f"test: {param}"

        config = realtime.get_config()

        # Verify tools were added
        assert "tools" in config
        assert len(config["tools"]) == 1
        assert "function_declarations" in config["tools"][0]
        assert len(config["tools"][0]["function_declarations"]) == 1
        assert config["tools"][0]["function_declarations"][0]["name"] == "test_func"

    async def test_get_config_without_tools(self):
        """Test config creation when no tools are available."""
        # Create a minimal instance without registering any functions
        realtime = _make_realtime()

        config = realtime.get_config()

        # Verify tools were not added
        assert "tools" not in config

    def test_default_turn_coverage_includes_video(self):
        realtime = _make_realtime()
        coverage = realtime._base_config["realtime_input_config"]["turn_coverage"]
        assert coverage == TurnCoverage.TURN_INCLUDES_AUDIO_ACTIVITY_AND_ALL_VIDEO

    def test_extended_thinking_applies_default_thinking_config(self):
        realtime = _make_realtime(model=LIVE_EXTENDED_THINKING_MODEL)
        thinking = realtime._base_config["thinking_config"]
        assert thinking["thinking_level"] == ThinkingLevel.HIGH

    def test_default_live_omits_thinking_config(self):
        realtime = _make_realtime()
        assert "thinking_config" not in realtime._base_config

    def test_blocking_rejected_on_extended_thinking(self):
        with pytest.raises(ValueError, match="NON_BLOCKING"):
            _make_realtime(model=LIVE_EXTENDED_THINKING_MODEL, blocking=True)

    def test_convert_tools_blocking_on_live(self):
        realtime = _make_realtime(blocking=True)
        result = realtime._convert_tools_to_provider_format(
            [
                {
                    "name": "get_weather",
                    "description": "Get weather",
                    "parameters_schema": {"type": "object", "properties": {}},
                }
            ]
        )
        assert result[0]["function_declarations"][0]["behavior"] == Behavior.BLOCKING

    async def test_send_client_content_forwards_to_session(self):
        realtime = _make_realtime()
        session = AsyncMock()
        realtime._real_session = session
        turn = Content(role="user", parts=[Part(text="hi")])

        await realtime.send_client_content(turns=turn, turn_complete=True)

        session.send_client_content.assert_awaited_once_with(
            turns=turn, turn_complete=True
        )

    async def test_function_response_omits_scheduling(self):
        """Setting `scheduling` makes several Live models close the socket (1007)."""
        realtime = _make_realtime()
        session = _CaptureSession()
        realtime._real_session = session

        @realtime.register_function(description="Get weather")
        async def get_weather(city: str) -> dict[str, bool]:
            return {"ok": True}

        await realtime._handle_function_call(
            FunctionCall(id="c1", name="get_weather", args={"city": "NYC"})
        )

        assert len(session.function_responses) == 1
        assert session.function_responses[0].scheduling is None


@pytest.fixture
async def realtime():
    rt = Realtime()
    try:
        await rt.connect()
        yield rt
    finally:
        await rt.close()


@pytest.fixture
async def realtime_with_tools():
    """Realtime with all function-calling test tools registered, then connected.

    Yields ``(rt, function_calls)`` where ``function_calls`` is a list each
    registered function appends to when invoked by the model.
    """
    rt = Realtime()
    function_calls: list[dict[str, Any]] = []

    @rt.register_function(description="Get current weather for a location")
    async def get_weather(location: str) -> dict[str, str]:
        """Get weather information for a location."""
        function_calls.append({"name": "get_weather", "location": location})
        return {
            "location": location,
            "temperature": "22°C",
            "condition": "Sunny",
            "humidity": "65%",
        }

    @rt.register_function(description="A function that sometimes fails")
    async def unreliable_function(input_data: str) -> dict[str, Any]:
        """A function that raises an error for testing."""
        function_calls.append({"name": "unreliable_function", "input": input_data})
        if "error" in input_data.lower():
            raise ValueError("Simulated error for testing")
        return {"result": f"Success: {input_data}"}

    @rt.register_function(description="Get current time")
    async def get_time() -> dict[str, str]:
        """Get current time."""
        function_calls.append({"name": "get_time"})
        return {"time": "2024-01-15 14:30:00", "timezone": "UTC"}

    @rt.register_function(description="Get system status")
    async def get_status() -> dict[str, str]:
        """Get system status."""
        function_calls.append({"name": "get_status"})
        return {"status": "healthy", "uptime": "24h"}

    try:
        await rt.connect()
        yield rt, function_calls
    finally:
        await rt.close()


@pytest.mark.integration
class TestGeminiRealtimeIntegration:
    """End-to-end tests against the live Gemini Realtime API."""

    async def test_simple_response_flow(self, realtime):
        async for _ in realtime.simple_response("Hello, can you hear me?"):
            pass

        items = await realtime.output.collect(timeout=10.0)
        audio = [i for i in items if isinstance(i, RealtimeAudioOutput)]
        agent_started = [i for i in items if isinstance(i, RealtimeAgentSpeechStarted)]
        agent_ended = [i for i in items if isinstance(i, RealtimeAgentSpeechEnded)]
        assert len(audio) > 0
        assert len(agent_started) >= 1
        assert len(agent_ended) >= 1
        assert any(not e.interrupted for e in agent_ended)

    async def test_audio_sending_flow(
        self, realtime, mia_audio_16khz, silence_1s_16khz
    ):
        participant = Participant(original=None, user_id="u", id="u")
        await realtime.simple_audio_response(mia_audio_16khz, participant)

        # Emulate a real pause in utterance so Gemini commits the turn and the
        # model starts responding (which is what triggers user_speech_ended).
        for _ in range(3):
            await realtime.simple_audio_response(silence_1s_16khz, participant)
            await asyncio.sleep(1.0)

        items = await realtime.output.collect(timeout=15.0)
        audio = [i for i in items if isinstance(i, RealtimeAudioOutput)]
        user_started = [i for i in items if isinstance(i, RealtimeUserSpeechStarted)]
        user_ended = [i for i in items if isinstance(i, RealtimeUserSpeechEnded)]
        assert len(audio) > 0
        assert len(user_started) >= 1
        assert len(user_ended) >= 1

    async def test_video_sending_flow(self, realtime, bunny_video_track):
        async for _ in realtime.simple_response(
            "Describe what you see in this video please"
        ):
            pass
        await asyncio.sleep(10.0)
        await realtime.watch_video_track(bunny_video_track)

        await asyncio.sleep(10.0)

        await realtime.stop_watching_video_track()
        audio = [
            i for i in realtime.output.peek() if isinstance(i, RealtimeAudioOutput)
        ]
        assert len(audio) > 0

    async def test_live_function_calling_basic(self, realtime_with_tools):
        rt, function_calls = realtime_with_tools

        async for _ in rt.simple_response(
            "What's the weather like in New York? Please use the get_weather function to check."
        ):
            pass
        await asyncio.sleep(8.0)

        weather_calls = [c for c in function_calls if c["name"] == "get_weather"]
        assert len(weather_calls) > 0, "get_weather was not called by Gemini"
        location = weather_calls[0]["location"].strip().lower()
        assert location == "nyc" or location.startswith("new york"), (
            f"Expected get_weather location to refer to New York, got "
            f"{weather_calls[0]['location']!r}"
        )

    async def test_live_function_calling_error_handling(self, realtime_with_tools):
        rt, function_calls = realtime_with_tools

        async for _ in rt.simple_response(
            "Please call the unreliable_function with 'error test' as input."
        ):
            pass
        await asyncio.sleep(8.0)

        unreliable_calls = [
            c for c in function_calls if c["name"] == "unreliable_function"
        ]
        assert len(unreliable_calls) > 0, "unreliable_function was not called"
        items = rt.output.peek()
        assert any(
            isinstance(i, (RealtimeAudioOutput, RealtimeAgentTranscript)) for i in items
        ), "No agent output received after function error"

    async def test_live_function_calling_multiple_functions(self, realtime_with_tools):
        rt, function_calls = realtime_with_tools

        async for _ in rt.simple_response(
            "Please check the current time and system status using the available functions."
        ):
            pass
        await asyncio.sleep(10.0)

        function_names = [call["name"] for call in function_calls]
        assert "get_time" in function_names, "get_time function was not called"
        assert "get_status" in function_names, "get_status function was not called"


@pytest.mark.integration
class TestGeminiRealtimeExtendedThinkingIntegration:
    """End-to-end tests against Gemini 3.8 Live Extended Thinking."""

    async def test_simple_response_and_tool_call(self):
        function_calls: list[dict[str, Any]] = []
        rt = Realtime(model=LIVE_EXTENDED_THINKING_MODEL)

        @rt.register_function(description="Get current weather for a location")
        async def get_weather(location: str) -> dict[str, str]:
            function_calls.append({"name": "get_weather", "location": location})
            return {
                "location": location,
                "temperature": "22°C",
                "condition": "Sunny",
            }

        try:
            await rt.connect()
        except (APIError, GeminiModelUnavailableError) as exc:
            pytest.skip(f"extended thinking model unavailable: {exc}")

        try:
            async for _ in rt.simple_response(
                "What's the weather like in New York? Please use the get_weather function to check."
            ):
                pass
            await asyncio.sleep(12.0)

            items = rt.output.peek()
            audio = [i for i in items if isinstance(i, RealtimeAudioOutput)]
            weather_calls = [c for c in function_calls if c["name"] == "get_weather"]
            assert len(audio) > 0 or len(weather_calls) > 0
            assert len(weather_calls) > 0, "get_weather was not called"
        finally:
            await rt.close()
