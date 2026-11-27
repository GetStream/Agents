import asyncio
import json
import os
import time
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest
import websockets
from dotenv import load_dotenv
from getstream.video.rtc import PcmData
from vision_agents.core.edge.types import Participant
from vision_agents.core.llm.realtime import (
    RealtimeAudioOutput,
    RealtimeConnectionState,
    RealtimeUserTranscript,
)
from vision_agents.plugins.xai import Realtime
from vision_agents.plugins.xai import xai_realtime as xai_realtime_module
from websockets.frames import Close

load_dotenv()


class TestXAIRealtimeConfiguration:
    """Unit tests for xAI Realtime configuration options."""

    async def test_default_configuration(self):
        """Test that default configuration is set correctly."""
        realtime = Realtime(api_key="test-key")
        assert realtime.model == "grok-voice-think-fast-2.0"
        assert realtime.voice == "ara"
        # xAI realtime emits PCM at 24 kHz natively.
        assert realtime.sample_rate == 24000
        assert realtime.turn_detection == "server_vad"
        assert realtime.provider_name == "xai_realtime"
        # VAD interrupt defaults to False to avoid mic-echo cancellation.
        assert realtime.vad_interrupt_response is False
        # Web search and X search enabled by default
        assert realtime.web_search is True
        assert realtime.x_search is True
        assert realtime.x_search_allowed_handles is None

    async def test_custom_configuration(self):
        """Test custom configuration options."""
        realtime = Realtime(
            api_key="test-key",
            voice="rex",
            turn_detection=None,
            vad_interrupt_response=True,
        )
        assert realtime.voice == "rex"
        assert realtime.sample_rate == 24000
        assert realtime.turn_detection is None
        assert realtime.vad_interrupt_response is True

    async def test_search_tools_can_be_disabled(self):
        """Test that web_search and x_search can be disabled."""
        realtime = Realtime(
            api_key="test-key",
            web_search=False,
            x_search=False,
        )
        assert realtime.web_search is False
        assert realtime.x_search is False

    async def test_x_search_allowed_handles(self):
        """Test that X search allowed handles can be configured."""
        realtime = Realtime(
            api_key="test-key",
            x_search_allowed_handles=["elonmusk", "xai"],
        )
        assert realtime.x_search_allowed_handles == ["elonmusk", "xai"]

    async def test_api_key_required(self):
        """Test that API key is required."""
        original_key = os.environ.pop("XAI_API_KEY", None)
        try:
            with pytest.raises(ValueError, match="XAI API key is required"):
                Realtime()
        finally:
            if original_key:
                os.environ["XAI_API_KEY"] = original_key

    async def test_instructions_setting(self):
        """Test that instructions can be set."""
        realtime = Realtime(api_key="test-key")
        realtime.set_instructions("You are a helpful assistant.")
        assert realtime._instructions == "You are a helpful assistant."

    @pytest.mark.parametrize(
        "model",
        [
            "grok-voice-think-fast-2.0",
            "grok-voice-latest",
            "grok-voice-think-fast-1.0",
        ],
    )
    async def test_model_is_sent_in_websocket_url(self, model):
        """Test that pinned and rolling model IDs are sent when connecting."""
        realtime = Realtime(api_key="test-key", model=model)
        websocket = AsyncMock()
        connection = MagicMock()
        connection.__aenter__ = AsyncMock(return_value=websocket)
        connection.__aexit__ = AsyncMock(return_value=None)

        with (
            patch(
                "vision_agents.plugins.xai.xai_realtime.websockets.connect",
                return_value=connection,
            ) as connect,
            patch.object(realtime, "_configure_session", new=AsyncMock()),
            patch.object(realtime, "_start_processing_task", new=AsyncMock()),
        ):
            await realtime.connect()

        connect.assert_called_once_with(
            uri=f"wss://api.x.ai/v1/realtime?model={model}",
            additional_headers={"Authorization": "Bearer test-key"},
        )
        await realtime.close()


class _FakeXAIWebSocket:
    """Minimal WebSocket double for testing reconnect and close behavior."""

    def __init__(
        self,
        incoming: list[str | Exception],
        *,
        fail_send_after_configure: Exception | None = None,
        hold_open: bool = False,
        enter_yield: bool = False,
        iter_gate: asyncio.Event | None = None,
    ) -> None:
        self._incoming = list(incoming)
        self._fail_send_after_configure = fail_send_after_configure
        self._hold_open = hold_open
        self._enter_yield = enter_yield
        self._iter_gate = iter_gate
        self._closed_event = asyncio.Event()
        self.sent: list[str] = []
        self.entered = False
        self.exited = False

    async def send(self, payload: str) -> None:
        self.sent.append(payload)
        if self._fail_send_after_configure is not None and len(self.sent) > 1:
            if self._iter_gate is not None:
                self._iter_gate.set()
            raise self._fail_send_after_configure

    def __aiter__(self) -> AsyncIterator[str]:
        return self._iter_messages()

    async def _iter_messages(self) -> AsyncIterator[str]:
        if self._iter_gate is not None:
            await self._iter_gate.wait()
        for item in self._incoming:
            if isinstance(item, Exception):
                raise item
            yield item
        if self._hold_open:
            await self._closed_event.wait()

    async def __aenter__(self) -> "_FakeXAIWebSocket":
        if self._enter_yield:
            await asyncio.sleep(0)
        self.entered = True
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: object,
    ) -> None:
        self.exited = True
        self._closed_event.set()


async def _wait_until(predicate, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            raise TimeoutError("Condition was not met before deadline")
        await asyncio.sleep(0.01)


class TestXAIRealtime:
    """Unit tests for xAI Realtime connection lifecycle and reconnects."""

    async def test_reconnect_from_processing_loop_closes_prior_ws_without_self_await(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        iter_gate = asyncio.Event()
        ws1 = _FakeXAIWebSocket(
            [websockets.ConnectionClosedError(Close(1011, "timeout"), None)],
            iter_gate=iter_gate,
        )
        ws2 = _FakeXAIWebSocket(
            [
                json.dumps(
                    {
                        "type": "conversation.item.input_audio_transcription.completed",
                        "transcript": "after reconnect",
                    }
                )
            ],
            hold_open=True,
            enter_yield=True,
        )
        sockets = [ws1, ws2]
        connect_calls = 0

        def fake_connect(*args: object, **kwargs: object) -> _FakeXAIWebSocket:
            nonlocal connect_calls
            connect_calls += 1
            return sockets.pop(0)

        monkeypatch.setattr(xai_realtime_module.websockets, "connect", fake_connect)

        rt = Realtime(api_key="test-key")
        rt._current_participant = Participant(original=None, user_id="u1", id="u1")

        try:
            await rt.connect()
            original_task = rt._processing_task
            assert original_task is not None
            iter_gate.set()

            await _wait_until(
                lambda: any(
                    isinstance(item, RealtimeUserTranscript)
                    and item.text == "after reconnect"
                    for item in rt.output.peek()
                )
            )
            assert connect_calls == 2
            assert ws1.exited is True
            assert ws2.entered is True
            assert ws2.exited is False
            assert rt.connected is True
            assert rt.connection_state is RealtimeConnectionState.CONNECTED
            assert rt._processing_task is original_task
            assert not rt._processing_task.done()
        finally:
            await rt.close()

        assert ws2.exited is True
        assert rt.connected is False
        assert rt.connection_state is RealtimeConnectionState.CLOSED

    async def test_concurrent_reconnects_are_serialized_and_deduplicated(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        reconnect_err = websockets.ConnectionClosedError(Close(1011, "timeout"), None)
        iter_gate = asyncio.Event()
        ws1 = _FakeXAIWebSocket(
            [reconnect_err],
            fail_send_after_configure=reconnect_err,
            iter_gate=iter_gate,
        )
        ws2 = _FakeXAIWebSocket([], hold_open=True)
        sockets = [ws1, ws2]
        connect_calls = 0
        ws1_exited_before_ws2_entered = False

        class _DelayedSocket:
            def __init__(self, inner: _FakeXAIWebSocket, delay: bool) -> None:
                self._inner = inner
                self._delay = delay

            async def __aenter__(self) -> _FakeXAIWebSocket:
                nonlocal ws1_exited_before_ws2_entered
                if self._delay:
                    ws1_exited_before_ws2_entered = ws1.exited
                    await asyncio.sleep(0.05)
                return await self._inner.__aenter__()

            async def __aexit__(
                self,
                exc_type: type[BaseException] | None,
                exc: BaseException | None,
                tb: object,
            ) -> None:
                await self._inner.__aexit__(exc_type, exc, tb)

        def fake_connect(*args: object, **kwargs: object) -> _DelayedSocket:
            nonlocal connect_calls
            connect_calls += 1
            sock = sockets.pop(0)
            return _DelayedSocket(sock, delay=(connect_calls > 1))

        monkeypatch.setattr(xai_realtime_module.websockets, "connect", fake_connect)

        rt = Realtime(api_key="test-key")
        pcm = PcmData(
            samples=np.zeros(240, dtype=np.int16), sample_rate=24000, format="s16"
        )
        try:
            await rt.connect()
            # Yield once so _processing_loop captures ws1 and waits on iter_gate;
            # ws1.send() will release iter_gate as both sends raise 1011 so all
            # three callers race into _reconnect(failed_ws=ws1) concurrently.
            await asyncio.sleep(0)

            async def _send_text() -> None:
                async for _ in rt.simple_response("hello"):
                    pass

            await asyncio.gather(
                _send_text(),
                rt.simple_audio_response(pcm),
            )
            await _wait_until(lambda: ws2.entered and rt.connected)
            assert connect_calls == 2
            assert ws1_exited_before_ws2_entered is True
            assert rt._ws is ws2
        finally:
            await rt.close()

        assert ws2.exited is True

    async def test_close_does_not_deadlock_when_pending_tool_triggers_reconnect(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        reconnect_err = websockets.ConnectionClosedError(Close(1011, "timeout"), None)
        ws1 = _FakeXAIWebSocket(
            [],
            fail_send_after_configure=reconnect_err,
            hold_open=True,
        )
        monkeypatch.setattr(
            xai_realtime_module.websockets,
            "connect",
            lambda *args, **kwargs: ws1,
        )

        rt = Realtime(api_key="test-key")
        await rt.connect()

        tool_started = asyncio.Event()
        proceed_tool = asyncio.Event()

        async def _pending_tool() -> None:
            tool_started.set()
            await proceed_tool.wait()
            async for _ in rt.simple_response("from tool"):
                pass

        rt._run_tool_in_background(_pending_tool())
        await asyncio.wait_for(tool_started.wait(), timeout=2.0)

        close_task = asyncio.create_task(rt.close())
        await asyncio.sleep(0)
        proceed_tool.set()

        await asyncio.wait_for(close_task, timeout=2.0)
        assert rt.connection_state is RealtimeConnectionState.CLOSED
        assert ws1.exited is True


@pytest.mark.integration
@pytest.mark.skipif(not os.getenv("XAI_API_KEY"), reason="XAI_API_KEY not set")
class TestXAIRealtimeIntegration:
    """End-to-end tests against the live xAI Realtime API."""

    @pytest.fixture
    async def realtime(self):
        rt = Realtime(api_key=os.getenv("XAI_API_KEY"), voice="ara")
        try:
            await rt.connect()
            yield rt
        finally:
            await rt.close()

    async def test_simple_response_flow(self, realtime):
        """A text prompt produces an audio response."""
        async for _ in realtime.simple_response(
            "Hello, can you hear me? Say yes briefly."
        ):
            pass

        await asyncio.sleep(5.0)
        audio = [
            i for i in realtime.output.peek() if isinstance(i, RealtimeAudioOutput)
        ]
        assert len(audio) > 0, "Expected audio output events"

    async def test_audio_sending_flow(self, realtime, mia_audio_16khz):
        """Audio chunks sent to xAI produce an audio response."""
        async for _ in realtime.simple_response(
            "Listen to the following audio and describe what you hear briefly."
        ):
            pass
        await asyncio.sleep(3.0)

        # Send audio in 100 ms chunks at the source sample rate.
        chunk_size = realtime.sample_rate // 10
        samples = mia_audio_16khz.samples
        for i in range(0, len(samples), chunk_size):
            chunk_samples = samples[i : i + chunk_size]
            if len(chunk_samples) > 0:
                chunk_pcm = PcmData(
                    samples=chunk_samples,
                    sample_rate=mia_audio_16khz.sample_rate,
                    format=mia_audio_16khz.format,
                )
                await realtime.simple_audio_response(chunk_pcm)
            await asyncio.sleep(0.05)

        await asyncio.sleep(8.0)
        audio = [
            i for i in realtime.output.peek() if isinstance(i, RealtimeAudioOutput)
        ]
        assert len(audio) > 0, "Expected audio output events after sending audio"

    @pytest.fixture
    async def realtime_with_tools(self):
        """Realtime with get_weather registered, then connected."""
        rt = Realtime(api_key=os.getenv("XAI_API_KEY"), voice="ara")

        @rt.register_function(description="Get the current weather")
        async def get_weather(location: str) -> str:
            """Get weather for a location."""
            return f"The weather in {location} is sunny and 72 degrees."

        try:
            await rt.connect()
            yield rt
        finally:
            await rt.close()

    async def test_function_calling(self, realtime_with_tools):
        """Function calling completes without raising."""
        async for _ in realtime_with_tools.simple_response(
            "What is the weather in San Francisco?"
        ):
            pass

        await asyncio.sleep(8.0)
