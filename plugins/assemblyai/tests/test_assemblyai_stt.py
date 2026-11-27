import asyncio
import json

import aiohttp
import pytest
import vision_agents.plugins.assemblyai.stt as assemblyai_stt_module
from vision_agents.core.edge.types import Participant
from vision_agents.core.stt import Transcript
from vision_agents.plugins import assemblyai


class TestAssemblyAISTT:
    """Integration tests for AssemblyAI STT."""

    @pytest.fixture
    def participant(self) -> Participant:
        return Participant({}, user_id="test-user", id="test-user")

    @pytest.fixture
    async def stt(self):
        stt = assemblyai.STT()
        await stt.start()
        yield stt
        await stt.close()

    @pytest.mark.integration
    async def test_transcribe_mia_audio_48khz(
        self, stt, mia_audio_48khz, silence_2s_48khz, participant
    ):
        for chunk in mia_audio_48khz.chunks(480):
            await stt.process_audio(chunk, participant=participant)
            await asyncio.sleep(0.001)

        for chunk in silence_2s_48khz.chunks(480):
            await stt.process_audio(chunk, participant=participant)
            await asyncio.sleep(0.001)

        items = await stt.output.collect(timeout=10.0)
        transcripts = [i for i in items if isinstance(i, Transcript)]
        finals = [t for t in transcripts if t.final]
        full_transcript = " ".join(t.text for t in finals)
        assert "forgotten treasures" in full_transcript.lower()
        assert transcripts[0].participant == participant

    async def test_prompt_and_keyterms_exclusive(self):
        with pytest.raises(ValueError, match="cannot be used together"):
            assemblyai.STT(
                api_key="test",
                prompt="test prompt",
                keyterms_prompt=["term1"],
            )

    async def test_default_configuration(self):
        stt = assemblyai.STT(api_key="test-key")
        assert stt._speech_model == "u3-rt-pro"
        assert stt._sample_rate == 16000
        assert stt.turn_detection is True
        assert stt.provider_name == "assemblyai"

    async def test_reconnect_defaults(self):
        stt = assemblyai.STT(api_key="test-key")
        assert stt._max_reconnect_attempts == 3
        assert stt._reconnect_backoff_initial_s == 0.5
        assert stt._reconnect_backoff_max_s == 4.0

    async def test_custom_reconnect_config(self):
        stt = assemblyai.STT(
            api_key="test-key",
            max_reconnect_attempts=5,
            reconnect_backoff_initial_s=1.0,
            reconnect_backoff_max_s=8.0,
        )
        assert stt._max_reconnect_attempts == 5
        assert stt._reconnect_backoff_initial_s == 1.0
        assert stt._reconnect_backoff_max_s == 8.0

    async def test_speaker_labels_default_disabled(self):
        stt = assemblyai.STT(api_key="test-key")
        assert stt._speaker_labels_enabled is False
        assert stt._max_speakers is None
        assert stt._speaker_participants == {}

    async def test_speaker_labels_enabled(self):
        stt = assemblyai.STT(api_key="test-key", speaker_labels=True)
        assert stt._speaker_labels_enabled is True

    async def test_speaker_labels_with_max_speakers(self):
        stt = assemblyai.STT(api_key="test-key", speaker_labels=True, max_speakers=3)
        assert stt._speaker_labels_enabled is True
        assert stt._max_speakers == 3

    async def test_max_speakers_without_speaker_labels_raises(self):
        with pytest.raises(
            ValueError, match="max_speakers requires speaker_labels=True"
        ):
            assemblyai.STT(api_key="test-key", max_speakers=2)

    async def test_build_ws_url_without_speaker_labels(self):
        stt = assemblyai.STT(api_key="test-key")
        url = stt._build_ws_url()
        assert "speaker_labels" not in url
        assert "max_speakers" not in url

    async def test_build_ws_url_with_speaker_labels(self):
        stt = assemblyai.STT(api_key="test-key", speaker_labels=True)
        url = stt._build_ws_url()
        assert "speaker_labels=true" in url
        assert "max_speakers" not in url

    async def test_build_ws_url_with_speaker_labels_and_max_speakers(self):
        stt = assemblyai.STT(api_key="test-key", speaker_labels=True, max_speakers=4)
        url = stt._build_ws_url()
        assert "speaker_labels=true" in url
        assert "max_speakers=4" in url

    async def test_resolve_participant_without_diarization(self):
        stt = assemblyai.STT(api_key="test-key")
        participant = Participant(original=None, user_id="user1", id="p1")
        stt._current_participant = participant

        result = stt._resolve_participant("A")
        assert result is participant

    async def test_resolve_participant_with_null_label(self):
        stt = assemblyai.STT(api_key="test-key", speaker_labels=True)
        participant = Participant(original=None, user_id="user1", id="p1")
        stt._current_participant = participant

        result = stt._resolve_participant(None)
        assert result is participant

    async def test_resolve_participant_creates_synthetic(self):
        stt = assemblyai.STT(api_key="test-key", speaker_labels=True)

        result = stt._resolve_participant("A")
        assert result is not None
        assert result.user_id == "speaker_A"
        assert result.id.startswith("speaker_A_")
        assert result.original is None

    async def test_resolve_participant_caches_synthetic(self):
        stt = assemblyai.STT(api_key="test-key", speaker_labels=True)

        first = stt._resolve_participant("B")
        second = stt._resolve_participant("B")
        assert first is second

    async def test_resolve_participant_distinct_per_label(self):
        stt = assemblyai.STT(api_key="test-key", speaker_labels=True)

        a = stt._resolve_participant("A")
        b = stt._resolve_participant("B")
        assert a is not b
        assert a.user_id == "speaker_A"
        assert b.user_id == "speaker_B"
        assert a.id != b.id

    async def test_handle_turn_with_speaker_label_emits_correct_participant(self):
        stt = assemblyai.STT(api_key="test-key", speaker_labels=True)

        stt._handle_turn(
            {
                "transcript": "Hello world",
                "end_of_turn": True,
                "speaker_label": "A",
            }
        )

        transcripts = [i for i in stt.output.peek() if isinstance(i, Transcript)]
        assert len(transcripts) == 1
        assert transcripts[0].text == "Hello world"
        assert transcripts[0].final
        assert transcripts[0].participant.user_id == "speaker_A"
        assert transcripts[0].response.other == {"speaker_label": "A"}

    async def test_handle_turn_partial_with_speaker_label(self):
        stt = assemblyai.STT(api_key="test-key", speaker_labels=True)

        stt._handle_turn(
            {
                "transcript": "Hello",
                "end_of_turn": False,
                "speaker_label": "B",
            }
        )

        transcripts = [i for i in stt.output.peek() if isinstance(i, Transcript)]
        assert len(transcripts) == 1
        assert transcripts[0].text == "Hello"
        assert transcripts[0].mode == "replacement"
        assert transcripts[0].participant.user_id == "speaker_B"
        assert transcripts[0].response.other == {"speaker_label": "B"}

    async def test_handle_turn_without_diarization_no_other_field(self):
        stt = assemblyai.STT(api_key="test-key")
        participant = Participant(original=None, user_id="user1", id="p1")
        stt._current_participant = participant

        stt._handle_turn(
            {
                "transcript": "Hello world",
                "end_of_turn": True,
            }
        )

        transcripts = [i for i in stt.output.peek() if isinstance(i, Transcript)]
        assert len(transcripts) == 1
        assert transcripts[0].response.other is None
        assert transcripts[0].participant is participant

    async def test_handle_turn_null_speaker_label_falls_back(self):
        stt = assemblyai.STT(api_key="test-key", speaker_labels=True)
        participant = Participant(original=None, user_id="user1", id="p1")
        stt._current_participant = participant

        stt._handle_turn(
            {
                "transcript": "Short",
                "end_of_turn": True,
                "speaker_label": None,
            }
        )

        transcripts = [i for i in stt.output.peek() if isinstance(i, Transcript)]
        assert len(transcripts) == 1
        assert transcripts[0].participant is participant
        assert transcripts[0].response.other == {"speaker_label": None}

    async def test_handle_turn_multi_speaker_conversation(self):
        stt = assemblyai.STT(api_key="test-key", speaker_labels=True)

        stt._handle_turn(
            {
                "transcript": "Good morning",
                "end_of_turn": True,
                "speaker_label": "A",
            }
        )
        stt._handle_turn(
            {
                "transcript": "Good morning, how are you?",
                "end_of_turn": True,
                "speaker_label": "B",
            }
        )
        stt._handle_turn(
            {
                "transcript": "I'm doing well, thanks",
                "end_of_turn": True,
                "speaker_label": "A",
            }
        )

        transcripts = [i for i in stt.output.peek() if isinstance(i, Transcript)]
        assert len(transcripts) == 3
        assert transcripts[0].participant.user_id == "speaker_A"
        assert transcripts[1].participant.user_id == "speaker_B"
        assert transcripts[2].participant.user_id == "speaker_A"
        assert transcripts[0].participant is transcripts[2].participant

    async def test_close_during_reconnect_backoff_does_not_spawn_zombie_websocket(
        self, monkeypatch
    ):
        class FakeMsg:
            def __init__(self, msg_type: aiohttp.WSMsgType, data: str = "") -> None:
                self.type = msg_type
                self.data = data

        class FakeWebSocket:
            def __init__(
                self, messages: list[FakeMsg], drop_event: asyncio.Event
            ) -> None:
                self._messages = list(messages)
                self._drop_event = drop_event
                self.closed = False

            def __aiter__(self):
                return self

            async def __anext__(self) -> FakeMsg:
                if self._messages:
                    return self._messages.pop(0)
                await self._drop_event.wait()
                raise StopAsyncIteration

            async def send_bytes(self, data: bytes) -> None:
                return None

            async def send_str(self, data: str) -> None:
                return None

            async def close(self) -> None:
                self.closed = True
                self._drop_event.set()

        ws_connect_count = 0
        disconnected = asyncio.Event()
        drop_event = asyncio.Event()

        class FakeClientSession:
            def __init__(self) -> None:
                self.closed = False

            async def ws_connect(self, url: str, headers: dict[str, str]):
                nonlocal ws_connect_count
                ws_connect_count += 1
                return FakeWebSocket(
                    [
                        FakeMsg(
                            aiohttp.WSMsgType.TEXT,
                            json.dumps({"type": "Begin", "id": "sess-1"}),
                        ),
                    ],
                    drop_event,
                )

            async def close(self) -> None:
                self.closed = True
                disconnected.set()

        monkeypatch.setattr(
            assemblyai_stt_module.aiohttp, "ClientSession", FakeClientSession
        )

        stt = assemblyai.STT(
            api_key="test-key",
            max_reconnect_attempts=3,
            reconnect_backoff_initial_s=5.0,
            reconnect_backoff_max_s=10.0,
        )
        await stt.start()
        receive_task = stt._receive_task
        assert receive_task is not None

        drop_event.set()
        await asyncio.wait_for(disconnected.wait(), timeout=0.5)
        await asyncio.wait_for(stt.close(), timeout=0.5)

        assert receive_task.done()
        assert ws_connect_count == 1
        assert stt._ws is None
        assert stt._session is None

    async def test_close_during_second_reconnect_backoff_cancels_reconnect_task(
        self, monkeypatch
    ):
        class FakeMsg:
            def __init__(self, msg_type: aiohttp.WSMsgType, data: str = "") -> None:
                self.type = msg_type
                self.data = data

        class FakeWebSocket:
            def __init__(
                self, messages: list[FakeMsg], drop_event: asyncio.Event | None = None
            ) -> None:
                self._messages = list(messages)
                self._drop_event = drop_event
                self.closed = False

            def __aiter__(self):
                return self

            async def __anext__(self) -> FakeMsg:
                if self._messages:
                    return self._messages.pop(0)
                if self._drop_event is not None:
                    await self._drop_event.wait()
                raise StopAsyncIteration

            async def send_bytes(self, data: bytes) -> None:
                return None

            async def send_str(self, data: str) -> None:
                return None

            async def close(self) -> None:
                self.closed = True
                if self._drop_event is not None:
                    self._drop_event.set()

        ws_connect_count = 0
        close_count = 0
        attempt1_disconnected = asyncio.Event()
        drop_event = asyncio.Event()

        class FakeClientSession:
            def __init__(self) -> None:
                self.closed = False

            async def ws_connect(self, url: str, headers: dict[str, str]):
                nonlocal ws_connect_count
                ws_connect_count += 1
                if ws_connect_count == 1:
                    return FakeWebSocket(
                        [
                            FakeMsg(
                                aiohttp.WSMsgType.TEXT,
                                json.dumps({"type": "Begin", "id": "sess-1"}),
                            ),
                        ],
                        drop_event,
                    )
                # Attempt 1 connects, then closes immediately before Begin.
                return FakeWebSocket([])

            async def close(self) -> None:
                nonlocal close_count
                self.closed = True
                close_count += 1
                if close_count >= 2:
                    attempt1_disconnected.set()

        monkeypatch.setattr(
            assemblyai_stt_module.aiohttp, "ClientSession", FakeClientSession
        )

        stt = assemblyai.STT(
            api_key="test-key",
            max_reconnect_attempts=3,
            reconnect_backoff_initial_s=0.1,
            reconnect_backoff_max_s=5.0,
        )
        await stt.start()
        reconnect_task = stt._receive_task
        assert reconnect_task is not None

        drop_event.set()
        await asyncio.wait_for(attempt1_disconnected.wait(), timeout=0.5)
        await asyncio.sleep(0.01)

        await asyncio.wait_for(stt.close(), timeout=0.5)

        assert reconnect_task.done()
        assert ws_connect_count == 2
        assert stt._ws is None
        assert stt._session is None

    async def test_process_audio_does_not_hang_on_close_while_waiting(
        self, silence_1s_16khz, participant: Participant
    ):
        stt = assemblyai.STT(api_key="test-key")
        task = asyncio.create_task(
            stt.process_audio(silence_1s_16khz, participant=participant)
        )
        await asyncio.sleep(0)
        await stt.close()

        await asyncio.wait_for(task, timeout=0.5)

    async def test_process_audio_does_not_hang_on_exhausted_reconnect(
        self, monkeypatch, silence_1s_16khz, participant: Participant
    ):
        class FakeMsg:
            def __init__(self, msg_type: aiohttp.WSMsgType, data: str = "") -> None:
                self.type = msg_type
                self.data = data

        class FakeWebSocket:
            def __init__(
                self, messages: list[FakeMsg], drop_event: asyncio.Event
            ) -> None:
                self._messages = list(messages)
                self._drop_event = drop_event
                self.closed = False

            def __aiter__(self):
                return self

            async def __anext__(self) -> FakeMsg:
                if self._messages:
                    return self._messages.pop(0)
                await self._drop_event.wait()
                raise StopAsyncIteration

            async def send_bytes(self, data: bytes) -> None:
                return None

            async def send_str(self, data: str) -> None:
                return None

            async def close(self) -> None:
                self.closed = True
                self._drop_event.set()

        attempts = 0
        reconnecting = asyncio.Event()
        drop_event = asyncio.Event()

        class FakeClientSession:
            def __init__(self) -> None:
                self.closed = False

            async def ws_connect(self, url: str, headers: dict[str, str]):
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    return FakeWebSocket(
                        [
                            FakeMsg(
                                aiohttp.WSMsgType.TEXT,
                                json.dumps({"type": "Begin", "id": "sess-1"}),
                            ),
                        ],
                        drop_event,
                    )
                raise OSError("network down")

            async def close(self) -> None:
                self.closed = True
                reconnecting.set()

        monkeypatch.setattr(
            assemblyai_stt_module.aiohttp, "ClientSession", FakeClientSession
        )

        stt = assemblyai.STT(
            api_key="test-key",
            max_reconnect_attempts=1,
            reconnect_backoff_initial_s=0.05,
            reconnect_backoff_max_s=0.05,
        )
        await stt.start()
        try:
            drop_event.set()
            await asyncio.wait_for(reconnecting.wait(), timeout=0.5)
            await asyncio.wait_for(
                stt.process_audio(silence_1s_16khz, participant=participant),
                timeout=0.5,
            )
        finally:
            await stt.close()

    async def test_start_timeout_disconnects_and_resets_started(self, monkeypatch):
        class HangingWebSocket:
            def __init__(self) -> None:
                self.closed = False
                self._closed_event = asyncio.Event()

            def __aiter__(self):
                return self

            async def __anext__(self):
                await self._closed_event.wait()
                raise StopAsyncIteration

            async def send_bytes(self, data: bytes) -> None:
                return None

            async def send_str(self, data: str) -> None:
                return None

            async def close(self) -> None:
                self.closed = True
                self._closed_event.set()

        created_sockets: list[HangingWebSocket] = []

        class FakeClientSession:
            def __init__(self) -> None:
                self.closed = False
                created_sessions.append(self)

            async def ws_connect(self, url: str, headers: dict[str, str]):
                ws = HangingWebSocket()
                created_sockets.append(ws)
                return ws

            async def close(self) -> None:
                self.closed = True

        created_sessions: list[FakeClientSession] = []

        real_wait_for = asyncio.wait_for

        async def fast_wait_for(coro, timeout=None):
            return await real_wait_for(coro, timeout=0.05)

        monkeypatch.setattr(
            assemblyai_stt_module.aiohttp, "ClientSession", FakeClientSession
        )
        monkeypatch.setattr(assemblyai_stt_module.asyncio, "wait_for", fast_wait_for)

        stt = assemblyai.STT(api_key="test-key")
        with pytest.raises(TimeoutError, match="Failed to connect to AssemblyAI"):
            await stt.start()

        assert stt.started is False
        assert stt._ws is None
        assert stt._session is None
        assert len(created_sockets) == 1
        assert created_sockets[0].closed is True
        assert len(created_sessions) == 1
        assert created_sessions[0].closed is True
