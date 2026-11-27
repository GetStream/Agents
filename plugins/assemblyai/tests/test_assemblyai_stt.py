import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest
from getstream.video.rtc.track_util import AudioFormat, PcmData
from vision_agents.core.edge.types import Participant
from vision_agents.core.stt import Transcript
from vision_agents.core.stt.events import STTErrorEvent
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

    async def test_model_attribute_matches_speech_model(self):
        default_stt = assemblyai.STT(api_key="test-key")
        custom_stt = assemblyai.STT(api_key="test-key", speech_model="universal-2")
        assert default_stt.model == "u3-rt-pro"
        assert custom_stt.model == "universal-2"

    async def test_close_pads_undersized_trailing_audio_buffer_to_min_chunk(self):
        stt = assemblyai.STT(api_key="test-key", sample_rate=16000)
        stt._connection_ready.set()

        # 20ms at 16kHz int16 mono = 320 samples = 640 bytes (< 50ms minimum of 1600 bytes)
        samples = np.arange(320, dtype=np.int16)
        pcm = PcmData(
            samples=samples, sample_rate=16000, format=AudioFormat.S16, channels=1
        )
        participant = Participant(original=None, user_id="user1", id="p1")

        await stt.process_audio(pcm, participant=participant)
        assert len(stt._audio_buffer) == 640

        sent_chunks: list[bytes] = []
        ws = MagicMock()
        ws.closed = False
        ws.send_bytes = AsyncMock(side_effect=sent_chunks.append)
        ws.send_str = AsyncMock()
        ws.close = AsyncMock()
        session = MagicMock()
        session.closed = False
        session.close = AsyncMock()

        stt._ws = ws
        stt._session = session
        stt._send_task = asyncio.create_task(stt._send_loop())

        await stt.close()

        assert len(sent_chunks) == 1
        assert len(sent_chunks[0]) == 1600
        assert sent_chunks[0][:640] == samples.tobytes()
        assert sent_chunks[0][640:] == bytes(1600 - 640)
        assert len(stt._audio_buffer) == 0

    async def test_close_preserves_trailing_audio_buffer_at_or_above_min_chunk(self):
        stt = assemblyai.STT(api_key="test-key", sample_rate=16000)
        stt._connection_ready.set()

        # 60ms at 16kHz int16 mono = 960 samples = 1920 bytes (>= 50ms minimum of 1600 bytes)
        samples = np.arange(960, dtype=np.int16)
        pcm = PcmData(
            samples=samples, sample_rate=16000, format=AudioFormat.S16, channels=1
        )
        participant = Participant(original=None, user_id="user1", id="p1")

        await stt.process_audio(pcm, participant=participant)
        assert len(stt._audio_buffer) == 1920

        sent_chunks: list[bytes] = []
        ws = MagicMock()
        ws.closed = False
        ws.send_bytes = AsyncMock(side_effect=sent_chunks.append)
        ws.send_str = AsyncMock()
        ws.close = AsyncMock()
        session = MagicMock()
        session.closed = False
        session.close = AsyncMock()

        stt._ws = ws
        stt._session = session
        stt._send_task = asyncio.create_task(stt._send_loop())

        await stt.close()

        assert len(sent_chunks) == 1
        assert sent_chunks[0] == samples.tobytes()

    async def test_start_timeout_disconnects_and_resets_started(self):
        stt = assemblyai.STT(api_key="test-key")

        ws = MagicMock()
        ws.closed = False
        ws.close = AsyncMock()
        session = MagicMock()
        session.closed = False
        session.close = AsyncMock()

        async def fake_connect():
            stt._connection_error = None
            stt._connection_ready.clear()
            stt._session = session
            stt._ws = ws
            stt._receive_task = asyncio.create_task(asyncio.sleep(60))
            stt._send_task = asyncio.create_task(asyncio.sleep(60))

        async def fake_wait_for(coro, timeout):
            coro.close()
            raise asyncio.TimeoutError

        with (
            patch.object(stt, "_connect", side_effect=fake_connect),
            patch(
                "vision_agents.plugins.assemblyai.stt.asyncio.wait_for",
                side_effect=fake_wait_for,
            ),
            pytest.raises(TimeoutError, match="Failed to connect to AssemblyAI"),
        ):
            await stt.start()

        assert stt.started is False
        assert stt._ws is None
        assert stt._session is None
        ws.close.assert_awaited_once()
        session.close.assert_awaited_once()

    async def test_start_fails_fast_on_server_error_frame_and_cleans_up(self):
        stt = assemblyai.STT(api_key="test-key")
        errors: list[STTErrorEvent] = []

        @stt.events.subscribe
        async def on_error(event: STTErrorEvent) -> None:
            errors.append(event)

        ws = MagicMock()
        ws.closed = False
        ws.close = AsyncMock()
        session = MagicMock()
        session.closed = False
        session.close = AsyncMock()

        async def fake_connect():
            stt._connection_error = None
            stt._connection_ready.clear()
            stt._session = session
            stt._ws = ws
            stt._receive_task = asyncio.create_task(asyncio.sleep(60))
            stt._send_task = asyncio.create_task(asyncio.sleep(60))
            stt._handle_message(
                {
                    "type": "Error",
                    "error": "Invalid API key",
                    "error_code": 1008,
                }
            )

        with (
            patch.object(stt, "_connect", side_effect=fake_connect),
            pytest.raises(RuntimeError, match=r"Invalid API key \(code 1008\)"),
        ):
            await stt.start()

        await stt.events.wait()
        assert stt.started is False
        assert stt._ws is None
        assert stt._session is None
        ws.close.assert_awaited_once()
        session.close.assert_awaited_once()
        assert len(errors) == 1
        assert "Invalid API key (code 1008)" in errors[0].error_message

    async def test_handle_message_error_emits_stt_error_event(self):
        stt = assemblyai.STT(api_key="test-key")
        stt._connection_ready.set()
        errors: list[STTErrorEvent] = []

        @stt.events.subscribe
        async def on_error(event: STTErrorEvent) -> None:
            errors.append(event)

        stt._handle_message({"type": "Error", "error": "Session expired"})
        await stt.events.wait()

        assert len(errors) == 1
        assert isinstance(errors[0].error, RuntimeError)
        assert errors[0].error_message == "Session expired"
        assert errors[0].context == "error"

    async def test_process_audio_unblocks_when_closed_while_waiting(self):
        stt = assemblyai.STT(api_key="test-key")
        pcm = PcmData(
            samples=np.zeros(1600, dtype=np.int16),
            sample_rate=16000,
            format=AudioFormat.S16,
            channels=1,
        )
        participant = Participant(original=None, user_id="user1", id="p1")

        task = asyncio.create_task(stt.process_audio(pcm, participant=participant))
        await asyncio.sleep(0)
        assert not task.done()

        await stt.close()
        await asyncio.wait_for(task, timeout=1.0)
        assert len(stt._audio_buffer) == 0
