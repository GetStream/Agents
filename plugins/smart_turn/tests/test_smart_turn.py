import logging

import numpy as np
import pytest
from conftest import skip_if_huggingface_model_unavailable
from getstream.video.rtc.track_util import AudioFormat, PcmData
from vision_agents.core.agents.conversation import InMemoryConversation
from vision_agents.core.edge.types import Participant
from vision_agents.core.turn_detection import TurnEnded, TurnStarted
from vision_agents.core.vad.silero import SileroVADSession, SileroVADSessionPool
from vision_agents.plugins.smart_turn.smart_turn_detection import (
    DetectorLifecycle,
    SmartTurnDetection,
    UtteranceState,
)

logger = logging.getLogger(__name__)


class _EnergyVADSession(SileroVADSession):
    def __init__(self) -> None:
        pass

    def predict_speech(self, pcm: PcmData) -> float:
        if len(pcm.samples) == 0:
            return 0.0
        return 0.9 if float(np.max(np.abs(pcm.samples))) > 0.1 else 0.0


class _ControlledSmartTurnDetection(SmartTurnDetection):
    def __init__(
        self,
        predictions: list[float | Exception],
        silence_duration_ms: int = 1000,
    ) -> None:
        super().__init__(silence_duration_ms=silence_duration_ms)
        self._vad_session = _EnergyVADSession()
        self._predictions: list[float | Exception] = list(predictions)
        self.prediction_count = 0

    async def _predict_turn_completed(
        self, pcm: PcmData, participant: Participant
    ) -> float:
        self.prediction_count += 1
        outcome = (
            self._predictions.pop(0)
            if len(self._predictions) > 1
            else self._predictions[0]
        )
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def _make_pcm(num_chunks: int, amplitude: float) -> PcmData:
    samples = np.full(num_chunks * 512, amplitude, dtype=np.float32)
    return PcmData(
        samples=samples,
        sample_rate=16000,
        channels=1,
        format=AudioFormat.F32,
    )


@pytest.fixture
async def smart_turn():
    td = SmartTurnDetection()
    try:
        await td.warmup()
    except Exception as exc:
        await td.close()
        skip_if_huggingface_model_unavailable(exc, "Smart Turn model")
        raise
    await td.start()
    yield td
    await td.close()


class TestSmartTurn:
    async def test_silero_predict(self, mia_audio_16khz, tmp_path):
        vad_pool = await SileroVADSessionPool.load(tmp_path.as_posix())
        vad = vad_pool.session()

        for pcm_chunk in mia_audio_16khz.chunks(chunk_size=512):
            if len(pcm_chunk.samples) != 512:
                continue
            result = vad.predict_speech(pcm_chunk)
            assert 1.0 > result > 0.0

    async def test_turn_detection_chunks(self, smart_turn, mia_audio_16khz):
        participant = Participant(user_id="mia", id="mia", original={})
        conversation = InMemoryConversation(instructions="be nice", messages=[])

        for pcm in mia_audio_16khz.chunks(chunk_size=304):
            await smart_turn.process_audio(pcm, participant, conversation)

        await smart_turn.wait_for_processing_complete()

        items = await smart_turn.output.collect(timeout=1.0)
        kinds = [
            "start"
            if isinstance(item, TurnStarted)
            else "stop"
            if isinstance(item, TurnEnded)
            else None
            for item in items
        ]
        assert kinds == ["start", "stop"] or kinds == [
            "start",
            "stop",
            "start",
            "stop",
        ]

    async def test_turn_detection(self, smart_turn, mia_audio_16khz):
        participant = Participant(user_id="mia", id="mia", original={})
        conversation = InMemoryConversation(instructions="be nice", messages=[])

        await smart_turn.process_audio(mia_audio_16khz, participant, conversation)

        await smart_turn.wait_for_processing_complete()

        items = await smart_turn.output.collect(timeout=1.0)
        kinds = [
            "start"
            if isinstance(item, TurnStarted)
            else "stop"
            if isinstance(item, TurnEnded)
            else None
            for item in items
        ]
        # With continuous processing, we may get multiple start/stop cycles
        assert kinds == ["start", "stop"] or kinds == [
            "start",
            "stop",
            "start",
            "stop",
        ]

    async def test_silence_does_not_start_segment(self, smart_turn, silence_1s_16khz):
        participant = Participant(user_id="mia", id="mia", original={})
        conversation = InMemoryConversation(instructions="be nice", messages=[])

        await smart_turn.process_audio(silence_1s_16khz, participant, conversation)
        await smart_turn.wait_for_processing_complete()

        items = await smart_turn.output.collect(timeout=0.5)
        assert items == []

    async def test_speech_starts_segment(self, smart_turn, mia_audio_16khz):
        participant = Participant(user_id="mia", id="mia", original={})
        conversation = InMemoryConversation(instructions="be nice", messages=[])

        await smart_turn.process_audio(mia_audio_16khz, participant, conversation)
        await smart_turn.wait_for_processing_complete()

        items = await smart_turn.output.collect(timeout=1.0)
        assert any(isinstance(item, TurnStarted) for item in items)

    async def test_incomplete_turn_enters_cooldown_instead_of_per_chunk_inference(self):
        td = _ControlledSmartTurnDetection(
            predictions=[0.2, 0.9],
            silence_duration_ms=1000,
        )
        participant = Participant(user_id="mia", id="mia", original={})
        assert td.lifecycle is DetectorLifecycle.STOPPED
        assert td.utterance_state is UtteranceState.IDLE
        await td.start()
        try:
            assert td.lifecycle is DetectorLifecycle.RUNNING
            # 5 speech chunks -> SPEAKING
            await td.process_audio(_make_pcm(5, 0.5), participant)
            await td.wait_for_processing_complete()
            assert td.utterance_state is UtteranceState.SPEAKING

            # 3 silence chunks -> TRAILING_SILENCE (480ms <= 1000ms)
            await td.process_audio(_make_pcm(3, 0.0), participant)
            await td.wait_for_processing_complete()
            assert td.utterance_state is UtteranceState.TRAILING_SILENCE
            assert td.prediction_count == 0

            # 9 more silence chunks: chunk 5 triggers eval (1120ms > 1000ms) -> incomplete (0.2) -> COOLDOWN
            # Remaining 4 silence chunks (640ms <= 1000ms) stay in COOLDOWN with no extra inference
            await td.process_audio(_make_pcm(9, 0.0), participant)
            await td.wait_for_processing_complete()

            assert td.prediction_count == 1
            assert td.utterance_state is UtteranceState.COOLDOWN
            items = await td.output.collect(timeout=0.1)
            assert [type(item) for item in items] == [TurnStarted]

            # 3 more silence chunks complete a full new 1000ms window -> re-evaluates (0.9) -> IDLE
            await td.process_audio(_make_pcm(3, 0.0), participant)
            await td.wait_for_processing_complete()

            assert td.prediction_count == 2
            assert td.utterance_state is UtteranceState.IDLE
            items = await td.output.collect(timeout=0.1)
            assert [type(item) for item in items] == [TurnEnded]
        finally:
            await td.close()
        assert td.lifecycle is DetectorLifecycle.STOPPED

    async def test_cooldown_returns_to_speaking_when_speech_resumes(self):
        td = _ControlledSmartTurnDetection(
            predictions=[0.2, 0.9],
            silence_duration_ms=500,
        )
        participant = Participant(user_id="mia", id="mia", original={})
        await td.start()
        try:
            # 5 speech chunks + 5 silence chunks (1 held in buffer -> 4 processed = 640ms > 500ms) -> COOLDOWN
            await td.process_audio(_make_pcm(5, 0.5), participant)
            await td.process_audio(_make_pcm(5, 0.0), participant)
            await td.wait_for_processing_complete()
            assert td.prediction_count == 1
            assert td.utterance_state is UtteranceState.COOLDOWN

            # Speech resumes (5 chunks -> 4 processed > 3 hysteresis) -> SPEAKING
            await td.process_audio(_make_pcm(5, 0.5), participant)
            await td.wait_for_processing_complete()
            assert td.utterance_state is UtteranceState.SPEAKING
            assert td.prediction_count == 1

            # Subsequent silence completes the turn -> IDLE
            await td.process_audio(_make_pcm(5, 0.0), participant)
            await td.wait_for_processing_complete()
            assert td.prediction_count == 2
            assert td.utterance_state is UtteranceState.IDLE
            items = await td.output.collect(timeout=0.1)
            assert [type(item) for item in items] == [TurnStarted, TurnEnded]
        finally:
            await td.close()

    async def test_incomplete_turn_at_max_duration_does_not_storm_inference(self):
        td = _ControlledSmartTurnDetection(
            predictions=[0.2],
            silence_duration_ms=1000,
        )
        participant = Participant(user_id="mia", id="mia", original={})
        await td.start()
        try:
            # 8s = 250 chunks at 32ms; feed 265 continuous speech chunks (~8.48s)
            await td.process_audio(_make_pcm(265, 0.5), participant)
            await td.wait_for_processing_complete()

            assert td.prediction_count == 1
        finally:
            await td.close()

    async def test_silence_duration_ms_config_is_honored(self):
        participant = Participant(user_id="mia", id="mia", original={})

        # Short threshold (500ms): 5 silence chunks (1 held in buffer -> 4 processed = 640ms > 500ms, < 2000ms)
        short_td = _ControlledSmartTurnDetection(
            predictions=[0.9],
            silence_duration_ms=500,
        )
        await short_td.start()
        try:
            await short_td.process_audio(_make_pcm(5, 0.5), participant)
            await short_td.process_audio(_make_pcm(5, 0.0), participant)
            await short_td.wait_for_processing_complete()

            items = await short_td.output.collect(timeout=0.1)
            assert [type(item) for item in items] == [TurnStarted, TurnEnded]
        finally:
            await short_td.close()

        # Long threshold (4000ms): 15 silence chunks = 2400ms > 2000ms but < 4000ms
        long_td = _ControlledSmartTurnDetection(
            predictions=[0.9],
            silence_duration_ms=4000,
        )
        await long_td.start()
        try:
            await long_td.process_audio(_make_pcm(5, 0.5), participant)
            await long_td.process_audio(_make_pcm(15, 0.0), participant)
            await long_td.wait_for_processing_complete()

            assert long_td.prediction_count == 0
            items = await long_td.output.collect(timeout=0.1)
            assert [type(item) for item in items] == [TurnStarted]
        finally:
            await long_td.close()

    async def test_dead_state_fields_removed(self):
        td = SmartTurnDetection()
        assert "_turn_in_progress" not in td.__dict__
        assert "_tail_silence_ms" not in td.__dict__

    async def test_restart_after_close_processes_audio(self):
        td = _ControlledSmartTurnDetection(
            predictions=[0.9],
            silence_duration_ms=500,
        )
        participant = Participant(user_id="mia", id="mia", original={})
        await td.start()
        await td.close()
        await td.start()
        try:
            await td.process_audio(_make_pcm(5, 0.5), participant)
            await td.process_audio(_make_pcm(5, 0.0), participant)
            await td.wait_for_processing_complete()

            items = await td.output.collect(timeout=0.2)
            assert [type(item) for item in items] == [TurnStarted, TurnEnded]
        finally:
            await td.close()

    async def test_audio_ignored_when_stopped_and_drained_on_close(self):
        td = _ControlledSmartTurnDetection(
            predictions=[0.9],
            silence_duration_ms=500,
        )
        participant = Participant(user_id="mia", id="mia", original={})

        # Enqueue while stopped before start(): must be dropped.
        await td.process_audio(_make_pcm(5, 0.5), participant)

        await td.start()
        # Buffer some pre-speech silence, then enqueue speech and close immediately
        # so the speech packet is left queued when close() runs.
        await td.process_audio(_make_pcm(3, 0.0), participant)
        await td.wait_for_processing_complete()
        await td.process_audio(_make_pcm(5, 0.5), participant)
        await td.close()

        # Enqueue while stopped after close(): must also be dropped.
        await td.process_audio(_make_pcm(5, 0.5), participant)
        assert len(td._pre_speech_buffer.samples) == 0

        await td.start()
        try:
            await td.wait_for_processing_complete()
            items = await td.output.collect(timeout=0.1)
            assert items == []
            assert td.utterance_state is UtteranceState.IDLE
        finally:
            await td.close()

    async def test_evaluation_exception_enters_cooldown_and_recovers(self):
        td = _ControlledSmartTurnDetection(
            predictions=[RuntimeError("inference failed"), 0.9],
            silence_duration_ms=1000,
        )
        participant = Participant(user_id="mia", id="mia", original={})
        await td.start()
        try:
            # 5 speech chunks + 8 silence chunks (1 held in buffer -> 7 processed):
            # 7th silence triggers eval -> raises RuntimeError -> enters COOLDOWN.
            await td.process_audio(_make_pcm(5, 0.5), participant)
            await td.process_audio(_make_pcm(8, 0.0), participant)
            await td.wait_for_processing_complete()

            assert td.utterance_state is UtteranceState.COOLDOWN
            assert td.prediction_count == 1
            items = await td.output.collect(timeout=0.1)
            assert [type(item) for item in items] == [TurnStarted]

            # 4 more silence chunks (640ms <= 1000ms) remain in COOLDOWN with no extra inference.
            await td.process_audio(_make_pcm(4, 0.0), participant)
            await td.wait_for_processing_complete()

            assert td.utterance_state is UtteranceState.COOLDOWN
            assert td.prediction_count == 1

            # 3 more silence chunks complete a fresh 1000ms window (1120ms > 1000ms) -> succeeds -> IDLE.
            await td.process_audio(_make_pcm(3, 0.0), participant)
            await td.wait_for_processing_complete()

            assert td.utterance_state is UtteranceState.IDLE
            assert td.prediction_count == 2
            items = await td.output.collect(timeout=0.1)
            assert [type(item) for item in items] == [TurnEnded]
        finally:
            await td.close()

    """
    TODO
    - Test that the 2nd turn detect includes the audio from the first turn
    - Test that turn detection is ran after 8s of audio
    - Test that turn detection is run after speech and 2s of silence
    """
