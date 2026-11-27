import logging

import numpy as np
import pytest
from getstream.video.rtc.track_util import AudioFormat, PcmData
from vision_agents.core.agents.conversation import InMemoryConversation
from vision_agents.core.edge.types import Participant
from vision_agents.core.turn_detection import TurnEnded, TurnStarted
from vision_agents.core.vad.silero import SileroVADSession
from vision_agents.plugins.vogent.vogent_turn_detection import (
    DetectorLifecycle,
    UtteranceState,
    VogentTurnDetection,
)

logger = logging.getLogger(__name__)


class _EnergyVADSession(SileroVADSession):
    def __init__(self) -> None:
        pass

    def predict_speech(self, pcm: PcmData) -> float:
        if len(pcm.samples) == 0:
            return 0.0
        return 0.9 if float(np.max(np.abs(pcm.samples))) > 0.1 else 0.0


class _ControlledVogentTurnDetection(VogentTurnDetection):
    def __init__(
        self,
        predictions: list[tuple[bool, float]],
        silence_duration_ms: int = 1000,
        max_segment_duration_seconds: int = 8,
    ) -> None:
        super().__init__(
            silence_duration_ms=silence_duration_ms,
            max_segment_duration_seconds=max_segment_duration_seconds,
        )
        self._vad_session = _EnergyVADSession()
        self._predictions = list(predictions)
        self.prediction_count = 0

    async def _transcribe_segment(self, pcm: PcmData) -> str:
        return "hello"

    async def _predict_turn_completed(
        self,
        pcm: PcmData,
        prev_line: str,
        curr_line: str,
    ) -> tuple[bool, float]:
        self.prediction_count += 1
        if len(self._predictions) > 1:
            return self._predictions.pop(0)
        return self._predictions[0]


def _make_pcm(num_chunks: int, amplitude: float) -> PcmData:
    samples = np.full(num_chunks * 512, amplitude, dtype=np.float32)
    return PcmData(
        samples=samples,
        sample_rate=16000,
        channels=1,
        format=AudioFormat.F32,
    )


@pytest.fixture
async def vogent_turn_detection():
    td = VogentTurnDetection()
    await td.warmup()
    await td.start()
    try:
        yield td
    finally:
        await td.close()


class TestVogentTurnDetection:
    @pytest.mark.skip()
    @pytest.mark.skip_blockbuster
    @pytest.mark.integration
    async def test_turn_detection(
        self, vogent_turn_detection, mia_audio_16khz, silence_2s_48khz
    ):
        participant = Participant(user_id="mia", original={}, id="mia")
        conversation = InMemoryConversation(instructions="be nice", messages=[])

        await vogent_turn_detection.process_audio(
            mia_audio_16khz, participant, conversation
        )
        await vogent_turn_detection.process_audio(
            silence_2s_48khz, participant, conversation
        )

        await vogent_turn_detection.wait_for_processing_complete()

        items = await vogent_turn_detection.output.collect(timeout=1.0)
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

    async def test_incomplete_turn_enters_cooldown_instead_of_per_chunk_inference(self):
        td = _ControlledVogentTurnDetection(
            predictions=[(False, 0.2), (True, 0.9)],
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

            # 9 more silence chunks: chunk 5 triggers eval (1120ms > 1000ms) -> incomplete -> COOLDOWN
            # Remaining 4 silence chunks (640ms <= 1000ms) stay in COOLDOWN with no extra inference
            await td.process_audio(_make_pcm(9, 0.0), participant)
            await td.wait_for_processing_complete()

            assert td.prediction_count == 1
            assert td.utterance_state is UtteranceState.COOLDOWN
            items = await td.output.collect(timeout=0.1)
            assert [type(item) for item in items] == [TurnStarted]

            # 3 more silence chunks complete a full new 1000ms window -> re-evaluates -> IDLE
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
        td = _ControlledVogentTurnDetection(
            predictions=[(False, 0.2), (True, 0.9)],
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
        td = _ControlledVogentTurnDetection(
            predictions=[(False, 0.2)],
            silence_duration_ms=1000,
            max_segment_duration_seconds=1,
        )
        participant = Participant(user_id="mia", id="mia", original={})
        await td.start()
        try:
            # 1s = 32 chunks at 32ms; feed 42 continuous speech chunks (~1.34s)
            await td.process_audio(_make_pcm(42, 0.5), participant)
            await td.wait_for_processing_complete()

            assert td.prediction_count == 1
        finally:
            await td.close()

    async def test_restart_after_close_processes_audio(self):
        td = _ControlledVogentTurnDetection(
            predictions=[(True, 0.9)],
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
