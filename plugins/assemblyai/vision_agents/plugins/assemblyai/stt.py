import asyncio
import json
import logging
import os
import time
from typing import Optional
from urllib.parse import urlencode

import aiohttp
from getstream.video.rtc.track_util import PcmData
from vision_agents.core import stt
from vision_agents.core.edge.types import Participant
from vision_agents.core.stt import (
    STTConnectionState,
    STTConnectionStateMachine,
    TranscriptResponse,
)

logger = logging.getLogger(__name__)

WS_BASE_URL = "wss://streaming.assemblyai.com/v3/ws"
API_VERSION = "2025-05-12"


class STT(stt.STT):
    """AssemblyAI Universal-3 Pro streaming Speech-to-Text.

    Uses aiohttp for a fully async WebSocket connection to AssemblyAI's v3
    streaming endpoint with built-in reconnection and native Turn Detection.

    Docs: https://www.assemblyai.com/docs/streaming/universal-3-pro
    """

    turn_detection: bool = True

    def __init__(
        self,
        api_key: Optional[str] = None,
        speech_model: str = "u3-rt-pro",
        sample_rate: int = 16000,
        min_turn_silence: Optional[int] = None,
        max_turn_silence: Optional[int] = None,
        prompt: Optional[str] = None,
        keyterms_prompt: Optional[list[str]] = None,
        speaker_labels: bool = False,
        max_speakers: Optional[int] = None,
        max_reconnect_attempts: int = 3,
        reconnect_backoff_initial_s: float = 0.5,
        reconnect_backoff_max_s: float = 4.0,
    ):
        """Initialize AssemblyAI STT.

        Args:
            api_key: AssemblyAI API key. Falls back to ASSEMBLYAI_API_KEY env var.
            speech_model: Model to use. Defaults to "u3-rt-pro".
            sample_rate: Audio sample rate in Hz. Defaults to 16000.
            min_turn_silence: Silence (ms) before speculative end-of-turn check.
            max_turn_silence: Maximum silence (ms) before forcing turn end.
            prompt: Custom transcription prompt. Cannot be used with keyterms_prompt.
            keyterms_prompt: Terms to boost recognition for. Cannot be used with prompt.
            speaker_labels: Enable streaming diarization. Each Turn event will
                include a speaker_label identifying the speaker.
            max_speakers: Hint for expected number of speakers (1-10). Requires
                speaker_labels=True.
            max_reconnect_attempts: Max reconnect attempts on transient failures.
            reconnect_backoff_initial_s: Initial backoff delay in seconds.
            reconnect_backoff_max_s: Maximum backoff delay in seconds.
        """
        super().__init__(provider_name="assemblyai")

        if prompt is not None and keyterms_prompt is not None:
            raise ValueError("prompt and keyterms_prompt cannot be used together")

        if max_speakers is not None and not speaker_labels:
            raise ValueError("max_speakers requires speaker_labels=True")

        self._api_key = api_key or os.environ.get("ASSEMBLYAI_API_KEY")
        self._speech_model = speech_model
        self._sample_rate = sample_rate
        self._min_turn_silence = min_turn_silence
        self._max_turn_silence = max_turn_silence
        self._prompt = prompt
        self._keyterms_prompt = keyterms_prompt
        self._speaker_labels_enabled = speaker_labels
        self._max_speakers = max_speakers

        self._max_reconnect_attempts = max_reconnect_attempts
        self._reconnect_backoff_initial_s = reconnect_backoff_initial_s
        self._reconnect_backoff_max_s = reconnect_backoff_max_s

        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._receive_task: Optional[asyncio.Task] = None
        self._send_task: Optional[asyncio.Task] = None
        self._audio_queue: asyncio.Queue[bytes | None] = asyncio.Queue()
        self._connection_state = STTConnectionStateMachine()
        self._begin_event = asyncio.Event()
        self._reconnecting = False
        self._current_participant: Optional[Participant] = None
        self._audio_start_time: Optional[float] = None
        self._speaker_participants: dict[str, Participant] = {}
        # AssemblyAI requires 50-1000ms per message; buffer to 100ms before sending
        self._chunk_size = self._sample_rate * 2 // 10  # 100ms of int16
        self._audio_buffer = bytearray()

    @property
    def connection_state(self) -> STTConnectionState:
        """Current lifecycle state of the AssemblyAI WebSocket connection."""
        return self._connection_state.state

    def _build_ws_url(self) -> str:
        params: dict[str, str | int] = {
            "sample_rate": self._sample_rate,
            "speech_model": self._speech_model,
        }
        if self._min_turn_silence is not None:
            params["min_turn_silence"] = self._min_turn_silence
        if self._max_turn_silence is not None:
            params["max_turn_silence"] = self._max_turn_silence
        if self._prompt is not None:
            params["prompt"] = self._prompt
        if self._keyterms_prompt is not None:
            params["keyterms_prompt"] = json.dumps(self._keyterms_prompt)
        if self._speaker_labels_enabled:
            params["speaker_labels"] = "true"
            if self._max_speakers is not None:
                params["max_speakers"] = self._max_speakers
        return f"{WS_BASE_URL}?{urlencode(params)}"

    async def start(self):
        """Start the AssemblyAI WebSocket connection and begin listening."""
        if self.closed:
            raise ValueError("STT is closed and cannot be started")

        await super().start()
        self._connection_state.transition(STTConnectionState.CONNECTING)
        try:
            await self._connect()
            await asyncio.wait_for(self._begin_event.wait(), timeout=10.0)
            if not self._connection_state.is_ready:
                raise RuntimeError(
                    "AssemblyAI connection did not become ready "
                    f"(state={self._connection_state.state.value})"
                )
        except asyncio.TimeoutError:
            await self._disconnect()
            self.started = False
            err = TimeoutError("Failed to connect to AssemblyAI within 10 seconds")
            self._connection_state.transition(STTConnectionState.FAILED, error=err)
            raise err from None
        except Exception as exc:
            await self._disconnect()
            self.started = False
            self._connection_state.transition(STTConnectionState.FAILED, error=exc)
            raise

    async def _connect(self) -> None:
        """Open the WebSocket connection and launch send/receive tasks."""
        if self.closed:
            return

        self._begin_event = asyncio.Event()
        session = aiohttp.ClientSession()
        self._session = session
        url = self._build_ws_url()
        headers = {
            "Authorization": self._api_key or "",
            "AssemblyAI-Version": API_VERSION,
        }
        try:
            ws = await session.ws_connect(url, headers=headers)
        except BaseException:
            if not session.closed:
                await session.close()
            if self._session is session:
                self._session = None
            raise

        if self.closed:
            if not ws.closed:
                await ws.close()
            if not session.closed:
                await session.close()
            if self._session is session:
                self._session = None
            return

        self._ws = ws
        self._receive_task = asyncio.create_task(self._receive_loop())
        self._send_task = asyncio.create_task(self._send_loop())
        self._on_connected()

    async def _disconnect(self) -> None:
        """Cancel tasks and close WebSocket + session."""
        current = asyncio.current_task()

        if self._send_task is not None and self._send_task is not current:
            self._send_task.cancel()
            try:
                await self._send_task
            except asyncio.CancelledError:
                pass
            self._send_task = None

        if self._receive_task is not None and self._receive_task is not current:
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass
            self._receive_task = None

        if self._ws is not None and not self._ws.closed:
            await self._ws.close()
        self._ws = None

        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    async def _reconnect(self) -> bool:
        """Attempt bounded reconnect with exponential backoff.

        Returns:
            True if reconnect succeeded, False if attempts exhausted or closed.
        """
        if self.closed or self._reconnecting:
            return False

        self._reconnecting = True
        try:
            self._connection_state.transition(STTConnectionState.RECONNECTING)
            await self._disconnect()
            self._speaker_participants.clear()

            delay = self._reconnect_backoff_initial_s
            for attempt in range(1, self._max_reconnect_attempts + 1):
                if self.closed:
                    return False

                logger.info(
                    "AssemblyAI reconnect attempt %d/%d in %.1fs",
                    attempt,
                    self._max_reconnect_attempts,
                    delay,
                )
                await asyncio.sleep(delay)
                if self.closed:
                    return False

                try:
                    await self._connect()
                    if self.closed:
                        await self._disconnect()
                        return False

                    await asyncio.wait_for(self._begin_event.wait(), timeout=10.0)
                    if self._connection_state.is_ready and not self.closed:
                        logger.info("AssemblyAI reconnected on attempt %d", attempt)
                        return True
                    await self._disconnect()
                    if self.closed:
                        return False
                except (aiohttp.WSServerHandshakeError, asyncio.TimeoutError, OSError):
                    logger.exception("AssemblyAI reconnect attempt %d failed", attempt)
                    await self._disconnect()
                delay = min(delay * 2, self._reconnect_backoff_max_s)

            if not self.closed:
                logger.error(
                    "AssemblyAI reconnect failed after %d attempts",
                    self._max_reconnect_attempts,
                )
                self.started = False
                self._connection_state.transition(STTConnectionState.FAILED)
                self._on_disconnected(
                    reason="reconnect attempts exhausted", clean=False
                )
            return False
        finally:
            self._reconnecting = False

    async def _receive_loop(self) -> None:
        """Read and dispatch incoming WebSocket messages."""
        ws = self._ws
        if ws is None:
            return
        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    self._handle_message(json.loads(msg.data))
                elif msg.type in (
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.CLOSING,
                    aiohttp.WSMsgType.ERROR,
                ):
                    break
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Error in AssemblyAI receive loop")
        finally:
            self._begin_event.set()

        if not self.closed and not self._reconnecting:
            await self._reconnect()

    async def _send_loop(self) -> None:
        """Drain audio queue and send binary frames."""
        try:
            while True:
                chunk = await self._audio_queue.get()
                if chunk is None:
                    break
                if self._ws is not None and not self._ws.closed:
                    await self._ws.send_bytes(chunk)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Error in AssemblyAI send loop")

    def _handle_message(self, data: dict) -> None:
        """Dispatch a parsed JSON message by its type field."""
        msg_type = data.get("type", "")

        if msg_type == "Begin":
            logger.debug("AssemblyAI session started: %s", data.get("id"))
            self._connection_state.transition(STTConnectionState.READY)
            self._begin_event.set()

        elif msg_type == "Turn":
            self._handle_turn(data)

        elif msg_type == "SpeechStarted":
            participant = self._current_participant
            if participant is not None:
                self._emit_turn_started_event(participant)

        elif msg_type == "Termination":
            logger.info(
                "AssemblyAI session terminated: %ss audio processed",
                data.get("audio_duration_seconds"),
            )

        elif "error" in data:
            error_msg = data.get("error", "Unknown error")
            logger.error("AssemblyAI streaming error: %s", error_msg)

        else:
            logger.debug("Unhandled AssemblyAI event: %s", msg_type)

    def _resolve_participant(
        self, speaker_label: Optional[str]
    ) -> Optional[Participant]:
        """Map a speaker label to a Participant, creating a synthetic one if needed."""
        if not self._speaker_labels_enabled or speaker_label is None:
            return self._current_participant

        cached = self._speaker_participants.get(speaker_label)
        if cached is not None:
            return cached

        participant = Participant(
            original=None,
            user_id=f"speaker_{speaker_label}",
            id=f"speaker_{speaker_label}_{self.session_id[:8]}",
        )
        self._speaker_participants[speaker_label] = participant
        return participant

    def _handle_turn(self, data: dict) -> None:
        transcript = data.get("transcript", "")
        if not transcript:
            return

        participant = self._resolve_participant(data.get("speaker_label"))
        if participant is None:
            logger.warning("Received transcript but no participant available")
            return

        processing_time_ms: Optional[float] = None
        if self._audio_start_time is not None:
            processing_time_ms = (time.perf_counter() - self._audio_start_time) * 1000

        other: Optional[dict] = None
        if self._speaker_labels_enabled:
            other = {"speaker_label": data.get("speaker_label")}

        response = TranscriptResponse(
            model_name=self._speech_model,
            processing_time_ms=processing_time_ms,
            other=other,
        )

        if data.get("end_of_turn"):
            self._emit_transcript_event(transcript, participant, response, mode="final")
            self._audio_start_time = None
            self._emit_turn_ended_event(participant)
        else:
            self._emit_transcript_event(
                transcript, participant, response, mode="replacement"
            )

    async def process_audio(
        self,
        pcm_data: PcmData,
        participant: Optional[Participant] = None,
    ):
        """Process audio data through AssemblyAI for transcription.

        Args:
            pcm_data: The PCM audio data to process.
            participant: Optional participant metadata.
        """
        if self.closed or self._connection_state.state == STTConnectionState.FAILED:
            logger.warning("AssemblyAI STT is not active, ignoring audio")
            return

        state = await self._connection_state.wait_until_settled()
        if state != STTConnectionState.READY or self.closed:
            logger.warning(
                "AssemblyAI connection not ready (%s), ignoring audio", state.value
            )
            return

        resampled = pcm_data.resample(self._sample_rate, 1)
        audio_bytes = resampled.samples.tobytes()

        self._current_participant = participant

        if self._audio_start_time is None:
            self._audio_start_time = time.perf_counter()

        self._audio_buffer.extend(audio_bytes)
        while len(self._audio_buffer) >= self._chunk_size:
            chunk = bytes(self._audio_buffer[: self._chunk_size])
            del self._audio_buffer[: self._chunk_size]
            await self._audio_queue.put(chunk)

    async def close(self):
        """Close the AssemblyAI WebSocket connection and clean up resources."""
        await super().close()
        self._connection_state.transition(STTConnectionState.CLOSED)
        self._begin_event.set()

        if self._audio_buffer:
            await self._audio_queue.put(bytes(self._audio_buffer))
            self._audio_buffer.clear()

        await self._audio_queue.put(None)
        if self._send_task is not None:
            await self._send_task
            self._send_task = None

        if self._ws is not None and not self._ws.closed:
            try:
                await self._ws.send_str(json.dumps({"type": "Terminate"}))
            except Exception:
                logger.debug("Could not send terminate message")

        await self._disconnect()
        self.started = False
        self._audio_start_time = None
        self._speaker_participants.clear()
        self._on_disconnected()
