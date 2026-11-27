import asyncio

import pytest
from deepgram.listen.v2.types import ListenV2TurnInfo, ListenV2TurnInfoWordsItem
from vision_agents.core.edge.types import Participant
from vision_agents.core.stt import Transcript
from vision_agents.core.turn_detection import TurnEnded
from vision_agents.plugins import deepgram


class TestDeepgramSTT:
    """Integration tests for Deepgram STT"""

    @pytest.fixture
    def participant(self) -> Participant:
        return Participant({}, user_id="test-user", id="test-user")

    @pytest.fixture(params=[True, False], ids=["eager", "no_eager"])
    async def deepgram_stt(self, request):
        stt = deepgram.STT(eager_turn_detection=request.param)
        await stt.start()
        yield stt
        await stt.close()

    @pytest.mark.integration
    async def test_transcribe_mia_audio_48khz(
        self, deepgram_stt, mia_audio_48khz, silence_2s_48khz, participant
    ):
        await deepgram_stt.process_audio(mia_audio_48khz, participant=participant)
        # Send 2 seconds of silence to trigger end of turn
        await deepgram_stt.process_audio(silence_2s_48khz, participant=participant)

        items = await deepgram_stt.output.collect(timeout=10.0)

        transcripts = [i for i in items if isinstance(i, Transcript)]
        assert transcripts, "No Transcript emitted by Deepgram STT"
        finals = [t for t in transcripts if t.final]
        assert finals, "No final Transcript emitted by Deepgram STT"
        full_transcript = " ".join(t.text for t in finals)
        assert "forgotten treasures" in full_transcript.lower()
        assert transcripts[0].participant == participant

        assert any(isinstance(i, TurnEnded) for i in items)

    async def test_close_closes_http_client(self):
        stt = deepgram.STT(api_key="fake")
        httpx_client = stt.client._client_wrapper.httpx_client.httpx_client

        assert httpx_client.is_closed is False
        await stt.close()
        assert httpx_client.is_closed is True

    async def test_on_message_handles_dict_turn_info(self, participant):
        stt = deepgram.STT(api_key="fake")
        stt._current_participant = participant

        stt._on_message(
            {
                "type": "TurnInfo",
                "event": "EndOfTurn",
                "transcript": "hello world",
                "end_of_turn_confidence": 0.9,
                "audio_window_end": 1.23,
                "words": [
                    {"word": "hello", "confidence": 0.9},
                    {"word": "world", "confidence": 0.8},
                ],
            }
        )

        items = await stt.output.collect(timeout=0)
        await stt.close()

        transcripts = [i for i in items if isinstance(i, Transcript)]
        assert [t.text for t in transcripts] == ["hello world"]
        assert transcripts[0].final
        assert transcripts[0].participant == participant
        assert any(isinstance(i, TurnEnded) for i in items)

    async def test_on_message_handles_typed_turn_info(self, participant):
        stt = deepgram.STT(api_key="fake")
        stt._current_participant = participant

        stt._on_message(
            ListenV2TurnInfo(
                request_id="req-1",
                sequence_id=1,
                event="EndOfTurn",
                turn_index=0,
                audio_window_start=0.0,
                audio_window_end=1.23,
                transcript="hello world",
                words=[
                    ListenV2TurnInfoWordsItem(word="hello", confidence=0.9),
                    ListenV2TurnInfoWordsItem(word="world", confidence=0.8),
                ],
                end_of_turn_confidence=0.9,
            )
        )

        items = await stt.output.collect(timeout=0)
        await stt.close()

        transcripts = [i for i in items if isinstance(i, Transcript)]
        assert [t.text for t in transcripts] == ["hello world"]
        assert transcripts[0].final
        assert transcripts[0].participant == participant
        assert any(isinstance(i, TurnEnded) for i in items)

    async def test_on_message_ignores_non_turn_info(self, participant):
        stt = deepgram.STT(api_key="fake")
        stt._current_participant = participant

        stt._on_message(object())

        items = await stt.output.collect(timeout=0)
        await stt.close()

        assert items == []

    async def test_process_audio_does_not_hang_on_close_while_waiting(
        self, silence_1s_16khz, participant: Participant
    ):
        stt = deepgram.STT(api_key="fake")
        task = asyncio.create_task(
            stt.process_audio(silence_1s_16khz, participant=participant)
        )
        await asyncio.sleep(0)
        await stt.close()

        await asyncio.wait_for(task, timeout=0.5)

    async def test_process_audio_does_not_hang_after_unexpected_close(
        self, silence_1s_16khz, participant: Participant
    ):
        class FakeSocket:
            def __init__(self) -> None:
                self._stop = asyncio.Event()

            def on(self, event: object, handler: object) -> None:
                return None

            async def start_listening(self) -> None:
                await self._stop.wait()

            async def send_media(self, data: bytes) -> None:
                return None

            async def send_close_stream(self, msg: object) -> None:
                self._stop.set()

        class FakeConnectCtx:
            def __init__(self, socket: FakeSocket) -> None:
                self._socket = socket

            async def __aenter__(self) -> FakeSocket:
                return self._socket

            async def __aexit__(self, exc_type, exc, tb) -> None:
                self._socket._stop.set()

        class FakeV2:
            def connect(self, **kwargs: object) -> FakeConnectCtx:
                return FakeConnectCtx(FakeSocket())

        class FakeListen:
            v2 = FakeV2()

        class FakeClient:
            listen = FakeListen()

        stt = deepgram.STT(api_key="fake", client=FakeClient())
        await stt.start()
        try:
            stt._on_close("server disconnected")
            await asyncio.wait_for(
                stt.process_audio(silence_1s_16khz, participant=participant),
                timeout=0.5,
            )
        finally:
            await stt.close()

    async def test_duplicate_start_does_not_leak_websocket_or_listen_task(self):
        listen_started = 0

        class FakeSocket:
            def __init__(self) -> None:
                self._stop = asyncio.Event()

            def on(self, event: object, handler: object) -> None:
                return None

            async def start_listening(self) -> None:
                nonlocal listen_started
                listen_started += 1
                await self._stop.wait()

            async def send_media(self, data: bytes) -> None:
                return None

            async def send_close_stream(self, msg: object) -> None:
                self._stop.set()

        opened_sockets: list[FakeSocket] = []

        class FakeConnectCtx:
            async def __aenter__(self) -> FakeSocket:
                socket = FakeSocket()
                opened_sockets.append(socket)
                return socket

            async def __aexit__(self, exc_type, exc, tb) -> None:
                if opened_sockets:
                    opened_sockets[-1]._stop.set()

        class FakeV2:
            def connect(self, **kwargs: object) -> FakeConnectCtx:
                return FakeConnectCtx()

        class FakeListen:
            v2 = FakeV2()

        class FakeClient:
            listen = FakeListen()

        stt = deepgram.STT(api_key="fake", client=FakeClient())
        await stt.start()
        try:
            # Simulate a duplicate start when started is already True even if connection cleared
            stt.connection = None
            with pytest.raises(ValueError, match="already started"):
                await stt.start()
            await asyncio.sleep(0)
            assert len(opened_sockets) == 1
            assert listen_started == 1
        finally:
            await stt.close()
