import asyncio
import logging
from dataclasses import dataclass, field
from enum import Enum

from opentelemetry import trace
from opentelemetry.trace import Tracer
from vision_agents.core.agents.conversation import named
from vision_agents.core.edge.types import Participant
from vision_agents.core.events import EventManager
from vision_agents.core.llm import LLM
from vision_agents.core.llm.events import LLMResponseFinalEvent
from vision_agents.core.llm.llm import (
    LLMResponseDelta,
    LLMResponseFinal,
    answered_speaker,
)
from vision_agents.core.utils.exceptions import log_task_exception
from vision_agents.core.utils.stream import Stream
from vision_agents.core.utils.utils import cancel_and_wait

logger = logging.getLogger(__name__)
tracer: Tracer = trace.get_tracer("agents")


__all__ = ["LLMTurn", "TurnState"]


class TurnState(Enum):
    IDLE = "idle"
    PENDING_CONFIRM = "pending_confirm"
    SPECULATIVE_RUNNING = "speculative_running"
    CONFIRMED_RUNNING = "confirmed_running"
    FINALIZING = "finalizing"
    FINALIZED = "finalized"
    CANCELLED = "cancelled"


@dataclass
class LLMTurn:
    transcript: str
    participant: Participant
    events: EventManager
    # Who said the transcript, put ahead of it for the model. None for one person.
    speaker: str | None = None
    stream: Stream[LLMResponseDelta | LLMResponseFinal] = field(
        init=False, default_factory=Stream
    )
    _state: TurnState = field(default=TurnState.IDLE, init=False)
    _llm_response_task: asyncio.Task[None] | None = field(default=None, init=False)
    _finalize_task: asyncio.Task[None] | None = field(default=None, init=False)

    @property
    def state(self) -> TurnState:
        return self._state

    @property
    def finalized(self) -> bool:
        """True once the finalize task has finished draining the LLM response stream."""
        return self._state is TurnState.FINALIZED

    @property
    def confirmed(self) -> bool:
        return self._state in (
            TurnState.PENDING_CONFIRM,
            TurnState.CONFIRMED_RUNNING,
            TurnState.FINALIZING,
            TurnState.FINALIZED,
        )

    @property
    def cancelled(self) -> bool:
        """True if ``cancel()`` has been invoked on this turn."""
        return self._state is TurnState.CANCELLED

    @property
    def started(self) -> bool:
        return self._state in (
            TurnState.SPECULATIVE_RUNNING,
            TurnState.CONFIRMED_RUNNING,
            TurnState.FINALIZING,
            TurnState.FINALIZED,
        )

    def start(self, llm: LLM) -> None:
        if self._state is TurnState.CANCELLED:
            raise RuntimeError("Cannot start a cancelled LLM turn")
        if self._state not in (TurnState.IDLE, TurnState.PENDING_CONFIRM):
            raise RuntimeError("LLM response task is already running")
        task = asyncio.create_task(self._do_llm_response(llm))
        task.add_done_callback(
            log_task_exception("LLMTurn: failed to get a response from LLM")
        )
        self._llm_response_task = task
        self._state = (
            TurnState.CONFIRMED_RUNNING
            if self._state is TurnState.PENDING_CONFIRM
            else TurnState.SPECULATIVE_RUNNING
        )

    async def cancel(self) -> None:
        if self._state in (TurnState.FINALIZED, TurnState.CANCELLED):
            # The turn is done, nothing to cancel
            return

        logger.info('🤖 Cancelling LLM turn for transcript "%s"', self.transcript)

        self._state = TurnState.CANCELLED
        if self._llm_response_task is not None:
            await cancel_and_wait(self._llm_response_task)
            self._llm_response_task = None
        if self._finalize_task is not None:
            await cancel_and_wait(self._finalize_task)
            self._finalize_task = None

    def finalize(self, output: Stream[LLMResponseDelta | LLMResponseFinal]) -> None:
        if self._state in (TurnState.FINALIZING, TurnState.FINALIZED):
            # Exit early if the turn has already been finalized or in progress
            return
        if self._state is TurnState.CANCELLED:
            raise RuntimeError("Cannot finalize a cancelled LLM turn")
        if self._state in (TurnState.IDLE, TurnState.PENDING_CONFIRM):
            raise RuntimeError("LLM turn must be started first")
        if self._state is TurnState.SPECULATIVE_RUNNING:
            raise RuntimeError("LLM turn must be confirmed first")

        logger.info('🤖 Finalizing LLM turn for transcript "%s"', self.transcript)
        self._state = TurnState.FINALIZING
        task = asyncio.create_task(self._do_finalize(output=output))
        task.add_done_callback(
            log_task_exception("LLMTurn: failed to finalize the turn")
        )
        self._finalize_task = task

    def confirm(self) -> None:
        """Mark the LLMTurn as confirmed. Idempotent if already confirmed."""
        if self._state is TurnState.CANCELLED:
            raise RuntimeError("Cannot confirm a cancelled LLM turn")
        if self._state is TurnState.IDLE:
            self._state = TurnState.PENDING_CONFIRM
        elif self._state is TurnState.SPECULATIVE_RUNNING:
            self._state = TurnState.CONFIRMED_RUNNING

    async def _do_llm_response(self, llm: LLM) -> None:
        # This runs in its own task, so the label is seen only by this turn's requests.
        answered_speaker.set(self.speaker)
        with tracer.start_as_current_span("simple_response"):
            try:
                async for item in llm.simple_response(
                    named(self.speaker, self.transcript), self.participant
                ):
                    await self.stream.send(item)
                    if isinstance(item, LLMResponseFinal):
                        self.events.send(
                            LLMResponseFinalEvent(
                                plugin_name=llm.provider_name,
                                text=item.text,
                                model=item.model or llm.model,
                            )
                        )
                        llm.metrics.on_llm_response(
                            provider=llm.provider_name,
                            model=item.model,
                            latency_ms=item.latency_ms,
                            time_to_first_token_ms=item.time_to_first_token_ms,
                            input_tokens=item.input_tokens,
                            output_tokens=item.output_tokens,
                        )

            except asyncio.CancelledError:
                # Interrupt llm when the turn is cancelled to give it a chance
                # to stop some heavy async work (e.g. inference in a different thread)
                await llm.interrupt()
                raise
            finally:
                # Close stream no matter what to free consumer iterators
                self.stream.close()

    async def _do_finalize(
        self, output: Stream[LLMResponseDelta | LLMResponseFinal]
    ) -> None:
        """
        The LLM turn is confirmed and final, we can now read the LLM response chunks
          and send them to the output.

        Args:
            output: output stream for LLM response chunks

        """
        try:
            async for item in self.stream:
                await output.send(item)
        finally:
            if self._state is TurnState.FINALIZING:
                self._state = TurnState.FINALIZED
