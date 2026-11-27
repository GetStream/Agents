import asyncio

import pytest
from vision_agents.core.stt import STTConnectionState, STTConnectionStateMachine


class TestSTTConnectionStateMachine:
    def test_initial_state_is_init(self) -> None:
        machine = STTConnectionStateMachine()

        assert machine.state == STTConnectionState.INIT
        assert machine.is_ready is False
        assert machine.error is None

    def test_closed_is_terminal(self) -> None:
        machine = STTConnectionStateMachine()
        assert machine.transition(STTConnectionState.CLOSED) is True
        assert machine.state == STTConnectionState.CLOSED

        for target in (
            STTConnectionState.INIT,
            STTConnectionState.CONNECTING,
            STTConnectionState.READY,
            STTConnectionState.RECONNECTING,
            STTConnectionState.FAILED,
            STTConnectionState.CLOSED,
        ):
            assert machine.transition(target) is False
            assert machine.state == STTConnectionState.CLOSED

    @pytest.mark.parametrize(
        "settled_state",
        [
            STTConnectionState.READY,
            STTConnectionState.FAILED,
            STTConnectionState.CLOSED,
        ],
    )
    async def test_wait_until_settled_wakes_on_settled_states(
        self, settled_state: STTConnectionState
    ) -> None:
        machine = STTConnectionStateMachine()
        machine.transition(STTConnectionState.CONNECTING)

        waiter = asyncio.create_task(machine.wait_until_settled())
        await asyncio.sleep(0)
        assert not waiter.done()

        machine.transition(settled_state)
        result = await asyncio.wait_for(waiter, timeout=0.5)
        assert result == settled_state

    async def test_wait_until_settled_raises_timeout_when_unsettled(self) -> None:
        machine = STTConnectionStateMachine()
        machine.transition(STTConnectionState.CONNECTING)

        with pytest.raises(asyncio.TimeoutError):
            await machine.wait_until_settled(timeout=0.05)

    def test_error_set_on_failed_and_cleared_on_recovery_or_close(self) -> None:
        machine = STTConnectionStateMachine()
        err = RuntimeError("connection lost")

        machine.transition(STTConnectionState.FAILED, error=err)
        assert machine.state == STTConnectionState.FAILED
        assert machine.error is err

        machine.transition(STTConnectionState.CONNECTING)
        assert machine.error is None

        machine.transition(STTConnectionState.FAILED, error=err)
        assert machine.error is err
        machine.transition(STTConnectionState.READY)
        assert machine.error is None
        assert machine.is_ready is True

        machine.transition(STTConnectionState.FAILED, error=err)
        assert machine.error is err
        machine.transition(STTConnectionState.CLOSED)
        assert machine.error is None
        assert machine.state == STTConnectionState.CLOSED
