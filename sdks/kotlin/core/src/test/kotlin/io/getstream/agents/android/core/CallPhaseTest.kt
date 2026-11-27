package io.getstream.agents.android.core

import io.getstream.agents.android.core.CallPhase.Step
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class CallPhaseTest {
    private val joining = CallPhase().join()!!
    private val joined = joining.joined(joining.generation)!!
    private val leaving = joined.leave()
    private val ended = leaving.left()

    @Test
    fun `a join is refused unless the call is idle`() {
        for (phase in listOf(joining, joined, leaving, ended)) {
            assertNull(phase.join(), "a join was let through while ${phase.step}")
        }
    }

    @Test
    fun `a leave while joining makes the join leave what it joined`() {
        val left = joining.leave()

        assertFalse(left.isCurrent(joining.generation))
        assertNull(left.joined(joining.generation))
        assertEquals(Step.Leaving, left.step)
        assertEquals(Step.Ended, left.left().step)
    }

    @Test
    fun `a join that failed goes back to idle and can be tried again`() {
        val idle = assertNotNull(joining.failed(joining.generation))

        assertEquals(Step.Idle, idle.step)
        val again = assertNotNull(idle.join())
        assertEquals(Step.Joined, again.joined(again.generation)?.step)
    }

    @Test
    fun `a join that failed after a leave is nobody's to report`() {
        val left = joining.leave()

        assertNull(left.failed(joining.generation))
        assertNull(left.join())
    }

    @Test
    fun `ended is for good`() {
        val again = ended.leave()

        assertEquals(Step.Ended, again.step)
        assertNull(again.join())
        assertNull(CallPhase().leave().left().join())
    }
}
