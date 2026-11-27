package io.getstream.agents.android.core

import android.content.ContextWrapper
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull

private const val VOICE_SESSION = """{"id":"s1","call_id":"s1","call_type":"agent","user_id":"john","agent_id":"a1","state":"live","modality":"voice","created_at":"2026-09-24T04:03:19Z","text":false}"""

/**
 * Joins and leaves that overlap, caught at the first thing [VoiceSession.join] waits for: the
 * token Stream Video connects with, which the gate holds until the test lets it go. It lets go by
 * failing, since a token let through builds a `StreamVideo`, and that connects to Stream at once.
 */
class VoiceSessionTest {
    private val router = TestRouter().apply { answer = { Reply(200, VOICE_SESSION) } }
    private val agents = StreamAgents(url = router.url, customerId = "acme", apiKey = "key")
    private val gate = TokenGate()

    /** Where the joins run, so one held at the gate holds its own job rather than the test. */
    private val joins = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    init {
        agents.setUser(User("john"), gate)
    }

    @AfterTest
    fun stop() {
        gate.fail()
        joins.cancel()
        agents.close()
        router.close()
    }

    @Test
    fun `a join while one is under way returns at once`() = runBlocking {
        val voice = attach()
        val first = joins.launch { voice.join() }
        assertTrue(eventually { gate.asked.get() == 1 })

        val second = joins.launch { voice.join() }
        val returned = finishes(second)
        gate.fail()
        first.join()
        second.join()

        assertTrue(returned)
        assertEquals(1, gate.asked.get())
        assertNull(voice.call.value)
        assertIs<Refused>(voice.failure.value)
        voice.session.close()
    }

    @Test
    fun `leaving while a join is under way leaves it with nothing joined or failed`() = runBlocking {
        val voice = attach()
        val joining = joins.launch { voice.join() }
        assertTrue(eventually { gate.asked.get() == 1 })

        voice.leave()
        gate.fail()
        joining.join()

        assertNull(voice.call.value)
        assertNull(voice.failure.value)
        assertFalse(voice.isCameraEnabled.value)
        voice.session.close()
    }

    @Test
    fun `a join after leaving asks for nothing`() = runBlocking {
        val voice = attach()
        voice.leave()

        val joining = joins.launch { voice.join() }
        val returned = finishes(joining)
        gate.fail()
        joining.join()

        assertTrue(returned)
        assertEquals(0, gate.asked.get())
        assertNull(voice.call.value)
        assertIs<AgentsException.Configuration>(voice.failure.value, "a join after leaving said nothing")
        voice.session.close()
    }

    @Test
    fun `a join that failed can be tried again`() = runBlocking {
        val voice = attach()
        val first = joins.launch { voice.join() }
        assertTrue(eventually { gate.asked.get() == 1 })
        gate.fail()
        first.join()
        assertIs<Refused>(voice.failure.value)

        val again = joins.launch { voice.join() }
        val askedAgain = eventually { gate.asked.get() == 2 }
        gate.fail()
        again.join()

        assertTrue(askedAgain)
        assertNull(voice.call.value)
        voice.session.close()
    }

    /** A voice session on a session this device attached to, so leaving closes nothing. */
    private suspend fun attach(): VoiceSession = VoiceSession.attach(ContextWrapper(null), agents, "s1")

    /** Whether [job] finished within five seconds. */
    private suspend fun finishes(job: Job): Boolean = withTimeoutOrNull(5.seconds) { job.join() } != null

    /** Whether [done] came to hold within five seconds. */
    private suspend fun eventually(done: () -> Boolean): Boolean =
        withTimeoutOrNull(5.seconds) { while (!done()) delay(10) } != null
}

/** Holds every Stream Video token asked for until the test fails them, counting the asks. */
private class TokenGate : TokenProvider {
    val asked = AtomicInteger()
    private val waiting = CopyOnWriteArrayList<CompletableDeferred<String>>()

    override suspend fun token(): String {
        val token = CompletableDeferred<String>()
        waiting.add(token)
        asked.incrementAndGet()
        return token.await()
    }

    fun fail() {
        for (token in waiting) token.completeExceptionally(Refused())
    }
}

private class Refused : Exception("refused")
