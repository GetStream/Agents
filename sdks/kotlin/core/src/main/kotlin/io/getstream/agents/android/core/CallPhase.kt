package io.getstream.agents.android.core

/**
 * Where a voice session is with its call: joining it, on it, leaving it, or done with it.
 *
 * Joining suspends in Stream's SDK several times, and a leave or a second join can arrive, from
 * any thread, at any of those points. So what each may do is decided here, as a value tested
 * without a call, which [VoiceSession] replaces under a lock. [generation] goes up with every
 * leave: a join that finds it moved on since it started lost to a leave, and leaves what it
 * joined rather than publishing it.
 */
internal data class CallPhase(val step: Step = Step.Idle, val generation: Int = 0) {
    enum class Step { Idle, Joining, Joined, Leaving, Ended }

    /**
     * A join starting, or null while another is under way, once the call is joined, and after a
     * leave: a session that left stays left, as Dart's does.
     */
    fun join(): CallPhase? = if (step == Step.Idle) copy(step = Step.Joining) else null

    /** Whether nothing has left since [generation]. */
    fun isCurrent(generation: Int): Boolean = this.generation == generation

    /**
     * The join started in [generation] joined, or null when a leave came first and what it
     * joined is to be left again.
     */
    fun joined(generation: Int): CallPhase? =
        if (step == Step.Joining && isCurrent(generation)) copy(step = Step.Joined) else null

    /**
     * The join started in [generation] failed or was cancelled, and can be tried again; or null
     * when a leave came first, and the failure is nobody's to hear.
     */
    fun failed(generation: Int): CallPhase? =
        if (step == Step.Joining && isCurrent(generation)) copy(step = Step.Idle) else null

    /** A leave starting, whatever was under way, which loses to it. */
    fun leave(): CallPhase =
        copy(step = if (step == Step.Ended) Step.Ended else Step.Leaving, generation = generation + 1)

    /** A leave done. */
    fun left(): CallPhase = copy(step = Step.Ended)
}
