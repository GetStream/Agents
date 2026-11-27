/// Where a voice session is with its call: joining it, on it, leaving it, or done with it.
///
/// Joining awaits Stream's SDK several times, and a leave or a second join can arrive at any of
/// those awaits. So what each may do is decided here, as a value tested without a call. The
/// generation goes up with every leave: a join or a camera change that finds it moved on since
/// it started lost to a leave, and leaves what it touched rather than publishing it.
struct CallPhase: Equatable {
    enum Step {
        case idle, joining, joined, leaving, ended
    }

    private(set) var step = Step.idle
    private(set) var generation = 0

    /// Starts a join, and answers the generation it joins in. Nil while another is under way,
    /// once the call is joined, and after a leave: a session that left stays left, as Dart's
    /// does.
    mutating func join() -> Int? {
        guard step == .idle else { return nil }
        step = .joining
        return generation
    }

    /// Whether nothing has left since `generation`.
    func isCurrent(_ generation: Int) -> Bool {
        self.generation == generation
    }

    /// Records that the join started in `generation` joined. False when a leave came first, and
    /// what it joined is to be left again.
    mutating func joined(_ generation: Int) -> Bool {
        guard step == .joining, isCurrent(generation) else { return false }
        step = .joined
        return true
    }

    /// Records that the join started in `generation` failed or was cancelled, so it can be
    /// tried again. False when a leave came first: the session is leaving, and the failure is
    /// nobody's to hear.
    mutating func failed(_ generation: Int) -> Bool {
        guard step == .joining, isCurrent(generation) else { return false }
        step = .idle
        return true
    }

    /// Starts a leave, whatever was under way, which loses to it.
    mutating func leave() {
        generation += 1
        if step != .ended {
            step = .leaving
        }
    }

    /// Records that a leave is done.
    mutating func left() {
        step = .ended
    }
}
