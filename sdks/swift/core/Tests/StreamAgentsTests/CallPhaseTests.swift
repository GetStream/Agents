import Testing

@testable import StreamAgents

@Suite struct CallPhaseTests {
    @Test(arguments: Phase.allCases)
    func aJoinIsRefusedUnlessTheCallIsIdle(_ phase: Phase) {
        var call = phase.made

        let joining = call.join()

        #expect(joining == nil)
        #expect(call == phase.made)
    }

    @Test func aLeaveWhileJoiningMakesTheJoinLeaveWhatItJoined() throws {
        var call = CallPhase()
        let started = call.join()
        let joining = try #require(started)

        call.leave()
        let joined = call.joined(joining)

        #expect(!joined)
        #expect(!call.isCurrent(joining))
        #expect(call.step == .leaving)
        call.left()
        #expect(call.step == .ended)
    }

    @Test func aJoinThatFailedGoesBackToIdleAndCanBeTriedAgain() throws {
        var call = CallPhase()
        let started = call.join()
        let first = try #require(started)

        let failed = call.failed(first)

        #expect(failed)
        #expect(call.step == .idle)
        let again = call.join()
        let second = try #require(again)
        let joined = call.joined(second)
        #expect(joined)
        #expect(call.step == .joined)
    }

    @Test func aJoinThatFailedAfterALeaveIsNobodysToReport() throws {
        var call = CallPhase()
        let started = call.join()
        let joining = try #require(started)

        call.leave()
        let failed = call.failed(joining)
        let again = call.join()

        #expect(!failed)
        #expect(again == nil)
        #expect(call.step == .leaving)
    }

    @Test func aCameraChangeStartedBeforeALeaveIsStaleAfterIt() {
        var call = Phase.joined.made
        let changing = call.generation

        #expect(call.isCurrent(changing))
        call.leave()
        #expect(!call.isCurrent(changing))
    }

    @Test func endedIsForGood() {
        var call = Phase.ended.made

        let joining = call.join()
        call.leave()
        let again = call.join()

        #expect(joining == nil)
        #expect(again == nil)
        #expect(call.step == .ended)
    }

    /// The phases a call can be in, as the transitions reach them.
    enum Phase: CaseIterable, Sendable {
        case joining, joined, leaving, ended

        var made: CallPhase {
            var call = CallPhase()
            guard let joining = call.join(), self != .joining else { return call }
            _ = call.joined(joining)
            if self == .joined { return call }
            call.leave()
            if self == .leaving { return call }
            call.left()
            return call
        }
    }
}
