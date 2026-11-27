import Foundation
import Testing

@testable import StreamAgents

@Suite struct RouterTests {
    @Test func aSearchRunsAsItsConfigAndSendsNoOptions() async throws {
        let server = try SessionServer(
            answer: .init(
                status: 200, headers: ["Content-Type": "application/json"],
                body: #"{"provider":"exa","model":"exa-fast","answer":"Yes.","results":[]}"#))
        defer { server.listener.cancel() }
        let agents = StreamAgents(url: try await server.url(), customerID: "acme")

        let found = try await agents.router(config: "healthcare").search("pricing")

        let request = try await server.request()
        #expect(request.path == "/v1/search")
        #expect(request.body == ["query": .string("pricing"), "config_id": .string("healthcare")])
        #expect(found.answer == "Yes.")
    }
}
