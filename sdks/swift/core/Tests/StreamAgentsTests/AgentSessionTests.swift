import Combine
import CryptoKit
import Foundation
import Network
import Testing
import os

@testable import StreamAgents

@MainActor
@Suite struct AgentSessionTests {
  /// The router refused the tools, so it offers none of them and nothing here answers them.
  @Test func aToolTheRouterRefusedIsNotAnswered() async throws {
    let server = try SessionServer(
      answer: .init(
        status: 409, headers: ["Content-Type": "application/json"],
        body: #"{"error":{"message":"taken","type":"conflict","code":"conflict","doc_url":""}}"#))
    defer { server.listener.cancel() }
    let session = AgentSession(
      backend: Backend(url: try await server.url(), customerID: "test"), session: Self.session, tools: [])

    await #expect(throws: AgentsError.self) { try await session.add(tools: [Self.reserveTable]) }

    #expect(session.tools["reserve_table"] == nil)
  }

  /// Whether a request that never came back added the tools is unknown, and a call of one that
  /// the router did add has to be answered.
  @Test func aToolWhoseAddingWasNeverAnsweredIsStillAnswered() async throws {
    let server = try SessionServer()
    let url = try await server.url()
    server.listener.cancel()
    let session = AgentSession(
      backend: Backend(url: url, customerID: "test"), session: Self.session, tools: [])

    await #expect(throws: AgentsError.self) { try await session.add(tools: [Self.reserveTable]) }

    #expect(session.tools["reserve_table"] != nil)
  }

  /// The router never replaces a tool the session has, so the handler answering that tool's
  /// calls stays the one it was added with, even when the request's answer is lost.
  @Test func aToolAddedUnderANameTheSessionHasKeepsItsHandler() async throws {
    let server = try SessionServer()
    let url = try await server.url()
    server.listener.cancel()
    let session = AgentSession(
      backend: Backend(url: url, customerID: "test"), session: Self.session,
      tools: [Self.reserveTable])
    let other = AgentTool(name: "reserve_table", description: "Reserve a table elsewhere") { _ in
      "Reserved elsewhere."
    }

    await #expect(throws: AgentsError.self) { try await session.add(tools: [other]) }

    #expect(session.tools["reserve_table"]?.description == "Reserve a table")
  }

  /// An add whose answer was lost may or may not have reached the router, so another tool
  /// under its name settles it first rather than leaving the first one answering calls the
  /// router now makes of the second.
  @Test func aToolWhoseAddingWasNeverAnsweredIsSettledBeforeAnotherTakesItsName() async throws {
    let server = try SessionServer { method, _, body in
      let described = body["tools"]?.arrayValue?.first?["description"]?.stringValue
      switch (method, described) {
      case ("POST", "Reserve a table"):
        return .init(status: 200, headers: ["Content-Type": "application/json"], body: "not json")
      case ("DELETE", _):
        return .init(
          status: 404, headers: ["Content-Type": "application/json"],
          body: #"{"error":{"message":"no tool","type":"not_found","code":"not_found","doc_url":""}}"#)
      default:
        return .init(status: 204, headers: [:], body: "")
      }
    }
    defer { server.listener.cancel() }
    let session = AgentSession(
      backend: Backend(url: try await server.url(), customerID: "test"), session: Self.session, tools: [])
    await #expect(throws: AgentsError.self) { try await session.add(tools: [Self.reserveTable]) }
    let other = AgentTool(name: "reserve_table", description: "Reserve a table elsewhere") { _ in
      "Reserved elsewhere."
    }

    try await session.add(tools: [other])

    #expect(session.tools["reserve_table"]?.description == "Reserve a table elsewhere")
  }

  /// A gateway in front of the router can answer a 5xx after the router added the tools, so
  /// they stay answered, and another tool under the name withdraws the first before it is added
  /// rather than being refused as one the session already has.
  @Test func aToolWhoseAddingAGatewayFailedIsStillAnsweredAndSettledBeforeAnotherTakesItsName()
    async throws
  {
    let router = OSAllocatedUnfairLock(initialState: (offered: Set<String>(), adds: 0))
    let server = try SessionServer { method, path, body in
      router.withLock { router in
        if method == "DELETE" {
          return router.offered.remove(String(path.split(separator: "/").last ?? "")) == nil
            ? .init(
              status: 404, headers: ["Content-Type": "application/json"],
              body: #"{"error":{"message":"no tool","type":"not_found","code":"not_found","doc_url":""}}"#)
            : .init(status: 204, headers: [:], body: "")
        }
        let names = Set(body["tools"]?.arrayValue?.compactMap { $0["name"]?.stringValue } ?? [])
        guard router.offered.isDisjoint(with: names) else {
          return .init(
            status: 409, headers: ["Content-Type": "application/json"],
            body: #"{"error":{"message":"taken","type":"conflict","code":"conflict","doc_url":""}}"#)
        }
        router.offered.formUnion(names)
        router.adds += 1
        return router.adds == 1 ? .badGateway : .init(status: 204, headers: [:], body: "")
      }
    }
    defer { server.listener.cancel() }
    let session = AgentSession(
      backend: Backend(url: try await server.url(), customerID: "test"), session: Self.session, tools: [])
    await #expect(throws: AgentsError.self) { try await session.add(tools: [Self.reserveTable]) }
    #expect(session.tools["reserve_table"]?.description == "Reserve a table", "the router offers it")
    let other = AgentTool(name: "reserve_table", description: "Reserve a table elsewhere") { _ in
      "Reserved elsewhere."
    }

    try await session.add(tools: [other])

    #expect(session.tools["reserve_table"]?.description == "Reserve a table elsewhere")
  }

  /// The router may have withdrawn a tool whose removal was never answered, or answered by a
  /// gateway's error, so asking again settles it when the router has none by that name, rather
  /// than failing.
  @Test(arguments: [SessionServer.Answer.dropped, .badGateway])
  func aToolWhoseRemovalWasNeverAnsweredIsRemovedByAskingAgain(lost: SessionServer.Answer) async throws {
    let server = try Self.losingTheFirstRemoval(lost)
    defer { server.listener.cancel() }
    let session = AgentSession(
      backend: Backend(url: try await server.url(), customerID: "test"), session: Self.session,
      tools: [Self.reserveTable])
    await #expect(throws: AgentsError.self) { try await session.remove(toolNamed: "reserve_table") }
    #expect(session.tools["reserve_table"] != nil, "the router may still offer it")

    try await session.remove(toolNamed: "reserve_table")

    #expect(session.tools["reserve_table"] == nil)
  }

  /// A removal whose answer was lost, or was a gateway's error, may or may not have reached the
  /// router, so another tool under its name settles it first rather than leaving the first one
  /// answering calls the router now makes of the second.
  @Test(arguments: [SessionServer.Answer.dropped, .badGateway])
  func aToolWhoseRemovalWasNeverAnsweredIsSettledBeforeAnotherTakesItsName(lost: SessionServer.Answer)
    async throws
  {
    let server = try Self.losingTheFirstRemoval(lost)
    defer { server.listener.cancel() }
    let session = AgentSession(
      backend: Backend(url: try await server.url(), customerID: "test"), session: Self.session,
      tools: [Self.reserveTable])
    await #expect(throws: AgentsError.self) { try await session.remove(toolNamed: "reserve_table") }
    let other = AgentTool(name: "reserve_table", description: "Reserve a table elsewhere") { _ in
      "Reserved elsewhere."
    }

    try await session.add(tools: [other])

    #expect(session.tools["reserve_table"]?.description == "Reserve a table elsewhere")
  }

  /// A router that withdraws the tool on the first removal but whose answer comes back as
  /// `lost`, so every removal after it finds no tool by that name.
  private static func losingTheFirstRemoval(_ lost: SessionServer.Answer) throws -> SessionServer {
    let removals = OSAllocatedUnfairLock(initialState: 0)
    return try SessionServer { method, _, _ in
      guard method == "DELETE" else { return .init(status: 204, headers: [:], body: "") }
      let first = removals.withLock {
        $0 += 1
        return $0 == 1
      }
      return first
        ? lost
        : .init(
          status: 404, headers: ["Content-Type": "application/json"],
          body: #"{"error":{"message":"no tool","type":"not_found","code":"not_found","doc_url":""}}"#)
    }
  }

  private static let session = Session(
    .init(
      agentId: "test", callId: "", callType: "agent", createdAt: Date(), id: "test",
      modality: .text, state: .live, text: true, userId: "test"))

  private static let reserveTable = AgentTool(name: "reserve_table", description: "Reserve a table") { _ in
    "Reserved."
  }

  @Test(arguments: ["get_video_frames", "server_lookup"], [false, true])
  func onlyLocallyOwnedToolsAreAnswered(remoteTool: String, localFailure: Bool) async throws {
    let server = try ToolEventServer(remoteTool: remoteTool)
    defer { server.listener.cancel() }
    let url = try #require(try await server.addresses.first(where: { @Sendable _ in true }))
    let tool = AgentTool(name: "local_lookup", description: "Read local state") { _ in
      // The remote owner may be slower than the observing phone. Give an erroneous
      // response to the unowned request time to reach the server first.
      try await Task.sleep(for: .milliseconds(50))
      if localFailure { throw AgentsError.unreadable("local lookup failed") }
      return "local result"
    }
    let session = AgentSession(
      backend: Backend(url: url, customerID: "test"),
      session: Session(
        .init(
          agentId: "test", callId: "", callType: "agent", createdAt: Date(), id: "test",
          modality: .text, state: .live, text: true, userId: "test")),
      tools: [tool])
    await session.start()
    do {
      let data = try #require(try await server.commands.first(where: { @Sendable _ in true }))
      let command = try JSONDecoder().decode([String: RawJSON].self, from: data)
      #expect(command["tool_call_id"]?.stringValue == "local")
      #expect(command["type"]?.stringValue == "tool_result")
      if localFailure {
        #expect(command["error"]?.stringValue?.contains("local lookup failed") == true)
      } else {
        #expect(command["output"]?.stringValue == "local result")
        #expect(command["error"]?.stringValue == "")
      }
      await session.close()
    } catch {
      await session.close()
      throw error
    }
  }

  @Test func aToolsPicturesGoBackBesideItsWords() async throws {
    let server = try ToolEventServer(remoteTool: "server_lookup")
    defer { server.listener.cancel() }
    let url = try #require(try await server.addresses.first(where: { @Sendable _ in true }))
    let tool = AgentTool(name: "local_lookup", description: "Photograph the room") { _ in
      ToolOutput(text: "the kitchen", images: [ImageSource(url: "data:image/png;base64,iVBORw0KGgo=")])
    }
    let session = AgentSession(
      backend: Backend(url: url, customerID: "test"),
      session: Session(
        .init(
          agentId: "test", callId: "", callType: "agent", createdAt: Date(), id: "test",
          modality: .text, state: .live, text: true, userId: "test")),
      tools: [tool])
    await session.start()
    do {
      let data = try #require(try await server.commands.first(where: { @Sendable _ in true }))
      let command = try JSONDecoder().decode([String: RawJSON].self, from: data)
      #expect(
        command["output"]
          == .array([
            .dictionary(["type": .string("text"), "text": .string("the kitchen")]),
            .dictionary([
              "type": .string("image_url"),
              "image_url": .dictionary(["url": .string("data:image/png;base64,iVBORw0KGgo=")]),
            ]),
          ]))
      await session.close()
    } catch {
      await session.close()
      throw error
    }
  }

  private struct Booking: Decodable {
    let location: String
    let guests: Int
    let notes: RawJSON?
  }

  /// A typed tool runs with the model's arguments decoded, and arguments missing a field reach
  /// the model as the tool failing, naming the field, rather than running it with a blank.
  /// Arguments nested deeper than a tool needs fail the same way, in a field the type reads or
  /// not, rather than overflowing the stack of the thread the tool runs on.
  @Test(arguments: [
    (#"{"location":"Boulder","guests":4}"#, "Booked Boulder for 4", ""),
    (#"{"guests":4}"#, nil, #""location""#),
    (#"{"location":"Boulder","guests":"four"}"#, nil, #""guests""#),
    (#"{"location":"Boulder","guests":4,"notes":\#(nested(20, open: "[", close: "]"))}"#, "Booked Boulder for 4", ""),
    (#"{"location":"Boulder","guests":4,"notes":"\"\#(String(repeating: "{[", count: 100))"}"#, "Booked Boulder for 4", ""),
    (#"{"location":"Boulder","guests":4,"extra":\#(nested(300, open: "{\"a\":", close: "}"))}"#, nil, "levels deep"),
    (#"{"location":"Boulder","guests":4,"notes":\#(nested(500, open: "[", close: "]"))}"#, nil, "levels deep"),
  ])
  func aTypedToolGetsItsArgumentsDecoded(arguments: String, output: String?, error: String) async throws {
    let server = try ToolEventServer(remoteTool: "server_lookup", arguments: arguments)
    defer { server.listener.cancel() }
    let url = try #require(try await server.addresses.first(where: { @Sendable _ in true }))
    let tool = AgentTool(name: "local_lookup", description: "Book a table") { (booking: Booking) in
      "Booked \(booking.location) for \(booking.guests)"
    }
    let session = AgentSession(
      backend: Backend(url: url, customerID: "test"), session: Self.session, tools: [tool])
    await session.start()
    do {
      let data = try #require(try await server.commands.first(where: { @Sendable _ in true }))
      let command = try JSONDecoder().decode([String: RawJSON].self, from: data)
      #expect(command["type"]?.stringValue == "tool_result")
      if let output {
        #expect(command["output"]?.stringValue == output)
        #expect(command["error"]?.stringValue == "")
      } else {
        #expect(command["output"]?.stringValue == "")
        #expect(command["error"]?.stringValue?.contains(error) == true, "\(command["error"] ?? .nil)")
      }
      await session.close()
    } catch {
      await session.close()
      throw error
    }
  }

  @Test(arguments: [true, false])
  func aCameraOnACallAnswersTheRequestToLook(recent: Bool) async throws {
    let server = try ToolEventServer(remoteTool: "get_video_frames")
    defer { server.listener.cancel() }
    let url = try #require(try await server.addresses.first(where: { @Sendable _ in true }))
    let frames = CameraFrames()
    frames.record(Data([0xff, 0xd8, 0xff]), at: recent ? Date() : Date().addingTimeInterval(-60))
    let session = AgentSession(
      backend: Backend(url: url, customerID: "test"),
      session: Session(
        .init(
          agentId: "test", callId: "", callType: "agent", createdAt: Date(), id: "test",
          modality: .voice, state: .live, text: false, userId: "test")),
      tools: [])
    session.cameraFrames = frames
    await session.start()
    do {
      let data = try #require(try await server.commands.first(where: { @Sendable _ in true }))
      let command = try JSONDecoder().decode([String: RawJSON].self, from: data)
      #expect(command["tool_call_id"]?.stringValue == "remote")
      if recent {
        let parts = try #require(command["output"]?.arrayValue)
        #expect(parts.count == 2, "what the frame is, then the frame")
        #expect(parts.first?["text"]?.stringValue?.contains("captured_at_ms") == true)
        #expect(parts.last?["image_url"]?["url"]?.stringValue == "data:image/jpeg;base64,/9j/")
      } else {
        #expect(command["output"]?.stringValue?.contains("point it at what they mean again") == true)
      }
      await session.close()
    } catch {
      await session.close()
      throw error
    }
  }

  @Test(arguments: [true, false])
  func aToolThatAsksFirstWaitsForThePerson(allowed: Bool) async throws {
    let server = try ApprovalServer()
    defer { server.listener.cancel() }
    let url = try #require(try await server.addresses.first(where: { @Sendable _ in true }))
    let refund = AgentTool(
      name: "issue_refund", description: "Refund an order",
      approval: .init(title: "Refund order A-1042?", reasonArgument: "reason")
    ) { arguments in
      "Refunded \(arguments["order_id"]?.stringValue ?? "")"
    }
    let session = AgentSession(
      backend: Backend(url: url, customerID: "test"),
      session: Session(
        .init(
          agentId: "test", callId: "", callType: "agent", createdAt: Date(), id: "test",
          modality: .text, state: .live, text: true, userId: "test")),
      tools: [refund])
    await session.start()
    defer { Task { await session.close() } }

    let deadline = ContinuousClock.now + .seconds(30)
    while session.approvals.isEmpty, ContinuousClock.now < deadline {
      try await Task.sleep(for: .milliseconds(10))
    }
    let waiting = try #require(session.approvals.first)
    #expect(waiting.approval.title == "Refund order A-1042?")
    #expect(waiting.reason == "it arrived unopened")

    try await session.decide("c1", allowed: allowed, summary: allowed ? "" : "Kept the order")

    #expect(session.approvals.isEmpty)
    var commands = server.commands.makeAsyncIterator()
    let answer = try JSONDecoder().decode(
      [String: RawJSON].self, from: try #require(try await commands.next()))
    #expect(answer["type"]?.stringValue == "tool_approval")
    #expect(answer["tool_call_id"]?.stringValue == "c1")
    #expect(answer["allowed"]?.boolValue == allowed)
    #expect(answer["request_id"]?.stringValue == "m1")
    #expect(answer["turn_id"]?.stringValue == "t1")
    let result = try JSONDecoder().decode(
      [String: RawJSON].self, from: try #require(try await commands.next()))
    #expect(result["type"]?.stringValue == "tool_result")
    #expect(result["tool_call_id"]?.stringValue == "c1")
    if allowed {
      #expect(result["output"]?.stringValue == "Refunded A-1042")
    } else {
      #expect(answer["summary"]?.stringValue == "Kept the order")
      #expect(result["output"]?.stringValue == "")
      #expect(result["error"]?.stringValue?.isEmpty == false)
    }
  }

  @Test func aDeclinedLoginNamesOnlyTheCallItWas() async throws {
    let server = try ApprovalServer()
    defer { server.listener.cancel() }
    let url = try #require(try await server.addresses.first(where: { @Sendable _ in true }))
    let session = AgentSession(
      backend: Backend(url: url, customerID: "test"),
      session: Session(
        .init(
          agentId: "test", callId: "", callType: "agent", createdAt: Date(), id: "test",
          modality: .text, state: .live, text: true, userId: "test")),
      tools: [])
    await session.start()
    defer { Task { await session.close() } }

    try await session.declineLogin("call-linear")

    var commands = server.commands.makeAsyncIterator()
    let answer = try JSONDecoder().decode(
      [String: RawJSON].self, from: try #require(try await commands.next()))
    #expect(
      answer == [
        "type": .string("tool_approval"), "tool_call_id": .string("call-linear"), "allowed": .bool(false),
      ])
  }

  /// `c1` as an approval card restored from the channel shows it, which this session's socket
  /// was never asked.
  private static let shownRefund = ShownToolCall(
    id: "c1", name: "issue_refund", arguments: #"{"order_id":"A-1042","reason":"it arrived unopened"}"#,
    requestID: "m1", turnID: "t1")

  /// The router asking this session for `c1`, as `frameOf` writes a tool call.
  private static let refundCall =
    #"{"type":"tool_call","id":"c1","name":"issue_refund","arguments":"{\"order_id\":\"A-1042\",\"reason\":\"it arrived unopened\"}","request_id":"m1","turn_id":"t1"}"#

  private static let allowedRefund: [String: RawJSON] = [
    "type": .string("tool_approval"), "tool_call_id": .string("c1"), "allowed": .bool(true),
    "request_id": .string("m1"), "turn_id": .string("t1"),
  ]
  private static let refunded: [String: RawJSON] = [
    "type": .string("tool_result"), "tool_call_id": .string("c1"), "output": .string("Refunded A-1042"),
    "error": .string(""), "request_id": .string("m1"), "turn_id": .string("t1"),
  ]
  private static let stillThere: [String: RawJSON] = ["type": .string("say"), "text": .string("still there?")]

  /// A session with the refund tool, which asks first, started against `server`, and the orders
  /// the tool refunded.
  private static func refunding(on server: ScriptedServer) async throws -> (AgentSession, OSAllocatedUnfairLock<[String]>) {
    let ran = OSAllocatedUnfairLock<[String]>(initialState: [])
    let refund = AgentTool(
      name: "issue_refund", description: "Refund an order",
      approval: .init(title: "Refund order A-1042?", reasonArgument: "reason")
    ) { arguments -> String in
      let order = arguments["order_id"]?.stringValue ?? ""
      ran.withLock { $0.append(order) }
      return "Refunded \(order)"
    }
    let url = try #require(try await server.addresses.first(where: { @Sendable _ in true }))
    let session = AgentSession(backend: Backend(url: url, customerID: "test"), session: Self.session, tools: [refund])
    await session.start()
    return (session, ran)
  }

  /// A frame the session sent, decoded.
  private static func decoded(_ data: Data?) throws -> [String: RawJSON] {
    try JSONDecoder().decode([String: RawJSON].self, from: try #require(data))
  }

  /// Waits until the session has read every frame the router sent before, and answered what it
  /// could.
  private static func readPast(_ session: AgentSession, on server: ScriptedServer) async throws {
    var events = session.events().makeAsyncIterator()
    try await server.send(#"{"type":"read_past"}"#)
    while let event = await events.next(), event.type != "read_past" {}
  }

  /// Waits until the session holds a call for the person.
  private static func held(_ session: AgentSession) async throws {
    let deadline = ContinuousClock.now + .seconds(30)
    while session.approvals.isEmpty, ContinuousClock.now < deadline {
      try await Task.sleep(for: .milliseconds(10))
    }
    try #require(!session.approvals.isEmpty)
  }

  @Test func aCallShownFromTheConversationAllowedIsAnsweredOnItsTurnAndRunsHere() async throws {
    let server = try ScriptedServer()
    defer { server.listener.cancel() }
    let (session, ran) = try await Self.refunding(on: server)
    defer { Task { await session.close() } }
    var commands = server.commands.makeAsyncIterator()

    try await session.decide(Self.shownRefund, allowed: true)

    #expect(try await Self.decoded(commands.next()) == Self.allowedRefund)
    #expect(try await Self.decoded(commands.next()) == Self.refunded)
    #expect(ran.withLock { $0 } == ["A-1042"])
  }

  @Test func aCallShownFromTheConversationDeclinedNeverRunsAndTheModelIsToldSo() async throws {
    let server = try ScriptedServer()
    defer { server.listener.cancel() }
    let (session, ran) = try await Self.refunding(on: server)
    defer { Task { await session.close() } }
    var commands = server.commands.makeAsyncIterator()

    try await session.decide(Self.shownRefund, allowed: false, summary: "Kept the order")

    #expect(
      try await Self.decoded(commands.next()) == [
        "type": .string("tool_approval"), "tool_call_id": .string("c1"), "allowed": .bool(false),
        "summary": .string("Kept the order"), "request_id": .string("m1"), "turn_id": .string("t1"),
      ])
    #expect(
      try await Self.decoded(commands.next()) == [
        "type": .string("tool_result"), "tool_call_id": .string("c1"), "output": .string(""),
        "error": .string("The person declined this call."), "request_id": .string("m1"),
        "turn_id": .string("t1"),
      ])
    #expect(ran.withLock { $0 }.isEmpty)
  }

  /// A call the router holds itself, such as a connector's under `require_approval` or the
  /// agent's `schedule_task`, reaches the app only as a step on the reply.
  @Test func aCallShownFromTheConversationOfAToolThisSessionLacksAllowedIsLeftToWhoeverHasIt() async throws {
    let server = try ScriptedServer()
    defer { server.listener.cancel() }
    let (session, _) = try await Self.refunding(on: server)
    defer { Task { await session.close() } }
    var commands = server.commands.makeAsyncIterator()

    try await session.decide(
      ShownToolCall(
        id: "c1", name: "schedule_task", arguments: #"{"title":"Call the bank"}"#, requestID: "m1",
        turnID: "t1"),
      allowed: true)
    try await session.say("still there?")

    #expect(
      try await Self.decoded(commands.next()) == [
        "type": .string("tool_approval"), "tool_call_id": .string("c1"), "allowed": .bool(true),
        "request_id": .string("m1"), "turn_id": .string("t1"),
      ])
    #expect(try await Self.decoded(commands.next()) == Self.stillThere)
  }

  @Test func aCallShownWithoutItsArgumentsAllowedRunsOnceTheRouterSendsIt() async throws {
    let server = try ScriptedServer()
    defer { server.listener.cancel() }
    let (session, ran) = try await Self.refunding(on: server)
    defer { Task { await session.close() } }
    var commands = server.commands.makeAsyncIterator()

    try await session.decide(
      ShownToolCall(id: "c1", name: "issue_refund", arguments: nil, requestID: "m1", turnID: "t1"),
      allowed: true)
    #expect(try await Self.decoded(commands.next()) == Self.allowedRefund)
    try await server.send(Self.refundCall)

    #expect(try await Self.decoded(commands.next()) == Self.refunded)
    #expect(ran.withLock { $0 } == ["A-1042"])
    #expect(session.approvals.isEmpty)
  }

  @Test func aCallShownFromTheConversationAllowedIsNeitherAskedAboutNorRunAgainWhenTheRouterSendsIt()
    async throws
  {
    let server = try ScriptedServer()
    defer { server.listener.cancel() }
    let (session, ran) = try await Self.refunding(on: server)
    defer { Task { await session.close() } }
    var commands = server.commands.makeAsyncIterator()

    try await session.decide(Self.shownRefund, allowed: true)
    #expect(try await Self.decoded(commands.next()) == Self.allowedRefund)
    #expect(try await Self.decoded(commands.next()) == Self.refunded)

    try await server.send(Self.refundCall)
    try await server.send(String(Self.refundCall.dropLast()) + #","approved":true}"#)
    try await Self.readPast(session, on: server)
    try await session.say("still there?")

    #expect(try await Self.decoded(commands.next()) == Self.stillThere)
    #expect(ran.withLock { $0 } == ["A-1042"])
    #expect(session.approvals.isEmpty)
  }

  @Test func aCallShownFromTheConversationDeclinedIsNotAskedAboutAgainWhenTheRouterSendsIt() async throws {
    let server = try ScriptedServer()
    defer { server.listener.cancel() }
    let (session, ran) = try await Self.refunding(on: server)
    defer { Task { await session.close() } }
    var commands = server.commands.makeAsyncIterator()

    try await session.decide(Self.shownRefund, allowed: false)
    #expect(try await Self.decoded(commands.next())["type"] == .string("tool_approval"))
    #expect(try await Self.decoded(commands.next())["type"] == .string("tool_result"))

    try await server.send(Self.refundCall)
    try await Self.readPast(session, on: server)
    try await session.say("still there?")

    #expect(try await Self.decoded(commands.next()) == Self.stillThere)
    #expect(session.approvals.isEmpty)
    #expect(ran.withLock { $0 }.isEmpty)
  }

  @Test func aCallShownFromTheConversationThatAlreadyRanHereIsNotRunAgain() async throws {
    let server = try ScriptedServer()
    defer { server.listener.cancel() }
    let (session, ran) = try await Self.refunding(on: server)
    defer { Task { await session.close() } }
    var commands = server.commands.makeAsyncIterator()

    // Allowed out loud, so the router sent it as already approved and it ran at once.
    try await server.send(String(Self.refundCall.dropLast()) + #","approved":true}"#)
    #expect(try await Self.decoded(commands.next()) == Self.refunded)
    try await session.decide(Self.shownRefund, allowed: true)
    try await session.say("still there?")

    #expect(try await Self.decoded(commands.next()) == Self.stillThere)
    #expect(ran.withLock { $0 } == ["A-1042"])
  }

  @Test func aCallShownFromTheConversationAnsweredTwiceSuchAsADoubleTapIsAnsweredAndRunOnce() async throws {
    let server = try ScriptedServer()
    defer { server.listener.cancel() }
    let (session, ran) = try await Self.refunding(on: server)
    defer { Task { await session.close() } }
    var commands = server.commands.makeAsyncIterator()

    try await session.decide(Self.shownRefund, allowed: true)
    try await session.decide(Self.shownRefund, allowed: true)
    try await session.decide(Self.shownRefund, allowed: false)

    #expect(try await Self.decoded(commands.next()) == Self.allowedRefund)
    #expect(try await Self.decoded(commands.next()) == Self.refunded)
    try await session.say("still there?")
    #expect(try await Self.decoded(commands.next()) == Self.stillThere)
    #expect(ran.withLock { $0 } == ["A-1042"])
  }

  @Test func aCallShownFromTheConversationThatTheSessionHoldsIsAnsweredAsTheOneItHoldsOnce() async throws {
    let server = try ScriptedServer()
    defer { server.listener.cancel() }
    let (session, ran) = try await Self.refunding(on: server)
    defer { Task { await session.close() } }
    var commands = server.commands.makeAsyncIterator()

    try await server.send(Self.refundCall)
    try await Self.held(session)
    try await session.decide(ShownToolCall(id: "c1", name: "issue_refund", arguments: nil), allowed: true)

    #expect(session.approvals.isEmpty)
    // The request and turn, and the arguments it runs with, are the socket's own.
    #expect(try await Self.decoded(commands.next()) == Self.allowedRefund)
    #expect(try await Self.decoded(commands.next()) == Self.refunded)
    try await session.decide(Self.shownRefund, allowed: true)
    try await session.say("still there?")
    #expect(try await Self.decoded(commands.next()) == Self.stillThere)
    #expect(ran.withLock { $0 } == ["A-1042"])
  }

  @Test func aCallTheRouterCancelledWhileItsCardShowedIsNotAnsweredFromTheCard() async throws {
    let server = try ScriptedServer()
    defer { server.listener.cancel() }
    let (session, ran) = try await Self.refunding(on: server)
    defer { Task { await session.close() } }
    var commands = server.commands.makeAsyncIterator()

    try await server.send(Self.refundCall)
    try await Self.held(session)
    var events = session.events().makeAsyncIterator()
    try await server.send(#"{"type":"tool_cancel","id":"c1","request_id":"m1","turn_id":"t1"}"#)
    while let event = await events.next(), event.kind != .toolCancel {}
    try await session.decide(Self.shownRefund, allowed: true)
    try await session.say("still there?")

    #expect(try await Self.decoded(commands.next()) == Self.stillThere)
    #expect(ran.withLock { $0 }.isEmpty)
  }

  @Test func aCallTheSessionHoldsAnsweredAtOnceByIDAndAsShownIsApprovedOnce() async throws {
    let server = try ScriptedServer()
    defer { server.listener.cancel() }
    let (session, _) = try await Self.refunding(on: server)
    defer { Task { await session.close() } }
    var commands = server.commands.makeAsyncIterator()

    try await server.send(Self.refundCall)
    try await Self.held(session)
    let answers = (0..<8).map { i in
      Task {
        if i.isMultiple(of: 2) {
          try await session.decide("c1", allowed: true)
        } else {
          try await session.decide(Self.shownRefund, allowed: true)
        }
      }
    }
    for answer in answers {
      try await answer.value
    }
    try await session.say("still there?")

    var approvals = 0
    while true {
      let sent = try await Self.decoded(commands.next())
      if sent == Self.stillThere { break }
      if sent["type"] == .string("tool_approval") { approvals += 1 }
    }
    #expect(approvals == 1)
  }

  @Test func everyReaderOfTheEventsGetsEachEventAndTheStreamEndsWithTheSession() async throws {
    let server = try ApprovalServer()
    defer { server.listener.cancel() }
    let url = try #require(try await server.addresses.first(where: { @Sendable _ in true }))
    let session = AgentSession(
      backend: Backend(url: url, customerID: "test"),
      session: Session(
        .init(
          agentId: "test", callId: "", callType: "agent", createdAt: Date(), id: "test",
          modality: .text, state: .live, text: true, userId: "test")),
      tools: [])
    var first = session.events().makeAsyncIterator()
    var second = session.events().makeAsyncIterator()

    await session.start()
    let seen = [await first.next(), await second.next()]
    await session.close()

    #expect(seen.map { $0?.string("id") } == ["c1", "c1"])
    #expect(await first.next() == nil)
    var late = session.events().makeAsyncIterator()
    #expect(await late.next() == nil)
  }

  @Test func sayingSomethingOverTheAgentStopsItFirst() async throws {
    let server = try ApprovalServer()
    defer { server.listener.cancel() }
    let url = try #require(try await server.addresses.first(where: { @Sendable _ in true }))
    let session = AgentSession(
      backend: Backend(url: url, customerID: "test"),
      session: Session(
        .init(
          agentId: "test", callId: "", callType: "agent", createdAt: Date(), id: "test",
          modality: .voice, state: .live, text: false, userId: "test")),
      tools: [])
    await session.start()
    defer { Task { await session.close() } }

    try await session.say("One moment.", interrupt: true)

    var commands = server.commands.makeAsyncIterator()
    let sent = try [#require(try await commands.next()), #require(try await commands.next())].map {
      try JSONDecoder().decode([String: RawJSON].self, from: $0)
    }
    #expect(sent == [["type": .string("interrupt")], ["type": .string("say"), "text": .string("One moment.")]])
  }

  @Test func anUpdateRefreshesTheSessionItHolds() async throws {
    let server = try SessionServer()
    defer { server.listener.cancel() }
    let session = AgentSession(
      backend: Backend(url: try await server.url(), customerID: "test"),
      session: Session(
        .init(
          agentId: "test", callId: "", callType: "agent", createdAt: Date(), id: "test",
          modality: .text, state: .live, text: true, title: "Before", userId: "test")),
      tools: [])

    try await session.update(title: "After")

    #expect(session.session.title == "After")
    #expect(try await server.request().body == ["title": .string("After")])
  }

  @Test(arguments: [("POST", "s1"), ("DELETE", "")])
  func startingAndStoppingVoiceMovesTheSessionOntoAndOffItsCall(method: String, callID: String)
    async throws
  {
    let server = try SessionServer(answer: .session(status: 200, callID: callID))
    defer { server.listener.cancel() }
    let session = AgentSession(
      backend: Backend(url: try await server.url(), customerID: "test"),
      session: Session(
        .init(
          agentId: "a1", callId: method == "POST" ? "" : "s1", callType: "agent",
          createdAt: Date(), id: "s1", modality: .text, state: .live, userId: "jlahey")),
      tools: [])

    if method == "POST" {
      try await session.startVoice()
    } else {
      try await session.stopVoice()
    }

    #expect(session.session.callID == callID)
    let request = try await server.request()
    #expect(request.method == method)
    #expect(request.path == "/v1/agents/sessions/s1/voice")
  }

  /// Closed while the token is on its way, a session opens nothing once it arrives, and
  /// starting it again opens nothing either.
  @Test func aSessionClosedWhileItsCredentialsAreOnTheirWayOpensNoSocket() async throws {
    let server = try HeldServer()
    defer { server.listener.cancel() }
    let asked = OSAllocatedUnfairLock(initialState: false)
    let (token, giveToken) = AsyncStream<String>.makeStream()
    let backend = Backend(apiKey: "key", url: try await server.url())
    backend.setUser(User(id: "jlahey")) {
      asked.withLock { $0 = true }
      for await given in token { return given }
      throw CancellationError()
    }
    let session = AgentSession(backend: backend, session: Self.session, tools: [])
    let connected = OSAllocatedUnfairLock(initialState: [Bool]())
    let watching = session.$isConnected.sink { value in connected.withLock { $0.append(value) } }
    defer { watching.cancel() }
    var events = session.events().makeAsyncIterator()
    let starting = Task { await session.start() }
    try #require(await until { asked.withLock { $0 } })

    await session.close()
    giveToken.yield("tok")
    await starting.value

    #expect(server.upgrades == 0)
    #expect(!connected.withLock { $0.contains(true) }, "it never showed itself connected")
    #expect(session.failure == nil)
    #expect(await events.next() == nil)
    await session.start()
    #expect(server.upgrades == 0)
  }

  /// Closed while the router is still answering its upgrade, a session never shows itself
  /// connected, the socket the router took is closed, and starting it again opens nothing.
  @Test func aSessionClosedWhileItsUpgradeIsOnItsWayClosesTheSocketItGot() async throws {
    let server = try HeldServer(released: false)
    defer { server.listener.cancel() }
    let session = AgentSession(
      backend: Backend(url: try await server.url(), customerID: "test"), session: Self.session, tools: [])
    let connected = OSAllocatedUnfairLock(initialState: [Bool]())
    let watching = session.$isConnected.sink { value in connected.withLock { $0.append(value) } }
    defer { watching.cancel() }
    var events = session.events().makeAsyncIterator()
    let starting = Task { await session.start() }
    try #require(await until { server.upgrades == 1 })

    // The task shares the main actor with this test, so by the time this sees the flag,
    // `close()` has run up to its first await: it began while the upgrade was still held.
    let closingBegan = OSAllocatedUnfairLock(initialState: false)
    let closing = Task {
      closingBegan.withLock { $0 = true }
      await session.close()
    }
    try #require(await until { closingBegan.withLock { $0 } })
    server.release()
    await closing.value
    await starting.value

    #expect(await until { server.hungUp })
    #expect(!connected.withLock { $0.contains(true) }, "it never showed itself connected")
    #expect(session.failure == nil)
    #expect(await events.next() == nil)
    await session.start()
    #expect(server.upgrades == 1)
  }

  /// Once closed, a session stays closed: starting it again opens no socket.
  @Test func aClosedSessionIsNotStartedAgain() async throws {
    let server = try HeldServer()
    defer { server.listener.cancel() }
    let session = AgentSession(
      backend: Backend(url: try await server.url(), customerID: "test"), session: Self.session, tools: [])
    await session.start()
    #expect(session.isConnected)

    await session.close()
    await session.start()

    #expect(server.upgrades == 1)
    #expect(!session.isConnected)
    #expect(session.failure == nil)
    var late = session.events().makeAsyncIterator()
    #expect(await late.next() == nil)
  }

  /// A socket opens once. Opening it again, even while its upgrade is on its way, opens
  /// nothing and leaves the first one running for `close` to close.
  @Test func aSocketOpensOnce() async throws {
    let server = try HeldServer(released: false)
    defer { server.listener.cancel() }
    var address = URLComponents(url: try await server.url(), resolvingAgainstBaseURL: false)
    address?.scheme = "ws"
    let socket = SessionSocket(url: try #require(address?.url), headers: [:])
    let first = Task { await socket.open() }
    try #require(await until { server.upgrades == 1 })

    let again = await socket.open()
    server.release()
    _ = await first.value

    do {
      for try await _ in again {}
      Issue.record("the second open was not refused")
    } catch AgentsError.configuration(let message) {
      #expect(message == "a session socket is opened once")
    }
    #expect(await socket.isOpen)
    #expect(server.upgrades == 1)
    await socket.close()
    #expect(await until { server.hungUp })
  }

  /// A call the router cancels stops its tool, which is not answered since the router answered
  /// it itself, whether the tool gives up when cancelled or finishes anyway.
  @Test(arguments: [false, true])
  func aToolCallTheRouterCancelledIsStoppedAndNotAnswered(finishesAnyway: Bool) async throws {
    let server = try HeldServer(greeting: [#"{"type":"tool_call","id":"c1","name":"slow_lookup","arguments":"{}"}"#])
    defer { server.listener.cancel() }
    let seen = OSAllocatedUnfairLock(initialState: [String]())
    let slow = AgentTool(name: "slow_lookup", description: "Takes its time") { (_: [String: RawJSON]) -> String in
      seen.withLock { $0.append("started") }
      do {
        try await Task.sleep(for: .seconds(30))
      } catch {
        seen.withLock { $0.append("cancelled") }
        if !finishesAnyway { throw error }
      }
      return "too late"
    }
    let session = AgentSession(
      backend: Backend(url: try await server.url(), customerID: "test"), session: Self.session, tools: [slow])
    await session.start()
    try #require(await until { seen.withLock { $0 } == ["started"] })

    server.send(#"{"type":"tool_cancel","id":"c1","request_id":"","turn_id":""}"#)
    try #require(await until { seen.withLock { $0 } == ["started", "cancelled"] })
    try await session.interrupt()
    try await session.say("Anything else?")
    await session.close()

    // Everything sent over the socket until it closed, so a result sent late is in it too.
    try #require(await until { server.hungUp })
    #expect(server.frames.map { $0["type"]?.stringValue } == ["interrupt", "say", "close"])
  }

  /// Closing stops the tools still running, and none of them is answered.
  @Test func closingStopsTheToolsStillRunning() async throws {
    let server = try HeldServer(greeting: [#"{"type":"tool_call","id":"c1","name":"slow_lookup","arguments":"{}"}"#])
    defer { server.listener.cancel() }
    let seen = OSAllocatedUnfairLock(initialState: [String]())
    let slow = AgentTool(name: "slow_lookup", description: "Takes its time") { (_: [String: RawJSON]) -> String in
      seen.withLock { $0.append("started") }
      do {
        try await Task.sleep(for: .seconds(30))
      } catch {
        seen.withLock { $0.append("cancelled") }
        throw error
      }
      return "too late"
    }
    let session = AgentSession(
      backend: Backend(url: try await server.url(), customerID: "test"), session: Self.session, tools: [slow])
    await session.start()
    try #require(await until { seen.withLock { $0 } == ["started"] })

    await session.close()

    try #require(await until { seen.withLock { $0 } == ["started", "cancelled"] })
    try #require(await until { server.hungUp })
    #expect(server.frames.map { $0["type"]?.stringValue } == ["close"])
  }

  /// A call the router cancels while the person's answer to it is on its way is neither run
  /// nor answered: the router has answered it itself.
  @available(iOS 26, *)
  @Test func aCallCancelledWhileItsApprovalIsOnItsWayIsNotRun() async throws {
    let server = try HeldServer(greeting: [
      #"{"type":"tool_call","id":"c1","name":"issue_refund","arguments":"{}","request_id":"m1","turn_id":"t1"}"#
    ])
    defer { server.listener.cancel() }
    let ran = OSAllocatedUnfairLock(initialState: false)
    let refund = AgentTool(
      name: "issue_refund", description: "Refund an order", approval: .init(title: "Refund the order?")
    ) { (_: [String: RawJSON]) -> String in
      ran.withLock { $0 = true }
      return "Refunded"
    }
    let session = AgentSession(
      backend: Backend(url: try await server.url(), customerID: "test"), session: Self.session, tools: [refund])
    // A handler hears the cancel before the session acts on it, so an answer begun there is on
    // its way, the call already out of `approvals`, when the session takes the cancel in.
    let deciding = OSAllocatedUnfairLock<Task<Void, any Error>?>(initialState: nil)
    let watching = session.on(.toolCancel) { _ in
      let decided = Task.immediate { try await session.decide("c1", allowed: true) }
      deciding.withLock { $0 = decided }
    }
    defer { watching.cancel() }
    await session.start()
    try #require(await until { session.approvals.count == 1 })

    server.send(#"{"type":"tool_cancel","id":"c1","request_id":"m1","turn_id":"t1"}"#)
    try #require(await until { deciding.withLock { $0 } != nil })
    try await deciding.withLock { $0 }?.value
    await session.close()

    try #require(await until { server.hungUp })
    #expect(!ran.withLock { $0 }, "the cancelled call ran")
    #expect(server.frames.map { $0["type"]?.stringValue } == ["tool_approval", "close"])
  }

  /// A tool that throws `CancellationError` without having been cancelled, such as from a
  /// timeout of its own, failed: the model is told so rather than left waiting for an answer.
  @Test func aToolThatThrowsCancellationOfItsOwnIsAnsweredWithTheFailure() async throws {
    let server = try HeldServer(greeting: [#"{"type":"tool_call","id":"c1","name":"slow_lookup","arguments":"{}"}"#])
    defer { server.listener.cancel() }
    let gaveUp = AgentTool(name: "slow_lookup", description: "Gives up") { (_: [String: RawJSON]) -> String in
      throw CancellationError()
    }
    let session = AgentSession(
      backend: Backend(url: try await server.url(), customerID: "test"), session: Self.session, tools: [gaveUp])
    await session.start()

    try #require(await until { !server.frames.isEmpty })
    let answer = try #require(server.frames.first)
    #expect(answer["type"]?.stringValue == "tool_result")
    #expect(answer["tool_call_id"]?.stringValue == "c1")
    #expect(answer["error"]?.stringValue?.isEmpty == false)
    await session.close()
  }

  /// A start whose credentials could not be had opens nothing and ends nothing, so starting
  /// again connects a whole session: its events reach a reader taken before, with no failure
  /// left from the first try.
  @Test func aSessionWhoseCredentialsFailedConnectsWhenStartedAgain() async throws {
    let server = try HeldServer(greeting: [#"{"type":"astonished","degree":11}"#])
    defer { server.listener.cancel() }
    let attempts = OSAllocatedUnfairLock(initialState: 0)
    let backend = Backend(apiKey: "key", url: try await server.url())
    backend.setUser(User(id: "jlahey")) {
      let first = attempts.withLock {
        $0 += 1
        return $0 == 1
      }
      if first { throw AgentsError.unreadable("no token yet") }
      return "tok"
    }
    let session = AgentSession(backend: backend, session: Self.session, tools: [])
    var events = session.events().makeAsyncIterator()
    await session.start()
    #expect(session.failure != nil)
    #expect(!session.isConnected)
    #expect(server.upgrades == 0)

    await session.start()

    #expect(session.isConnected)
    #expect(session.failure == nil)
    #expect(await events.next()?.type == "astonished")
    await session.close()
  }
}


/// Waits for `done` to hold, for up to `seconds`, and says whether it did.
@MainActor
private func until(_ seconds: Int = 10, _ done: () -> Bool) async -> Bool {
  let deadline = ContinuousClock.now + .seconds(seconds)
  while !done() {
    guard ContinuousClock.now < deadline, (try? await Task.sleep(for: .milliseconds(10))) != nil else {
      return false
    }
  }
  return true
}


/// A JSON value nested `depth` times, such as `{"a":{"a":1}}`.
private func nested(_ depth: Int, open: String, close: String) -> String {
  String(repeating: open, count: depth) + "1" + String(repeating: close, count: depth)
}

/// A real WebSocket peer broadcasting one remote tool and one locally owned tool.
private struct ToolEventServer {
  let listener: NWListener
  let addresses: AsyncThrowingStream<URL, any Error>
  let commands: AsyncThrowingStream<Data, any Error>

  init(remoteTool: String, arguments: String = "{}") throws {
    let quoted = String(decoding: try JSONEncoder().encode(arguments), as: UTF8.self)
    let queue = DispatchQueue(label: "tool-event-server")
    let webSocket = NWProtocolWebSocket.Options()
    webSocket.autoReplyPing = true
    webSocket.setClientRequestHandler(queue) { _, _ in
      .init(status: .accept, subprotocol: nil)
    }
    let parameters = NWParameters.tcp
    parameters.requiredLocalEndpoint = .hostPort(host: "127.0.0.1", port: .any)
    parameters.defaultProtocolStack.applicationProtocols.insert(webSocket, at: 0)
    let listener = try NWListener(using: parameters)
    self.listener = listener
    let (addresses, address) = AsyncThrowingStream<URL, any Error>.makeStream()
    let (commands, command) = AsyncThrowingStream<Data, any Error>.makeStream()
    self.addresses = addresses
    self.commands = commands
    listener.stateUpdateHandler = { state in
      switch state {
      case .ready:
        if let port = listener.port {
          address.yield(URL(string: "http://127.0.0.1:\(port.rawValue)")!)
          address.finish()
        }
      case .failed(let error):
        address.finish(throwing: error)
        command.finish(throwing: error)
      default: break
      }
    }
    listener.newConnectionHandler = { connection in
      connection.stateUpdateHandler = { state in
        guard case .ready = state else { return }
        for (id, name) in [("remote", remoteTool), ("local", "local_lookup")] {
          let frame = #"{"type":"tool_call","id":"\#(id)","name":"\#(name)","arguments":\#(quoted)}"#
          let context = NWConnection.ContentContext(
            identifier: id,
            metadata: [NWProtocolWebSocket.Metadata(opcode: .text)])
          connection.send(
            content: Data(frame.utf8), contentContext: context,
            completion: .contentProcessed { error in
              if let error { command.finish(throwing: error) }
            })
        }
        connection.receiveMessage { data, _, _, error in
          if let data { command.yield(data) }
          command.finish(throwing: error)
          connection.cancel()
        }
      }
      connection.start(queue: queue)
      queue.asyncAfter(deadline: .now() + 30) { connection.cancel() }
    }
    queue.asyncAfter(deadline: .now() + 30) {
      let error = AgentsError.unreadable("test socket timed out")
      address.finish(throwing: error)
      command.finish(throwing: error)
    }
    listener.start(queue: queue)
  }
}

/// A real WebSocket peer that asks for one call of a tool that asks first, from a durable
/// command, and hands over every frame the session sends back.
private struct ApprovalServer {
  let listener: NWListener
  let addresses: AsyncThrowingStream<URL, any Error>
  let commands: AsyncThrowingStream<Data, any Error>

  init() throws {
    let queue = DispatchQueue(label: "approval-server")
    let webSocket = NWProtocolWebSocket.Options()
    webSocket.autoReplyPing = true
    webSocket.setClientRequestHandler(queue) { _, _ in
      .init(status: .accept, subprotocol: nil)
    }
    let parameters = NWParameters.tcp
    parameters.requiredLocalEndpoint = .hostPort(host: "127.0.0.1", port: .any)
    parameters.defaultProtocolStack.applicationProtocols.insert(webSocket, at: 0)
    let listener = try NWListener(using: parameters)
    self.listener = listener
    let (addresses, address) = AsyncThrowingStream<URL, any Error>.makeStream()
    let (commands, command) = AsyncThrowingStream<Data, any Error>.makeStream()
    self.addresses = addresses
    self.commands = commands
    listener.stateUpdateHandler = { state in
      switch state {
      case .ready:
        if let port = listener.port {
          address.yield(URL(string: "http://127.0.0.1:\(port.rawValue)")!)
          address.finish()
        }
      case .failed(let error):
        address.finish(throwing: error)
        command.finish(throwing: error)
      default: break
      }
    }
    @Sendable func receive(_ connection: NWConnection) {
      connection.receiveMessage { data, _, _, error in
        if let error {
          command.finish(throwing: error)
          return
        }
        if let data { command.yield(data) }
        receive(connection)
      }
    }
    listener.newConnectionHandler = { connection in
      connection.stateUpdateHandler = { state in
        guard case .ready = state else { return }
        let frame =
          #"{"type":"tool_call","id":"c1","name":"issue_refund","arguments":"{\"order_id\":\"A-1042\",\"reason\":\"it arrived unopened\"}","request_id":"m1","turn_id":"t1"}"#
        connection.send(
          content: Data(frame.utf8),
          contentContext: NWConnection.ContentContext(
            identifier: "call", metadata: [NWProtocolWebSocket.Metadata(opcode: .text)]),
          completion: .contentProcessed { error in
            if let error { command.finish(throwing: error) }
          })
        receive(connection)
      }
      connection.start(queue: queue)
      queue.asyncAfter(deadline: .now() + 30) { connection.cancel() }
    }
    queue.asyncAfter(deadline: .now() + 30) {
      let error = AgentsError.unreadable("test socket timed out")
      address.finish(throwing: error)
      command.finish(throwing: error)
    }
    listener.start(queue: queue)
  }
}

/// A real WebSocket peer that sends a frame whenever the test says, once the session has
/// connected, and hands over every frame the session sends back.
private struct ScriptedServer {
  let listener: NWListener
  let addresses: AsyncThrowingStream<URL, any Error>
  let commands: AsyncThrowingStream<Data, any Error>
  private let connected: OSAllocatedUnfairLock<NWConnection?>

  init() throws {
    let queue = DispatchQueue(label: "scripted-server")
    let webSocket = NWProtocolWebSocket.Options()
    webSocket.autoReplyPing = true
    webSocket.setClientRequestHandler(queue) { _, _ in
      .init(status: .accept, subprotocol: nil)
    }
    let parameters = NWParameters.tcp
    parameters.requiredLocalEndpoint = .hostPort(host: "127.0.0.1", port: .any)
    parameters.defaultProtocolStack.applicationProtocols.insert(webSocket, at: 0)
    let listener = try NWListener(using: parameters)
    self.listener = listener
    let (addresses, address) = AsyncThrowingStream<URL, any Error>.makeStream()
    let (commands, command) = AsyncThrowingStream<Data, any Error>.makeStream()
    let connected = OSAllocatedUnfairLock<NWConnection?>(initialState: nil)
    self.addresses = addresses
    self.commands = commands
    self.connected = connected
    listener.stateUpdateHandler = { state in
      switch state {
      case .ready:
        if let port = listener.port {
          address.yield(URL(string: "http://127.0.0.1:\(port.rawValue)")!)
          address.finish()
        }
      case .failed(let error):
        address.finish(throwing: error)
        command.finish(throwing: error)
      default: break
      }
    }
    @Sendable func receive(_ connection: NWConnection) {
      connection.receiveMessage { data, _, _, error in
        if let error {
          command.finish(throwing: error)
          return
        }
        if let data { command.yield(data) }
        receive(connection)
      }
    }
    listener.newConnectionHandler = { connection in
      connection.stateUpdateHandler = { state in
        guard case .ready = state else { return }
        connected.withLock { $0 = connection }
        receive(connection)
      }
      connection.start(queue: queue)
      queue.asyncAfter(deadline: .now() + 30) { connection.cancel() }
    }
    queue.asyncAfter(deadline: .now() + 30) {
      let error = AgentsError.unreadable("test socket timed out")
      address.finish(throwing: error)
      command.finish(throwing: error)
    }
    listener.start(queue: queue)
  }

  /// Sends `frame` to the session, once it has connected. Frames go in the order they are sent.
  func send(_ frame: String) async throws {
    let deadline = ContinuousClock.now + .seconds(30)
    while connected.withLock({ $0 }) == nil, ContinuousClock.now < deadline {
      try await Task.sleep(for: .milliseconds(10))
    }
    let connection = try #require(connected.withLock { $0 })
    connection.send(
      content: Data(frame.utf8),
      contentContext: NWConnection.ContentContext(
        identifier: "frame", metadata: [NWProtocolWebSocket.Metadata(opcode: .text)]),
      completion: .idempotent)
  }
}

/// A real socket peer that upgrades by hand, holding each upgrade until `release()` unless it
/// was made released, so a test can act while a session waits for one. It counts the upgrades
/// asked for, sends `greeting` over each socket it takes, and tells what the session sent over
/// it and whether the session hung up.
private final class HeldServer: Sendable {
  private struct State {
    var released: Bool
    var upgrades = 0
    var held: [(NWConnection, String)] = []
    var open: [NWConnection] = []
    var frames: [[String: RawJSON]] = []
    var hungUp = false
  }

  let listener: NWListener
  private let addresses: AsyncThrowingStream<URL, any Error>
  private let state: OSAllocatedUnfairLock<State>
  private let greeting: [String]
  private let queue = DispatchQueue(label: "held-server")

  init(released: Bool = true, greeting: [String] = []) throws {
    let parameters = NWParameters.tcp
    parameters.requiredLocalEndpoint = .hostPort(host: "127.0.0.1", port: .any)
    listener = try NWListener(using: parameters)
    state = OSAllocatedUnfairLock(initialState: State(released: released))
    self.greeting = greeting
    let (addresses, address) = AsyncThrowingStream<URL, any Error>.makeStream()
    self.addresses = addresses
    listener.stateUpdateHandler = { [listener] state in
      switch state {
      case .ready:
        if let port = listener.port {
          address.yield(URL(string: "http://127.0.0.1:\(port.rawValue)")!)
          address.finish()
        }
      case .failed(let error):
        address.finish(throwing: error)
      default: break
      }
    }
    listener.newConnectionHandler = { [weak self, queue] connection in
      connection.start(queue: queue)
      self?.read(connection, Data())
      queue.asyncAfter(deadline: .now() + 30) { connection.cancel() }
    }
    queue.asyncAfter(deadline: .now() + 30) {
      address.finish(throwing: AgentsError.unreadable("test server timed out"))
    }
    listener.start(queue: queue)
  }

  /// The upgrades asked for, the ones still held included.
  var upgrades: Int { state.withLock { $0.upgrades } }
  /// Every frame the session sent, in order.
  var frames: [[String: RawJSON]] { state.withLock { $0.frames } }
  /// Whether the session closed a socket the server took, or the connection under it.
  var hungUp: Bool { state.withLock { $0.hungUp } }

  func url() async throws -> URL {
    try #require(try await addresses.first(where: { @Sendable _ in true }))
  }

  /// Answers the upgrades held, and every one after this, by switching to the socket.
  func release() {
    let held = state.withLock { state in
      state.released = true
      defer { state.held = [] }
      return state.held
    }
    for (connection, key) in held {
      upgrade(connection, key: key)
    }
  }

  /// Sends a frame over every socket open.
  func send(_ text: String) {
    for connection in state.withLock({ $0.open }) {
      connection.send(content: Self.frame(0x1, Data(text.utf8)), completion: .idempotent)
    }
  }

  private func read(_ connection: NWConnection, _ buffer: Data) {
    connection.receive(minimumIncompleteLength: 1, maximumLength: 65536) { [weak self] data, _, done, error in
      guard let self else { return }
      var buffer = buffer
      if let data { buffer.append(data) }
      guard let end = buffer.range(of: Data("\r\n\r\n".utf8)) else {
        if error == nil, !done { self.read(connection, buffer) }
        return
      }
      let head = String(decoding: buffer[..<end.lowerBound], as: UTF8.self).components(separatedBy: "\r\n")
      let key = head.dropFirst()
        .map { $0.split(separator: ":", maxSplits: 1).map { $0.trimmingCharacters(in: .whitespaces) } }
        .first { $0.count == 2 && $0[0].lowercased() == "sec-websocket-key" }?[1]
      guard let key else { return connection.cancel() }
      let released = self.state.withLock { state in
        state.upgrades += 1
        if !state.released { state.held.append((connection, key)) }
        return state.released
      }
      if released { self.upgrade(connection, key: key) }
    }
  }

  private func upgrade(_ connection: NWConnection, key: String) {
    let accept = Data(Insecure.SHA1.hash(data: Data((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").utf8)))
      .base64EncodedString()
    let head =
      "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
      + "Sec-WebSocket-Accept: \(accept)\r\n\r\n"
    connection.send(
      content: greeting.reduce(Data(head.utf8)) { $0 + Self.frame(0x1, Data($1.utf8)) }, completion: .idempotent)
    state.withLock { $0.open.append(connection) }
    listen(connection, Data())
  }

  /// Reads the frames the session sends, answering its pings, until it closes the socket or
  /// the connection goes.
  private func listen(_ connection: NWConnection, _ buffer: Data) {
    var buffer = buffer
    while let next = Self.nextFrame(&buffer) {
      let (opcode, payload) = next
      switch opcode {
      case 0x1, 0x2:
        let frame = (try? JSONDecoder().decode([String: RawJSON].self, from: payload)) ?? [:]
        state.withLock { $0.frames.append(frame) }
      case 0x9:
        connection.send(content: Self.frame(0xA, payload), completion: .idempotent)
      case 0x8:
        state.withLock { $0.hungUp = true }
        return connection.cancel()
      default: break
      }
    }
    connection.receive(minimumIncompleteLength: 1, maximumLength: 65536) { [weak self, buffer] data, _, done, error in
      guard let self else { return }
      guard let data, error == nil else {
        self.state.withLock { $0.hungUp = true }
        return connection.cancel()
      }
      self.listen(connection, buffer + data)
      if done {
        self.state.withLock { $0.hungUp = true }
        connection.cancel()
      }
    }
  }

  /// A server's frame, which is never masked.
  private static func frame(_ opcode: UInt8, _ payload: Data) -> Data {
    var data = Data([0x80 | opcode])
    if payload.count < 126 {
      data.append(UInt8(payload.count))
    } else {
      data.append(contentsOf: [126, UInt8(payload.count >> 8), UInt8(payload.count & 0xff)])
    }
    return data + payload
  }

  /// Takes the first whole frame off `buffer`, unmasking it, or nil while it has none.
  private static func nextFrame(_ buffer: inout Data) -> (UInt8, Data)? {
    let bytes = [UInt8](buffer)
    guard bytes.count >= 2 else { return nil }
    var length = Int(bytes[1] & 0x7f)
    var offset = 2
    if length == 126 {
      guard bytes.count >= 4 else { return nil }
      length = Int(bytes[2]) << 8 | Int(bytes[3])
      offset = 4
    } else if length == 127 {
      guard bytes.count >= 10 else { return nil }
      length = bytes[2..<10].reduce(0) { $0 << 8 | Int($1) }
      offset = 10
    }
    let masked = bytes[1] & 0x80 != 0
    guard bytes.count >= offset + (masked ? 4 : 0) + length else { return nil }
    let mask = masked ? Array(bytes[offset..<offset + 4]) : [0, 0, 0, 0]
    if masked { offset += 4 }
    let payload = Data((0..<length).map { bytes[offset + $0] ^ mask[$0 % 4] })
    buffer = Data(bytes[(offset + length)...])
    return (bytes[0] & 0x0f, payload)
  }
}
