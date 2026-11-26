import Combine
import Foundation
import StreamVideo

private let kind = "video"

extension StreamAgents {
    /// Hands over the app's own Stream Video client, so every call is joined on it.
    ///
    /// An app with a `StreamVideo` of its own must hand it over: building another makes that
    /// one the process's current instance, which Stream's call views and CallKit read. It is
    /// never disconnected here.
    public func use(_ video: StreamVideo) {
        backend.give(kind, video)
    }
}

/// A spoken conversation: the agent on a call, and this device on the same call.
///
/// Two things happen, in this order:
///
/// 1. The router starts a session with voice on, which puts the agent on the session's own
///    call, `agent:<session id>`.
/// 2. Stream's Video SDK joins that call as the user `setUser` named, and audio starts
///    flowing.
///
/// The transcript comes over the session socket rather than out of the call, so what is said
/// is readable even before anybody is listening to it. That is `session`, which is the same
/// `AgentSession` a text conversation uses. A change to `session` is a change to this, so a
/// view observing the voice session follows its conversation too.
@MainActor
public final class VoiceSession: ObservableObject {
    /// The conversation: the transcript, and what the agent is doing.
    public let session: AgentSession

    /// The Stream call this device is on, once it has joined.
    @Published public private(set) var call: Call?

    /// Whether this device's microphone is on.
    @Published public private(set) var isMuted = false

    /// Whether this device's camera is on.
    @Published public private(set) var isCameraEnabled = false

    /// Why joining failed, or nil. Set rather than thrown because joining happens in a
    /// `task`, where there is nobody to throw to.
    @Published public private(set) var failure: (any Error)?

    /// Passes the conversation's changes on as this one's.
    private var following: AnyCancellable?

    private let agents: StreamAgents
    /// True when this device called `start`, false when it attached to a session something
    /// else started. Leaving closes only a session this device owns.
    private let createdLocally: Bool
    /// What the camera showed lately, while it is on, for the agent to look at.
    private let frames = CameraFrames()
    /// Whether the call is being joined, is joined or was left, which says what a join, a
    /// leave and a camera change arriving at each other's awaits may do.
    private var phase = CallPhase()

    /// Starts an agent on the new session's own call, `agent:<session id>`, follows it, and
    /// prepares to join it.
    public static func start(
        agents: StreamAgents,
        agent: String? = nil,
        tools: [AgentTool] = []
    ) async throws -> VoiceSession {
        var options = SessionOptions(agent: agent)
        options.tools = tools
        options.startVoice = true
        let session = try await agents.sessions.create(options)
        return VoiceSession(agents: agents, session: session, createdLocally: true)
    }

    /// Joins a call an agent is already on, without creating a session.
    ///
    /// `sessionID` is the id the router holds the session by, which is what the events socket
    /// addresses. A session this device did not open is not found, since reading one is
    /// reading a conversation.
    public static func attach(
        agents: StreamAgents,
        sessionID: String,
        tools: [AgentTool] = []
    ) async throws -> VoiceSession {
        let session = try await agents.attach(sessionID: sessionID, tools: tools)
        return VoiceSession(agents: agents, session: session, createdLocally: false)
    }

    init(agents: StreamAgents, session: AgentSession, createdLocally: Bool) {
        self.agents = agents
        self.session = session
        self.createdLocally = createdLocally
        following = session.objectWillChange.sink { [weak self] _ in self?.objectWillChange.send() }
    }

    /// Joins the call from this device.
    ///
    /// The agent is already there: it joined when the session was created. This is the other
    /// half of the conversation arriving. The microphone is on; the camera is off unless
    /// `camera` is true.
    ///
    /// A camera starts on the back lens, since what an agent is being shown is whatever the
    /// caller is pointing at. Both are join settings rather than changed afterwards, so no
    /// front-facing frame is ever published.
    ///
    /// It joins on the `StreamVideo` handed over with `use`, or else on one built for the
    /// agents' key and user, which every voice session then shares.
    ///
    /// A join while one is under way, or after the call is joined, does nothing; so does one
    /// after `leave` or `end`, which are for good. Leaving while it is under way wins: whatever
    /// it joined is left again. A join that failed or was cancelled leaves what it joined too,
    /// and can be tried again.
    public func join(camera: Bool = false) async {
        guard let generation = phase.join() else { return }
        // The call from when Stream starts joining it. Unless it became `call`, because joining
        // failed, was cancelled or lost to a leave, it is left again rather than kept open with
        // its microphone on and nobody to hang it up.
        var joining: Call?
        defer {
            if let joining, joining !== call {
                stopLooking(joining)
                joining.leave()
            }
        }
        do {
            let backend = agents.backend
            let video = try await backend.shared(
                kind, open: { build(backend, $0) }, disconnect: { await $0.disconnect() })
            if let user = backend.user, video.user.id != user.id {
                throw AgentsError.configuration(
                    "the video client is connected as \(video.user.id) but setUser named "
                        + "\(user.id): connect it as the same user, or the call is somebody else's")
            }
            guard phase.isCurrent(generation) else { return }

            let call = video.call(callType: session.session.callType, callId: session.session.callID)
            joining = call
            // Created rather than only joined: which of the two arrives first is a race, and
            // the agent's own join creates it the same way.
            try await call.join(
                create: true,
                callSettings: CallSettings(videoOn: camera, cameraPosition: .back))
            guard phase.isCurrent(generation) else { return }
            if camera {
                try await call.camera.enable()
                look(through: call)
            } else {
                try await call.camera.disable()
            }
            try await call.microphone.enable()
            // Remote audio plays through the audio session on its own once joined, but out of
            // the earpiece. An agent you talk to hands-free wants the speaker.
            try await call.speaker.enableSpeakerPhone()
            // Stream's device calls above do not throw when the task is cancelled, so a join
            // cancelled since `call.join` is caught here, before it is published.
            try Task.checkCancellation()
            guard phase.joined(generation) else { return }
            self.call = call
            isCameraEnabled = camera
            failure = nil
        } catch {
            if phase.failed(generation), !(error is CancellationError) {
                failure = error
            }
        }
    }

    /// Turns this device's microphone on or off. The agent stays on the call either way.
    public func setMuted(_ muted: Bool) async {
        guard let call else { return }
        let generation = phase.generation
        do {
            try await muted ? call.microphone.disable() : call.microphone.enable()
            // Left meanwhile: there is no call to be muted on.
            guard phase.isCurrent(generation) else { return }
            isMuted = muted
        } catch {
            if phase.isCurrent(generation) {
                failure = error
            }
        }
    }

    /// Turns this device's camera on or off. Re-enabling keeps the back lens.
    ///
    /// While it is on, the agent can look through it: its vision skill asks for what the
    /// camera showed when a question was put, and `session` answers with those frames.
    public func setCameraEnabled(_ enabled: Bool) async {
        guard let call else { return }
        let generation = phase.generation
        do {
            if enabled {
                if call.camera.direction != .back {
                    try await call.camera.flip()
                }
                try await call.camera.enable()
                // Left while the camera was turning on: it goes off again, and nothing looks
                // through a call this device is no longer on.
                guard phase.isCurrent(generation) else {
                    try? await call.camera.disable()
                    return
                }
                look(through: call)
            } else {
                try await call.camera.disable()
                stopLooking(call)
            }
            isCameraEnabled = enabled
        } catch {
            if phase.isCurrent(generation) {
                failure = error
            }
        }
    }

    /// Leaves this device's RTC call. Closes the router session only if this
    /// device started it; an attached device navigating away leaves it running.
    /// After leaving, `join()` does nothing: another call needs another `VoiceSession`.
    public func leave() async {
        await leaveCall(closeSession: createdLocally)
    }

    /// Hangs up: leaves the RTC call and ends the agent session, whoever started it.
    /// After hanging up, `join()` does nothing: another call needs another `VoiceSession`.
    public func end() async {
        await leaveCall(closeSession: true)
    }

    /// Samples the call's camera for the agent, and lets `session` answer with what it showed.
    private func look(through call: Call) {
        call.setVideoFilter(frames.filter)
        session.cameraFrames = frames
    }

    private func stopLooking(_ call: Call?) {
        call?.setVideoFilter(nil)
        frames.clear()
        session.cameraFrames = nil
    }

    private func leaveCall(closeSession: Bool) async {
        phase.leave()
        stopLooking(call)
        call?.leave()
        call = nil
        isCameraEnabled = false
        if closeSession {
            await session.close()
        }
        phase.left()
    }
}

/// A client for the agents' user. The token provider is what the SDK calls when the token
/// expires, an hour in, so asking the agents again is what keeps a long call from dropping.
private func build(_ backend: Backend, _ credentials: StreamCredentials) -> StreamVideo {
    StreamVideo(
        apiKey: credentials.apiKey,
        user: credentials.user,
        token: UserToken(rawValue: credentials.token),
        tokenProvider: { result in
            Task {
                do {
                    result(.success(UserToken(rawValue: try await backend.streamCredentials(refresh: true).token)))
                } catch {
                    result(.failure(error))
                }
            }
        })
}
