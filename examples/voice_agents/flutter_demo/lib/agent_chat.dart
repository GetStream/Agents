import 'dart:async';
import 'dart:convert';

import 'package:flutter/foundation.dart';
import 'package:stream_chat/stream_chat.dart' as stream;
import 'package:stream_chat_flutter_ai/stream_chat_flutter_ai.dart';
import 'package:stream_agents/stream_agents.dart';

/// Connects Stream's [AIConversation] to a written conversation's session and channel.
///
/// The router writes everything here. Asking goes through the session, which records the
/// question in the channel and answers it there: the reply's text, and its steps (each round
/// of reasoning and each tool call) as attachments. The session's socket is what runs this
/// phone's tools and carries the person's answers to the ones that ask first.
///
/// An incognito session keeps no channel: its messages are its socket's `turnMessages`.
class AgentChat with AIConversationTransport<AgentMessage> {
  AgentChat._(this.session, stream.Channel? channel)
    : _channel = channel,
      _kept = channel != null;

  /// Opens the session's channel and starts following it, or follows its socket alone when it
  /// keeps no channel.
  ///
  /// The session is closed if its channel cannot be opened, so it does not run on with
  /// nobody to show it.
  static Future<AgentChat> open(AgentSession session) async {
    if (session.session.conversationId.isEmpty) {
      return AgentChat.unkept(session);
    }
    stream.Channel? channel;
    try {
      channel = await session.chat();
      final watched = await channel.watch(
        messagesPagination: const stream.PaginationParams(limit: _pageSize),
      );
      return AgentChat._(session, channel)
        .._hasOlderMessages = (watched.messages?.length ?? 0) >= _pageSize
        .._follow(channel);
    } on Object {
      channel?.dispose();
      await session.close();
      rethrow;
    }
  }

  /// Follows [channel], already open, for [session].
  @visibleForTesting
  factory AgentChat.following(
    AgentSession session,
    stream.Channel channel, {
    bool hasOlderMessages = false,
  }) => AgentChat._(session, channel)
    .._hasOlderMessages = hasOlderMessages
    .._follow(channel);

  /// Follows [session], which keeps no channel, from its socket alone.
  factory AgentChat.unkept(AgentSession session) =>
      AgentChat._(session, null).._follow(null);

  static const _pageSize = 30;

  /// What a reply that failed says, in place of the provider's raw error.
  static const unanswered = "The agent couldn't answer.";

  final AgentSession session;
  stream.Channel? _channel;

  final bool _kept;

  /// Why the session's socket stopped, or null: tools and approvals stop until a new
  /// conversation is opened.
  ValueListenable<String?> get sessionFailure => _sessionFailure;
  final _sessionFailure = ValueNotifier<String?>(null);

  /// Each reply's thinking in full, from its live updates. A stored step only keeps its
  /// opening.
  final reasoning = LiveReasoning();

  AIConversationFeed<AgentMessage>? _feed;
  final _subscriptions = <StreamSubscription<Object?>>[];
  final _shown = Expando<AgentMessage>();
  var _hasOlderMessages = false;
  AIAgentActivity? _activity;
  var _unanswered = false;
  var _closed = false;

  /// The messages as Stream's AI components show them.
  List<AgentMessage> get listMessages => _kept
      ? listed(_channel?.state?.messages ?? const [], reasoning, shown: _shown)
      : [
          for (final message in session.turnMessages)
            if (!message.isUnwrittenReply) AgentMessage.unkept(message),
        ];

  /// The [messages] as Stream's AI components show them, oldest first, without those deleted
  /// and without the replies that ended with nothing written, reusing those in [shown].
  static List<AgentMessage> listed(
    List<stream.Message> messages,
    LiveReasoning reasoning, {
    Expando<AgentMessage>? shown,
  }) {
    final known = shown ?? Expando<AgentMessage>();
    return [
      for (final message in messages)
        if (message.deletedAt == null && !message.isUnwrittenReply)
          known[message] ??= AgentMessage(message, reasoning),
    ];
  }

  @override
  void connect(AIConversationFeed<AgentMessage> feed) {
    _feed = feed;
    _publish();
    _tell(_unanswered ? _failedActivity : activityOf(session.state));
  }

  void _follow(stream.Channel? channel) {
    if (channel != null) {
      _subscriptions
        ..add(channel.state!.messagesStream.listen((_) => _publish()))
        // The client's events rather than the channel's list: the list can fold several live
        // updates into one, and each update carries its own window of the reply's thinking.
        ..add(
          channel.client.on(stream.EventType.messageUpdated).listen((event) {
            final message = event.message;
            if (event.cid == channel.cid &&
                message != null &&
                _readReasoning(message)) {
              _publish();
            }
          }),
        );
    } else {
      // A call waiting for the person asks on its step of the reply.
      _subscriptions.add(session.approvals.stream.listen((_) => _publish()));
    }
    _subscriptions
      ..add(session.conversation.stream.listen((_) => _sessionChanged()))
      ..add(session.connection.stream.listen((_) => _sessionChanged()))
      // A reply that fails ends with nothing written in the channel: only the socket says so.
      ..add(session.events().listen(_report));
    _sessionChanged();
  }

  bool _readReasoning(stream.Message message) {
    if (message.extraData['reasoning'] case {'id': final String step}) {
      final key = (message: message.id, step: step);
      final before = reasoning[key];
      reasoning.read(message);
      return !identical(reasoning[key], before);
    }
    return false;
  }

  void _publish() {
    final feed = _feed;
    if (feed == null || _closed) return;
    feed
      ..updateMessages(listMessages)
      ..updateHasOlderMessages(_hasOlderMessages);
  }

  void _sessionChanged() {
    _sessionFailure.value = session.failure?.toString();
    if (!_unanswered) _tell(activityOf(session.state));
    // With no channel, the socket is where the conversation is.
    if (!_kept) _publish();
  }

  void _report(AgentEvent event) {
    if (failedReply(event) case final failed?) {
      _unanswered = true;
      _activity = failed;
      _feed?.updateActivity(failed);
    } else if (event.kind == AgentEventKind.responding) {
      _unanswered = false;
      _sessionChanged();
    }
  }

  void _tell(AIAgentActivity? activity) {
    if (activity == _activity) return;
    _activity = activity;
    _feed?.updateActivity(activity);
  }

  static const _failedActivity = AIAgentActivity(
    state: AIAgentActivityState.error,
    text: unanswered,
  );

  /// What the agent is doing in [state]: thinking while it answers, or working on its skills.
  static AIAgentActivity? activityOf(ConversationState state) =>
      switch (state) {
        Responding() => const AIAgentActivity(
          state: AIAgentActivityState.thinking,
        ),
        Working(:final skills) when skills.isEmpty => const AIAgentActivity(
          state: AIAgentActivityState.thinking,
        ),
        Working(:final skills) => AIAgentActivity(
          state: AIAgentActivityState.checkingExternalSources,
          text: describeWork(skills),
        ),
        _ => null,
      };

  /// What the agent was doing in [event] when it could not answer, or null otherwise.
  static AIAgentActivity? failedReply(AgentEvent event) =>
      event.kind == AgentEventKind.error && event['context'] == 'llm'
      ? _failedActivity
      : null;

  @override
  Future<void> send(AIConversationRequest<AgentMessage> request) async {
    if (_closed) throw const AIConversationCancelledException();
    await session.responses.create(request.text);
  }

  @override
  Future<void> stop() async {
    try {
      session.interrupt();
    } on SocketClosedException {
      // Nothing is answering: the reply in flight stopped with the socket.
    }
  }

  /// Answers a tool call's question from the card that shows it.
  ///
  /// A call the router holds for the person, such as a connector's under `require_approval`,
  /// never reaches the session's socket, and nor does one the socket was not open for. So the
  /// answer carries what the reply shows of the call, and the request and turn the reply was
  /// written for. Only a call whose step still waits for the person is answered: one answered
  /// elsewhere is not run here.
  ///
  /// Throws a [StateError] when no loaded reply shows the call waiting and the session holds
  /// none by its id, which would leave the step saying it was sent while nothing runs.
  void decide(AIToolCallPart call, {required bool allowed}) {
    final shown = shownCall(_channel?.state?.messages ?? const [], call.id);
    if (shown != null) {
      session.decideShown(shown, allowed: allowed);
      return;
    }
    if (!session.approvals.value.any((request) => request.id == call.id)) {
      throw StateError('This question is no longer waiting for an answer.');
    }
    session.decide(call.id, allowed: allowed);
  }

  /// The call [id] as the latest of [messages] that shows it waiting for the person has it:
  /// its `ai_tool_call` step, and the request and turn the router wrote the reply for
  /// (`support_runtime`). Null when no message shows it waiting, such as once it was answered.
  static ShownToolCall? shownCall(List<stream.Message> messages, String id) {
    for (final message in messages.reversed) {
      for (final attachment in message.attachments) {
        // The step's own id, as [AgentReply.steps] reads it.
        final step = {'id': attachment.id, ...attachment.extraData};
        if (attachment.rawType != 'ai_tool_call' ||
            step['id'] != id ||
            step['status'] != 'awaiting_approval') {
          continue;
        }
        final runtime = message.extraData['support_runtime'];
        return ShownToolCall(
          id: id,
          name: step['name'] is String ? step['name'] as String : '',
          // Kept as JSON on the step, and left off one too long to show.
          arguments: switch (step['arguments']) {
            null => null,
            final String json => json,
            final Object value => jsonEncode(value),
          },
          requestId: switch (runtime) {
            {'command_id': final String id} => id,
            _ => '',
          },
          turnId: switch (runtime) {
            {'turn_id': final String id} => id,
            _ => '',
          },
        );
      }
    }
    return null;
  }

  @override
  Future<void> loadOlderMessages() async {
    final channel = _channel;
    final first = channel?.state?.messages.firstOrNull;
    if (channel == null || first == null) return;
    final state = await channel.query(
      messagesPagination: stream.PaginationParams(
        lessThan: first.id,
        limit: _pageSize,
      ),
    );
    if (_closed) return;
    _hasOlderMessages = (state.messages?.length ?? 0) >= _pageSize;
    _publish();
  }

  /// Stops following the channel and closes the session.
  Future<void> close() async {
    _closed = true;
    for (final subscription in _subscriptions) {
      unawaited(subscription.cancel());
    }
    _channel?.dispose();
    _channel = null;
    await session.close();
  }

  /// Releases what the chat holds, once [close] has completed.
  void dispose() {
    _sessionFailure.dispose();
  }
}

/// What the agent says it is doing while its [skills] work, or null when it names none.
String? describeWork(List<String> skills) {
  if (skills.isEmpty) return null;
  return 'Working on ${skills.map((skill) => skill.replaceAll('_', ' ')).join(' and ')}';
}

extension AgentReply on stream.Message {
  /// Whether the agent wrote this message, which Stream's AI components render as a reply.
  bool get isAgentReply => extraData['ai_generated'] == true;

  /// Whether the agent is still writing it.
  bool get isGenerating => extraData['generating'] == true;

  /// Whether this is a reply the agent ended without writing anything: no text and no steps.
  bool get isUnwrittenReply =>
      isAgentReply &&
      !isGenerating &&
      (text ?? '').isEmpty &&
      attachments.isEmpty;

  /// The reply's steps, in the order they happened.
  List<AIMessagePart> get steps => AIMessagePart.parseAll([
    for (final attachment in attachments)
      if (attachment.rawType case final type?)
        (type: type, payload: {'id': attachment.id, ...attachment.extraData}),
  ]);
}

extension UnkeptReply on TurnMessage {
  /// Whether this is a reply the agent ended without writing anything, as
  /// [AgentReply.isUnwrittenReply] is for a kept one.
  bool get isUnwrittenReply =>
      isAgentReply && !isGenerating && text.isEmpty && steps.isEmpty;
}

/// A message of the conversation as Stream's AI components show it: the channel's, or an
/// incognito session's.
class AgentMessage with AIMessage {
  AgentMessage(stream.Message this.message, LiveReasoning this._reasoning)
    : _turn = null,
      id = message.id,
      role = message.isAgentReply
          ? AIMessageRole.assistant
          : AIMessageRole.user,
      text = message.text ?? '',
      isGenerating = message.isGenerating,
      parts = message.steps;

  /// An incognito session's message, from its socket.
  AgentMessage.unkept(TurnMessage this._turn)
    : message = null,
      _reasoning = null,
      id = _turn.id,
      role = _turn.isAgentReply ? AIMessageRole.assistant : AIMessageRole.user,
      text = _turn.text,
      isGenerating = _turn.isGenerating,
      parts = AIMessagePart.parseAll(_turn.steps);

  /// The channel's message, or null for one built from the socket.
  final stream.Message? message;
  final TurnMessage? _turn;
  final LiveReasoning? _reasoning;

  @override
  final String id;

  @override
  final AIMessageRole role;

  @override
  final String text;

  @override
  final bool isGenerating;

  @override
  final List<AIMessagePart> parts;

  @override
  late final List<AIMessageAttachment> attachments = [
    for (final attachment
        in message?.attachments ?? const <stream.Attachment>[])
      if (attachment.type == 'image' && attachment.imageUrl != null)
        AIMessageAttachment.image(
          Uri.parse(attachment.imageUrl!),
          name: attachment.title,
        )
      else if (attachment.type == 'file' && attachment.assetUrl != null)
        AIMessageAttachment.file(
          Uri.parse(attachment.assetUrl!),
          name: attachment.title,
          size: attachment.fileSize,
          mimeType: attachment.mimeType,
        ),
  ];

  /// A step keeps only the opening of its thinking; the whole of it came with the live
  /// updates, while the reply was being written. An incognito one holds all of it.
  @override
  String? reasoningText(AIReasoningPart step) => switch (_turn) {
    final turn? => turn.reasoning[step.id],
    null => _reasoning?[(message: id, step: step.id)],
  };
}
