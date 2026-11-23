import 'agent_session.dart';
import 'conversation.dart';

/// A message of an incognito session, which keeps no channel, in the shape a kept one has in
/// Stream Chat, so Stream's AI components show both alike.
///
/// The agent's turns between two of the person's are one reply, as the router writes a reply
/// whose tools it waited on.
final class TurnMessage {
  TurnMessage._({
    required this.id,
    required this.isAgentReply,
    required this.text,
    required this.at,
    this.isGenerating = false,
    this.steps = const [],
    this.reasoning = const {},
    this.customData = const {},
  });

  /// Its first turn's id.
  final String id;

  /// Whether the agent wrote it, rather than the person.
  final bool isAgentReply;
  final String text;

  /// When its first turn began.
  final DateTime at;

  /// Whether the agent is still writing it.
  final bool isGenerating;

  /// The reply's `ai_reasoning` and `ai_tool_call` steps, in order, as `AIMessagePart.parseAll`
  /// reads them. Rounds are numbered from `r1` across the reply.
  final List<({String type, Map<String, Object?> payload})> steps;

  /// The whole of each round of thinking, by its step's id.
  final Map<String, String> reasoning;

  /// The reply's UI under `ai_ui`, as a kept one holds it, or nothing.
  final Map<String, Object?> customData;

  /// The messages of [conversation], oldest first.
  ///
  /// A call in [approvals] asks [userId] on its step: the person, whom `AIToolApprover` matches,
  /// not `Session.userId`, the agent's name on the call. Only the latest reply is generating,
  /// and only while [isConnected].
  static List<TurnMessage> messages(
    Conversation conversation, {
    List<ToolApprovalRequest> approvals = const [],
    required String userId,
    required bool isConnected,
  }) {
    final groups = <List<Turn>>[];
    for (final turn in conversation.turns) {
      if (turn.speaker.isAgent && (groups.lastOrNull?.first.speaker.isAgent ?? false)) {
        groups.last.add(turn);
      } else {
        groups.add([turn]);
      }
    }
    return [
      for (final (index, turns) in groups.indexed)
        _of(
          turns,
          isGenerating: index == groups.length - 1 && isConnected && !turns.last.isDone,
          approvals: approvals,
          userId: userId,
        ),
    ];
  }

  static TurnMessage _of(
    List<Turn> turns, {
    required bool isGenerating,
    required List<ToolApprovalRequest> approvals,
    required String userId,
  }) {
    final first = turns.first;
    if (!first.speaker.isAgent) {
      return TurnMessage._(id: first.id, isAgentReply: false, text: first.text, at: first.at);
    }
    final steps = <({String type, Map<String, Object?> payload})>[];
    final reasoning = <String, String>{};
    for (final step in turns.expand((turn) => turn.steps)) {
      switch (step) {
        case ReasoningStep():
          final id = 'r${reasoning.length + 1}';
          reasoning[id] = step.text;
          steps.add((
            type: 'ai_reasoning',
            payload: {
              'id': id,
              'status': step.isStreaming ? 'streaming' : 'completed',
              'duration_ms': ?step.duration?.inMilliseconds,
            },
          ));
        case ToolCallStep():
          final waiting = step.status == ToolCallStepStatus.running
              ? approvals.where((request) => request.id == step.id).firstOrNull
              : null;
          steps.add((
            type: 'ai_tool_call',
            payload: {
              'id': step.id,
              'name': step.name,
              'status': waiting != null ? 'awaiting_approval' : step.status.wire,
              if (step.summary.isNotEmpty) 'summary': step.summary,
              'duration_ms': ?step.duration?.inMilliseconds,
              if (waiting != null) ...{'target_user_id': userId, 'approval': _approval(waiting)},
            },
          ));
      }
    }
    final ui = turns.lastWhere((turn) => turn.ui != null, orElse: () => first).ui;
    return TurnMessage._(
      id: first.id,
      isAgentReply: true,
      text: turns.map((turn) => turn.text).where((text) => text.isNotEmpty).join('\n\n'),
      at: first.at,
      isGenerating: isGenerating,
      steps: steps,
      reasoning: reasoning,
      customData: {if (ui != null) 'ai_ui': ui.toWireJson()},
    );
  }

  static Map<String, Object?> _approval(ToolApprovalRequest waiting) => {
    'title': waiting.approval.title,
    if (waiting.approval.message case final message? when message.isNotEmpty) 'message': message,
    if (waiting.reason.isNotEmpty) 'reason': waiting.reason,
    if (waiting.approval.allowTitle case final title? when title.isNotEmpty) 'allow_title': title,
    if (waiting.approval.declineTitle case final title? when title.isNotEmpty)
      'decline_title': title,
  };

  /// Equal when it shows the same, its maps compared by value.
  @override
  bool operator ==(Object other) =>
      other is TurnMessage &&
      other.id == id &&
      other.isAgentReply == isAgentReply &&
      other.text == text &&
      other.at == at &&
      other.isGenerating == isGenerating &&
      _sameValue(other.reasoning, reasoning) &&
      _sameValue(other.customData, customData) &&
      other.steps.length == steps.length &&
      [
        for (var i = 0; i < steps.length; i++)
          other.steps[i].type == steps[i].type &&
              _sameValue(other.steps[i].payload, steps[i].payload),
      ].every((same) => same);

  @override
  int get hashCode => Object.hash(id, isAgentReply, text, at, isGenerating, steps.length);

  @override
  String toString() => 'TurnMessage($id, ${isAgentReply ? 'agent' : 'person'}: $text)';
}

/// An incognito session's conversation as messages.
extension TurnMessages on AgentSession {
  /// The conversation as messages, for a session with no channel.
  ///
  /// Read it again when [AgentSession.conversation], [AgentSession.approvals] or
  /// [AgentSession.connection] changes.
  List<TurnMessage> get turnMessages => TurnMessage.messages(
    conversation.value,
    approvals: approvals.value,
    userId: backend.userId,
    isConnected: isConnected,
  );
}

bool _sameValue(Object? a, Object? b) => switch ((a, b)) {
  (final Map<Object?, Object?> a, final Map<Object?, Object?> b) =>
    a.length == b.length &&
        a.entries.every(
          (entry) => b.containsKey(entry.key) && _sameValue(entry.value, b[entry.key]),
        ),
  (final List<Object?> a, final List<Object?> b) =>
    a.length == b.length &&
        [for (var i = 0; i < a.length; i++) _sameValue(a[i], b[i])].every((same) => same),
  _ => a == b,
};
