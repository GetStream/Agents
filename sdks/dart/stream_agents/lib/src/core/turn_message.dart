import 'dart:convert';

import 'agent_session.dart';
import 'conversation.dart';

/// A message of a conversation kept in no channel, an incognito session's, in the shape a kept
/// one has in Stream Chat, so an app shows both alike with Stream's AI components.
///
/// Each of the person's turns is a message, and the agent's turns between two of them are one
/// reply, as the router writes a reply whose tools it waited on: their text, the latest UI, and
/// each round of thinking and each tool call as a step, generating until the agent is done with
/// it. A call waiting in the session's `approvals` asks its user on its step, as a kept reply's
/// does.
final class TurnMessage {
  TurnMessage({
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

  /// The reply's steps, in order, as a kept reply's attachments hold them: an `ai_reasoning`
  /// for each round of thinking, numbered from `r1` across the reply, and an `ai_tool_call` for
  /// each call. Stream Chat's `AIMessagePart.parseAll` reads them as they are.
  final List<({String type, Map<String, Object?> payload})> steps;

  /// The whole of each round of thinking, by its step's id.
  final Map<String, String> reasoning;

  /// The reply's custom data as a kept one holds it: its UI under `ai_ui`, which
  /// `AIUIDocument.fromCustomData` reads, or nothing.
  final Map<String, Object?> customData;

  /// The messages of [conversation], oldest first.
  ///
  /// [approvals] are the calls waiting for the person, each of which asks on its step. [userId]
  /// is the person the session is with, whom a waiting call asks: the agents' own user, which an
  /// `AIToolApprover` matches against `target_user_id`, not the session's `userId`, which is the
  /// name the agent joined the call as. The latest reply is written only while [isConnected],
  /// whether the session's socket still carries the conversation.
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
        // Only the latest reply can still be written: one the person asked over is as it was
        // left.
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
      return TurnMessage(id: first.id, isAgentReply: false, text: first.text, at: first.at);
    }
    final steps = <({String type, Map<String, Object?> payload})>[];
    final reasoning = <String, String>{};
    for (final step in turns.expand((turn) => turn.steps)) {
      switch (step) {
        case ReasoningStep():
          // Numbered across the reply, which a tool's result carries on in a turn of its own.
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
          final waiting = step.status == ToolCallStatus.running
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
    return TurnMessage(
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

  /// The question a waiting call asks, as its step in Stream Chat holds it.
  static Map<String, Object?> _approval(ToolApprovalRequest waiting) => {
    'title': waiting.approval.title,
    if (waiting.approval.message case final message? when message.isNotEmpty) 'message': message,
    if (waiting.reason.isNotEmpty) 'reason': waiting.reason,
    if (waiting.approval.allowTitle case final title? when title.isNotEmpty) 'allow_title': title,
    if (waiting.approval.declineTitle case final title? when title.isNotEmpty)
      'decline_title': title,
  };

  /// What it shows besides its id, text and time, compared as one value: the steps' maps are
  /// written in a fixed order, so a step that did not change encodes the same.
  late final String _shown = jsonEncode([
    for (final step in steps) [step.type, step.payload],
    reasoning,
    customData,
  ]);

  /// Equal when it shows the same, so a view can keep a message that did not change.
  @override
  bool operator ==(Object other) =>
      other is TurnMessage &&
      other.id == id &&
      other.isAgentReply == isAgentReply &&
      other.text == text &&
      other.at == at &&
      other.isGenerating == isGenerating &&
      other._shown == _shown;

  @override
  int get hashCode => Object.hash(id, isAgentReply, text, at, isGenerating, _shown);

  @override
  String toString() => 'TurnMessage($id, ${isAgentReply ? 'agent' : 'person'}: $text)';
}

/// The conversation as messages, for a session kept in no channel.
extension TurnMessages on AgentSession {
  /// The conversation as messages, for a session kept in no channel, an incognito one, to show
  /// with Stream's AI components; a kept session's messages are its channel's (`chat()`). Build
  /// it again whenever [AgentSession.conversation], [AgentSession.approvals] or
  /// [AgentSession.connection] changes. A waiting call asks the agents' own user.
  List<TurnMessage> get turnMessages => TurnMessage.messages(
    conversation.value,
    approvals: approvals.value,
    userId: backend.userId,
    isConnected: isConnected,
  );
}
