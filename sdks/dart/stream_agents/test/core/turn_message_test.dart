import 'package:test/test.dart';
import 'package:stream_agents/src/core/core.dart';

/// Frames exactly as `frameOf` in the router writes them.
AgentEvent frame(String json) => AgentEvent.tryParse(json)!;

void main() {
  group('TurnMessage', () {
    final start = DateTime.utc(2026, 11, 19, 9);
    DateTime ms(int milliseconds) => start.add(Duration(milliseconds: milliseconds));

    Conversation play(List<(int, String)> frames, {Conversation from = const Conversation()}) {
      var conversation = from;
      for (final (at, json) in frames) {
        conversation = conversation.apply(frame(json), at: ms(at));
      }
      return conversation;
    }

    // A question, and a reply that waits on a tool and carries on in a turn of its own.
    final asked = const Conversation().said('Weather in Lisbon?', at: ms(0));
    final toolTurn = [
      (10, '{"type":"responding","turn_id":"t1"}'),
      (1000, '{"type":"reasoning_delta","turn_id":"t1","text":"They want Lisbon\'s weather."}'),
      // As the router sends it: answered as waiting on its tool, which then runs.
      (
        1500,
        '{"pending_work":true,"type":"responded","turn_id":"t1","text":"Let me look.",'
            '"time_to_first_token_ms":90}',
      ),
      (1500, '{"type":"tool_started","tool_call_id":"call_1","tool":"weather","turn_id":"t1"}'),
      (
        1600,
        '{"type":"tool_updated","id":"call_1","turn_id":"t1","tool":"weather",'
            '"text":"Reading the forecast","verbatim":false}',
      ),
      (
        2000,
        '{"type":"tool_ran","tool_call_id":"call_1","turn_id":"t1","tool":"weather","error":""}',
      ),
    ];
    final answerTurn = [
      (2200, '{"type":"responding","turn_id":"t2"}'),
      (2600, '{"type":"reasoning_delta","turn_id":"t2","text":"Sunny, 24."}'),
      (3000, '{"type":"response_delta","turn_id":"t2","text":"Sunny and 24°C."}'),
    ];
    const answered =
        '{"pending_work":false,"type":"responded","turn_id":"t2","text":"Sunny and 24°C.",'
        '"time_to_first_token_ms":90}';

    test('a reply is one message across its tools\' turns, with every step in order', () {
      final conversation = play([...toolTurn, ...answerTurn, (3100, answered)], from: asked);

      final messages = TurnMessage.messages(conversation, userId: 'ada', isConnected: true);

      expect(messages.map((message) => (message.isAgentReply, message.text)), [
        (false, 'Weather in Lisbon?'),
        (true, 'Let me look.\n\nSunny and 24°C.'),
      ]);
      final reply = messages.last;
      expect(reply.id, 't1');
      expect(reply.at, ms(10));
      expect(reply.isGenerating, isFalse);
      // Records compare their maps by identity, so each step is compared as a list.
      expect(reply.steps.map((step) => [step.type, step.payload]), [
        [
          'ai_reasoning',
          {'id': 'r1', 'status': 'completed', 'duration_ms': 1490},
        ],
        [
          'ai_tool_call',
          {
            'id': 'call_1',
            'name': 'weather',
            'status': 'completed',
            'summary': 'Reading the forecast',
            'duration_ms': 500,
          },
        ],
        // The second turn's round is its own r1, and the reply's r2.
        [
          'ai_reasoning',
          {'id': 'r2', 'status': 'completed', 'duration_ms': 800},
        ],
      ]);
      expect(reply.reasoning, {'r1': "They want Lisbon's weather.", 'r2': 'Sunny, 24.'});
      expect(reply.customData, isEmpty);
    });

    test('the latest reply is being written while the socket carries it', () {
      final writing = play([...toolTurn, ...answerTurn], from: asked);

      expect(
        TurnMessage.messages(writing, userId: 'ada', isConnected: true).last.isGenerating,
        isTrue,
      );
    });

    test('a reply whose tools are done is still being written until its next turn is', () {
      final betweenTurns = play(toolTurn, from: asked);

      expect(
        TurnMessage.messages(betweenTurns, userId: 'ada', isConnected: true).last.isGenerating,
        isTrue,
      );
    });

    test('a reply is not being written once the socket is gone', () {
      final writing = play([...toolTurn, ...answerTurn], from: asked);

      expect(
        TurnMessage.messages(writing, userId: 'ada', isConnected: false).last.isGenerating,
        isFalse,
      );
    });

    test('a reply asked over mid-thought is left as it was, no longer being written', () {
      final thinking = play([...toolTurn, ...answerTurn.take(2)], from: asked);
      final askedAgain = thinking.said('Never mind.', at: ms(4000));

      final messages = TurnMessage.messages(askedAgain, userId: 'ada', isConnected: true);

      expect(messages.map((message) => message.isGenerating), [false, false, false]);
      expect(
        messages[1].steps.last.payload['status'],
        'streaming',
        reason: 'the round it was left on',
      );
    });

    test('a message built again unchanged is equal, and one whose step or card moved is not', () {
      final running = play([
        ...toolTurn.take(4),
        (
          1700,
          '{"type":"ui_completed","turn_id":"t1","format":"openui","mode":"replace",'
              r'"content":"root = Card([])"}',
        ),
      ], from: asked);
      TurnMessage reply(Conversation conversation) =>
          TurnMessage.messages(conversation, userId: 'ada', isConnected: true).last;

      // Built twice from the same conversation, with maps that are not the same objects.
      expect(reply(running), reply(running));
      expect(reply(running).hashCode, reply(running).hashCode);

      final ran = play([
        (
          2000,
          '{"type":"tool_ran","tool_call_id":"call_1","turn_id":"t1","tool":"weather","error":""}',
        ),
      ], from: running);
      expect(reply(ran), isNot(reply(running)), reason: 'the call completed');

      final recarded = play([
        (
          1800,
          '{"type":"ui_completed","turn_id":"t1","format":"openui","mode":"replace",'
              r'"content":"root = Card([Text(\"Sunny\")])"}',
        ),
      ], from: running);
      expect(reply(recarded), isNot(reply(running)), reason: 'the card changed');
    });

    test('a reply with UI carries its latest card as a kept one does', () {
      final conversation = play([
        (10, '{"type":"responding","turn_id":"t1"}'),
        (
          20,
          '{"type":"ui_completed","turn_id":"t1","format":"openui","mode":"replace",'
              r'"content":"root = Card([])"}',
        ),
      ], from: asked);

      final reply = TurnMessage.messages(conversation, userId: 'ada', isConnected: true).last;

      expect(reply.customData, {
        'ai_ui': {'v': 1, 'format': 'openui', 'status': 'complete', 'content': 'root = Card([])'},
      });
    });
  });
}
