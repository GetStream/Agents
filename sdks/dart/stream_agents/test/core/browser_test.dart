@TestOn('browser')
library;

import 'package:stream_agents/src/core/backend.dart';
import 'package:test/test.dart';

void main() {
  test('a page sends no User-Agent, which the CORS preflight would refuse', () async {
    final backend = Backend(url: Uri.parse('https://router.test'), customerId: 'acme');

    final headers = await backend.headers();

    expect(headers.keys.map((name) => name.toLowerCase()), isNot(contains('user-agent')));
    expect(headers['X-Stream-Client'], 'stream-agents-dart-v$sdkVersion');
  });
}
