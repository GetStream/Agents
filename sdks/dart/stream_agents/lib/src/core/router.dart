import 'generated/api.dart' as api;

/// What a search found.
final class SearchAnswer {
  const SearchAnswer({
    required this.provider,
    required this.model,
    this.answer = '',
    this.results = const [],
  });

  final String provider;
  final String model;

  /// The provider's own summary, where it offers one: a sentence to say rather than a page.
  final String answer;

  /// The sources behind it, most relevant first.
  final List<SearchHit> results;
}

/// One source a search found.
final class SearchHit {
  const SearchHit({required this.url, this.title = '', this.text = '', this.score = 0});

  final String url;
  final String title;

  /// The relevant extract, which is what a model reads.
  final String text;
  final double score;
}

/// Looking something up, configured once.
///
/// Search is the one routed modality a device may reach: a question and its answer are one
/// round trip, and the answer is for whoever asked. Transcription, a voice and a model on
/// their own run over a socket the router refuses to a device.
final class SearchRouter {
  SearchRouter(this._operations, {this.config = '', this.tags = const {}});

  final api.Operations _operations;

  /// A stored router config, by name or id. Without one, every call says what it wants.
  final String config;

  /// Cost labels carried onto everything routed here, on top of the config's own.
  final Map<String, String> tags;

  /// Answers one question out of what is true now, run as [config] says, or as the app's default
  /// does. How a search is run, and so what it costs, is the app's backend's to choose.
  Future<SearchAnswer> search(String query) async {
    final answer = await _operations.search(
      body: api.SearchRequest(
        query: query,
        configId: config.isEmpty ? null : config,
        tags: tags.isEmpty ? null : tags,
      ),
    );
    return SearchAnswer(
      provider: answer.provider,
      model: answer.model,
      answer: answer.answer ?? '',
      results: [
        for (final hit in answer.results)
          SearchHit(
            url: hit.url,
            title: hit.title ?? '',
            text: hit.text ?? '',
            score: hit.score ?? 0,
          ),
      ],
    );
  }
}
