import Foundation

/// What a search found.
public struct SearchAnswer: Sendable, Hashable {
    public let provider: String
    public let model: String
    /// The provider's own summary, where it offers one: a sentence to say rather than a page to
    /// read.
    public let answer: String
    /// The sources behind it, most relevant first.
    public let results: [SearchHit]

    init(_ schema: SearchAnswerPayload) {
        provider = schema.provider
        model = schema.model
        answer = schema.answer ?? ""
        results = schema.results.map(SearchHit.init)
    }
}

/// One source a search found.
public struct SearchHit: Sendable, Hashable {
    public let title: String
    public let url: String
    /// The relevant extract, which is what a model reads.
    public let text: String
    public let score: Double

    init(_ schema: SearchResult) {
        title = schema.title ?? ""
        url = schema.url
        text = schema.text ?? ""
        score = schema.score ?? 0
    }
}
