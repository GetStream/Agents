package io.getstream.agents.android.core

import io.getstream.agents.android.core.generated.SearchAnswer as AnswerSchema
import io.getstream.agents.android.core.generated.SearchRequest
import io.getstream.agents.android.core.generated.SearchResult

/**
 * Looking something up, configured once.
 *
 * Search is the one routed modality a device may reach: a question and its answer are one
 * round trip, and the answer is for whoever asked rather than for the app.
 *
 *     val router = agents.router(config = "healthcare")
 *     val answer = router.search("what changed in the pricing page")
 *
 * A router comes from [StreamAgents.router], which is where the URL and credentials were
 * settled.
 *
 * Transcription, a voice and a model are not here. Those run over the per-modality socket,
 * which the router refuses a device, so a pipeline of your own belongs to a backend.
 */
public class Router internal constructor(
    public val backend: Backend,
    /** A stored router config, by name or id. Empty means every call says what it wants. */
    public val config: String = "",
    /** Cost labels carried onto everything routed here, on top of the config's own. */
    public val tags: Map<String, String> = emptyMap(),
) {
    /**
     * Answers one question out of what is true now, run as [config] says, or as the app's
     * default does. How a search is run, and so what it costs, is the app's backend's to choose.
     * Each search counts as one message on the device's daily quota.
     */
    public suspend fun search(query: String): SearchAnswer {
        val request = SearchRequest(
            query = query,
            configId = config.ifEmpty { null },
            tags = tags.ifEmpty { null },
        )
        val answer = backend.post(
            listOf("v1", "search"),
            SearchRequest.serializer(),
            request,
            AnswerSchema.serializer(),
        )
        return SearchAnswer.of(answer)
    }
}

/** What a search found. */
public data class SearchAnswer(
    val provider: String,
    val model: String,
    /** The provider's own summary, where it offers one: a sentence to say rather than a page to read. */
    val answer: String,
    /** The sources behind it, most relevant first. */
    val results: List<SearchHit>,
) {
    internal companion object {
        fun of(schema: AnswerSchema): SearchAnswer = SearchAnswer(
            provider = schema.provider,
            model = schema.model,
            answer = schema.answer.orEmpty(),
            results = schema.results.map(SearchHit::of),
        )
    }
}

/** One source a search found. */
public data class SearchHit(
    val title: String,
    val url: String,
    /** The relevant extract, which is what a model reads. */
    val text: String,
    val score: Double,
) {
    internal companion object {
        fun of(schema: SearchResult): SearchHit = SearchHit(
            title = schema.title.orEmpty(),
            url = schema.url,
            text = schema.text.orEmpty(),
            score = schema.score?.toDouble() ?: 0.0,
        )
    }
}
