package com.netflix.conductor.client.kotlin.http.ktor

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.netflix.conductor.client.kotlin.http.EventClient
import com.netflix.conductor.common.metadata.events.EventHandler
import io.ktor.client.engine.mock.*
import io.ktor.http.*
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource


class KtorEventClientTest : KtorClientTest() {

    private lateinit var eventClient: EventClient

    @BeforeEach
    fun setup() {
        val objectMapper = jacksonObjectMapper()

        val mockEngine = MockEngine { request ->
            val handlers = listOf(EventHandler(), EventHandler())
            println("URL ${request.url}")
            when (request.url.toString()) {
                "$ROOT_URL/event" -> respond(
                    content = "",
                    status = HttpStatusCode.NoContent
                )
                "$ROOT_URL/event/test" -> respond(
                    content = "",
                    status = HttpStatusCode.NoContent
                )
                "$ROOT_URL/event/test?activeOnly=true" -> respond(
                    content = objectMapper.writeValueAsString(handlers),
                    status = HttpStatusCode.OK,
                    headers = headersOf(HttpHeaders.ContentType, "application/json")
                )
                "$ROOT_URL/event/test?activeOnly=false" -> respond(
                    content = objectMapper.writeValueAsString(handlers),
                    status = HttpStatusCode.OK,
                    headers = headersOf(HttpHeaders.ContentType, "application/json")
                )
                else -> throw IllegalArgumentException("Wrong url")
            }

        }

        eventClient = KtorEventClient(ROOT_URL, httpClient(mockEngine))
    }

    @Test
    fun registerEventHandler(): Unit = runBlocking {
        val handler = EventHandler()
        eventClient.registerEventHandler(handler)
    }

    @Test
    fun updateEventHandler(): Unit = runBlocking {
        val handler = EventHandler()
        eventClient.updateEventHandler(handler)
    }

    @Test
    fun unregisterEventHandler(): Unit = runBlocking {
        val eventName = "test"
        eventClient.unregisterEventHandler(eventName)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun getEventHandlers(activeOnly: Boolean): Unit = runBlocking {
        val eventName = "test"
        val eventHandlers = eventClient.getEventHandlers(eventName, activeOnly)
        assertTrue { eventHandlers.size == 2 }
    }
}