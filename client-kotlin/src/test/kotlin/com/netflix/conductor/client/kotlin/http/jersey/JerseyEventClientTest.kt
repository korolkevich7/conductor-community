package com.netflix.conductor.client.kotlin.http.jersey

import com.netflix.conductor.common.metadata.events.EventHandler
import com.nhaarman.mockito_kotlin.mock
import com.sun.jersey.api.client.ClientResponse
import com.sun.jersey.api.client.WebResource
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mockito

class JerseyEventClientTest : JerseyClientTest() {

    private var eventClient: JerseyEventClient = JerseyEventClient(requestHandler)

    @BeforeEach
    fun setup() {
        eventClient.setRootURI(ROOT_URL)
    }

    @Test
    fun registerEventHandler(): Unit = runBlocking {
        val handler = EventHandler()
        val uri = createURI("event")

        val webResourceBuilder: WebResource.Builder = mock()

        Mockito.`when`(requestHandler.getWebResourceBuilder(uri, handler)).thenReturn(webResourceBuilder)

        eventClient.registerEventHandler(handler)

        Mockito.verify(requestHandler, Mockito.times(1))
            .getWebResourceBuilder(uri, handler)
    }

    @Test
    fun updateEventHandler(): Unit = runBlocking {
        val handler = EventHandler()
        val uri = createURI("event")

        val webResourceBuilder: WebResource.Builder = mock()
        Mockito.`when`(requestHandler.getWebResourceBuilder(uri, handler)).thenReturn(webResourceBuilder)

        eventClient.updateEventHandler(handler)

        Mockito.verify(requestHandler, Mockito.times(1))
            .getWebResourceBuilder(uri, handler)
    }

    @Test
    fun unregisterEventHandler(): Unit = runBlocking {
        val eventName = "test"
        val uri = createURI("event/$eventName")

        eventClient.unregisterEventHandler(eventName)

        Mockito.verify(requestHandler, Mockito.times(1))
            .delete(uri, null)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun getEventHandlers(activeOnly: Boolean): Unit = runBlocking {
        val handlers = arrayListOf(EventHandler(), EventHandler())
        val eventName = "test"
        val uri = createURI("event/$eventName?activeOnly=$activeOnly")

        val clientResponse: ClientResponse = mock()
        Mockito.`when`(clientResponse.getEntity(Constants.eventHandlerList)).thenReturn(handlers)
        Mockito.`when`(requestHandler.get(uri)).thenReturn(clientResponse)

        val eventHandlers = eventClient.getEventHandlers(eventName, activeOnly)

        Mockito.verify(requestHandler, Mockito.times(1))
            .get(uri)

        assertTrue{ eventHandlers.size == 2}
    }
}