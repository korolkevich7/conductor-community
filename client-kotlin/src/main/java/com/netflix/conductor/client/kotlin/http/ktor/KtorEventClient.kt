package com.netflix.conductor.client.kotlin.http.ktor

import com.netflix.conductor.client.kotlin.http.EventClient
import com.netflix.conductor.common.metadata.events.EventHandler
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.request.*
import io.ktor.http.*

class KtorEventClient(rootURI: String, httpClient: HttpClient) : EventClient, KtorBaseClient(rootURI, httpClient) {
    override suspend fun registerEventHandler(eventHandler: EventHandler) {
        httpClient.post {
            url("$rootURI/event")
            setBody(eventHandler)
            contentType(ContentType.Application.Json)
        }
    }

    override suspend fun updateEventHandler(eventHandler: EventHandler) {
        httpClient.put {
            url("$rootURI/event")
            setBody(eventHandler)
            contentType(ContentType.Application.Json)
        }
    }

    override suspend fun getEventHandlers(event: String, activeOnly: Boolean): List<EventHandler> {
        require(event.isNotBlank()) { "Event cannot be blank" }
        val response = httpClient.get {
            url("$rootURI/event/$event")
            parameter("activeOnly", activeOnly)
        }
        return response.body()
    }

    override suspend fun unregisterEventHandler(name: String) {
        require(name.isNotBlank()) { "Event cannot be blank" }
        httpClient.delete {
            url("$rootURI/event/$name")
        }
    }
}