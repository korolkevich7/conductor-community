package com.netflix.conductor.client.kotlin.http.ktor

import com.netflix.conductor.client.kotlin.http.EventClient
import com.netflix.conductor.common.metadata.events.EventHandler
import io.ktor.client.call.*
import io.ktor.client.request.*

class KtorEventClient(override var rootURI: String) : EventClient, KtorBaseClient(rootURI) {
    override suspend fun registerEventHandler(eventHandler: EventHandler) {
        httpClient.post {
            url("$rootURI/event")
            setBody(eventHandler)
        }
    }

    override suspend fun updateEventHandler(eventHandler: EventHandler) {
        httpClient.put {
            url("$rootURI/event")
            setBody(eventHandler)
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