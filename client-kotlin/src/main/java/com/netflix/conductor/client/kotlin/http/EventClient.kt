package com.netflix.conductor.client.kotlin.http

import com.netflix.conductor.common.metadata.events.EventHandler

/** Client for all Event Handler operations  */
interface EventClient {
    var rootURI: String

    /**
     * Register an event handler with the server
     *
     * @param eventHandler the eventHandler definition
     */
    suspend fun registerEventHandler(eventHandler: EventHandler)

    /**
     * Updates an event handler with the server
     *
     * @param eventHandler the eventHandler definition
     */
    suspend fun updateEventHandler(eventHandler: EventHandler)

    /**
     * @param event name of the event
     * @param activeOnly if true, returns only the active handlers
     * @return Returns the list of all the event handlers for a given event
     */
    suspend fun getEventHandlers(event: String, activeOnly: Boolean): List<EventHandler>

    /**
     * Removes the event handler definition from the conductor server
     *
     * @param name the name of the event handler to be unregistered
     */
    suspend fun unregisterEventHandler(name: String)
}
