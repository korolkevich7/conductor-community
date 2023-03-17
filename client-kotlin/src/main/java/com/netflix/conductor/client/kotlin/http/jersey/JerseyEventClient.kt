package com.netflix.conductor.client.kotlin.http.jersey

import com.netflix.conductor.client.kotlin.config.ConductorClientConfiguration
import com.netflix.conductor.client.kotlin.config.DefaultConductorClientConfiguration
import com.netflix.conductor.client.kotlin.http.EventClient
import com.netflix.conductor.common.metadata.events.EventHandler
import com.sun.jersey.api.client.ClientHandler
import com.sun.jersey.api.client.config.ClientConfig
import com.sun.jersey.api.client.config.DefaultClientConfig
import com.sun.jersey.api.client.filter.ClientFilter
import org.apache.commons.lang3.Validate
import java.util.Collections


// Client class for all Event Handler operations
open class JerseyEventClient : EventClient {

    private var jerseyBaseClient: JerseyBaseClient

    /** Creates a default metadata client  */
    constructor() : this(DefaultClientConfig(), DefaultConductorClientConfiguration(), null)

    /**
     * @param clientConfig REST Client configuration
     */
    constructor(clientConfig: ClientConfig) : this(clientConfig, DefaultConductorClientConfiguration(), null)

    /**
     * @param clientConfig REST Client configuration
     * @param clientHandler Jersey client handler. Useful when plugging in various http client
     * interaction modules (e.g. ribbon)
     */
    constructor(clientConfig: ClientConfig, clientHandler: ClientHandler?) : this(
        clientConfig,
        DefaultConductorClientConfiguration(),
        clientHandler
    )

    /**
     * @param config config REST Client configuration
     * @param handler handler Jersey client handler. Useful when plugging in various http client
     * interaction modules (e.g. ribbon)
     * @param filters Chain of client side filters to be applied per request
     */
    constructor(config: ClientConfig, handler: ClientHandler?, vararg filters: ClientFilter) : this(
        config,
        DefaultConductorClientConfiguration(),
        handler,
        *filters
    )

    /**
     * @param config REST Client configuration
     * @param clientConfiguration Specific properties configured for the client, see [     ]
     * @param handler Jersey client handler. Useful when plugging in various http client interaction
     * modules (e.g. ribbon)
     * @param filters Chain of client side filters to be applied per request
     */
    constructor(
        config: ClientConfig,
        clientConfiguration: ConductorClientConfiguration,
        handler: ClientHandler?,
        vararg filters: ClientFilter
    ) {
        jerseyBaseClient = JerseyBaseClient(JerseyClientRequestHandler(config, handler, *filters), clientConfiguration)
    }

    internal constructor(requestHandler: JerseyClientRequestHandler) {
        jerseyBaseClient = JerseyBaseClient(requestHandler, DefaultConductorClientConfiguration())
    }

    override fun setRootURI(root: String) = jerseyBaseClient.setRootURI(root)

    /**
     * Register an event handler with the server
     *
     * @param eventHandler the eventHandler definition
     */
    override suspend fun registerEventHandler(eventHandler: EventHandler) = jerseyBaseClient.postForEntityWithRequestOnly("event", eventHandler)

    /**
     * Updates an event handler with the server
     *
     * @param eventHandler the eventHandler definition
     */
    override suspend fun updateEventHandler(eventHandler: EventHandler) = jerseyBaseClient.put("event", null, eventHandler)

    /**
     * @param event name of the event
     * @param activeOnly if true, returns only the active handlers
     * @return Returns the list of all the event handlers for a given event
     */
    override suspend fun getEventHandlers(event: String, activeOnly: Boolean): List<EventHandler> {
        Validate.notBlank(event, "Event cannot be blank")
        return jerseyBaseClient.getForEntity(
            "event/{event}", arrayOf("activeOnly", activeOnly), Constants.eventHandlerList, event
        )?:Collections.emptyList()
    }

    /**
     * Removes the event handler definition from the conductor server
     *
     * @param name the name of the event handler to be unregistered
     */
    override suspend fun unregisterEventHandler(name: String) {
        Validate.notBlank(name, "Event handler name cannot be blank")
        jerseyBaseClient.delete(url = "event/{name}", uriVariables = arrayOf(name))
    }

}
