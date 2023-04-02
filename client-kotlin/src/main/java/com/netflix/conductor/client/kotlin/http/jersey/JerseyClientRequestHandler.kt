package com.netflix.conductor.client.kotlin.http.jersey

import com.netflix.conductor.client.kotlin.http.BaseClientRequestHandler
import com.netflix.conductor.common.model.BulkResponse
import com.sun.jersey.api.client.Client
import com.sun.jersey.api.client.ClientHandler
import com.sun.jersey.api.client.ClientResponse
import com.sun.jersey.api.client.WebResource
import com.sun.jersey.api.client.config.ClientConfig
import com.sun.jersey.api.client.filter.ClientFilter
import java.net.URI
import javax.ws.rs.core.MediaType


open class JerseyClientRequestHandler(
    config: ClientConfig, handler: ClientHandler?, vararg filters: ClientFilter
) : BaseClientRequestHandler() {
    private var client: Client

    init {
        config.singletons.add(provider)
        client = if (handler == null) {
            Client.create(config)
        } else {
            Client(handler, config)
        }
        filters.forEach {
            client.addFilter(it)
        }
    }

    override suspend fun delete(uri: URI?, body: Any?): BulkResponse? {
        return if (body != null) {
            client.resource(uri)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .delete(BulkResponse::class.java, body)
        } else {
            client.resource(uri).delete()
            null
        }
    }

    open suspend fun get(uri: URI?): ClientResponse {
        return client.resource(uri)
            .accept(MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN)
            .get(ClientResponse::class.java)
    }

    open suspend fun getWebResourceBuilder(URI: URI?, entity: Any?): WebResource.Builder {
        return client.resource(URI)
            .type(MediaType.APPLICATION_JSON)
            .entity(entity)
            .accept(MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON)
    }
}
