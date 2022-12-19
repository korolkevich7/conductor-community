package com.netflix.conductor.client.kotlin.http

import com.fasterxml.jackson.databind.cfg.PackageVersion
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider
import com.netflix.conductor.common.config.ObjectMapperProvider
import com.netflix.conductor.common.model.BulkResponse
import com.sun.jersey.api.client.Client
import com.sun.jersey.api.client.ClientHandler
import com.sun.jersey.api.client.ClientResponse
import com.sun.jersey.api.client.WebResource
import com.sun.jersey.api.client.config.ClientConfig
import com.sun.jersey.api.client.filter.ClientFilter
import java.net.URI
import javax.ws.rs.core.MediaType


class ClientRequestHandler(
    config: ClientConfig, handler: ClientHandler?, vararg filters: ClientFilter
) {
    private var client: Client? = null

    init {
        val objectMapper = ObjectMapperProvider().objectMapper

        // https://github.com/FasterXML/jackson-databind/issues/2683
        if (isNewerJacksonVersion) {
            objectMapper.registerModule(JavaTimeModule())
        }
        val provider = JacksonJsonProvider(objectMapper)
        config.singletons.add(provider)
        if (handler == null) {
            client = Client.create(config)
        } else {
            client = Client(handler, config)
        }
        for (filter in filters) {
            client!!.addFilter(filter)
        }
    }

    fun delete(uri: URI?, body: Any?): BulkResponse? {
        if (body != null) {
            return client!!.resource(uri)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .delete(BulkResponse::class.java, body)
        } else {
            client!!.resource(uri).delete()
        }
        return null
    }

    operator fun get(uri: URI?): ClientResponse {
        return client!!.resource(uri)
            .accept(MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN)
            .get(ClientResponse::class.java)
    }

    fun getWebResourceBuilder(URI: URI?, entity: Any?): WebResource.Builder {
        return client!!.resource(URI)
            .type(MediaType.APPLICATION_JSON)
            .entity(entity)
            .accept(MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON)
    }

    private val isNewerJacksonVersion: Boolean
        get() {
            val version = PackageVersion.VERSION
            return version.majorVersion == 2 && version.minorVersion >= 12
        }
}
