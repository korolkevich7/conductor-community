package com.netflix.conductor.client.kotlin.http

import com.fasterxml.jackson.databind.cfg.PackageVersion
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider
import com.netflix.conductor.common.config.ObjectMapperProvider
import com.netflix.conductor.common.model.BulkResponse
import java.net.URI

abstract class BaseClientRequestHandler {

    val provider: JacksonJsonProvider

    init {
        val objectMapper = ObjectMapperProvider().objectMapper
        // https://github.com/FasterXML/jackson-databind/issues/2683
        if (isNewerJacksonVersion) {
            objectMapper.registerModule(JavaTimeModule())
        }
        provider = JacksonJsonProvider(objectMapper)
    }

    private val isNewerJacksonVersion: Boolean
        get() {
            val version = PackageVersion.VERSION
            return version.majorVersion == 2 && version.minorVersion >= 12
        }

    abstract suspend fun delete(uri: URI?, body: Any?): BulkResponse?
}
