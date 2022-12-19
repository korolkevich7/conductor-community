package com.netflix.conductor.client.kotlin.http

import com.netflix.conductor.client.kotlin.config.ConductorClientConfiguration
import com.netflix.conductor.client.kotlin.exception.ConductorClientException
import com.netflix.conductor.common.model.BulkResponse
import com.netflix.conductor.common.validation.ErrorResponse
import com.sun.jersey.api.client.ClientHandlerException
import com.sun.jersey.api.client.ClientResponse
import com.sun.jersey.api.client.GenericType
import com.sun.jersey.api.client.UniformInterfaceException
import com.sun.jersey.api.client.WebResource
import org.slf4j.LoggerFactory
import java.io.IOException
import java.net.URI
import java.util.function.Function


/** Abstract jersey client for the REST server  */
abstract class JerseyBaseClient protected constructor(
    protected var requestHandler: JerseyClientRequestHandler,
    clientConfiguration: ConductorClientConfiguration?
) : BaseClient(requestHandler, clientConfiguration) {

    override suspend fun delete(
        queryParams: Array<Any>?, url: String, vararg uriVariables: Any?, body: Any?
    ): BulkResponse? {
        var uri: URI? = null
        var response: BulkResponse? = null
        try {
            uri = getURIBuilder(root + url, queryParams).build(*uriVariables)
            response = requestHandler.delete(uri, body)
        } catch (e: UniformInterfaceException) {
            handleUniformInterfaceException(e, uri)
        } catch (e: RuntimeException) {
            handleRuntimeException(e, uri)
        }
        return response
    }

    protected suspend fun put(url: String, queryParams: Array<Any>?, request: Any?, vararg uriVariables: Any?) {
        var uri: URI? = null
        try {
            uri = getURIBuilder(root + url, queryParams).build(*uriVariables)
            requestHandler.getWebResourceBuilder(uri, request).put()
        } catch (e: RuntimeException) {
            handleException(uri, e)
        }
    }

    protected suspend fun postForEntityWithRequestOnly(url: String, request: Any?) {
        val type: Class<*>? = null
        postForEntity(url, request, null, type)
    }

    protected suspend fun postForEntityWithUriVariablesOnly(url: String, vararg uriVariables: Any?) {
        val type: Class<*>? = null
        postForEntity(url, null, null, type, *uriVariables)
    }

    protected suspend fun <T> postForEntity(
        url: String,
        request: Any?,
        queryParams: Array<Any>?,
        responseType: Class<T>?,
        vararg uriVariables: Any?
    ): T? {
        return postForEntity(
            url = url,
            request = request,
            queryParams = queryParams,
            responseType = responseType,
            postWithEntity = Function { builder: WebResource.Builder ->
                builder.post(
                    responseType
                )
            },
            uriVariables = uriVariables
        )
    }

    protected suspend fun <T> postForEntity(
        url: String,
        request: Any?,
        queryParams: Array<Any>?,
        responseType: GenericType<T>?,
        vararg uriVariables: Any?
    ): T? {
        return postForEntity(
            url = url,
            request = request,
            queryParams = queryParams,
            responseType = responseType,
            postWithEntity = Function { builder: WebResource.Builder ->
                builder.post(
                    responseType
                )
            },
            uriVariables = uriVariables
        )
    }

    private suspend fun <T> postForEntity(
        url: String,
        request: Any?,
        queryParams: Array<Any>?,
        responseType: Any?,
        postWithEntity: Function<WebResource.Builder, T>,
        vararg uriVariables: Any?
    ): T? {
        var uri: URI? = null
        try {
            uri = getURIBuilder(root + url, queryParams).build(*uriVariables)
            val webResourceBuilder: WebResource.Builder = requestHandler.getWebResourceBuilder(uri, request)
            if (responseType == null) {
                webResourceBuilder.post()
                return null
            }
            return postWithEntity.apply(webResourceBuilder)
        } catch (e: UniformInterfaceException) {
            handleUniformInterfaceException(e, uri)
        } catch (e: RuntimeException) {
            handleRuntimeException(e, uri)
        }
        return null
    }

    override suspend fun <T> getForEntity(
        url: String, queryParams: Array<Any>?, responseType: Class<T>?, vararg uriVariables: Any?
    ): T? {
        return getForEntity<T>(
            url, queryParams,
            Function { response: ClientResponse ->
                response.getEntity(
                    responseType
                )
            }, *uriVariables
        )
    }

    protected suspend fun <T> getForEntity(
        url: String, queryParams: Array<Any>?, responseType: GenericType<T>?, vararg uriVariables: Any?
    ): T? {
        return getForEntity<T>(
            url, queryParams,
            Function { response: ClientResponse ->
                response.getEntity(
                    responseType
                )
            }, *uriVariables
        )
    }

    private suspend fun <T> getForEntity(
        url: String,
        queryParams: Array<Any>?,
        entityProvider: Function<ClientResponse, T>,
        vararg uriVariables: Any?
    ): T? {
        var uri: URI? = null
        val clientResponse: ClientResponse
        try {
            uri = getURIBuilder(root + url, queryParams).build(*uriVariables)
            clientResponse = requestHandler.get(uri)
            return if (clientResponse.status < 300) {
                entityProvider.apply(clientResponse)
            } else {
                throw UniformInterfaceException(clientResponse)
            }
        } catch (e: UniformInterfaceException) {
            handleUniformInterfaceException(e, uri)
        } catch (e: RuntimeException) {
            handleRuntimeException(e, uri)
        }
        return null
    }

    private fun handleClientHandlerException(exception: ClientHandlerException, uri: URI?) {
        val errorMessage = "Unable to invoke Conductor API with uri: $uri, failure to process request or response"
        LOGGER.error(errorMessage, exception)
        throw ConductorClientException(errorMessage, exception)
    }

    private fun handleRuntimeException(exception: RuntimeException, uri: URI?) {
        val errorMessage = "Unable to invoke Conductor API with uri: $uri, runtime exception occurred"
        LOGGER.error(errorMessage, exception)
        throw ConductorClientException(errorMessage, exception)
    }

    private fun handleUniformInterfaceException(exception: UniformInterfaceException, uri: URI?) {
        val clientResponse = exception.response
            ?: throw ConductorClientException("Unable to invoke Conductor API with uri: $uri")
        try {
            if (clientResponse.status < 300) {
                return
            }
            val errorMessage = clientResponse.getEntity(String::class.java)
            LOGGER.warn(
                "Unable to invoke Conductor API with uri: {}, unexpected response from server: statusCode={}, responseBody='{}'.",
                uri,
                clientResponse.status,
                errorMessage
            )
            val errorResponse: ErrorResponse = try {
                objectMapper.readValue(
                    errorMessage,
                    ErrorResponse::class.java
                )
            } catch (e: IOException) {
                throw ConductorClientException(clientResponse.status, errorMessage)
            }
            throw ConductorClientException(clientResponse.status, errorResponse)
        } catch (e: ConductorClientException) {
            throw e
        } catch (e: ClientHandlerException) {
            handleClientHandlerException(e, uri)
        } catch (e: RuntimeException) {
            handleRuntimeException(e, uri)
        } finally {
            clientResponse.close()
        }
    }

    private fun handleException(uri: URI?, e: RuntimeException) {
        when (e) {
            is UniformInterfaceException -> {
                handleUniformInterfaceException(e, uri)
            }
            is ClientHandlerException -> {
                handleClientHandlerException(e, uri)
            }
            else -> {
                handleRuntimeException(e, uri)
            }
        }
    }

    /**
     * Converts ClientResponse object to string with detailed debug information including status
     * code, media type, response headers, and response body if exists.
     */
    private fun clientResponseToString(response: ClientResponse?): String? {
        if (response == null) {
            return null
        }
        val builder = StringBuilder()
        builder.append("[status: ").append(response.status)
        builder.append(", media type: ").append(response.type)
        if (response.status != 404) {
            try {
                val responseBody = response.getEntity(String::class.java)
                if (responseBody != null) {
                    builder.append(", response body: ").append(responseBody)
                }
            } catch (ignore: RuntimeException) {
                // Ignore if there is no response body, or IO error - it may have already been read
                // in certain scenario.
            }
        }
        builder.append(", response headers: ").append(response.headers)
        builder.append("]")
        return builder.toString()
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(JerseyBaseClient::class.java)
    }
}
