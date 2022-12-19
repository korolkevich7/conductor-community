package com.netflix.conductor.client.kotlin.http

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.cfg.PackageVersion
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.netflix.conductor.client.kotlin.config.ConductorClientConfiguration
import com.netflix.conductor.client.kotlin.config.DefaultConductorClientConfiguration
import com.netflix.conductor.client.kotlin.exception.ConductorClientException
import com.netflix.conductor.common.config.ObjectMapperProvider
import com.netflix.conductor.common.model.BulkResponse
import com.netflix.conductor.common.utils.ExternalPayloadStorage
import com.netflix.conductor.common.validation.ErrorResponse
import com.sun.jersey.api.client.ClientHandlerException
import com.sun.jersey.api.client.ClientResponse
import com.sun.jersey.api.client.GenericType
import com.sun.jersey.api.client.UniformInterfaceException
import com.sun.jersey.api.client.WebResource
import org.apache.commons.lang3.Validate
import org.slf4j.LoggerFactory
import java.io.ByteArrayInputStream
import java.io.IOException
import java.net.URI
import java.util.function.Function
import javax.ws.rs.core.UriBuilder


/** Abstract client for the REST server  */
public abstract class ClientBase protected constructor(
    requestHandler: ClientRequestHandler, clientConfiguration: ConductorClientConfiguration?
) {
    protected var requestHandler: ClientRequestHandler
    protected var root = ""
    protected var objectMapper: ObjectMapper
    protected var payloadStorage: PayloadStorage
    protected var conductorClientConfiguration: ConductorClientConfiguration

    init {
        objectMapper = ObjectMapperProvider().objectMapper

        // https://github.com/FasterXML/jackson-databind/issues/2683
        if (isNewerJacksonVersion) {
            objectMapper.registerModule(JavaTimeModule())
        }
        this.requestHandler = requestHandler
        conductorClientConfiguration = clientConfiguration ?: DefaultConductorClientConfiguration()
        payloadStorage = PayloadStorage(this)
    }

    fun setRootURI(root: String) {
        this.root = root
    }

    protected fun delete(url: String?, vararg uriVariables: Any?) {
        deleteWithUriVariables(null, url, *uriVariables)
    }

    protected fun deleteWithUriVariables(
        queryParams: Array<Any>?, url: String?, vararg uriVariables: Any?
    ) {
        delete(queryParams, url, uriVariables = uriVariables, null)
    }

    protected fun deleteWithRequestBody(queryParams: Array<Any>?, url: String, body: Any?): BulkResponse? {
        return delete(queryParams, url, body = body)
    }

    private fun delete(
        queryParams: Array<Any>?, url: String?, vararg uriVariables: Any?, body: Any?
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

    protected fun put(url: String, queryParams: Array<Any>?, request: Any?, vararg uriVariables: Any?) {
        var uri: URI? = null
        try {
            uri = getURIBuilder(root + url, queryParams).build(*uriVariables)
            requestHandler.getWebResourceBuilder(uri, request).put()
        } catch (e: RuntimeException) {
            handleException(uri, e)
        }
    }

    protected fun postForEntityWithRequestOnly(url: String?, request: Any?) {
        val type: Class<*>? = null
        postForEntity(url, request, null, type)
    }

    protected fun postForEntityWithUriVariablesOnly(url: String?, vararg uriVariables: Any?) {
        val type: Class<*>? = null
        postForEntity(url, null, null, type, *uriVariables)
    }

    protected fun <T> postForEntity(
        url: String?,
        request: Any?,
        queryParams: Array<Any?>?,
        responseType: Class<T>?,
        vararg uriVariables: Any?
    ): T {
        return postForEntity(
            url,
            request,
            queryParams,
            responseType,
            Function { builder: WebResource.Builder ->
                builder.post(
                    responseType
                )
            },
            *uriVariables
        )
    }

    protected fun <T> postForEntity(
        url: String?,
        request: Any?,
        queryParams: Array<Any?>?,
        responseType: GenericType<T>?,
        vararg uriVariables: Any?
    ): T {
        return postForEntity(
            url,
            request,
            queryParams,
            responseType,
            Function { builder: WebResource.Builder ->
                builder.post(
                    responseType
                )
            },
            *uriVariables
        )
    }

    private fun <T> postForEntity(
        url: String,
        request: Any,
        queryParams: Array<Any>?,
        responseType: Any?,
        postWithEntity: Function<WebResource.Builder, T>,
        vararg uriVariables: Any
    ): T? {
        var uri: URI? = null
        try {
            uri = getURIBuilder(root + url, queryParams).build(*uriVariables)
            val webResourceBuilder = requestHandler.getWebResourceBuilder(uri, request)
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

    internal fun <T> getForEntity(
        url: String, queryParams: Array<Any>?, responseType: Class<T>, vararg uriVariables: Any?
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

    protected fun <T> getForEntity(
        url: String, queryParams: Array<Any>?, responseType: GenericType<T>, vararg uriVariables: Any?
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

    private fun <T> getForEntity(
        url: String,
        queryParams: Array<Any>?,
        entityProvider: Function<ClientResponse, T>,
        vararg uriVariables: Any?
    ): T? {
        var uri: URI? = null
        val clientResponse: ClientResponse
        try {
            uri = getURIBuilder(root + url, queryParams).build(*uriVariables)
            clientResponse = requestHandler[uri]
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

    /**
     * Uses the [PayloadStorage] for storing large payloads. Gets the uri for storing the
     * payload from the server and then uploads to this location
     *
     * @param payloadType the [     ] to be uploaded
     * @param payloadBytes the byte array containing the payload
     * @param payloadSize the size of the payload
     * @return the path where the payload is stored in external storage
     */
    protected fun uploadToExternalPayloadStorage(
        payloadType: ExternalPayloadStorage.PayloadType, payloadBytes: ByteArray?, payloadSize: Long
    ): String {
        Validate.isTrue(
            payloadType == ExternalPayloadStorage.PayloadType.WORKFLOW_INPUT || payloadType == ExternalPayloadStorage.PayloadType.TASK_OUTPUT,
            "Payload type must be workflow input or task output"
        )
        val externalStorageLocation =
            payloadStorage.getLocation(ExternalPayloadStorage.Operation.WRITE, payloadType, "")
        payloadStorage.upload(
            externalStorageLocation.uri,
            ByteArrayInputStream(payloadBytes),
            payloadSize
        )
        return externalStorageLocation.path
    }

    /**
     * Uses the [PayloadStorage] for downloading large payloads to be used by the client. Gets
     * the uri of the payload fom the server and then downloads from this location.
     *
     * @param payloadType the [     ] to be downloaded
     * @param path the relative of the payload in external storage
     * @return the payload object that is stored in external storage
     */
    protected fun downloadFromExternalStorage(
        payloadType: ExternalPayloadStorage.PayloadType, path: String
    ): Map<String, Any> {
        Validate.notBlank(path, "uri cannot be blank")
        val externalStorageLocation = payloadStorage.getLocation(
            ExternalPayloadStorage.Operation.READ, payloadType, path
        )
        try {
            val typeRef = object : TypeReference<MutableMap<String, Any>>() {}
            payloadStorage.download(externalStorageLocation.uri).use { inputStream ->
                return objectMapper.readValue(inputStream, typeRef)
            }
        } catch (e: IOException) {
            val errorMsg = String.format(
                "Unable to download payload from external storage location: %s", path
            )
            LOGGER.error(errorMsg, e)
            throw ConductorClientException(errorMsg, e)
        }
    }

    private fun getURIBuilder(path: String?, queryParams: Array<Any>?): UriBuilder {
        val builder = UriBuilder.fromPath(path ?: "")
        if (queryParams != null) {
            var i = 0
            while (i < queryParams.size) {
                val param = queryParams[i].toString()
                val value = queryParams[i + 1]
                if (value != null) {
                    if (value is Collection<*>) {
                        val values: Array<Any?> = value.toTypedArray()
                        builder.queryParam(param, *values)
                    } else {
                        builder.queryParam(param, value)
                    }
                }
                i += 2
            }
        }
        return builder
    }

    protected val isNewerJacksonVersion: Boolean
        get() {
            val version = PackageVersion.VERSION
            return version.majorVersion == 2 && version.minorVersion >= 12
        }

    private fun handleClientHandlerException(exception: ClientHandlerException, uri: URI?) {
        val errorMessage = String.format(
            "Unable to invoke Conductor API with uri: %s, failure to process request or response",
            uri
        )
        LOGGER.error(errorMessage, exception)
        throw ConductorClientException(errorMessage, exception)
    }

    private fun handleRuntimeException(exception: RuntimeException, uri: URI?) {
        val errorMessage = String.format(
            "Unable to invoke Conductor API with uri: %s, runtime exception occurred",
            uri
        )
        LOGGER.error(errorMessage, exception)
        throw ConductorClientException(errorMessage, exception)
    }

    private fun handleUniformInterfaceException(exception: UniformInterfaceException, uri: URI?) {
        val clientResponse = exception.response
            ?: throw ConductorClientException(String.format("Unable to invoke Conductor API with uri: %s", uri))
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
            val errorResponse: ErrorResponse
            errorResponse = try {
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
        if (e is UniformInterfaceException) {
            handleUniformInterfaceException(e, uri)
        } else if (e is ClientHandlerException) {
            handleClientHandlerException(e, uri)
        } else {
            handleRuntimeException(e, uri)
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
        private val LOGGER = LoggerFactory.getLogger(ClientBase::class.java)
    }
}
