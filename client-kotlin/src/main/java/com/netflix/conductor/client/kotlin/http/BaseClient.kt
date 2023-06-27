package com.netflix.conductor.client.kotlin.http

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.cfg.PackageVersion
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.netflix.conductor.client.kotlin.config.ConductorClientConfiguration
import com.netflix.conductor.client.kotlin.config.DefaultConductorClientConfiguration
import com.netflix.conductor.client.kotlin.exception.ConductorClientException
import com.netflix.conductor.common.config.ObjectMapperProvider
import com.netflix.conductor.common.model.BulkResponse
import com.netflix.conductor.common.run.ExternalStorageLocation
import com.netflix.conductor.common.utils.ExternalPayloadStorage
import org.apache.commons.lang3.Validate
import org.slf4j.LoggerFactory
import java.io.ByteArrayInputStream
import java.io.IOException
import javax.ws.rs.core.UriBuilder

/** Abstract client for the REST server  */
abstract class BaseClient protected constructor(
    requestHandler: BaseClientRequestHandler,
    clientConfiguration: ConductorClientConfiguration?
) {
    protected var root = ""
    protected var objectMapper: ObjectMapper = ObjectMapperProvider().objectMapper
    protected var payloadStorage: PayloadStorage
    protected var conductorClientConfiguration: ConductorClientConfiguration

    init {
        // https://github.com/FasterXML/jackson-databind/issues/2683
        if (isNewerJacksonVersion) {
            objectMapper.registerModule(JavaTimeModule())
        }
        objectMapper.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
        conductorClientConfiguration = clientConfiguration ?: DefaultConductorClientConfiguration()
        payloadStorage = PayloadStorage(this)
    }

    fun setRootURI(root: String) {
        this.root = root
    }

    internal abstract suspend fun delete(
        queryParams: Array<Any?>? = null, url: String, vararg uriVariables: Any? = arrayOf(null), body: Any? = null
    ): BulkResponse?

    internal abstract suspend fun <T> getForEntity(
        url: String, queryParams: Array<Any?>?, responseType: Class<T>?, vararg uriVariables: Any?
    ): T?

    /**
     * Uses the [PayloadStorage] for storing large payloads. Gets the uri for storing the
     * payload from the server and then uploads to this location
     *
     * @param payloadType the [     ] to be uploaded
     * @param payloadBytes the byte array containing the payload
     * @param payloadSize the size of the payload
     * @return the path where the payload is stored in external storage
     */
    internal fun uploadToExternalPayloadStorage(
        payloadType: ExternalPayloadStorage.PayloadType, payloadBytes: ByteArray?, payloadSize: Long
    ): String {
        Validate.isTrue(
            payloadType == ExternalPayloadStorage.PayloadType.WORKFLOW_INPUT || payloadType == ExternalPayloadStorage.PayloadType.TASK_OUTPUT,
            "Payload type must be workflow input or task output"
        )
        val externalStorageLocation: ExternalStorageLocation =
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
    internal fun downloadFromExternalStorage(
        payloadType: ExternalPayloadStorage.PayloadType, path: String
    ): Map<String, Any> {
        Validate.notBlank(path, "uri cannot be blank")
        val externalStorageLocation: ExternalStorageLocation = payloadStorage.getLocation(
            ExternalPayloadStorage.Operation.READ, payloadType, path
        )
        try {
            val typeRef = object : TypeReference<MutableMap<String, Any>>() {}
            payloadStorage.download(externalStorageLocation.uri).use { inputStream ->
                return objectMapper.readValue(inputStream, typeRef)
            }
        } catch (e: IOException) {
            val errorMsg = "Unable to download payload from external storage location: $path"
            LOGGER.error(errorMsg, e)
            throw ConductorClientException(errorMsg, e)
        }
    }

    protected fun getURIBuilder(path: String?, queryParams: Array<Any?>?): UriBuilder {
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

    private val isNewerJacksonVersion: Boolean
        get() {
            val version = PackageVersion.VERSION
            return version.majorVersion == 2 && version.minorVersion >= 12
        }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(BaseClient::class.java)
    }
}
