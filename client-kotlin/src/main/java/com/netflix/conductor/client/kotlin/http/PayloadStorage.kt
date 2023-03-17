package com.netflix.conductor.client.kotlin.http

import com.amazonaws.util.IOUtils
import com.netflix.conductor.client.kotlin.exception.ConductorClientException
import com.netflix.conductor.common.run.ExternalStorageLocation
import com.netflix.conductor.common.utils.ExternalPayloadStorage
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.io.BufferedOutputStream
import java.io.IOException
import java.io.InputStream
import java.net.HttpURLConnection
import java.net.MalformedURLException
import java.net.URI
import java.net.URISyntaxException
import javax.ws.rs.core.Response
import kotlin.coroutines.CoroutineContext


/** An implementation of [ExternalPayloadStorage] for storing large JSON payload data.  */
class PayloadStorage(private val clientBase: BaseClient) :
    ExternalPayloadStorage, CoroutineScope {

    private val job = Job()

    override val coroutineContext: CoroutineContext
        get() = Dispatchers.Default + job

    /**
     * This method is not intended to be used in the client. The client makes a request to the
     * server to get the [ExternalStorageLocation]
     */
    override fun getLocation(
        operation: ExternalPayloadStorage.Operation, payloadType: ExternalPayloadStorage.PayloadType, path: String
    ): ExternalStorageLocation {
        val uri: String = when (payloadType) {
                ExternalPayloadStorage.PayloadType.WORKFLOW_INPUT, ExternalPayloadStorage.PayloadType.WORKFLOW_OUTPUT -> "workflow"
                ExternalPayloadStorage.PayloadType.TASK_INPUT, ExternalPayloadStorage.PayloadType.TASK_OUTPUT -> "tasks"
                else -> throw ConductorClientException("Invalid payload type: $payloadType for operation: $operation")
            }

        var storageLocation: ExternalStorageLocation? = null
        launch {
            val storageLocationDeferred = async {
                clientBase.getForEntity(
                    "$uri/externalstoragelocation", arrayOf(
                        "path",
                        path,
                        "operation",
                        operation.toString(),
                        "payloadType",
                        payloadType.toString()
                    ),
                    ExternalStorageLocation::class.java
                )
            }
            storageLocation = storageLocationDeferred.await()
        }

        return storageLocation ?: throw ConductorClientException("ExternalStorageLocation not found")

    }

    /**
     * Uploads the payload to the uri specified.
     *
     * @param uri the location to which the object is to be uploaded
     * @param payload an [InputStream] containing the json payload which is to be uploaded
     * @param payloadSize the size of the json payload in bytes
     * @throws ConductorClientException if the upload fails due to an invalid path or an error from
     * external storage
     */
    override fun upload(uri: String, payload: InputStream, payloadSize: Long) {
        var connection: HttpURLConnection? = null
        try {
            val url = URI(uri).toURL()
            connection = url.openConnection() as HttpURLConnection
            connection.doOutput = true
            connection.requestMethod = "PUT"
            BufferedOutputStream(connection.outputStream).use { bufferedOutputStream ->
                val count = IOUtils.copy(payload, bufferedOutputStream)
                bufferedOutputStream.flush()
                // Check the HTTP response code
                val responseCode = connection.responseCode
                if (Response.Status.fromStatusCode(responseCode).family
                    != Response.Status.Family.SUCCESSFUL
                ) {
                    val errorMsg = "Unable to upload. Response code: $responseCode"
                    LOGGER.error(errorMsg)
                    throw ConductorClientException(errorMsg)
                }
                LOGGER.debug(
                    "Uploaded {} bytes to uri: {}, with HTTP response code: {}",
                    count,
                    uri,
                    responseCode
                )
            }
        } catch (e: URISyntaxException) {
            val errorMsg = "Invalid path specified: $uri"
            LOGGER.error(errorMsg, e)
            throw ConductorClientException(errorMsg, e)
        } catch (e: MalformedURLException) {
            val errorMsg = "Invalid path specified: $uri"
            LOGGER.error(errorMsg, e)
            throw ConductorClientException(errorMsg, e)
        } catch (e: IOException) {
            val errorMsg = "Error uploading to path: $uri"
            LOGGER.error(errorMsg, e)
            throw ConductorClientException(errorMsg, e)
        } finally {
            connection?.disconnect()
            try {
                if (payload != null) {
                    payload.close()
                }
            } catch (e: IOException) {
                LOGGER.warn("Unable to close inputstream when uploading to uri: {}", uri)
            }
        }
    }

    /**
     * Downloads the payload from the given uri.
     *
     * @param uri the location from where the object is to be downloaded
     * @return an inputstream of the payload in the external storage
     * @throws ConductorClientException if the download fails due to an invalid path or an error
     * from external storage
     */
    override fun download(uri: String): InputStream {
        var connection: HttpURLConnection? = null
        var errorMsg: String
        try {
            val url = URI(uri).toURL()
            connection = url.openConnection() as HttpURLConnection
            connection.doOutput = false

            // Check the HTTP response code
            val responseCode = connection.responseCode
            if (responseCode == HttpURLConnection.HTTP_OK) {
                LOGGER.debug(
                    "Download completed with HTTP response code: {}",
                    connection.responseCode
                )
                return org.apache.commons.io.IOUtils.toBufferedInputStream(
                    connection.inputStream
                )
            }
            errorMsg = "Unable to download. Response code: $responseCode"
            LOGGER.error(errorMsg)
            throw ConductorClientException(errorMsg)
        } catch (e: URISyntaxException) {
            errorMsg = "Invalid uri specified: $uri"
            LOGGER.error(errorMsg, e)
            throw ConductorClientException(errorMsg, e)
        } catch (e: MalformedURLException) {
            errorMsg = "Invalid uri specified: $uri"
            LOGGER.error(errorMsg, e)
            throw ConductorClientException(errorMsg, e)
        } catch (e: IOException) {
            errorMsg = "Error downloading from uri: $uri"
            LOGGER.error(errorMsg, e)
            throw ConductorClientException(errorMsg, e)
        } finally {
            connection?.disconnect()
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(PayloadStorage::class.java)
    }
}
