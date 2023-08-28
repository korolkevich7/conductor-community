package com.netflix.conductor.client.kotlin.http

import com.netflix.conductor.common.run.ExternalStorageLocation
import java.io.InputStream


interface PayloadStorage {
    enum class Operation {
        READ,
        WRITE
    }

    enum class PayloadType {
        WORKFLOW_INPUT,
        WORKFLOW_OUTPUT,
        TASK_INPUT,
        TASK_OUTPUT
    }

    /**
     * Obtain a uri used to store/access a json payload in external storage.
     *
     * @param operation the type of [Operation] to be performed with the uri
     * @param payloadType the [PayloadType] that is being accessed at the uri
     * @param path (optional) the relative path for which the external storage location object is to
     * be populated. If path is not specified, it will be computed and populated.
     * @return a [ExternalStorageLocation] object which contains the uri and the path for the
     * json payload
     */
    suspend fun getLocation(operation: Operation?, payloadType: PayloadType?, path: String?): ExternalStorageLocation

    /**
     * Upload a json payload to the specified external storage location.
     *
     * @param path the location to which the object is to be uploaded
     * @param payload an [InputStream] containing the json payload which is to be uploaded
     * @param payloadSize the size of the json payload in bytes
     */
    suspend fun upload(path: String, payload: ByteArray, payloadSize: Long)

    /**
     * Download the json payload from the specified external storage location.
     *
     * @param path the location from where the object is to be downloaded
     * @return an [InputStream] of the json payload at the specified location
     */
    suspend fun download(path: String): ByteArray?
}
