package com.netflix.conductor.client.kotlin.exception

import com.netflix.conductor.common.validation.ErrorResponse
import com.netflix.conductor.common.validation.ValidationError


/** Client exception thrown from Conductor api clients.  */
class ConductorClientException : RuntimeException {
    var status = 0
    var instance: String? = null
    var code: String? = null
    var isRetryable = false
    var validationErrors: List<ValidationError>? = null

    constructor(message: String?) : super(message)

    constructor(message: String?, cause: Throwable?) : super(message, cause)

    constructor(status: Int, message: String?) : super(message) {
        this.status = status
    }

    constructor(status: Int, errorResponse: ErrorResponse) : super(errorResponse.message) {
        this.status = status
        isRetryable = errorResponse.isRetryable
        this.code = errorResponse.code
        instance = errorResponse.instance
        validationErrors = errorResponse.validationErrors
    }

    override fun toString(): String {
        val builder = StringBuilder()
        builder.append(javaClass.name).append(": ")
        if (message != null) {
            builder.append(message)
        }
        if (status > 0) {
            builder.append(" {status=").append(status)
            if (this.code != null) {
                builder.append(", code='").append(code).append("'")
            }
            builder.append(", retryable: ").append(isRetryable)
        }
        if (instance != null) {
            builder.append(", instance: ").append(instance)
        }
        if (validationErrors != null) {
            builder.append(", validationErrors: ").append(validationErrors.toString())
        }
        builder.append("}")
        return builder.toString()
    }
}
