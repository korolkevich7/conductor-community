package com.netflix.conductor.client.kotlin.exception

import com.netflix.conductor.common.validation.ErrorResponse
import com.netflix.conductor.common.validation.ValidationError


/** Client exception thrown from Conductor api clients.  */
open class ConductorClientException : RuntimeException {
    var status = 0
    var instance: String? = null
    var code: String? = null
    var isRetryable = false
    var validationErrors: List<ValidationError> = emptyList()

    constructor(message: String) : super(message)

    constructor(message: String, cause: Throwable) : super(message, cause)

    constructor(status: Int, message: String) : super(message) {
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
        message?.also { builder.append(it) }
        if (status > 0) {
            builder.append(" {status=").append(status)
            code?.also { builder.append(", code='").append(it).append("'") }
            builder.append(", retryable: ").append(isRetryable)
        }
        instance?.also { builder.append(", instance: ").append(it) }
        if (validationErrors.isNotEmpty()) {
            builder.append(", validationErrors: ").append(validationErrors.toString())
        }
        builder.append("}")
        return builder.toString()
    }
}
