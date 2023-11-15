package com.netflix.conductor.client.kotlin.http.ktor

import io.ktor.client.call.body
import io.ktor.client.statement.HttpResponse
import io.ktor.http.HttpStatusCode

suspend inline fun <reified T> HttpResponse.bodyListSafe(): List<T> {
    if (call.response.status == HttpStatusCode.NoContent) return emptyList()
    return body()
}

suspend inline fun <reified T> HttpResponse.bodySafe(): T? {
    if (call.response.status == HttpStatusCode.NoContent) return null
    return body()
}