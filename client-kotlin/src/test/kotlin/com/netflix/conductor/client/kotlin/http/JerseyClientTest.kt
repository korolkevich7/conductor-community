package com.netflix.conductor.client.kotlin.http

import com.nhaarman.mockito_kotlin.mock
import java.net.URI

abstract class JerseyClientTest {
    protected val ROOT_URL = "dummyroot/"

    protected val requestHandler: JerseyClientRequestHandler = mock()

    fun createURI(path: String): URI = URI.create(ROOT_URL + path)
}