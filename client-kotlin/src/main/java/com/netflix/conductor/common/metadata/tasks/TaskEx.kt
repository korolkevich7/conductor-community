package com.netflix.conductor.common.metadata.tasks

import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

val Task.responseTimeoutFromNow: Duration
    get() {
        if (responseTimeoutSeconds == 0L) return Duration.INFINITE
        val wastedTime = System.currentTimeMillis() - startTime
        return responseTimeoutSeconds.seconds - wastedTime.milliseconds
    }