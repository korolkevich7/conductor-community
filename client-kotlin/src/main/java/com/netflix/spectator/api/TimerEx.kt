package com.netflix.spectator.api

import kotlin.time.Duration
import kotlin.time.toJavaDuration

fun Timer.record(amount: Duration) = record(amount.toJavaDuration())

