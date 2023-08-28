package com.netflix.conductor.client.kotlin.telemetry

import com.netflix.spectator.api.Timer
import java.util.concurrent.TimeUnit
import kotlin.time.Duration

fun Timer.record(amount: Duration) = record(amount.inWholeNanoseconds, TimeUnit.NANOSECONDS)