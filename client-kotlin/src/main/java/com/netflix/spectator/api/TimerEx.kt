package com.netflix.spectator.api

import java.util.concurrent.TimeUnit
import kotlin.time.Duration

internal fun Timer.record(amount: Duration) = record(amount.inWholeNanoseconds, TimeUnit.NANOSECONDS)

