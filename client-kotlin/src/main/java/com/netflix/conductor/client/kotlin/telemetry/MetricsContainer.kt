/*
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.client.kotlin.telemetry

import com.netflix.spectator.api.*
import com.netflix.spectator.api.patterns.PolledMeter
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration

internal sealed interface MetricOperation {
    val name: String
    val additionalTags: List<String>
}

internal data class TimerMetric(
    override val name: String,
    override val additionalTags: List<String>,
    val amount: Duration): MetricOperation

internal data class CounterMetric(
    override val name: String,
    override val additionalTags: List<String>,
    val incrementValue: Long = 1): MetricOperation

internal data class GaugeMetric(
    override val name: String,
    override val additionalTags: List<String>,
    val payloadSize: Long): MetricOperation

@OptIn(DelicateCoroutinesApi::class)
object MetricsContainer {
    const val TASK_TYPE = "taskType"

    private val REGISTRY: Registry = Spectator.globalRegistry()

    private val METRIC_CHANNEL: Channel<MetricOperation> = Channel(capacity = 80) {  }

    private val TIMERS: MutableMap<String, Timer> = HashMap()
    private val COUNTERS: MutableMap<String, Counter> = HashMap()
    private val GAUGES: MutableMap<String, AtomicLong> = HashMap()

    private val CLASS_NAME = MetricsContainer::class.qualifiedName

    init {
        val metricDispatcher = newSingleThreadContext("metrics dispatcher")
        val workerScope = CoroutineScope(metricDispatcher + SupervisorJob())
        workerScope.launch {
            for (operation in METRIC_CHANNEL) {
                when (operation) {
                    is TimerMetric -> getTimer(operation.name, operation.additionalTags).record(operation.amount)
                    is CounterMetric -> getCounter(operation.name, operation.additionalTags).increment(operation.incrementValue)
                    is GaugeMetric -> getGauge(operation.name, operation.additionalTags).getAndSet(operation.payloadSize)
                }
            }
        }
    }
    suspend fun recordTaskTimer(operationName: String, taskType: String, amount: Duration) {
        METRIC_CHANNEL.send(
            TimerMetric(
                operationName,
                listOf(TASK_TYPE, taskType, "unit", TimeUnit.MILLISECONDS.name),
                amount = amount))
    }

    suspend fun incrementCount(name: String, vararg additionalTags: String, incrementValue: Long = 1) {
        METRIC_CHANNEL.send(
            CounterMetric(
                name,
                listOf(*additionalTags),
                incrementValue = incrementValue)
        )
    }

    suspend fun updateGaugeValue(name: String, vararg additionalTags: String, payloadSize: Long) {
        METRIC_CHANNEL.send(
            GaugeMetric(
                name,
                listOf(*additionalTags),
                payloadSize = payloadSize
            )
        )
    }

    private fun spectatorKey(name: String, additionalTags: List<String>): String =
        "$CLASS_NAME.$name.${additionalTags.joinToString(separator = ",")}"

    private fun getTimer(name: String, additionalTags: List<String>): Timer {
        val key = spectatorKey(name, additionalTags)
        return TIMERS.getOrPut(key) {
            val tagList = getTags(additionalTags)
            REGISTRY.timer(name, tagList)
        }
    }

    private fun getCounter(name: String, additionalTags: List<String>): Counter {
        val key = spectatorKey(name, additionalTags)
        return COUNTERS.getOrPut(key) {
            val tags = getTags(additionalTags)
            REGISTRY.counter(name, tags)
        }
    }

    private fun getGauge(name: String, additionalTags: List<String>): AtomicLong {
        val key = spectatorKey(name, additionalTags)
        return GAUGES.getOrPut(key) {
            val id = REGISTRY.createId(name, getTags(additionalTags))
            PolledMeter.using(REGISTRY).withId(id).monitorValue(AtomicLong(0))
        }
    }

    private fun getTags(additionalTags: List<String>): List<Tag> {
        val tagList: MutableList<Tag> = mutableListOf()
        tagList.add(BasicTag("class", CLASS_NAME))
        var j = 0
        while (j < additionalTags.size - 1) {
            tagList.add(BasicTag(additionalTags[j], additionalTags[j + 1]))
            j++
            j++
        }
        return tagList
    }
}