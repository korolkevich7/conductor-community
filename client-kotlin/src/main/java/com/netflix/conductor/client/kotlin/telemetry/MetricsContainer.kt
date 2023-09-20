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

interface MetricOperation {
    val name: String
    val additionalTags: List<String>
}

data class TimerMetric(
    override val name: String,
    override val additionalTags: List<String>,
    val amount: Duration): MetricOperation

data class CounterMetric(
    override val name: String,
    override val additionalTags: List<String>,
    val incrementValue: Long = 1): MetricOperation

data class GaugeMetric(
    override val name: String,
    override val additionalTags: List<String>,
    val payloadSize: Long): MetricOperation

@OptIn(DelicateCoroutinesApi::class)
object MetricsContainer {
    private const val TASK_TYPE = "taskType"
    private const val WORKFLOW_TYPE = "workflowType"
    private const val WORKFLOW_VERSION = "version"
    private const val EXCEPTION = "exception"
    private const val ENTITY_NAME = "entityName"
    private const val OPERATION = "operation"
    private const val PAYLOAD_TYPE = "payload_type"
    private const val TASK_EXECUTION_QUEUE_FULL = "task_execution_queue_full"
    private const val TASK_POLL_ERROR = "task_poll_error"
    private const val TASK_PAUSED = "task_paused"
    private const val TASK_EXECUTE_ERROR = "task_execute_error"
    private const val TASK_ACK_FAILED = "task_ack_failed"
    private const val TASK_ACK_ERROR = "task_ack_error"
    private const val TASK_UPDATE_ERROR = "task_update_error"
    private const val TASK_LEASE_EXTEND_ERROR = "task_lease_extend_error"
    private const val TASK_LEASE_EXTEND_COUNTER = "task_lease_extend_counter"
    private const val TASK_POLL_COUNTER = "task_poll_counter"
    private const val TASK_EXECUTE_TIME = "task_execute_time"
    private const val TASK_POLL_TIME = "task_poll_time"
    private const val TASK_RESULT_SIZE = "task_result_size"
    private const val WORKFLOW_INPUT_SIZE = "workflow_input_size"
    private const val EXTERNAL_PAYLOAD_USED = "external_payload_used"
    private const val WORKFLOW_START_ERROR = "workflow_start_error"
    private const val THREAD_UNCAUGHT_EXCEPTION = "thread_uncaught_exceptions"
    private val REGISTRY: Registry = Spectator.globalRegistry()

    private val METRIC_CHANNEL: Channel<MetricOperation> = Channel(capacity = 80) {  }

    private val TIMERS: MutableMap<String, Timer> = HashMap()
    private val COUNTERS: MutableMap<String, Counter> = HashMap()
    private val GAUGES: MutableMap<String, AtomicLong> = HashMap()
    private val CLASS_NAME = MetricsContainer::class.java.simpleName

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

    suspend fun recordPollTimer(taskType: String, amount: Duration) = recordTaskTimer(TASK_POLL_TIME, taskType, amount)

    suspend fun recordExecutionTimer(taskType: String, amount: Duration) = recordTaskTimer(TASK_EXECUTE_TIME, taskType, amount)

    private suspend fun recordTaskTimer(operationName: String, taskType: String, amount: Duration) {
        METRIC_CHANNEL.send(
            TimerMetric(
                operationName,
                listOf(TASK_TYPE, taskType, "unit", TimeUnit.MILLISECONDS.name),
                amount = amount))
    }

    private suspend fun incrementCount(name: String, vararg additionalTags: String, incrementValue: Long = 1) {
        METRIC_CHANNEL.send(
            CounterMetric(
                name,
                listOf(*additionalTags),
                incrementValue = incrementValue)
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

    private suspend fun updateGaugeValue(name: String, vararg additionalTags: String, payloadSize: Long) {
        METRIC_CHANNEL.send(
            GaugeMetric(
                name,
                listOf(*additionalTags),
                payloadSize = payloadSize
            )
        )
    }

    suspend fun incrementTaskExecutionQueueFullCount(taskType: String) {
        incrementCount(TASK_EXECUTION_QUEUE_FULL, TASK_TYPE, taskType)
    }

    suspend fun incrementUncaughtExceptionCount() {
        incrementCount(THREAD_UNCAUGHT_EXCEPTION)
    }

    suspend fun incrementTaskPollErrorCount(taskType: String, e: Exception) {
        incrementCount(
            TASK_POLL_ERROR, TASK_TYPE, taskType, EXCEPTION, e.javaClass.simpleName
        )
    }

    suspend fun incrementTaskPausedCount(taskType: String) {
        incrementCount(TASK_PAUSED, TASK_TYPE, taskType)
    }

    suspend fun incrementTaskExecutionErrorCount(taskType: String, e: Throwable) {
        incrementCount(
            TASK_EXECUTE_ERROR, TASK_TYPE, taskType, EXCEPTION, e.javaClass.simpleName
        )
    }

    suspend fun incrementTaskAckFailedCount(taskType: String) {
        incrementCount(TASK_ACK_FAILED, TASK_TYPE, taskType)
    }

    suspend fun incrementTaskAckErrorCount(taskType: String, e: Exception) {
        incrementCount(
            TASK_ACK_ERROR, TASK_TYPE, taskType, EXCEPTION, e.javaClass.simpleName
        )
    }

    suspend fun recordTaskResultPayloadSize(taskType: String, payloadSize: Long) {
        updateGaugeValue(TASK_RESULT_SIZE, TASK_TYPE, taskType, payloadSize = payloadSize)
    }

    suspend fun incrementTaskUpdateErrorCount(taskType: String, t: Throwable) {
        incrementCount(
            TASK_UPDATE_ERROR, TASK_TYPE, taskType, EXCEPTION, t.javaClass.simpleName
        )
    }

    suspend fun incrementTaskLeaseExtendErrorCount(taskType: String, t: Throwable) {
        incrementCount(
            TASK_LEASE_EXTEND_ERROR,
            TASK_TYPE,
            taskType,
            EXCEPTION,
            t.javaClass.simpleName
        )
    }

    suspend fun incrementTaskLeaseExtendCount(taskType: String, taskCount: Int) {
        incrementCount(
            TASK_LEASE_EXTEND_COUNTER,
            TASK_TYPE,
            taskType,
            incrementValue = taskCount.toLong()
        )
    }

    suspend fun incrementTaskPollCount(taskType: String, taskCount: Int) {
        incrementCount(
            TASK_POLL_COUNTER,
            TASK_TYPE,
            taskType,
            incrementValue= taskCount.toLong())
    }

    suspend fun recordWorkflowInputPayloadSize(
        workflowType: String, version: String, payloadSize: Long) {
        updateGaugeValue(
            WORKFLOW_INPUT_SIZE,
            WORKFLOW_TYPE,
            workflowType,
            WORKFLOW_VERSION,
            version,
            payloadSize = payloadSize)
    }

    suspend fun incrementExternalPayloadUsedCount(
        name: String, operation: String, payloadType: String
    ) {
        incrementCount(
            EXTERNAL_PAYLOAD_USED,
            ENTITY_NAME,
            name,
            OPERATION,
            operation,
            PAYLOAD_TYPE,
            payloadType
        )
    }

    suspend fun incrementWorkflowStartErrorCount(workflowType: String, t: Throwable) {
        incrementCount(
            WORKFLOW_START_ERROR,
            WORKFLOW_TYPE,
            workflowType,
            EXCEPTION,
            t.javaClass.simpleName
        )
    }
}

fun Timer.record(amount: Duration) = record(amount.inWholeNanoseconds, TimeUnit.NANOSECONDS)