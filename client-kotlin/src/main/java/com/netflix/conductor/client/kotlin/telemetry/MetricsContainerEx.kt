package com.netflix.conductor.client.kotlin.telemetry

import kotlin.time.Duration

const val TASK_EXECUTION_QUEUE_FULL = "task_execution_queue_full"
const val WORKFLOW_TYPE = "workflowType"
const val WORKFLOW_VERSION = "version"
const val EXCEPTION = "exception"
const val ENTITY_NAME = "entityName"
const val OPERATION = "operation"
const val PAYLOAD_TYPE = "payload_type"
const val TASK_POLL_ERROR = "task_poll_error"
const val TASK_PAUSED = "task_paused"
const val TASK_EXECUTE_ERROR = "task_execute_error"
const val TASK_ACK_FAILED = "task_ack_failed"
const val TASK_ACK_ERROR = "task_ack_error"
const val TASK_UPDATE_ERROR = "task_update_error"
const val TASK_LEASE_EXTEND_ERROR = "task_lease_extend_error"
const val TASK_LEASE_EXTEND_COUNTER = "task_lease_extend_counter"
const val TASK_POLL_COUNTER = "task_poll_counter"
const val TASK_EXECUTE_TIME = "task_execute_time"
const val TASK_POLL_TIME = "task_poll_time"
const val TASK_RESULT_SIZE = "task_result_size"
const val WORKFLOW_INPUT_SIZE = "workflow_input_size"
const val EXTERNAL_PAYLOAD_USED = "external_payload_used"
const val WORKFLOW_START_ERROR = "workflow_start_error"
const val THREAD_UNCAUGHT_EXCEPTION = "thread_uncaught_exceptions"

suspend fun MetricsContainer.recordPollTimer(taskType: String, amount: Duration) =
    recordTaskTimer(TASK_POLL_TIME, taskType, amount)

suspend fun MetricsContainer.recordExecutionTimer(taskType: String, amount: Duration) =
    recordTaskTimer(TASK_EXECUTE_TIME, taskType, amount)


suspend fun MetricsContainer.incrementTaskExecutionQueueFullCount(taskType: String) {
    incrementCount(TASK_EXECUTION_QUEUE_FULL, TASK_TYPE, taskType)
}

suspend fun MetricsContainer.incrementUncaughtExceptionCount() {
    incrementCount(THREAD_UNCAUGHT_EXCEPTION)
}

suspend fun MetricsContainer.incrementTaskPollErrorCount(taskType: String, e: Exception) {
    incrementCount(
        TASK_POLL_ERROR,
        TASK_TYPE,
        taskType,
        EXCEPTION,
        e::class.simpleName ?: "Undefined"
    )
}

suspend fun MetricsContainer.incrementTaskPausedCount(taskType: String) {
    incrementCount(TASK_PAUSED, TASK_TYPE, taskType)
}

suspend fun MetricsContainer.incrementTaskExecutionErrorCount(taskType: String, e: Throwable) {
    incrementCount(
        TASK_EXECUTE_ERROR,
        TASK_TYPE,
        taskType,
        EXCEPTION,
        e::class.simpleName ?: "Undefined"
    )
}

suspend fun MetricsContainer.incrementTaskAckFailedCount(taskType: String) {
    incrementCount(TASK_ACK_FAILED, TASK_TYPE, taskType)
}

suspend fun MetricsContainer.incrementTaskAckErrorCount(taskType: String, e: Exception) {
    incrementCount(
        TASK_ACK_ERROR,
        TASK_TYPE,
        taskType,
        EXCEPTION,
        e::class.simpleName ?: "Undefined"
    )
}

suspend fun MetricsContainer.incrementTaskUpdateErrorCount(taskType: String, t: Throwable) {
    incrementCount(
        TASK_UPDATE_ERROR,
        TASK_TYPE,
        taskType,
        EXCEPTION,
        t::class.simpleName ?: "Undefined"
    )
}

suspend fun MetricsContainer.incrementTaskLeaseExtendErrorCount(taskType: String, t: Throwable) {
    incrementCount(
        TASK_LEASE_EXTEND_ERROR,
        TASK_TYPE,
        taskType,
        EXCEPTION,
        t::class.simpleName ?: "Undefined"
    )
}

suspend fun MetricsContainer.incrementTaskLeaseExtendCount(taskType: String, taskCount: Int) {
    incrementCount(
        TASK_LEASE_EXTEND_COUNTER,
        TASK_TYPE,
        taskType,
        incrementValue = taskCount.toLong()
    )
}

suspend fun MetricsContainer.incrementTaskPollCount(taskType: String, taskCount: Int) {
    incrementCount(
        TASK_POLL_COUNTER,
        TASK_TYPE,
        taskType,
        incrementValue = taskCount.toLong()
    )
}

suspend fun MetricsContainer.incrementExternalPayloadUsedCount(
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

suspend fun MetricsContainer.incrementWorkflowStartErrorCount(workflowType: String, t: Throwable) {
    incrementCount(
        WORKFLOW_START_ERROR,
        WORKFLOW_TYPE,
        workflowType,
        EXCEPTION,
        t::class.simpleName ?: "Undefined"
    )
}

suspend fun MetricsContainer.recordTaskResultPayloadSize(taskType: String, payloadSize: Long) {
    updateGaugeValue(TASK_RESULT_SIZE, TASK_TYPE, taskType, payloadSize = payloadSize)
}

suspend fun MetricsContainer.recordWorkflowInputPayloadSize(
    workflowType: String, version: String, payloadSize: Long) {
    updateGaugeValue(
        WORKFLOW_INPUT_SIZE,
        WORKFLOW_TYPE,
        workflowType,
        WORKFLOW_VERSION,
        version,
        payloadSize = payloadSize
    )
}

