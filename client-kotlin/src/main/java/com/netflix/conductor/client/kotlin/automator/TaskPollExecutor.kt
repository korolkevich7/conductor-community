package com.netflix.conductor.client.kotlin.automator

import com.netflix.appinfo.InstanceInfo
import com.netflix.conductor.client.kotlin.config.PropertyFactory
import com.netflix.conductor.client.kotlin.http.TaskClient
import com.netflix.conductor.client.kotlin.telemetry.MetricsContainer
import com.netflix.conductor.client.kotlin.worker.Worker
import com.netflix.conductor.common.metadata.tasks.Task
import com.netflix.conductor.common.metadata.tasks.TaskResult
import com.netflix.discovery.EurekaClient
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.*
import java.io.PrintWriter
import java.io.StringWriter
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime
import kotlin.time.TimeSource.Monotonic.markNow
import kotlin.time.measureTimedValue

private val logger = KotlinLogging.logger {}

/**
 * Manages the threadpool used by the workers for execution and server communication (polling and
 * task update).
 */
class TaskPollExecutor(
        private val eurekaClient: EurekaClient?,
        private val taskClient: TaskClient,
        private val updateRetryCount: Int,
        private val taskToDomain: Map<String, String>,
        private val leaseThreadCount: Int
) {
    @OptIn(ExperimentalCoroutinesApi::class)
    private val leaseExtendDispatcher = Dispatchers.IO.limitedParallelism(leaseThreadCount)

    init {
        //TODO Coroutine monitor???
//        ThreadPoolMonitor.attach(REGISTRY, executorService, workerNamePrefix)
    }

    @OptIn(ExperimentalTime::class)
    suspend fun poll(worker: Worker): List<Task> {
        requireNotNull(worker.identity) { "Worker identity cannot be null" }

        val discoveryOverride = isDiscoveryOverride(worker.taskDefName)
        if (eurekaClient != null && eurekaClient.instanceRemoteStatus != InstanceInfo.InstanceStatus.UP
            && !discoveryOverride
        ) {
            logger.debug { "Instance is NOT UP in discovery - will not poll" }
            return emptyList()
        }

        if (worker.paused()) {
            MetricsContainer.incrementTaskPausedCount(worker.taskDefName)
            logger.debug { "Worker ${worker.javaClass} has been paused. Not polling anymore!" }
            return emptyList()
        }

        val taskType: String = worker.taskDefName
        val domain = taskDomain(taskType)
        logger.debug { "Polling task of type: $taskType in domain: '$domain'" }
        val (tasks, duration) = measureTimedValue {
            taskClient.batchPollTasksInDomain(
                taskType,
                domain,
                worker.identity!!,
                worker.batchPollCount,
                worker.batchPollTimeoutInMS
            )
        }
        MetricsContainer.recordPollTimer(taskType, duration)
        return tasks
    }

    fun shutdown(timeout: Int) {
        TODO("cancel leaseScope instead")
        //leaseExtendMap.clear()
    }

//todo coroutine uncaughtExceptionHandler
@OptIn(DelicateCoroutinesApi::class)
val handler = CoroutineExceptionHandler{ context, exception ->
    GlobalScope.launch {
        MetricsContainer.incrementUncaughtExceptionCount()
    }
    logger.error(exception) { "Uncaught exception. Context $context will exit now" }
}


    @OptIn(ExperimentalTime::class)
    suspend fun processTask(worker: Worker, task: Task): Pair<Task, TaskResult> {
        MetricsContainer.incrementTaskPollCount(task.taskType, 1)
        val domain = taskDomain(task.taskType)
        val taskResponseTimeoutSeconds = task.responseTimeoutSeconds
        logger.debug {
            "Polled task: ${task.taskId} of type: ${task.taskType} in domain: '$domain', from worker: ${worker.identity}" }
        val responseTimeout = markNow().elapsedNow().minus(task.startTime.milliseconds)
        //TODO
        return withTimeout(responseTimeout) {
            val deferred = async {
                executeTask(worker, task)
            }
            val interval = (taskResponseTimeoutSeconds * LEASE_EXTEND_DURATION_FACTOR).seconds
            var extendLeaseJob: Job? = null
            if (taskResponseTimeoutSeconds > 0 && worker.leaseExtendEnabled) {
                extendLeaseJob = launchExtendLease(interval, task, deferred)
            }
            val result = deferred.await()
            logger.debug {
                "Task:${task.taskId} of type:${task.taskDefName} finished processing with status:${result.first.status}" }
            extendLeaseJob?.cancel()
            result
        }
        //TODO: should update after it
    }

    @OptIn(ExperimentalTime::class)
    suspend fun executeTask(worker: Worker, task: Task): Pair<Task, TaskResult> {
        logger.debug {
            "Executing task: ${task.taskId} of type: ${task.taskDefName} in worker: ${worker.javaClass.simpleName} at ${worker.identity}" }
        val (result, duration) = measureTimedValue {
            _executeTask(worker, task)
        }
        MetricsContainer.recordExecutionTimer(worker.taskDefName, duration)
        logger.debug {
            "Task: ${task.taskId} executed by worker: ${worker.javaClass.simpleName} at ${worker.identity} with status: ${result.second.status}" }
        return result
    }

    private suspend fun _executeTask(worker: Worker, task: Task): Pair<Task, TaskResult> {
        var result: TaskResult? = null
        try {
            result = worker.execute(task)
            result.workflowInstanceId = task.workflowInstanceId
            result.taskId = task.taskId
            result.workerId = worker.identity
        } catch (e: Exception) {
            logger.error(e) {
                "Unable to execute task: ${task.taskId} of type: ${task.taskDefName}" }
            if (result == null) {
                task.status = Task.Status.FAILED
                result = TaskResult(task)
            }
            result = handleException(e, result, worker, task)
        }
        return task to result!!
    }

    private suspend fun updateTaskResult(
            count: Int,
            task: Task,
            result: TaskResult,
            worker: Worker
    ) {
        try {
            // upload if necessary
            val externalStorageLocation = retryIO(count) {
                upload(result, task.taskType)
            }
            externalStorageLocation?.let {
                result.externalOutputPayloadStoragePath = externalStorageLocation
                result.outputData = null
            }
            retryIO(times = count) {
                taskClient.updateTask(result)
            }
        } catch (e: Exception) {
            worker.onErrorUpdate(task)
            MetricsContainer.incrementTaskUpdateErrorCount(
                    worker.taskDefName,
                    e
            )
            logger.error(e) { "Failed to update result: $result for task: ${task.taskDefName} in worker: ${worker.identity}" }
        }
    }

    private suspend fun upload(result: TaskResult, taskType: String): String? {
        return try {
            taskClient.evaluateAndUploadLargePayload(result.outputData, taskType)
        } catch (iae: IllegalArgumentException) {
            result.reasonForIncompletion = iae.message
            result.outputData = null
            result.status = TaskResult.Status.FAILED_WITH_TERMINAL_ERROR
            null
        }
    }

    private suspend fun <T> retryIO(
        times: Int,
        initialDelay: Long = 500, // 0.5 second
        maxDelay: Long = initialDelay,
        factor: Double = 1.0,
        block: suspend () -> T): T
    {
        var currentDelay = initialDelay
        repeat(times - 1) {
            try {
                return block()
            } catch (ex: Exception) {
                logger.warn(ex) { "Operation failed with $it attempt" }
            }
            delay(currentDelay)
            currentDelay = (currentDelay * factor).toLong().coerceAtMost(maxDelay)
        }
        return block() // last attempt
    }

    private suspend fun handleException(
            t: Throwable,
            result: TaskResult,
            worker: Worker,
            task: Task
    ): TaskResult {
        logger.error(t) { "Error while executing task $task" }
        MetricsContainer.incrementTaskExecutionErrorCount(
                worker.taskDefName,
                t
        )
        result.status = TaskResult.Status.FAILED
        result.reasonForIncompletion = "Error while executing the task: $t"
        result.log(t)
        return result
    }

    suspend fun updateTaskResult(task: Task, result: TaskResult, worker: Worker) {
        updateTaskResult(updateRetryCount, task, result, worker)
    }

    @OptIn(ExperimentalTime::class)
    suspend fun launchExtendLease(interval: Duration, task: Task, taskDeferred: Job): Job =
        coroutineScope {
            timer(interval, interval, leaseExtendDispatcher) {
                extendLease(task, taskDeferred)
            }
        }

    private suspend fun extendLease(task: Task, taskDeferred: Job) {
        if (taskDeferred.isCompleted) {
            logger.warn {
                "Task processing for ${task.taskId} completed, but its lease extend was not cancelled" }
            return
        }
        logger.info { "Attempting to extend lease for ${task.taskId}" }
        try {
            val result = TaskResult(task)
            result.isExtendLease = true
            retryIO(times = LEASE_EXTEND_RETRY_COUNT) {
                taskClient.updateTask(result)
            }
            MetricsContainer.incrementTaskLeaseExtendCount(
                task.taskDefName,
                1
            )
        } catch (e: Exception) {
            MetricsContainer.incrementTaskLeaseExtendErrorCount(
                task.taskDefName,
                e
            )
            logger.error(e) { "Failed to extend lease for ${task.taskId}" }
        }
    }

    private fun isDiscoveryOverride(taskDefName: String): Boolean {
        return PropertyFactory.getBoolean(taskDefName, OVERRIDE_DISCOVERY)
            ?: PropertyFactory.getBoolean(ALL_WORKERS, OVERRIDE_DISCOVERY, false)
    }

    private fun taskDomain(taskType: String): String? {
        return PropertyFactory.getString(taskType, DOMAIN)
            ?: (PropertyFactory.getString(ALL_WORKERS, DOMAIN)
                ?: taskToDomain[taskType])
    }

    companion object {
        private const val DOMAIN = "domain"
        private const val OVERRIDE_DISCOVERY = "pollOutOfDiscovery"
        private const val ALL_WORKERS = "all"
        private const val LEASE_EXTEND_RETRY_COUNT = 3
        private const val LEASE_EXTEND_DURATION_FACTOR = 0.8
    }
}

fun TaskResult.log(t: Throwable) {
    val stringWriter = StringWriter()
    t.printStackTrace(PrintWriter(stringWriter))
    this.log(stringWriter.toString())
}