package com.netflix.conductor.client.kotlin.automator

import com.netflix.appinfo.InstanceInfo
import com.netflix.conductor.client.kotlin.config.PropertyFactory
import com.netflix.conductor.client.kotlin.http.TaskClient
import com.netflix.conductor.client.kotlin.telemetry.MetricsContainer
import com.netflix.conductor.client.kotlin.worker.Worker
import com.netflix.conductor.common.metadata.tasks.Task
import com.netflix.conductor.common.metadata.tasks.TaskResult
import com.netflix.discovery.EurekaClient
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.Spectator
import com.netflix.spectator.api.patterns.ThreadPoolMonitor
import kotlinx.coroutines.*
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.concurrent.BasicThreadFactory
import org.apache.commons.lang3.time.StopWatch
import org.slf4j.LoggerFactory
import java.io.PrintWriter
import java.io.StringWriter
import java.lang.Runnable
import java.util.*
import java.util.concurrent.*
import kotlin.coroutines.CoroutineContext
import kotlin.math.roundToLong

/**
 * Manages the threadpool used by the workers for execution and server communication (polling and
 * task update).
 */
internal class TaskPollExecutor(
    private val eurekaClient: EurekaClient?,
    private val taskClient: TaskClient,
    private val updateRetryCount: Int,
    private val taskToDomain: Map<String, String>,
    workerNamePrefix: String?,
    taskThreadCount: Map<String, Int>
) : CoroutineScope {
    private val executorService: ExecutorService
    private val pollingSemaphoreMap: MutableMap<String, PollingSemaphore>
    private val leaseExtendExecutorService: ScheduledExecutorService
    var leaseExtendMap: MutableMap<String, ScheduledFuture<*>> = HashMap()

    fun pollAndExecute(worker: Worker) {
        val discoveryOverride =
            PropertyFactory.getBoolean(
                worker.taskDefName, OVERRIDE_DISCOVERY, null
            ) ?: PropertyFactory.getBoolean(
                ALL_WORKERS, OVERRIDE_DISCOVERY, false
            )
        if (eurekaClient != null && eurekaClient.instanceRemoteStatus != InstanceInfo.InstanceStatus.UP
            && !discoveryOverride
        ) {
            LOGGER.debug("Instance is NOT UP in discovery - will not poll")
            return
        }
        if (worker.paused()) {
            MetricsContainer.incrementTaskPausedCount(worker.taskDefName)
            LOGGER.debug("Worker {} has been paused. Not polling anymore!", worker.javaClass)
            return
        }
        val taskType: String = worker.taskDefName
        val pollingSemaphore: PollingSemaphore? = getPollingSemaphore(taskType)
        val slotsToAcquire: Int = pollingSemaphore?.availableSlots() ?: 0
        if (slotsToAcquire <= 0 || !(pollingSemaphore ?: return).acquireSlots(slotsToAcquire)) {
            return
        }
        var acquiredTasks = 0
        try {
            val domain = PropertyFactory.getString(
                taskType,
                DOMAIN,
                null
            ) ?: (
                    PropertyFactory.getString(
                        ALL_WORKERS, DOMAIN, null
                    ) ?: taskToDomain[taskType]
                    )
            LOGGER.debug("Polling task of type: {} in domain: '{}'", taskType, domain)

            val tasks: List<Task> = MetricsContainer.getPollTimer(taskType)
                .record(
                    Callable {
                        runBlocking {
                            taskClient.batchPollTasksInDomain(
                                taskType,
                                domain,
                                worker.identity,
                                slotsToAcquire,
                                worker.batchPollTimeoutInMS
                            )
                        }
                    })

            acquiredTasks = tasks.size

            for (task: Task in tasks) {
                if (Objects.nonNull(task) && StringUtils.isNotBlank(task.taskId)) {
                    MetricsContainer.incrementTaskPollCount(taskType, 1)
                    LOGGER.debug(
                        "Polled task: {} of type: {} in domain: '{}', from worker: {}",
                        task.taskId,
                        taskType,
                        domain,
                        worker.identity
                    )
                    val taskCompletableFuture = CompletableFuture.supplyAsync(
                        {
                            processTask(
                                task,
                                worker,
                                pollingSemaphore
                            )
                        },
                        executorService
                    )
                    if (task.responseTimeoutSeconds > 0 && worker.leaseExtendEnabled()) {
                        val leaseExtendFuture = leaseExtendExecutorService.scheduleWithFixedDelay(
                            extendLease(task, taskCompletableFuture),
                            (task.responseTimeoutSeconds
                                    * LEASE_EXTEND_DURATION_FACTOR).roundToLong(),
                            (
                                    task.responseTimeoutSeconds
                                            * LEASE_EXTEND_DURATION_FACTOR).roundToLong(),
                            TimeUnit.SECONDS
                        )
                        leaseExtendMap[task.taskId] = leaseExtendFuture
                    }
                    taskCompletableFuture.whenComplete { task: Task, throwable: Throwable? ->
                        finalizeTask(
                            task,
                            throwable
                        )
                    }
                } else {
                    // no task was returned in the poll, release the permit
                    pollingSemaphore.complete(1)
                }
            }
        } catch (e: Exception) {
            MetricsContainer.incrementTaskPollErrorCount(
                worker.taskDefName,
                e
            )
            LOGGER.error("Error when polling for tasks", e)
        }

        // immediately release unused permits
        pollingSemaphore.complete(slotsToAcquire - acquiredTasks)
    }

    fun shutdown(timeout: Int) {
        shutdownAndAwaitTermination(executorService, timeout)
        shutdownAndAwaitTermination(leaseExtendExecutorService, timeout)
        leaseExtendMap.clear()
    }

    fun shutdownAndAwaitTermination(executorService: ExecutorService, timeout: Int) {
        try {
            executorService.shutdown()
            if (executorService.awaitTermination(timeout.toLong(), TimeUnit.SECONDS)) {
                LOGGER.debug("tasks completed, shutting down")
            } else {
                LOGGER.warn(String.format("forcing shutdown after waiting for %s second", timeout))
                executorService.shutdownNow()
            }
        } catch (ie: InterruptedException) {
            LOGGER.warn("shutdown interrupted, invoking shutdownNow")
            executorService.shutdownNow()
            Thread.currentThread().interrupt()
        }
    }

    private val uncaughtExceptionHandler = Thread.UncaughtExceptionHandler { thread: Thread?, error: Throwable? ->
        // JVM may be in unstable state, try to send metrics then exit
        MetricsContainer.incrementUncaughtExceptionCount()
        LOGGER.error("Uncaught exception. Thread {} will exit now", thread, error)
    }

    init {
        pollingSemaphoreMap = HashMap<String, PollingSemaphore>()
        var totalThreadCount = 0
        for ((taskType, count) in taskThreadCount) {
            totalThreadCount += count
            pollingSemaphoreMap[taskType] = PollingSemaphore(count)
        }
        LOGGER.info("Initialized the TaskPollExecutor with {} threads", totalThreadCount)
        executorService = Executors.newFixedThreadPool(
            totalThreadCount,
            BasicThreadFactory.Builder()
                .namingPattern(workerNamePrefix)
                .uncaughtExceptionHandler(uncaughtExceptionHandler)
                .build()
        )
        ThreadPoolMonitor.attach(REGISTRY, executorService as ThreadPoolExecutor, workerNamePrefix)
        LOGGER.info("Initialized the task lease extend executor")
        leaseExtendExecutorService = Executors.newSingleThreadScheduledExecutor(
            BasicThreadFactory.Builder()
                .namingPattern("workflow-lease-extend-%d")
                .daemon(true)
                .uncaughtExceptionHandler(uncaughtExceptionHandler)
                .build()
        )
    }

    private fun processTask(
        task: Task,
        worker: Worker,
        pollingSemaphore: PollingSemaphore?
    ): Task {
        LOGGER.debug(
            "Executing task: {} of type: {} in worker: {} at {}",
            task.taskId,
            task.taskDefName,
            worker.javaClass.simpleName,
            worker.identity
        )
        try {
            executeTask(worker, task)
        } catch (t: Throwable) {
            task.status = Task.Status.FAILED
            val result = TaskResult(task)
            handleException(t, result, worker, task)
        } finally {
            pollingSemaphore?.complete(1)
        }
        return task
    }

    private fun executeTask(worker: Worker, task: Task) {
        val stopwatch = StopWatch()
        stopwatch.start()
        var result: TaskResult? = null
        try {
            LOGGER.debug(
                "Executing task: {} in worker: {} at {}",
                task.taskId,
                worker.javaClass.simpleName,
                worker.identity
            )
            result = worker.execute(task)
            result.workflowInstanceId = task.workflowInstanceId
            result.taskId = task.taskId
            result.workerId = worker.identity
        } catch (e: Exception) {
            LOGGER.error(
                "Unable to execute task: {} of type: {}",
                task.taskId,
                task.taskDefName,
                e
            )
            if (result == null) {
                task.status = Task.Status.FAILED
                result = TaskResult(task)
            }
            handleException(e, result, worker, task)
        } finally {
            stopwatch.stop()
            MetricsContainer.getExecutionTimer(worker.taskDefName)
                .record(stopwatch.getTime(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
        }
        LOGGER.debug(
            "Task: {} executed by worker: {} at {} with status: {}",
            task.taskId,
            worker.javaClass.simpleName,
            worker.identity,
            (result ?: return).status
        )
        launch {
            updateTaskResult(updateRetryCount, task, result, worker)
        }
    }

    private fun finalizeTask(task: Task, throwable: Throwable?) {
        if (throwable != null) {
            LOGGER.error(
                "Error processing task: {} of type: {}",
                task.taskId,
                task.taskType,
                throwable
            )
            MetricsContainer.incrementTaskExecutionErrorCount(
                task.taskType,
                throwable
            )
        } else {
            LOGGER.debug(
                "Task:{} of type:{} finished processing with status:{}",
                task.taskId,
                task.taskDefName,
                task.status
            )
            val taskId = task.taskId
            val leaseExtendFuture = leaseExtendMap[taskId]
            if (leaseExtendFuture != null) {
                leaseExtendFuture.cancel(true)
                leaseExtendMap.remove(taskId)
            }
        }
    }

    private suspend fun updateTaskResult(
        count: Int,
        task: Task,
        result: TaskResult,
        worker: Worker
    ) {
        try {
            // upload if necessary
            val externalStorageLocation = retryOperation(
                { taskResult: TaskResult ->
                    upload(
                        taskResult,
                        task.taskType
                    )
                },
                count,
                result,
                "evaluateAndUploadLargePayload"
            )
            externalStorageLocation?.let {
                result.externalOutputPayloadStoragePath = externalStorageLocation
                result.outputData = null
            }

            retryOperation<TaskResult, Any?>(
                { taskResult: TaskResult ->
                    taskClient.updateTask(taskResult)
                    null
                },
                count,
                result,
                "updateTask"
            )
        } catch (e: Exception) {
            worker.onErrorUpdate(task)
            MetricsContainer.incrementTaskUpdateErrorCount(
                worker.taskDefName,
                e
            )
            LOGGER.error(
                String.format(
                    "Failed to update result: %s for task: %s in worker: %s",
                    result.toString(), task.taskDefName, worker.identity
                ),
                e
            )
        }
    }

    private fun upload(result: TaskResult, taskType: String): String? {
        return try {
            taskClient.evaluateAndUploadLargePayload(result.outputData, taskType)
        } catch (iae: IllegalArgumentException) {
            result.reasonForIncompletion = iae.message
            result.outputData = null
            result.status = TaskResult.Status.FAILED_WITH_TERMINAL_ERROR
            null
        }
    }

    private suspend fun <T, R> retryOperation(operation: suspend (t: T) -> R, count: Int, input: T, opName: String): R {
        var index = 0
        while (index < count) {
            try {
                return operation(input)
            } catch (e: Exception) {
                index++
                try {
                    delay(500L)
                } catch (ie: InterruptedException) {
                    LOGGER.error("Retry interrupted", ie)
                }
            }
        }
        throw RuntimeException("Exhausted retries performing $opName")
    }

    private fun handleException(
        t: Throwable,
        result: TaskResult,
        worker: Worker,
        task: Task
    ) {
        LOGGER.error(String.format("Error while executing task %s", task.toString()), t)
        MetricsContainer.incrementTaskExecutionErrorCount(
            worker.taskDefName,
            t
        )
        result.status = TaskResult.Status.FAILED
        result.reasonForIncompletion = "Error while executing the task: $t"
        val stringWriter = StringWriter()
        t.printStackTrace(PrintWriter(stringWriter))
        result.log(stringWriter.toString())
        launch {
            updateTaskResult(updateRetryCount, task, result, worker)
        }
    }

    private fun getPollingSemaphore(taskType: String): PollingSemaphore? {
        return pollingSemaphoreMap[taskType]
    }

    private fun extendLease(task: Task, taskCompletableFuture: CompletableFuture<Task>): Runnable {
        return Runnable {
            if (taskCompletableFuture.isDone) {
                LOGGER.warn(
                    "Task processing for {} completed, but its lease extend was not cancelled",
                    task.taskId
                )
                return@Runnable
            }
            LOGGER.info("Attempting to extend lease for {}", task.taskId)
            try {
                val result = TaskResult(task)
                result.isExtendLease = true
                runBlocking {
                    retryOperation(
                        { taskResult: TaskResult ->
                            taskClient.updateTask(taskResult)
                            null
                        },
                        LEASE_EXTEND_RETRY_COUNT,
                        result,
                        "extend lease"
                    )
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
                LOGGER.error("Failed to extend lease for {}", task.taskId, e)
            }
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(TaskPollExecutor::class.java)
        private val REGISTRY: Registry = Spectator.globalRegistry()
        private const val DOMAIN = "domain"
        private const val OVERRIDE_DISCOVERY = "pollOutOfDiscovery"
        private const val ALL_WORKERS = "all"
        private const val LEASE_EXTEND_RETRY_COUNT = 3
        private const val LEASE_EXTEND_DURATION_FACTOR = 0.8
    }

    private val job = Job()
    override val coroutineContext: CoroutineContext get() = Dispatchers.Default + job
}
