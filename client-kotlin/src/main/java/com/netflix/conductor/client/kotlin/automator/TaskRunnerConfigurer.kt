package com.netflix.conductor.client.kotlin.automator

import com.netflix.conductor.client.kotlin.exception.ConductorTimeoutClientException
import com.netflix.conductor.client.kotlin.http.TaskClient
import com.netflix.conductor.client.kotlin.worker.Worker
import com.netflix.conductor.common.metadata.tasks.Task
import com.netflix.discovery.EurekaClient
import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.utils.io.core.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.flow.*
import kotlin.time.Duration.Companion.microseconds
import kotlin.time.ExperimentalTime


private val logger = KotlinLogging.logger {}

/** Configures automated polling of tasks and execution via the registered [Worker]s.  */
@OptIn(DelicateCoroutinesApi::class, ExperimentalCoroutinesApi::class)
class TaskRunnerConfigurer private constructor(builder: Builder) {
    private val eurekaClient: EurekaClient?
    private val taskClient: TaskClient
    private val workers: MutableList<Worker> = mutableListOf()

    /**
     * @return sleep time in millisecond before task update retry is done when receiving error from
     * the Conductor server
     */
    val sleepWhenRetry: Int

    /**
     * @return Number of times updateTask should be retried when receiving error from Conductor
     * server
     */
    val updateRetryCount: Int

    /**
     * @return seconds before forcing shutdown of worker
     */
    val shutdownGracePeriodSeconds: Int

    /**
     * @return prefix used for worker names
     */
    val workerNamePrefix: String
    private val taskToDomain: Map<String, String>

    private val taskPollExecutor: TaskPollExecutor

    /**
     * @return Thread Count for individual task type
     */
    val taskThreadCount: MutableMap<String, Int>

    /**
     * @return Thread count for leasing tasks
     */
    private val leaseLimitedParallelism: Int

    val workersDispatcher: ExecutorCoroutineDispatcher

    /**
     * @see Builder
     *
     * @see TaskRunnerConfigurer.init
     */
    init {
        for (worker in builder.workers) {
            if (!builder.taskThreadCount.containsKey(worker.taskDefName)) {
                logger.warn {
                    "No thread count specified for task type ${worker.taskDefName}, default to 1 thread"
                }
                builder.taskThreadCount[worker.taskDefName] = 1
            }
            workers.add(worker)
        }
        taskThreadCount = builder.taskThreadCount
        eurekaClient = builder.eurekaClient
        taskClient = builder.taskClient!!
        sleepWhenRetry = builder.sleepWhenRetry
        updateRetryCount = builder.updateRetryCount
        workerNamePrefix = builder.workerNamePrefix
        taskToDomain = builder.taskToDomain
        shutdownGracePeriodSeconds = builder.shutdownGracePeriodSeconds
        leaseLimitedParallelism = builder.leaseLimitedParallelism

        workersDispatcher = newFixedThreadPoolContext(workers.size, "$workerNamePrefix-worker-poll-context")
        val leasingDispatcher = Dispatchers.IO.limitedParallelism(leaseLimitedParallelism)

        taskPollExecutor = TaskPollExecutor(
            eurekaClient,
            taskClient,
            updateRetryCount,
            taskToDomain,
            leasingDispatcher)

    }

    /**
     * Starts the polling. Must be called after [Builder.build] method.
     */
    @OptIn(DelicateCoroutinesApi::class)
    @Synchronized
    fun init() {
        workers.forEach { startWorkerWithChannel(it, workersDispatcher) }
        //workers.forEach { startWorker(it, workersDispatcher) }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    private fun startWorkerWithChannel(worker: Worker, workersDispatcher: CoroutineDispatcher) {
        val taskDispatcher = taskDispatcher(worker, workersDispatcher)
        val workerScope = CoroutineScope(taskDispatcher + SupervisorJob())
        val taskChannel: ReceiveChannel<Task> = workerScope.produce(capacity = worker.batchPollCount) {
            val pollingResult = runCatching { taskPollExecutor.poll(worker) }
            pollingResult.getOrNull()?.forEach { send(it) }
        }

        workerScope.launch {
            for (task in taskChannel) {
                runCatching { taskPollExecutor.processTask(worker, task) }
                    .onSuccess { taskPollExecutor.updateTaskResult(it.first, it.second, worker) }
                    .onFailure { handleProcessException(it) }
            }
        }
    }

    private fun startWorker(worker: Worker, workersDispatcher: CoroutineDispatcher) {
        val taskDispatcher = taskDispatcher(worker, workersDispatcher)
        val workerScope = CoroutineScope(
            taskDispatcher +
                    SupervisorJob() +
                    CoroutineName("Task ${worker.taskDefName} context"))
        val flow = workerScope.taskComputation(taskPollExecutor, worker)
            .map {
                taskPollExecutor.processTask(worker, it) }
            .catch { handleProcessException(it) }
            .onEach { taskPollExecutor.updateTaskResult(it.first, it.second, worker) }
            .flowOn(taskDispatcher)

        CoroutineScope(SupervisorJob()).launch(workersDispatcher) {
            flow.collect()
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    private fun taskDispatcher(worker: Worker, coroutineDispatcher: CoroutineDispatcher): CoroutineDispatcher {
        return worker.limitedParallelism?.let { coroutineDispatcher.limitedParallelism(it) } ?: coroutineDispatcher
    }

    private fun handleProcessException(t: Throwable) {
        when(t) {
            is ConductorTimeoutClientException -> logger.warn { t.message }
            else -> logger.error { t.message }
        }
    }

    @OptIn(ExperimentalTime::class)
    fun CoroutineScope.taskComputation(executor: TaskPollExecutor, worker: Worker): Flow<Task> = flow {
        timerExact(interval = worker.batchPollTimeoutInMS.microseconds) {
            emitAll(executor.poll(worker).asFlow())
        }
    }.buffer(worker.batchPollCount, onBufferOverflow = BufferOverflow.SUSPEND)

    /**
     * Invoke this method within a PreDestroy block within your application to facilitate a graceful
     * shutdown of your worker, during process termination.
     */
    fun shutdown() {
        TODO("maybe not needed")
        taskPollExecutor.shutdown(shutdownGracePeriodSeconds)
        workersDispatcher.close()
    }

    /** Builder used to create the instances of TaskRunnerConfigurer  */
    class Builder {
        //TODO
        /**
         * prefix to be used for worker names, defaults to workflow-worker-
         * if not supplied.
         */
        var workerNamePrefix = "workflow-worker"
        /**
         * time in milliseconds, for which the thread should sleep when task
         * update call fails, before retrying the operation.
         */
        var sleepWhenRetry = 500
        /**
         * number of times to retry the failed updateTask operation
         * @see sleepWhenRetry
         */
        var updateRetryCount = 3
        var shutdownGracePeriodSeconds = 10
        /**
         * limited threads for lease extend tasks
         */
        var leaseLimitedParallelism = 2
        var workers: Collection<Worker> = emptyList()
        /**
         * Eureka client - used to identify if the server is in discovery or
         * not. When the server goes out of discovery, the polling is terminated. If passed
         * null, discovery check is not done.
         */
        var eurekaClient: EurekaClient? = null
        var taskClient: TaskClient? = null
        var taskToDomain: Map<String, String> = mapOf()
        var taskThreadCount: MutableMap<String, Int> = HashMap()

        /**
         * Builds an instance of the TaskRunnerConfigurer.
         *
         *
         * Please see [TaskRunnerConfigurer.init] method. The method must be called after
         * this constructor for the polling to start.
         */
        fun build(): TaskRunnerConfigurer {
            require(shutdownGracePeriodSeconds >= 1) { "Seconds of shutdownGracePeriod cannot be less than 1" }
            require(taskThreadCount.isNotEmpty()) { "Task thread map should not be empty" }
            require(workers.isNotEmpty()) { "Workers cannot be empty" }
            requireNotNull(taskClient) { "task client should not be null" }
            return TaskRunnerConfigurer(this)
        }
    }

}

fun TaskRunnerConfigurer(block: TaskRunnerConfigurer.Builder.() -> Unit): TaskRunnerConfigurer {
    val builder = TaskRunnerConfigurer.Builder()
    builder.block()
    return builder.build()
}