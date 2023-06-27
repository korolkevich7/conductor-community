package com.netflix.conductor.client.kotlin.automator

import com.netflix.conductor.client.kotlin.exception.ConductorClientException
import com.netflix.conductor.client.kotlin.http.TaskClient
import com.netflix.conductor.client.kotlin.worker.Worker
import com.netflix.discovery.EurekaClient
import kotlinx.coroutines.*
import org.apache.commons.lang3.Validate
import org.slf4j.LoggerFactory
import java.util.*
import java.util.function.Consumer
import java.util.stream.Collectors
import kotlin.time.DurationUnit
import kotlin.time.ExperimentalTime
import kotlin.time.toDuration


/** Configures automated polling of tasks and execution via the registered [Worker]s.  */
class TaskRunnerConfigurer private constructor(builder: Builder) {
    private val eurekaClient: EurekaClient
    private val taskClient: TaskClient
    private val workers: MutableList<Worker> = LinkedList()

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
     * @return Thread Count for the shared executor pool
     */
    @get:Deprecated("")
    @Deprecated("")
    var threadCount = 0

    /**
     * @return seconds before forcing shutdown of worker
     */
    val shutdownGracePeriodSeconds: Int

    /**
     * @return prefix used for worker names
     */
    val workerNamePrefix: String
    private val taskToDomain: Map<String, String>


    private val workerJobsMap: MutableMap<String, Job> = HashMap()

    /**
     * @return Thread Count for individual task type
     */
    var taskThreadCount: MutableMap<String, Int>
    private var taskPollExecutor: TaskPollExecutor? = null

    /**
     * @see Builder
     *
     * @see TaskRunnerConfigurer.init
     */
    init {
        // only allow either shared thread pool or per task thread pool
        if (builder.threadCount != -1 && builder.taskThreadCount.isNotEmpty()) {
            LOGGER.error(INVALID_THREAD_COUNT)
            throw ConductorClientException(INVALID_THREAD_COUNT)
        } else if (builder.taskThreadCount.isNotEmpty()) {
            for (worker in builder.workers) {
                if (!builder.taskThreadCount.containsKey(worker.taskDefName)) {
                    LOGGER.info(
                            "No thread count specified for task type {}, default to 1 thread",
                            worker.taskDefName)
                    builder.taskThreadCount[worker.taskDefName] = 1
                }
                workers.add(worker)
            }
            taskThreadCount = builder.taskThreadCount
            threadCount = -1
        } else {
            val taskTypes: MutableSet<String> = HashSet()
            for (worker in builder.workers) {
                taskTypes.add(worker.taskDefName)
                workers.add(worker)
            }
            threadCount = if (builder.threadCount == -1) workers.size else builder.threadCount
            // shared thread pool will be evenly split between task types
            val splitThreadCount = threadCount / taskTypes.size
            taskThreadCount = taskTypes.stream().collect(Collectors.toMap({ v: String? -> v }, { v: String? -> splitThreadCount }))
        }
        eurekaClient = builder.eurekaClient!!
        taskClient = builder.taskClient
        sleepWhenRetry = builder.sleepWhenRetry
        updateRetryCount = builder.updateRetryCount
        workerNamePrefix = builder.workerNamePrefix
        taskToDomain = builder.taskToDomain
        shutdownGracePeriodSeconds = builder.shutdownGracePeriodSeconds
    }

    /** Builder used to create the instances of TaskRunnerConfigurer  */
    class Builder(taskClient: TaskClient, workers: Iterable<Worker>) {
        var workerNamePrefix = "workflow-worker-%d"
        var sleepWhenRetry = 500
        var updateRetryCount = 3

        @Deprecated("")
        var threadCount = -1
        var shutdownGracePeriodSeconds = 10
        val workers: Iterable<Worker>
        var eurekaClient: EurekaClient? = null
        val taskClient: TaskClient
        var taskToDomain: Map<String, String> = HashMap()
        var taskThreadCount: MutableMap<String, Int> = HashMap()

        init {
            Validate.notNull(taskClient, "TaskClient cannot be null")
            Validate.notNull(workers, "Workers cannot be null")
            this.taskClient = taskClient
            this.workers = workers
        }

        /**
         * @param workerNamePrefix prefix to be used for worker names, defaults to workflow-worker-
         * if not supplied.
         * @return Returns the current instance.
         */
        fun withWorkerNamePrefix(workerNamePrefix: String): Builder {
            this.workerNamePrefix = workerNamePrefix
            return this
        }

        /**
         * @param sleepWhenRetry time in milliseconds, for which the thread should sleep when task
         * update call fails, before retrying the operation.
         * @return Returns the current instance.
         */
        fun withSleepWhenRetry(sleepWhenRetry: Int): Builder {
            this.sleepWhenRetry = sleepWhenRetry
            return this
        }

        /**
         * @param updateRetryCount number of times to retry the failed updateTask operation
         * @return Builder instance
         * @see .withSleepWhenRetry
         */
        fun withUpdateRetryCount(updateRetryCount: Int): Builder {
            this.updateRetryCount = updateRetryCount
            return this
        }

        /**
         * @param threadCount # of threads assigned to the workers. Should be at-least the size of
         * taskWorkers to avoid starvation in a busy system.
         * @return Builder instance
         */
        @Deprecated("Use {@link Builder#withTaskThreadCount(Map)} instead.")
        fun withThreadCount(threadCount: Int): Builder {
            require(threadCount >= 1) { "No. of threads cannot be less than 1" }
            this.threadCount = threadCount
            return this
        }

        /**
         * @param shutdownGracePeriodSeconds waiting seconds before forcing shutdown of your worker
         * @return Builder instance
         */
        fun withShutdownGracePeriodSeconds(shutdownGracePeriodSeconds: Int): Builder {
            require(shutdownGracePeriodSeconds >= 1) { "Seconds of shutdownGracePeriod cannot be less than 1" }
            this.shutdownGracePeriodSeconds = shutdownGracePeriodSeconds
            return this
        }

        /**
         * @param eurekaClient Eureka client - used to identify if the server is in discovery or
         * not. When the server goes out of discovery, the polling is terminated. If passed
         * null, discovery check is not done.
         * @return Builder instance
         */
        fun withEurekaClient(eurekaClient: EurekaClient?): Builder {
            this.eurekaClient = eurekaClient
            return this
        }

        fun withTaskToDomain(taskToDomain: Map<String, String>): Builder {
            this.taskToDomain = taskToDomain
            return this
        }

        fun withTaskThreadCount(taskThreadCount: MutableMap<String, Int>): Builder {
            this.taskThreadCount = taskThreadCount
            return this
        }

        /**
         * Builds an instance of the TaskRunnerConfigurer.
         *
         *
         * Please see [TaskRunnerConfigurer.init] method. The method must be called after
         * this constructor for the polling to start.
         */
        fun build(): TaskRunnerConfigurer {
            return TaskRunnerConfigurer(this)
        }
    }

    /**
     * Starts the polling. Must be called after [Builder.build] method.
     */
    @OptIn(ExperimentalTime::class)
    @Synchronized
    fun init() {

        taskPollExecutor = TaskPollExecutor(
                eurekaClient,
                taskClient,
                updateRetryCount,
                taskToDomain,
                workerNamePrefix,
                taskThreadCount)
        workers.forEach(
                Consumer { worker: Worker ->
                    CoroutineScope(Dispatchers.IO).launch {
                        val interval = worker.pollingInterval.toDuration(DurationUnit.MILLISECONDS)
                        timer(interval, interval, Dispatchers.IO) { taskPollExecutor!!.pollAndExecute(worker) }
                    }
                })
    }

    /**
     * Invoke this method within a PreDestroy block within your application to facilitate a graceful
     * shutdown of your worker, during process termination.
     */
    fun shutdown() {
        taskPollExecutor?.shutdown(shutdownGracePeriodSeconds)
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(TaskRunnerConfigurer::class.java)
        private const val INVALID_THREAD_COUNT = "Invalid worker thread count specified, use either shared thread pool or config thread count per task"
    }
}
