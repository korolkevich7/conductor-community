/*
 * Copyright 2021 Netflix, Inc.
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
package com.netflix.conductor.client.kotlin.worker

import com.amazonaws.util.EC2MetadataUtils
import com.netflix.conductor.client.kotlin.config.PropertyFactory
import com.netflix.conductor.common.metadata.tasks.Task
import com.netflix.conductor.common.metadata.tasks.TaskResult
import org.slf4j.LoggerFactory
import java.net.InetAddress
import java.net.UnknownHostException
import java.util.function.Function

interface Worker {
    /**
     * Retrieve the name of the task definition the worker is currently working on.
     *
     * @return the name of the task definition.
     */
    val taskDefName: String

    /**
     * Executes a task and returns the updated task.
     *
     * @param task Task to be executed.
     * @return the [TaskResult] object If the task is not completed yet, return with the
     * status as IN_PROGRESS.
     */
    suspend fun execute(task: Task): TaskResult

    /**
     * Called when the task coordinator fails to update the task to the server. Client should store
     * the task id (in a database) and retry the update later
     *
     * @param task Task which cannot be updated back to the server.
     */
    fun onErrorUpdate(task: Task) {}

    /**
     * Override this method to pause the worker from polling.
     *
     * @return true if the worker is paused and no more tasks should be polled from server.
     */
    fun paused(): Boolean {
        return PropertyFactory.getBoolean(taskDefName, "paused", false)
    }

    val identity: String?
        /**
         * Override this method to app specific rules.
         *
         * @return returns the serverId as the id of the instance that the worker is running.
         */
        get() {
            var serverId: String?
            serverId = try {
                InetAddress.getLocalHost().hostName
            } catch (e: UnknownHostException) {
                System.getenv("HOSTNAME")
            }
            if (serverId == null) {
                serverId =
                    if (EC2MetadataUtils.getInstanceId() == null) System.getProperty("user.name") else EC2MetadataUtils.getInstanceId()
            }
            LOGGER.debug("Setting worker id to {}", serverId)
            return serverId
        }
    val pollingInterval: Int
        /**
         * Override this method to change the interval between polls.
         *
         * @return interval in millisecond at which the server should be polled for worker tasks.
         */
        get() = PropertyFactory.getInteger(taskDefName, "pollInterval", 1000)

    val leaseExtendEnabled: Boolean
        get() = PropertyFactory.getBoolean(taskDefName, "leaseExtendEnabled", false)

    val batchPollTimeoutInMS: Int
        get() = PropertyFactory.getInteger(taskDefName, "batchPollTimeoutInMS", 1000)

    val batchPollCount: Int
        get() = PropertyFactory.getInteger(taskDefName, "batchPollCount", 10)

    val limitedParallelism: Int?
        get() = PropertyFactory.getInteger(taskDefName, "limitedParallelism")

    companion object {
        @JvmStatic
        fun create(taskType: String, executor: Function<Task, TaskResult>): Worker {
            return object : Worker {
                override val taskDefName: String = taskType

                override suspend fun execute(task: Task): TaskResult {
                    return executor.apply(task)
                }
            }
        }

        private val LOGGER = LoggerFactory.getLogger(Worker::class.java)
    }
}
