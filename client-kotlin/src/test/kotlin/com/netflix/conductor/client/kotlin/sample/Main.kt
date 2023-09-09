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
package com.netflix.conductor.client.kotlin.sample

import com.netflix.conductor.client.kotlin.automator.TaskRunnerConfigurer
import com.netflix.conductor.client.kotlin.http.TaskClient
import com.netflix.conductor.client.kotlin.http.ktor.KtorTaskClient
import com.netflix.conductor.client.kotlin.worker.Worker
import io.ktor.client.*
import io.ktor.client.engine.okhttp.*
import java.util.*

fun main() {
    val taskClient: TaskClient = KtorTaskClient("http://localhost:8080/api/", HttpClient(OkHttp))
    val threadCount = 2 // number of threads used to execute workers.  To avoid starvation, should be
    // same or more than number of workers
    val worker1: Worker = SampleWorker("task_1")
    val worker2: Worker = SampleWorker("task_5")

    // Create TaskRunnerConfigurer
    val configurer = TaskRunnerConfigurer.Builder(taskClient, Arrays.asList(worker1, worker2))
        .build()

    // Start the polling and execution of tasks
    configurer.init()
}
