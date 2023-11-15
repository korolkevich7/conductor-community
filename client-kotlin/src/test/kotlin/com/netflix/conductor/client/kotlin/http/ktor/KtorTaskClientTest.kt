package com.netflix.conductor.client.kotlin.http.ktor

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.netflix.conductor.client.kotlin.http.TaskClient
import com.netflix.conductor.common.metadata.tasks.Task
import com.netflix.conductor.common.run.SearchResult
import com.netflix.conductor.common.run.TaskSummary
import io.ktor.client.engine.mock.*
import io.ktor.http.*
import kotlinx.coroutines.runBlocking
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertTrue

class KtorTaskClientTest : KtorClientTest() {

    private lateinit var taskClient: TaskClient

    @BeforeTest
    fun setup() {
        val query = "my_complex_query"
        val resultSummary = SearchResult<TaskSummary>()
        resultSummary.totalHits = 1
        resultSummary.results = listOf(TaskSummary())

        val resultTask = SearchResult<Task>()
        resultTask.totalHits = 1
        resultTask.results = listOf(Task())

        val mockEngine = MockEngine { request ->
            println("URL from test ${request.url}")
            when (request.url.toString()) {
                "${ROOT_URL}/tasks/search?query=$query" -> respond(
                    content = objectMapper.writeValueAsString(resultSummary),
                    headers = headerJson()
                )
                "${ROOT_URL}/tasks/search-v2?query=$query" -> respond(
                    content = objectMapper.writeValueAsString(resultTask),
                    headers = headerJson()
                )
                "${ROOT_URL}/tasks/search?start=0&size=10&sort=sort&freeText=text&query=$query" -> respond(
                    content = objectMapper.writeValueAsString(resultSummary),
                    headers = headerJson()
                )
                "${ROOT_URL}/tasks/search-v2?start=0&size=10&sort=sort&freeText=text&query=$query" -> respond(
                    content = objectMapper.writeValueAsString(resultTask),
                    headers = headerJson()
                )
                "$ROOT_URL/tasks/poll/task_1?workerid=worker-id&domain=domain" -> respond(
                    content = objectMapper.writeValueAsString(Task().apply { this.taskType = "task_1" }),
                    headers = headerJson()
                )
                "$ROOT_URL/tasks/poll/batch/task_1?workerid=worker-id&count=10&timeout=100&domain=domain" -> respond(
                    content = objectMapper.writeValueAsString(listOf(Task(), Task())),
                    headers = headerJson()
                )
                "$ROOT_URL/tasks/poll/batch/task_2?workerid=worker-id&count=10&timeout=100&domain=domain" -> respond(
                    content = "",
                    status = HttpStatusCode.NoContent
                )
                else -> throw IllegalArgumentException("Wrong url")
            }

        }
        taskClient =  KtorTaskClient(ROOT_URL, httpClient(mockEngine))
    }

    @Test
    fun search(): Unit = runBlocking {
        val query = "my_complex_query"
        val result = SearchResult<TaskSummary>()
        result.totalHits = 1
        result.results = listOf(TaskSummary())

        val searchResult = taskClient.search(query)

        assertTrue {
            searchResult.totalHits == result.totalHits
                    && searchResult.results?.isNotEmpty() == true
                    && searchResult.results?.size == 1
                    && searchResult.results?.get(0) is TaskSummary
        }
    }

    @Test
    fun searchV2(): Unit = runBlocking {
        val query = "my_complex_query"
        val result = SearchResult<Task>()
        result.totalHits = 1
        result.results = listOf(Task())

        val searchResult = taskClient.searchV2(query)

        assertTrue {
            searchResult.totalHits == result.totalHits
                    && searchResult.results?.isNotEmpty() == true
                    && searchResult.results?.size == 1
                    && searchResult.results?.get(0) is Task
        }
    }

    @Test
    fun searchWithParams(): Unit = runBlocking {
        val query = "my_complex_query"
        val start = 0
        val size = 10
        val sort = "sort"
        val freeText = "text"
        val result = SearchResult<TaskSummary>()
        result.totalHits = 1
        result.results = listOf(TaskSummary())

        val searchResult: SearchResult<TaskSummary> = taskClient.search(start, size, sort, freeText, query)

        assertTrue {
            searchResult.totalHits == result.totalHits
                    && searchResult.results?.isNotEmpty() == true
                    && searchResult.results?.size == 1
                    && searchResult.results?.get(0) is TaskSummary
        }
    }

    @Test
    fun searchV2WithParams(): Unit = runBlocking {
        val query = "my_complex_query"
        val start = 0
        val size = 10
        val sort = "sort"
        val freeText = "text"
        val result = SearchResult<Task>()
        result.totalHits = 1
        result.results = listOf(Task())

        val searchResult: SearchResult<Task> = taskClient.searchV2(start, size, sort, freeText, query)

        assertTrue {
            searchResult.totalHits == result.totalHits
                    && searchResult.results?.isNotEmpty() == true
                    && searchResult.results?.size == 1
                    && searchResult.results?.get(0) is Task
        }
    }

    @Test
    fun batchPollTasksInDomain(): Unit = runBlocking {
        val tasks = taskClient.batchPollTasksInDomain(
            "task_1",
            "domain",
            "worker-id",
            10,
            100
        )

        assertTrue { tasks.size == 2 }
    }

    @Test
    fun batchEmptyPollTasksInDomain(): Unit = runBlocking {
        val tasks = taskClient.batchPollTasksInDomain(
            "task_2",
            "domain",
            "worker-id",
            10,
            100
        )

        assertTrue { tasks.isEmpty() }
    }

    @Test
    fun pollTasksInDomain(): Unit = runBlocking {
        val task = taskClient.pollTask(
            "task_1",
            "worker-id",
            "domain"
        )

        assertTrue { task.taskType == "task_1"}
    }
}