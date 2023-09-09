package com.netflix.conductor.client.kotlin.http.ktor

import com.netflix.conductor.client.kotlin.http.WorkflowClient
import com.netflix.conductor.common.metadata.workflow.WorkflowDef
import com.netflix.conductor.common.run.SearchResult
import com.netflix.conductor.common.run.Workflow
import com.netflix.conductor.common.run.WorkflowSummary
import io.ktor.client.engine.mock.*
import io.ktor.http.*
import kotlinx.coroutines.runBlocking
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertTrue

class KtorWorkflowClientTest : KtorClientTest() {
    private lateinit var workflowClient: WorkflowClient

    @BeforeTest
    fun setup() {
        val resultSummary = SearchResult<WorkflowSummary>()
        resultSummary.totalHits = 1
        resultSummary.results = listOf(WorkflowSummary())

        val resultWorkflow = SearchResult<Workflow>()
        val workflow = Workflow()
        workflow.workflowDefinition = WorkflowDef()
        workflow.createTime = System.currentTimeMillis()
        resultWorkflow.totalHits = 1
        resultWorkflow.results = listOf(workflow)

        val query = "my_complex_query"
        val mockEngine = MockEngine { request ->
            println("URL from test ${request.url}")
            when (request.url.toString()) {
                "${ROOT_URL}/workflow/search?query=$query" -> respond(
                    content = objectMapper.writeValueAsString(resultSummary),
                    headers = headersOf(HttpHeaders.ContentType, "application/json")
                )
                "${ROOT_URL}/workflow/search-v2?query=$query" -> respond(
                    content = objectMapper.writeValueAsString(resultWorkflow),
                    headers = headersOf(HttpHeaders.ContentType, "application/json")
                )
                "${ROOT_URL}/workflow/search?start=0&size=10&sort=sort&freeText=text&query=$query" -> respond(
                    content = objectMapper.writeValueAsString(resultSummary),
                    headers = headersOf(HttpHeaders.ContentType, "application/json")
                )
                "${ROOT_URL}/workflow/search-v2?start=0&size=10&sort=sort&freeText=text&query=$query" -> respond(
                    content = objectMapper.writeValueAsString(resultWorkflow),
                    headers = headersOf(HttpHeaders.ContentType, "application/json")
                )

                else -> throw IllegalArgumentException("Wrong url")
            }

        }
        workflowClient =  KtorWorkflowClient(ROOT_URL, httpClient(mockEngine))
    }

    @Test
    fun search(): Unit = runBlocking {
        val query = "my_complex_query"
        val result = SearchResult<WorkflowSummary>()
        result.totalHits = 1
        result.results = listOf(WorkflowSummary())

        val searchResult = workflowClient.search(query)

        assertTrue {
            searchResult.totalHits == result.totalHits
                    && searchResult.results?.isNotEmpty() == true
                    && searchResult.results?.size == 1
                    && searchResult.results?.get(0) is WorkflowSummary
        }
    }

    @Test
    fun searchV2(): Unit = runBlocking {
        val query = "my_complex_query"
        val result = SearchResult<Workflow>()
        result.totalHits = 1
        result.results = listOf(Workflow())

        val searchResult = workflowClient.searchV2(query)

        assertTrue {
            searchResult.totalHits == result.totalHits
                    && searchResult.results?.isNotEmpty() == true
                    && searchResult.results?.size == 1
                    && searchResult.results?.get(0) is Workflow
        }
    }

    @Test
    fun searchWithParams(): Unit = runBlocking {
        val query = "my_complex_query"
        val start = 0
        val size = 10
        val sort = "sort"
        val freeText = "text"
        val result = SearchResult<WorkflowSummary>()
        result.totalHits = 1
        result.results = listOf(WorkflowSummary())

        val searchResult = workflowClient.search(start, size, sort, freeText, query)

        assertTrue {
            searchResult.totalHits == result.totalHits
                    && searchResult.results?.isNotEmpty() == true
                    && searchResult.results?.size == 1
                    && searchResult.results?.get(0) is WorkflowSummary
        }
    }

    @Test
    fun searchV2WithParams(): Unit = runBlocking {
        val query = "my_complex_query"
        val start = 0
        val size = 10
        val sort = "sort"
        val freeText = "text"
        val result = SearchResult<Workflow>()
        result.totalHits = 1
        result.results = listOf(Workflow())

        val searchResult = workflowClient.searchV2(start, size, sort, freeText, query)

        assertTrue {
            searchResult.totalHits == result.totalHits
                    && searchResult.results?.isNotEmpty() == true
                    && searchResult.results?.size == 1
                    && searchResult.results?.get(0) is Workflow
        }
    }
}