package com.netflix.conductor.client.kotlin.http.jersey

import com.netflix.conductor.client.kotlin.http.TaskClient
import com.netflix.conductor.client.kotlin.http.jersey.JerseyTaskClient.Companion.searchResultTask
import com.netflix.conductor.client.kotlin.http.jersey.JerseyTaskClient.Companion.searchResultTaskSummary
import com.netflix.conductor.common.metadata.tasks.Task
import com.netflix.conductor.common.run.SearchResult
import com.netflix.conductor.common.run.TaskSummary
import com.nhaarman.mockito_kotlin.mock
import com.sun.jersey.api.client.ClientResponse
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import kotlin.test.assertTrue

class JerseyTaskClientTest : JerseyClientTest() {

    private var taskClient: TaskClient = JerseyTaskClient(requestHandler)

    @BeforeEach
    fun setup() {
        taskClient.setRootURI(ROOT_URL)
    }

    @Test
    fun search(): Unit = runBlocking {
        val query = "my_complex_query"
        val result = SearchResult<TaskSummary>()
        result.totalHits = 1
        result.results = listOf(TaskSummary())

        val uri = createURI("tasks/search?query=$query")

        val clientResponse: ClientResponse = mock()
        Mockito.`when`(clientResponse.getEntity(searchResultTaskSummary)).thenReturn(result)
        Mockito.`when`(requestHandler.get(uri)).thenReturn(clientResponse)

        val searchResult = taskClient.search(query)

        Mockito.verify(requestHandler, Mockito.times(1))
            .get(uri)

        assertTrue {
            searchResult?.totalHits == result.totalHits
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

        val uri = createURI("tasks/search-v2?query=$query")

        val clientResponse: ClientResponse = mock()
        Mockito.`when`(clientResponse.getEntity(searchResultTask)).thenReturn(result)
        Mockito.`when`(requestHandler.get(uri)).thenReturn(clientResponse)

        val searchResult = taskClient.searchV2("my_complex_query")

        assertTrue {
            searchResult?.totalHits == result.totalHits
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

        val uri = createURI("tasks/search?start=$start&size=$size&sort=$sort&freeText=$freeText&query=$query")

        val clientResponse: ClientResponse = mock()
        Mockito.`when`(clientResponse.getEntity(searchResultTaskSummary)).thenReturn(result)
        Mockito.`when`(requestHandler.get(uri)).thenReturn(clientResponse)

        val searchResult: SearchResult<TaskSummary>? = taskClient.search(start, size, sort, freeText, query)

        assertTrue {
            searchResult?.totalHits == result.totalHits
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

        val uri = createURI("tasks/search-v2?start=$start&size=$size&sort=$sort&freeText=$freeText&query=$query")

        val clientResponse: ClientResponse = mock()
        Mockito.`when`(clientResponse.getEntity(searchResultTask)).thenReturn(result)
        Mockito.`when`(requestHandler.get(uri)).thenReturn(clientResponse)

        val searchResult: SearchResult<Task>? = taskClient.searchV2(start, size, sort, freeText, query)

        assertTrue {
            searchResult?.totalHits == result.totalHits
                    && searchResult.results?.isNotEmpty() == true
                    && searchResult.results?.size == 1
                    && searchResult.results?.get(0) is Task
        }
    }
}