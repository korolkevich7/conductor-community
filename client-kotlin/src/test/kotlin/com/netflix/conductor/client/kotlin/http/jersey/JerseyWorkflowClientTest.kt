package com.netflix.conductor.client.kotlin.http.jersey

import com.netflix.conductor.client.kotlin.http.jersey.JerseyWorkflowClient.Companion.searchResultWorkflow
import com.netflix.conductor.client.kotlin.http.jersey.JerseyWorkflowClient.Companion.searchResultWorkflowSummary
import com.netflix.conductor.common.run.SearchResult
import com.netflix.conductor.common.run.Workflow
import com.netflix.conductor.common.run.WorkflowSummary
import com.nhaarman.mockito_kotlin.mock
import com.sun.jersey.api.client.ClientResponse
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import kotlin.test.assertTrue

class JerseyWorkflowClientTest : JerseyClientTest() {
    private var workflowClient: JerseyWorkflowClient = JerseyWorkflowClient(requestHandler)

    @BeforeEach
    fun setup() {
        workflowClient.setRootURI(ROOT_URL)
    }

    @Test
    fun search(): Unit = runBlocking {
        val query = "my_complex_query"
        val result = SearchResult<WorkflowSummary>()
        result.totalHits = 1
        result.results = listOf(WorkflowSummary())

        val uri = createURI("workflow/search?query=$query")

        val clientResponse: ClientResponse = mock()
        Mockito.`when`(clientResponse.getEntity(searchResultWorkflowSummary)).thenReturn(result)
        Mockito.`when`(requestHandler.get(uri)).thenReturn(clientResponse)

        val searchResult = workflowClient.search(query)

        Mockito.verify(requestHandler, Mockito.times(1))
            .get(uri)

        assertTrue { searchResult?.totalHits == result.totalHits
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

        val uri = createURI("workflow/search-v2?query=$query")

        val clientResponse: ClientResponse = mock()
        Mockito.`when`(clientResponse.getEntity(searchResultWorkflow)).thenReturn(result)
        Mockito.`when`(requestHandler.get(uri)).thenReturn(clientResponse)

        val searchResult = workflowClient.searchV2("my_complex_query")

        Mockito.verify(requestHandler, Mockito.times(1))
            .get(uri)

        assertTrue { searchResult?.totalHits == result.totalHits
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

        val uri = createURI("workflow/search?start=$start&size=$size&sort=$sort&freeText=$freeText&query=$query")

        val clientResponse: ClientResponse = mock()
        Mockito.`when`(clientResponse.getEntity(searchResultWorkflowSummary)).thenReturn(result)
        Mockito.`when`(requestHandler.get(uri)).thenReturn(clientResponse)

        val searchResult = workflowClient.search(start, size, sort, freeText, query)

        Mockito.verify(requestHandler, Mockito.times(1))
            .get(uri)

        assertTrue { searchResult?.totalHits == result.totalHits
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

        val uri = createURI("workflow/search-v2?start=$start&size=$size&sort=$sort&freeText=$freeText&query=$query")

        val clientResponse: ClientResponse = mock()
        Mockito.`when`(clientResponse.getEntity(searchResultWorkflow)).thenReturn(result)
        Mockito.`when`(requestHandler.get(uri)).thenReturn(clientResponse)

        val searchResult = workflowClient.searchV2(start, size, sort, freeText, query)

        Mockito.verify(requestHandler, Mockito.times(1))
            .get(uri)

        assertTrue { searchResult?.totalHits == result.totalHits
                && searchResult.results?.isNotEmpty() == true
                && searchResult.results?.size == 1
                && searchResult.results?.get(0) is Workflow
        }
    }
}